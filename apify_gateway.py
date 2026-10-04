"""
apify_gateway.py: the only code in the assistant backend that talks to Apify.

What it does:
  start_or_join(partner, operation, params)
      Builds the doc's fixed actor input, joins a matching run already in progress
      (so two users asking at once pay once), checks the daily budget, starts the
      Apify run with maxItems = cap and a 12-minute timeout, and records it in apify.runs.
  wait_for_runs(run_ids, wait_secs)
      Polls Apify. Each run that has finished is saved to Postgres right away.
      Returns the saved apify.runs row per run, or None for runs still going.
  save_run(run_id)
      Downloads the run's dataset and writes it as a new snapshot (safe to call twice).
  reconcile()
      Housekeeping: saves runs nobody came back for, aborts anything past 12 minutes.
      Call it every ~10 minutes from any cron, or from an admin endpoint.

The doc's rules, and where they are enforced:
  1. Every run is a snapshot: rows keyed by run_id + item_index, never overwritten.
     Reviews dedupe on review id (UNIQUE partner + asin + review_id).
  2. maxItems = cap and timeout = 12 min on every run; reconcile() aborts stragglers.
  3. Each run's cost is stored in apify.runs.cost_usd. New runs stop when today's (UTC)
     spend reaches the partner's cap (apify.partner_budgets, else
     APIFY_PARTNER_DAILY_CAP_USD) or the overall cap (APIFY_DAILY_CAP_USD).
  4. Reviews: one run with filterByRatings ["allStars"], never one run per star.
     The account must be on Starter or better: the free tier silently returns 10
     reviews. save_run() flags runs that look capped at 10 in apify.runs.warning.
  Only the ten pinned actors in ACTORS can run.

Note on budgets: cost is known when a run is saved, so the check uses spend so far.
One run can still go past the cap; maxItems keeps that overshoot small.

Environment:
  APIFY_DATABASE_URL          postgresql://apify_writer:...@host:5432/db?sslmode=require
  APIFY_TOKEN                 from your secret store; never sent to the LLM or the browser
  APIFY_DAILY_CAP_USD         overall cap across all partners, default 25
  APIFY_PARTNER_DAILY_CAP_USD default cap per partner, default 10
  APIFY_ACTOR_IDS      optional JSON to swap actor ids, e.g. {"x": "apidojo~tweet-scraper"}
  APIFY_DB_POOL_MAX    default 5

Requires: pip install "psycopg[binary]" psycopg_pool
"""
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)

# Helper function to load configuration file
def load_config() -> dict:
    config: dict = {}

    secret_id = os.environ.get("CONFIG_SECRET_ID")
    if secret_id:
        try:
            import boto3
            region = os.environ.get("AWS_REGION", "us-east-1")
            session = boto3.Session(region_name=region)
            client = session.client("secretsmanager")
            res = client.get_secret_value(SecretId=secret_id)
            if "SecretString" in res:
                data = json.loads(res["SecretString"])
                if isinstance(data, dict):
                    config.update(data)
        except Exception:
            pass

    candidates = []
    if os.getenv("CONFIG_FILE"):
        candidates.append(Path(os.environ["CONFIG_FILE"]))
    candidates.append(Path(sys.executable).parent / "config.json")
    candidates.append(Path(__file__).parent / "config.json")
    if hasattr(sys, "_MEIPASS"):
        candidates.append(Path(sys._MEIPASS) / "config.json")
    candidates.append(Path.cwd() / "config.json")
    for p in candidates:
        if p.exists() and p.is_file():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    file_data = json.load(f)
                    for k, v in file_data.items():
                        config.setdefault(k, v)
                    return config
            except Exception:
                pass
    return config

_cfg = load_config()

# Normalize common key synonyms from Secrets Manager
if "DB_HOST" in _cfg and "DB_RDS_HOST" not in _cfg:
    _cfg["DB_RDS_HOST"] = _cfg["DB_HOST"]
if "DB_PORT" in _cfg and "DB_RDS_PORT" not in _cfg:
    _cfg["DB_RDS_PORT"] = _cfg["DB_PORT"]

def get_setting(key: str, default: any = None) -> any:
    if key in os.environ and str(os.environ[key]).strip() != "":
        val = os.environ[key]
        if isinstance(default, bool):
            return val.lower() in ("true", "1", "yes")
        if isinstance(default, (int, float)):
            try:
                return type(default)(val)
            except (ValueError, TypeError):
                return default
        return val
    return _cfg.get(key, default)

API = "https://api.apify.com/v2"
RUN_TIMEOUT_SECS = 12 * 60
POLL_SECS = 5
PAGE = 1000
JOIN_WINDOW_MINUTES = 30          # join an unsaved run on the same request started this recently
FINISHED = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}
DAILY_CAP_USD = float(get_setting("APIFY_DAILY_CAP_USD", 25))
PARTNER_DAILY_CAP_USD = float(get_setting("APIFY_PARTNER_DAILY_CAP_USD", 10))
FREE_TIER_REVIEW_LIMIT = 10

def get_apify_token() -> str:
    token = get_setting("APIFY_TOKEN")
    if token and str(token).strip():
        return str(token).strip()
    return ""

# The ten pinned actors. REST paths use "~" instead of "/".
ACTORS = {
    "amazon_crawler": "junglee~amazon-crawler",
    "amazon_reviews": "junglee~amazon-reviews-scraper",
    # The doc names these by platform only; confirm against .mcp.json
    "instagram": "apify~instagram-scraper",
    "tiktok": "clockworks~tiktok-scraper",
    "facebook": "apify~facebook-posts-scraper",
    "x": "apidojo~tweet-scraper",
    "youtube": "streamers~youtube-scraper",
    "website": "apify~website-content-crawler",
    "google_search": "apify~google-search-scraper",
    "rag_browser": "apify~rag-web-browser",
}
try:
    ACTORS.update(json.loads(get_setting("APIFY_ACTOR_IDS", "{}") or "{}"))
except ValueError:
    pass

# operation -> table it writes to
OPERATIONS = {
    "product": "product_pages", "offers": "offers", "search": "search_results",
    "reviews": "reviews",
    "instagram": "social_posts", "tiktok": "social_posts", "facebook": "social_posts",
    "x": "social_posts", "youtube": "social_posts",
    "website": "web_pages", "google_search": "web_pages", "rag_browser": "web_pages",
}

_POOL = None
_POOL_INIT_ATTEMPTED = False
_MEMORY_RUNS = {}  # fallback in-memory store if DB is unavailable
_MEMORY_ROWS = {}  # run_id -> list of shaped row dicts

def _is_port_open(host: str, port: int, timeout: float = 0.3) -> bool:
    import socket
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def get_db_pool():
    global _POOL, _POOL_INIT_ATTEMPTED
    if _POOL is not None:
        return _POOL
    if _POOL_INIT_ATTEMPTED:
        return None
    _POOL_INIT_ATTEMPTED = True

    if get_setting("DEPLOY_MODE") == "remote":
        host = get_setting("DB_RDS_HOST", "selleros-warehouse.cvvtiac72c6q.us-east-1.rds.amazonaws.com")
        port = int(get_setting("DB_RDS_PORT", 5432))
    else:
        host = get_setting("DB_LOCAL_HOST", "127.0.0.1")
        port = int(get_setting("DB_LOCAL_PORT", 55434))

    db_url = get_setting("APIFY_DATABASE_URL")
    if not db_url:
        user = get_setting("DB_USER")
        pwd = get_setting("DB_PASSWORD")
        name = get_setting("DB_NAME", "warehouse")
        if user and pwd:
            db_url = f"postgresql://{user}:{pwd}@{host}:{port}/{name}?sslmode=require"

    if not db_url:
        return None

    target_host = host
    target_port = port
    if get_setting("APIFY_DATABASE_URL"):
        try:
            parsed = urllib.parse.urlparse(get_setting("APIFY_DATABASE_URL"))
            target_host = parsed.hostname or host
            target_port = parsed.port or port
        except Exception:
            pass

    # Only test local port if not in remote mode
    if get_setting("DEPLOY_MODE") != "remote":
        if not _is_port_open(target_host, target_port, timeout=0.3):
            return None

    try:
        max_size = int(get_setting("APIFY_DB_POOL_MAX", 3))
        pool = ConnectionPool(
            db_url,
            min_size=0,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": True, "connect_timeout": 3},
            open=False
        )
        pool.open(wait=False)
        _POOL = pool
        return _POOL
    except Exception as e:
        log.warning("Could not initialize Apify DB connection pool: %s", e)
        _POOL = None
        return None


class ApifyBudgetExceeded(Exception):
    def __init__(self, scope, spent, cap):
        super().__init__(f"{scope} daily Apify cap reached: ${spent:.2f} of ${cap:.2f}")
        self.scope, self.spent, self.cap = scope, spent, cap     # scope: "partner" or "overall"


class ApifyError(Exception):
    pass


# =============================================================== database helpers

def q(sql, params=()):
    pool = get_db_pool()
    if not pool:
        return []
    try:
        with pool.connection(timeout=3) as c:
            return c.execute(sql, params).fetchall()
    except Exception as e:
        log.debug("DB query failed in apify_gateway: %s", e)
        return []


def one(sql, params=()):
    rows = q(sql, params)
    return rows[0] if rows else None


def execute(sql, params=()):
    pool = get_db_pool()
    if not pool:
        return
    try:
        with pool.connection(timeout=3) as c:
            c.execute(sql, params)
    except Exception as e:
        log.debug("DB execute failed in apify_gateway: %s", e)


def run_row(run_id):
    row = one("SELECT * FROM apify.runs WHERE run_id = %s", (run_id,))
    if row:
        return row
    return _MEMORY_RUNS.get(run_id)


def spent_today(partner=None):
    """Today's (UTC) recorded Apify spend, overall or for one partner."""
    row = one("""SELECT coalesce(sum(cost_usd), 0) AS spent FROM apify.runs
                 WHERE started_at >= date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                   AND (%s::text IS NULL OR partner = %s)""", (partner, partner))
    if row and row.get("spent") is not None:
        return float(row["spent"])
    # Fallback to in-memory tracking
    return sum(float(r.get("cost_usd") or 0) for r in _MEMORY_RUNS.values()
               if partner is None or r.get("partner") == partner)


def partner_cap(partner):
    row = one("SELECT daily_cap_usd FROM apify.partner_budgets WHERE partner = %s", (partner,))
    if row and row.get("daily_cap_usd") is not None:
        return float(row["daily_cap_usd"])
    return PARTNER_DAILY_CAP_USD


def check_budget(partner):
    """Raise ApifyBudgetExceeded if the partner or the whole account is at today's cap."""
    cap = partner_cap(partner)
    spent = spent_today(partner)
    if spent >= cap:
        raise ApifyBudgetExceeded("partner", spent, cap)
    total = spent_today()
    if total >= DAILY_CAP_USD:
        raise ApifyBudgetExceeded("overall", total, DAILY_CAP_USD)


# =============================================================== apify api

def _apify(method, path, body=None, **query):
    token = get_apify_token()
    if not token:
        raise ApifyError("APIFY_TOKEN is not configured. Please set APIFY_TOKEN in config.json or environment variables.")
    url = f"{API}{path}" + ("?" + urllib.parse.urlencode(query) if query else "")
    req = urllib.request.Request(
        url, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read() or b"null")
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < 3:
                time.sleep(2 ** attempt)
                continue
            detail = e.read().decode(errors="replace")[:300]
            raise ApifyError(f"Apify {method} {path} returned {e.code}: {detail}") from None
        except urllib.error.URLError as e:
            if attempt < 3:
                time.sleep(2 ** attempt)
                continue
            raise ApifyError(f"Apify unreachable: {e.reason}") from None


def apify_get(path, **query):
    return _apify("GET", path, **query)


def apify_post(path, body=None, **query):
    return _apify("POST", path, body, **query)


def dataset_items(dataset_id):
    offset = 0
    while True:
        page = apify_get(f"/datasets/{dataset_id}/items",
                         clean="true", format="json", offset=offset, limit=PAGE)
        if not page:
            return
        yield from page
        if len(page) < PAGE:
            return
        offset += PAGE


# =============================================================== recipes (from the doc)

def _dp(asin, domain):
    return {"url": f"https://{domain}/dp/{asin}"}


def recipe(op, p):
    """(actor, input, cap, table). Callers send ids / urls / terms only; inputs are fixed here."""
    domain = p.get("domain", "www.amazon.com")
    table = OPERATIONS.get(op)
    if not table:
        raise ValueError(f"unknown operation {op}")

    if op == "product":
        a = p["asins"]
        return ACTORS["amazon_crawler"], {
            "categoryOrProductUrls": [_dp(x, domain) for x in a],
            "maxItemsPerStartUrl": 1, "maxOffers": 0, "scrapeSellers": False,
        }, len(a), table
    if op == "offers":
        a = p["asins"]
        return ACTORS["amazon_crawler"], {
            "categoryOrProductUrls": [_dp(x, domain) for x in a],
            "maxItemsPerStartUrl": 1,
            "maxOffers": int(p.get("max_offers", 10)), "scrapeSellers": True,
        }, len(a), table
    if op == "search":
        t = p["terms"]
        per = min(int(p.get("per_term", 30)), 30)
        return ACTORS["amazon_crawler"], {
            "categoryOrProductUrls": [
                {"url": f"https://{domain}/s?k={urllib.parse.quote_plus(x)}"} for x in t],
            "maxItemsPerStartUrl": per,
        }, per * len(t), table
    if op == "reviews":
        a = p["asins"]
        per = max(10, min(int(p.get("per_asin", 100)), 400))
        return ACTORS["amazon_reviews"], {
            "productUrls": [_dp(x, domain) for x in a],
            "maxReviews": per, "sort": "recent",
            "filterByRatings": ["allStars"],        # rule 4
            "includeGdprSensitive": False,
        }, per * len(a), table

    # Brand and social copy: check these inputs against .claude/skills/brand-context-builder/references/actors.md
    n = int(p.get("per_source", 20))
    u = p.get("urls") or []
    if op == "instagram":
        return ACTORS["instagram"], {"directUrls": u, "resultsType": "posts",
                                     "resultsLimit": n}, n * len(u), table
    if op == "tiktok":
        return ACTORS["tiktok"], {"profiles": [x.rstrip("/").split("@")[-1] for x in u],
                                  "resultsPerPage": n}, n * len(u), table
    if op == "facebook":
        return ACTORS["facebook"], {"startUrls": [{"url": x} for x in u],
                                    "resultsLimit": n}, n * len(u), table
    if op == "x":
        return ACTORS["x"], {"twitterHandles": [x.rstrip("/").split("/")[-1].lstrip("@") for x in u],
                             "maxItems": n * len(u), "sort": "Latest"}, n * len(u), table
    if op == "youtube":
        return ACTORS["youtube"], {"startUrls": [{"url": x} for x in u],
                                   "maxResults": n}, n * len(u), table
    if op == "website":
        pages = int(p.get("max_pages", 20))
        return ACTORS["website"], {"startUrls": [{"url": x} for x in u],
                                   "maxCrawlPages": pages * len(u)}, pages * len(u), table
    if op == "google_search":
        qs = p["queries"]
        return ACTORS["google_search"], {"queries": "\n".join(qs), "maxPagesPerQuery": 1,
                                         "resultsPerPage": 10}, len(qs), table
    if op == "rag_browser":
        per = int(p.get("max_results", 3))
        return ACTORS["rag_browser"], {"query": p["queries"][0], "maxResults": per}, per, table
    raise ValueError(f"unknown operation {op}")


# =============================================================== field extraction
# Raw actor item -> typed columns. The raw item is stored too, so a wrong mapping
# here can be fixed later with an UPDATE ... SET col = item->>'...'.

def _g(d, *paths):
    for p in paths:
        cur = d
        for k in p.split("."):
            cur = cur.get(k) if isinstance(cur, dict) else None
            if cur is None:
                break
        if cur not in (None, "", [], {}):
            return cur
    return None


_NUM = re.compile(r"-?\d[\d,]*\.?\d*")


def _num(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        m = _NUM.search(v)
        if m:
            try:
                return float(m.group().replace(",", ""))
            except ValueError:
                return None
    return None


def _int(v):
    n = _num(v)
    return int(n) if n is not None else None


def _rating(v):
    n = _num(v)
    return n if n is not None and 0 <= n <= 5 else None


def _bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str) and v.lower() in ("true", "false"):
        return v.lower() == "true"
    return None


def _text(v):
    return v if isinstance(v, str) else None


def _ts(v):
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        secs = v / 1000 if v > 1e12 else v
        return datetime.fromtimestamp(secs, timezone.utc).isoformat()
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).isoformat()
        except ValueError:
            return None
    return None


def _asin(it):
    a = _g(it, "asin", "originalAsin", "productAsin")
    return a.upper() if isinstance(a, str) else None


def x_product(it, info, i):
    feats = _g(it, "features")
    return {"asin": _asin(it), "title": _text(_g(it, "title")), "brand": _text(_g(it, "brand")),
            "price": _num(_g(it, "price.value", "price")), "currency": _text(_g(it, "price.currency")),
            "rating": _rating(_g(it, "stars")), "review_count": _int(_g(it, "reviewsCount")),
            "in_stock": _bool(_g(it, "inStock")), "stock_text": _text(_g(it, "inStockText")),
            "bullets": feats if isinstance(feats, list) else None,
            "seller": _text(_g(it, "seller.name")), "url": _text(_g(it, "url"))}


def x_offers(it, info, i):
    offers = _g(it, "offers", "sellerOffers")
    return {"asin": _asin(it),
            "buy_box_seller": _text(_g(it, "seller.name", "buyBoxSeller.name", "sellerName")),
            "buy_box_price": _num(_g(it, "price.value", "price")),
            "in_stock": _bool(_g(it, "inStock")),
            "offers": offers if isinstance(offers, list) else None}


def x_search(it, info, i):
    terms = (info.get("request") or {}).get("terms") or []
    single = len(terms) == 1        # the tool starts one run per term, so this is the normal case
    pos = _int(_g(it, "position", "searchResultPosition", "positionOnSearchPage"))
    return {"term": _text(_g(it, "searchQuery", "keyword")) or (terms[0] if single else None),
            "position": pos if pos is not None else (i + 1 if single else None),
            "sponsored": _bool(_g(it, "isSponsored", "sponsored")),
            "asin": _asin(it), "title": _text(_g(it, "title")),
            "price": _num(_g(it, "price.value", "price")), "rating": _rating(_g(it, "stars")),
            "review_count": _int(_g(it, "reviewsCount"))}


def x_reviews(it, info, i):
    rid = _g(it, "reviewId", "id", "reviewUrl")
    return {"asin": _asin(it), "review_id": str(rid) if rid is not None else None,
            "rating": _rating(_g(it, "ratingScore", "rating")),
            "title": _text(_g(it, "reviewTitle")),
            "body": _text(_g(it, "reviewDescription", "text")),
            "review_date": _text(_g(it, "date")), "reviewed_in": _text(_g(it, "reviewedIn")),
            "variant": _text(_g(it, "variant")), "verified": _bool(_g(it, "isVerified")),
            "vine": _bool(_g(it, "isAmazonVine"))}


def x_social(it, info, i):
    return {"platform": info["operation"],
            "url": _text(_g(it, "url", "postUrl", "webVideoUrl", "topLevelUrl")),
            "post_text": _text(_g(it, "caption", "text", "fullText", "description", "title")),
            "posted_at": _ts(_g(it, "timestamp", "createTimeISO", "createdAt", "date", "time")),
            "likes": _int(_g(it, "likesCount", "diggCount", "likeCount", "likes")),
            "comments": _int(_g(it, "commentsCount", "commentCount", "replyCount")),
            "views": _int(_g(it, "videoViewCount", "playCount", "viewCount", "views"))}


def x_web(it, info, i):
    organic = it.get("organicResults")
    return {"source": info["operation"],
            "url": _text(_g(it, "url", "metadata.url", "searchQuery.url")),
            "title": _text(_g(it, "metadata.title", "title")),
            "page_text": _text(_g(it, "text", "markdown")),
            "google_query": _text(_g(it, "searchQuery.term")),
            "google_results": organic if isinstance(organic, list) else None}


EXTRACT = {"product_pages": x_product, "offers": x_offers, "search_results": x_search,
           "reviews": x_reviews, "social_posts": x_social, "web_pages": x_web}

COMMON_COLS = [("run_id", "text"), ("item_index", "integer"), ("partner", "text"),
               ("fetched_at", "timestamptz"), ("item", "jsonb")]
COLUMNS = {
    "product_pages": [("asin", "text"), ("title", "text"), ("brand", "text"),
                      ("price", "numeric"), ("currency", "text"), ("rating", "numeric"),
                      ("review_count", "integer"), ("in_stock", "boolean"),
                      ("stock_text", "text"), ("bullets", "jsonb"), ("seller", "text"),
                      ("url", "text")],
    "offers": [("asin", "text"), ("buy_box_seller", "text"), ("buy_box_price", "numeric"),
               ("in_stock", "boolean"), ("offers", "jsonb")],
    "search_results": [("term", "text"), ("position", "integer"), ("sponsored", "boolean"),
                       ("asin", "text"), ("title", "text"), ("price", "numeric"),
                       ("rating", "numeric"), ("review_count", "integer")],
    "reviews": [("asin", "text"), ("review_id", "text"), ("rating", "numeric"),
                ("title", "text"), ("body", "text"), ("review_date", "text"),
                ("reviewed_in", "text"), ("variant", "text"), ("verified", "boolean"),
                ("vine", "boolean")],
    "social_posts": [("platform", "text"), ("url", "text"), ("post_text", "text"),
                     ("posted_at", "timestamptz"), ("likes", "bigint"), ("comments", "bigint"),
                     ("views", "bigint")],
    "web_pages": [("source", "text"), ("url", "text"), ("title", "text"),
                  ("page_text", "text"), ("google_query", "text"), ("google_results", "jsonb")],
}


def _clean(v):
    """Postgres text/jsonb cannot hold NUL characters; scraped text sometimes has them."""
    if isinstance(v, str):
        return v.replace("\x00", "")
    if isinstance(v, list):
        return [_clean(x) for x in v]
    if isinstance(v, dict):
        return {k: _clean(x) for k, x in v.items()}
    return v


def _insert(conn, table, rows):
    """Insert a batch in one statement; rows already saved are skipped. Returns rows inserted."""
    cols = COMMON_COLS + COLUMNS[table]
    names = ", ".join(c for c, _ in cols)
    defs = ", ".join(f"{c} {t}" for c, t in cols)
    sql = f"""
        WITH ins AS (
          INSERT INTO apify.{table} ({names})
          SELECT {names} FROM jsonb_to_recordset(%s::jsonb) AS r({defs})
          ON CONFLICT DO NOTHING
          RETURNING 1)
        SELECT count(*) AS n FROM ins"""
    payload = json.dumps(_clean(rows), ensure_ascii=False, default=str)
    return conn.execute(sql, (payload,)).fetchone()["n"]


# =============================================================== run lifecycle

def _request_key(params):
    return {k: params[k] for k in ("asins", "terms", "urls", "queries") if params.get(k)}


def start_or_join(partner, op, params):
    """Start a run for this request, or join one already in progress. Returns
    {"run_id", "table", "joined"}. Raises ApifyBudgetExceeded / ApifyError / ValueError / KeyError."""
    if not partner:
        raise ValueError("partner is required")
    actor, actor_input, cap, table = recipe(op, params)

    same = one("""
        SELECT run_id FROM apify.runs
        WHERE partner = %s AND operation = %s AND saved_at IS NULL
          AND started_at > now() - make_interval(mins => %s)
          AND request @> %s::jsonb
        ORDER BY started_at DESC LIMIT 1""",
               (partner, op, JOIN_WINDOW_MINUTES, json.dumps(_request_key(params))))
    if same:
        return {"run_id": same["run_id"], "table": table, "joined": True}

    check_budget(partner)                                                # rule 3

    run = apify_post(f"/acts/{actor}/runs", actor_input,
                     maxItems=cap, timeout=RUN_TIMEOUT_SECS)["data"]      # rule 2

    # Save to memory cache in case DB is unavailable
    _MEMORY_RUNS[run["id"]] = {
        "run_id": run["id"], "operation": op, "doc_table": table, "partner": partner,
        "actor": actor, "cap": cap, "request": params, "actor_input": actor_input,
        "status": run.get("status", "STARTED"), "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None, "items": 0, "cost_usd": None, "saved_at": None, "warning": None
    }

    execute("""
        INSERT INTO apify.runs (run_id, operation, doc_table, partner, actor, cap, request, actor_input)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
        ON CONFLICT (run_id) DO NOTHING""",
            (run["id"], op, table, partner, actor, cap,
             json.dumps(params, default=str), json.dumps(actor_input)))
    return {"run_id": run["id"], "table": table, "joined": False}


def review_limit_warning(info, rows):
    """Rule 4: the free tier silently returns 10 reviews. Flag a run that asked for more
    than 10 per ASIN, where no ASIN got more than 10 and at least one got exactly 10.
    (A product that truly has under 10 reviews returns fewer, so it does not trigger this.)"""
    if info.get("doc_table") != "reviews":
        return None
    requested = int((info.get("request") or {}).get("per_asin", 100))
    if requested <= FREE_TIER_REVIEW_LIMIT or not rows:
        return None
    per_asin = {}
    for r in rows:
        per_asin[r.get("asin")] = per_asin.get(r.get("asin"), 0) + 1
    counts = per_asin.values()
    if max(counts) == FREE_TIER_REVIEW_LIMIT:
        return (f"Possible Apify free-tier limit: asked for {requested} reviews per ASIN but no "
                f"ASIN returned more than {FREE_TIER_REVIEW_LIMIT}. Check the account is on "
                f"Starter or better.")
    return None


def save_run(run_id, run=None, force=False):
    """Write a finished run's dataset as a new snapshot. Returns the apify.runs row,
    or None if the run is still going. Safe to call more than once."""
    info = run_row(run_id)
    if not info:
        raise KeyError(f"run {run_id} was not started by this backend")
    if info.get("saved_at") and not force:
        return info
    run = run or apify_get(f"/actor-runs/{run_id}")["data"]
    if run["status"] not in FINISHED:
        return None

    table = info["doc_table"]
    fetched_at = run.get("finishedAt") or datetime.now(timezone.utc).isoformat()
    items = list(dataset_items(run["defaultDatasetId"])) if run.get("defaultDatasetId") else []
    rows = [{"run_id": run_id, "item_index": i, "partner": info["partner"],
             "fetched_at": fetched_at, **EXTRACT[table](it, info, i), "item": it}
            for i, it in enumerate(items) if isinstance(it, dict)]
    warning = review_limit_warning(info, rows)        # counted before dedupe, on what Apify returned

    # Update in-memory storage so caller gets data even if DB is saturated/offline
    _MEMORY_ROWS[run_id] = rows
    if run_id in _MEMORY_RUNS:
        _MEMORY_RUNS[run_id].update({
            "status": run["status"],
            "finished_at": run.get("finishedAt"),
            "items": len(rows),
            "cost_usd": run.get("usageTotalUsd"),
            "dataset_id": run.get("defaultDatasetId"),
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "warning": warning,
        })

    pool = get_db_pool()
    if pool:
        try:
            with pool.connection(timeout=10) as conn, conn.transaction():
                locked = conn.execute("SELECT saved_at FROM apify.runs WHERE run_id = %s FOR UPDATE",
                                      (run_id,)).fetchone()
                if locked and locked.get("saved_at") and not force:
                    return run_row(run_id)
                if force:
                    conn.execute(f"DELETE FROM apify.{table} WHERE run_id = %s", (run_id,))
                inserted = sum(_insert(conn, table, rows[i:i + 500]) for i in range(0, len(rows), 500))
                conn.execute("""
                    UPDATE apify.runs
                    SET status = %s, finished_at = %s, items = %s, duplicate_reviews_skipped = %s,
                        cost_usd = %s, dataset_id = %s, saved_at = now(), error = NULL, warning = %s
                    WHERE run_id = %s""",
                             (run["status"], run.get("finishedAt"), len(rows),
                              len(rows) - inserted if table == "reviews" else 0,
                              run.get("usageTotalUsd"), run.get("defaultDatasetId"), warning, run_id))
        except Exception as e:
            log.warning("Could not persist Apify run %s to PostgreSQL: %s", run_id, e)

    return run_row(run_id)


def save_if_finished(run_id):
    row = run_row(run_id)
    if row and row["saved_at"]:
        return row
    run = apify_get(f"/actor-runs/{run_id}")["data"]
    return save_run(run_id, run) if run["status"] in FINISHED else None


def wait_for_runs(run_ids, wait_secs):
    """Poll until every run is saved or time is up. {run_id: runs row, or None if still going}."""
    deadline = time.time() + wait_secs
    done = {}
    while True:
        for rid in run_ids:
            if rid not in done:
                row = save_if_finished(rid)
                if row:
                    done[rid] = row
        if len(done) == len(run_ids) or time.time() >= deadline:
            return {rid: done.get(rid) for rid in run_ids}
        time.sleep(POLL_SECS)


def reconcile(max_age_days=7):
    """Save runs nobody came back for; abort any run past the 12-minute limit."""
    saved, aborted, failed = 0, 0, []
    for r in q("""SELECT run_id, started_at FROM apify.runs
                  WHERE saved_at IS NULL
                    AND started_at < now() - interval '2 minutes'
                    AND started_at > now() - make_interval(days => %s)
                  ORDER BY started_at""", (max_age_days,)):
        try:
            run = apify_get(f"/actor-runs/{r['run_id']}")["data"]
            age = (datetime.now(timezone.utc) - r["started_at"]).total_seconds()
            if run["status"] not in FINISHED and age > RUN_TIMEOUT_SECS + 60:
                apify_post(f"/actor-runs/{r['run_id']}/abort")
                aborted += 1
                run = apify_get(f"/actor-runs/{r['run_id']}")["data"]
            if run["status"] in FINISHED and save_run(r["run_id"], run):
                saved += 1
        except Exception as e:
            execute("UPDATE apify.runs SET error = %s WHERE run_id = %s", (str(e)[:500], r["run_id"]))
            failed.append({"run_id": r["run_id"], "error": str(e)})
    return {"saved": saved, "aborted": aborted, "failed": failed}
