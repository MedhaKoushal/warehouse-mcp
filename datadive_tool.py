"""
datadive_tool
=============
On-demand Data Dive access for the analytics app. No separate Lambda or API:
the app imports this module and calls `handle_datadive_tool(args)` when the
LLM chooses the `datadive_data` tool (see tool_schema.json).

What one call does
------------------
1. Check `datadive_snapshot_registry` for the requested dataset + entity.
2. Fresh  -> answer from the `datadive_*` tables in PostgreSQL.
3. Stale  -> claim the refresh (stampede-safe), GET from Data Dive, upsert into
             PostgreSQL in one transaction, update the registry, write a raw
             copy to S3 (source=app), then answer from PostgreSQL.
4. Data Dive fails -> answer from the stored copy with stale=true.

READ-ONLY: only Data Dive GET endpoints. Never starts a dive, re-dive, or
Rank Radar. Old niche data comes back with redive_recommended=true.

Datasets
--------
On demand (refreshed here when stale):
    niche_keywords, niche_roots, niche_competitors, ranking_juice
        -> pulled only when the niche's dive date changed
    alerts           -> TTL (ALERT_TTL_HOURS, default 6)
    rank_daily       -> gap fill: only days missing after the daily load
    listing_changes  -> top-up when last load is older than LISTING_TTL_HOURS
    niches           -> niche list, TTL (NICHE_LIST_TTL_HOURS)
    quota            -> live call (free), falls back to the last stored reading
Warehouse only (loaded by the daily schedule):
    seller_profiles, rank_radars

Environment variables (all optional except where noted)
-------------------------------------------------------
DD_SECRET_ID            Data Dive API key secret     (default prod/datadive/api-key)
DW_SECRET_ID            PostgreSQL credentials secret (default prod/datadive/dw-credentials)
                        JSON: {"host", "port", "dbname" | "database",
                               "username" | "user", "password"}
                        Set to "none" to skip this secret.
DW_SSL                  "require" (default: encrypted, like libpq sslmode=require),
                        "verify-full" (also verify the server certificate) or "disable"
DW_SSL_CA               CA bundle file for DW_SSL=verify-full (e.g. the RDS bundle)
DATADIVE_DB_SCHEMA      Schema holding the datadive_* tables (default datadive)
DATADIVE_BUCKET/BUCKET  S3 bucket for raw copies (default simpliworks-ai-integreation)
ALERT_TTL_HOURS         default 6
RANK_TTL_HOURS          default 12
LISTING_TTL_HOURS       default 24
NICHE_LIST_TTL_HOURS    default 24
NICHE_CHECK_MINUTES     Skip the dive-date check if checked this recently (default 60)
FORCE_COOLDOWN_MINUTES  force_refresh is ignored if refreshed this recently (default 10)
REDIVE_AFTER_DAYS       Flag niches whose last dive is older than this (default 30)

Dependencies: pg8000, boto3

Running inside the SellerOS Warehouse MCP server (server.py)
------------------------------------------------------------
server.py exposes this as the `datadive_data` MCP tool and calls `configure()`
once before the first call, passing values from the MCP config
(selleros/mcp/config in Secrets Manager, or config.json locally).

Credentials are resolved in this order:
  Data Dive API key : DATADIVE_API_KEY (configure / env)  ->  DD_SECRET_ID secret
  Warehouse writer  : DATADIVE_DB_USER + DATADIVE_DB_PASSWORD (configure / env)
                      ->  DW_SECRET_ID secret
                      ->  the MCP's own (read-only) DB login, given by configure()
If the login in use cannot write to the datadive_* tables, the tool still
answers from the stored copy and says that a refresh was not possible.

TO VERIFY AGAINST SWAGGER (developer.datadive.tools/docs)
---------------------------------------------------------
- Niche object: field holding the latest dive / research date
  (several spellings accepted in niche_dive_date()).
- Shapes of /keywords, /roots, /competitors, /ranking-juices responses
  (lists are found defensively in items_of()).
"""

import datetime as dt
import decimal
import json
import os
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
import pg8000.native


# ============================================================
# CONFIGURATION
# ============================================================

AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

DD_SECRET_ID = os.environ.get("DD_SECRET_ID", "prod/datadive/api-key")
DW_SECRET_ID = os.environ.get("DW_SECRET_ID", "prod/datadive/dw-credentials")
DW_SSL = os.environ.get("DW_SSL", "require").lower()
DW_SSL_CA = os.environ.get("DW_SSL_CA") or None

# The datadive_* tables live in the "datadive" schema of the warehouse
SCHEMA = os.environ.get("DATADIVE_DB_SCHEMA", "datadive")
if not re.fullmatch(r"[a-z_][a-z0-9_]*", SCHEMA):
    raise ValueError(f"Invalid DATADIVE_DB_SCHEMA: {SCHEMA!r}")

BUCKET = (
    (os.environ.get("DATADIVE_BUCKET") or os.environ.get("BUCKET")
     or "simpliworks-ai-integreation")
    .strip().removeprefix("s3://").split("/")[0]
)

ALERT_TTL_HOURS = float(os.environ.get("ALERT_TTL_HOURS", "6"))
RANK_TTL_HOURS = float(os.environ.get("RANK_TTL_HOURS", "12"))
LISTING_TTL_HOURS = float(os.environ.get("LISTING_TTL_HOURS", "24"))
NICHE_LIST_TTL_HOURS = float(os.environ.get("NICHE_LIST_TTL_HOURS", "24"))
NICHE_CHECK_MINUTES = float(os.environ.get("NICHE_CHECK_MINUTES", "60"))
FORCE_COOLDOWN_MINUTES = float(os.environ.get("FORCE_COOLDOWN_MINUTES", "10"))
REDIVE_AFTER_DAYS = int(os.environ.get("REDIVE_AFTER_DAYS", "30"))

REFRESH_LOCK_SECONDS = 180
DEFAULT_MAX_ROWS = 200
HARD_MAX_ROWS = 1000
MAX_RADARS_PER_CALL = 10
MAX_WINDOW_DAYS = 90
MAX_PAGE_SIZE = 50  # Data Dive rejects pageSize > 50

BASE_URL = "https://api.datadive.tools"
USER_AGENT = "datadive-analytics-app/1.0"
API_TIMEOUT_SECONDS = 30   # a user is waiting
API_ATTEMPTS = 3
RETRYABLE_STATUS = (429, 500, 502, 503, 504)

SOURCE = "app"
S3_PREFIX = "datadive"

NICHE_DATASETS = {
    "niche_keywords": "/v1/niches/{id}/keywords",
    "niche_roots": "/v1/niches/{id}/roots",
    "niche_competitors": "/v1/niches/{id}/competitors",
    "ranking_juice": "/v1/niches/{id}/ranking-juices",
}

ALERT_ENDPOINTS = {
    "blind_spend": "/v1/alerts/blind-spend",
    "indexing": "/v1/alerts/indexing-issues",
}

DATASETS = [
    "niches", "niche_keywords", "niche_roots", "niche_competitors",
    "ranking_juice", "alerts", "rank_daily", "listing_changes",
    "seller_profiles", "rank_radars", "quota",
]


# ============================================================
# TABLE DEFINITIONS (match the DDL in the design doc)
# ============================================================

META = [("_run_id", "text"), ("_source", "text"), ("_fetched_at", "timestamptz")]

TABLES = {
    "niches": {
        "table": "datadive_niches",
        "cols": [("niche_id", "text"), ("marketplace", "text"),
                 ("latest_dive_date", "date"), *META, ("raw", "jsonb")],
        "key": ["niche_id"], "on_conflict": "update",
    },
    "niche_keywords": {
        "table": "datadive_niche_keywords",
        "cols": [("niche_id", "text"), ("keyword", "text"), ("dive_date", "date"),
                 *META, ("raw", "jsonb")],
        "key": ["niche_id", "keyword", "dive_date"], "on_conflict": "nothing",
    },
    "niche_roots": {
        "table": "datadive_niche_roots",
        "cols": [("niche_id", "text"), ("root", "text"), ("dive_date", "date"),
                 *META, ("raw", "jsonb")],
        "key": ["niche_id", "root", "dive_date"], "on_conflict": "nothing",
    },
    "niche_competitors": {
        "table": "datadive_niche_competitors",
        "cols": [("niche_id", "text"), ("asin", "text"), ("dive_date", "date"),
                 *META, ("raw", "jsonb")],
        "key": ["niche_id", "asin", "dive_date"], "on_conflict": "nothing",
    },
    "ranking_juice": {
        "table": "datadive_ranking_juice",
        "cols": [("niche_id", "text"), ("asin", "text"), ("dive_date", "date"),
                 *META, ("raw", "jsonb")],
        "key": ["niche_id", "asin", "dive_date"], "on_conflict": "nothing",
    },
    "alerts": {
        "table": "datadive_alerts",
        "cols": [("alert_type", "text"), ("alert_id", "text"), ("asin", "text"),
                 ("seller_id", "text"), ("marketplace", "text"),
                 ("last_alerted_at", "timestamptz"), ("resolved_at", "timestamptz"),
                 ("wasted_spend", "numeric"), *META, ("raw", "jsonb")],
        "key": ["alert_type", "alert_id"], "on_conflict": "update",
    },
    "rank_daily": {
        "table": "datadive_rank_daily",
        "cols": [("radar_id", "text"), ("keyword_id", "text"), ("rank_date", "date"),
                 ("asin", "text"), ("marketplace", "text"), ("keyword", "text"),
                 ("search_volume", "integer"), ("relevancy", "numeric"),
                 ("organic_rank", "integer"), ("sponsored_rank", "integer"),
                 ("impression_rank", "integer"), *META],
        "key": ["radar_id", "keyword_id", "rank_date"], "on_conflict": "update",
    },
    "listing_changes": {
        "table": "datadive_listing_changes",
        "cols": [("seller_id", "text"), ("marketplace", "text"), ("asin", "text"),
                 ("changed_at", "timestamptz"), ("change_type", "text"),
                 ("content_type", "text"), *META, ("raw", "jsonb")],
        "key": ["seller_id", "marketplace", "asin", "changed_at",
                "change_type", "content_type"], "on_conflict": "update",
    },
}


def tbl(name):
    return f"{SCHEMA}.{name}"


# ============================================================
# SMALL HELPERS
# ============================================================

def log(event, **fields):
    print(json.dumps({"event": event, **fields}, default=str))


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


def iso(ts):
    return ts.isoformat(timespec="seconds").replace("+00:00", "Z") if ts else None


def pick(obj, *names):
    if not isinstance(obj, dict):
        return None
    for name in names:
        value = obj.get(name)
        if value is not None:
            return value
    return None


def to_date(value):
    if value is None or value == "":
        return None
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def age_hours(ts):
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return (utc_now() - ts).total_seconds() / 3600


def jsonable(value):
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


def items_of(body, *keys):
    """Find the list of items in a response that may be a list or a dict."""
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in (*keys, "data", "items", "results"):
            value = body.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                nested = items_of(value, *keys)
                if nested:
                    return nested
    return []


class ToolError(Exception):
    """A problem with the request itself (shown to the user)."""


# ============================================================
# SECRETS + CONNECTIONS
# ============================================================

_clients = {}
_api_key = None
_conn = None
_conn_writable = False
_conn_source = None

# Filled in by configure() (server.py) — see the module docstring
_cfg_api_key = os.environ.get("DATADIVE_API_KEY") or None
_cfg_writer = None          # {"user", "password", optional "host", "port", "database"}
_fallback_db = None         # callable -> {"host", "port", "database", "user", "password"}
_connect_via = None         # (host, port) to reach the DB through, e.g. a local SSM tunnel


def configure(api_key=None, writer_user=None, writer_password=None, writer_host=None,
              writer_port=None, writer_database=None, fallback_db=None, connect_via=None):
    """
    Called by the MCP server before the first tool call.
    api_key:        Data Dive API key (skips the DD_SECRET_ID lookup)
    writer_*:       a warehouse login that can write the datadive_* tables
    fallback_db:    callable returning the MCP's own DB login (used when no
                    writer login is available; refreshes are then skipped)
    connect_via:    (host, port) to connect through instead of the host in the
                    credentials (local SSM tunnel)
    """
    global _cfg_api_key, _cfg_writer, _fallback_db, _connect_via, _api_key
    if api_key:
        _cfg_api_key = str(api_key).strip()
        _api_key = None
    if writer_user and writer_password:
        _cfg_writer = {"user": writer_user, "password": writer_password,
                       "host": writer_host, "port": writer_port,
                       "database": writer_database}
    if fallback_db is not None:
        _fallback_db = fallback_db
    if connect_via:
        _connect_via = (connect_via[0], int(connect_via[1]))


AWS_PROFILE = None   # set by server.py in local mode; on ECS the task role is used


def _client(name):
    if name not in _clients:
        session = boto3.session.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION) \
            if AWS_PROFILE else boto3.session.Session(region_name=AWS_REGION)
        _clients[name] = session.client(name)
    return _clients[name]


def _secret(secret_id):
    return _client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"].strip()


def get_api_key():
    global _api_key
    if _api_key is None:
        raw = _cfg_api_key or _secret(DD_SECRET_ID)
        if raw.startswith("{"):
            try:
                data = json.loads(raw)
                for name in ("api_key", "api-key", "apiKey", "x-api-key", "key", "API_KEY"):
                    if name in data:
                        raw = str(data[name])
                        break
                else:
                    if len(data) == 1:
                        raw = str(next(iter(data.values())))
            except json.JSONDecodeError:
                pass
        _api_key = raw.strip().strip('"').strip("'").strip()
    return _api_key


def _ssl_context():
    if DW_SSL == "disable":
        return None
    if DW_SSL in ("verify-full", "verify-ca", "verify"):
        ctx = ssl.create_default_context(cafile=DW_SSL_CA)
        ctx.check_hostname = DW_SSL != "verify-ca"
        return ctx
    # "require": encrypted but not verified — same as libpq sslmode=require,
    # which the rest of the MCP server uses (the RDS CA is not in the image).
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _credential_candidates():
    """(source, creds-callable) in priority order."""
    fallback = _fallback_db
    cfg_writer = _cfg_writer
    if cfg_writer is None and os.environ.get("DATADIVE_DB_USER") \
            and os.environ.get("DATADIVE_DB_PASSWORD"):
        cfg_writer = {"user": os.environ["DATADIVE_DB_USER"],
                      "password": os.environ["DATADIVE_DB_PASSWORD"],
                      "host": os.environ.get("DATADIVE_DB_HOST"),
                      "port": os.environ.get("DATADIVE_DB_PORT"),
                      "database": os.environ.get("DATADIVE_DB_NAME")}
    if cfg_writer:
        def writer():
            base = fallback() if fallback else {}
            return {"host": cfg_writer.get("host") or base.get("host"),
                    "port": cfg_writer.get("port") or base.get("port") or 5432,
                    "database": cfg_writer.get("database") or base.get("database"),
                    "user": cfg_writer["user"], "password": cfg_writer["password"]}
        yield "config:DATADIVE_DB_USER", writer
    if DW_SECRET_ID and DW_SECRET_ID.lower() != "none":
        def dw_secret():
            creds = json.loads(_secret(DW_SECRET_ID))
            return {"host": creds["host"], "port": creds.get("port", 5432),
                    "database": pick(creds, "dbname", "database"),
                    "user": pick(creds, "username", "user"),
                    "password": creds.get("password")}
        yield f"secret:{DW_SECRET_ID}", dw_secret
    if fallback:
        yield "mcp-readonly-login", fallback


def _open(creds):
    host, port = creds["host"], int(creds.get("port") or 5432)
    if _connect_via:
        host, port = _connect_via
    return pg8000.native.Connection(
        user=creds["user"],
        password=creds.get("password"),
        host=host,
        port=port,
        database=creds.get("database"),
        ssl_context=_ssl_context(),
        timeout=30,
        application_name="datadive-analytics-app",
    )


def _can_write(con):
    try:
        return bool(con.run(
            "SELECT has_table_privilege(current_user, :t, 'INSERT')",
            t=f"{SCHEMA}.datadive_snapshot_registry",
        )[0][0])
    except Exception:  # noqa: BLE001
        return False


def get_connection():
    """Reuse one PostgreSQL connection; reconnect if it dropped."""
    global _conn, _conn_writable, _conn_source
    if _conn is not None:
        try:
            _conn.run("SELECT 1")
            return _conn
        except Exception:  # noqa: BLE001
            try:
                _conn.close()
            except Exception:  # noqa: BLE001
                pass
            _conn = None

    errors = []
    backup = None   # first working but read-only connection
    for source, get_creds in _credential_candidates():
        try:
            con = _open(get_creds())
        except Exception as error:  # noqa: BLE001
            log("db_login_failed", source=source, error=str(error)[:300])
            errors.append(f"{source}: {str(error)[:200]}")
            continue
        if _can_write(con):
            if backup:
                backup[0].close()
            _conn, _conn_writable, _conn_source = con, True, source
            log("db_connected", source=source, writable=True)
            return _conn
        log("db_login_read_only", source=source)
        if backup is None:
            backup = (con, source)
        else:
            con.close()

    if backup:
        con, source = backup
        con.run("SET default_transaction_read_only = on")
        _conn, _conn_writable, _conn_source = con, False, source
        log("db_connected", source=source, writable=False)
        return _conn

    raise RuntimeError("Could not connect to the warehouse for Data Dive: "
                       + ("; ".join(errors) or "no credentials configured"))


def query(con, sql, **params):
    rows = con.run(sql, **params)
    names = [c["name"] for c in (con.columns or [])]
    return [dict(zip(names, row)) for row in rows]


# ============================================================
# DATA DIVE API (GET only)
# ============================================================

PAGINATION_KEYS = {"currentPage", "lastPage", "hasNext", "pageSize", "total"}


def api_get(path, params=None):
    query_params = {k: v for k, v in (params or {}).items() if v is not None}
    url = f"{BASE_URL}{path}"
    if query_params:
        url = f"{url}?{urllib.parse.urlencode(query_params)}"

    for attempt in range(API_ATTEMPTS):
        request = urllib.request.Request(
            url,
            headers={"x-api-key": get_api_key(), "Accept": "application/json",
                     "User-Agent": USER_AGENT},
            method="GET",
        )
        log("api_request", path=path, params=query_params, attempt=attempt + 1)
        try:
            with urllib.request.urlopen(request, timeout=API_TIMEOUT_SECONDS) as response:
                body = json.loads(response.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as error:
            detail = error.read().decode(errors="replace")[:300]
            log("api_error", path=path, status_code=error.code, response=detail)
            if error.code in RETRYABLE_STATUS and attempt < API_ATTEMPTS - 1:
                time.sleep(2 * (2 ** attempt))
                continue
            raise RuntimeError(f"Data Dive API error {error.code} on {path}: {detail}")
        except urllib.error.URLError as error:
            log("network_error", path=path, error=str(error))
            if attempt < API_ATTEMPTS - 1:
                time.sleep(2 * (2 ** attempt))
                continue
            raise RuntimeError(f"Network error on {path}: {error}")

    if isinstance(body, dict) and "data" in body and not (PAGINATION_KEYS & body.keys()):
        return body["data"]
    return body


def api_paged(path, params=None, item_keys=()):
    """Every item from an endpoint that may or may not paginate."""
    page = 1
    while True:
        body = api_get(path, {**(params or {}), "currentPage": page,
                              "pageSize": MAX_PAGE_SIZE})
        items = items_of(body, *item_keys)
        yield from items

        has_next = isinstance(body, dict) and body.get("hasNext", False)
        last_page = body.get("lastPage", page) if isinstance(body, dict) else page
        if not items or not has_next or page >= last_page:
            break
        page += 1


# ============================================================
# WAREHOUSE WRITES
# ============================================================

def upsert(con, dataset, rows, batch_size=200):
    """Insert rows into a datadive_* table on its natural key."""
    if not rows:
        return 0

    spec = TABLES[dataset]
    cols = [c for c, _ in spec["cols"]]
    types = dict(spec["cols"])
    key = spec["key"]

    # One row per key, last wins (ON CONFLICT cannot touch a row twice)
    unique = {}
    for row in rows:
        unique[tuple(row.get(k) for k in key)] = row
    rows = [r for k, r in unique.items() if all(v is not None for v in k)]

    if spec["on_conflict"] == "update":
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key)
        conflict = (
            f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}, _loaded_at = now() "
            f"WHERE t._fetched_at IS NULL OR EXCLUDED._fetched_at >= t._fetched_at"
        )
    else:
        conflict = f"ON CONFLICT ({', '.join(key)}) DO NOTHING"

    written = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        params, values = {}, []
        for i, row in enumerate(batch):
            slots = []
            for j, col in enumerate(cols):
                name = f"p{i}_{j}"
                value = row.get(col)
                if types[col] == "jsonb" and value is not None:
                    value = json.dumps(value, default=str)
                params[name] = None if value is None else str(value) \
                    if types[col] != "jsonb" else value
                slots.append(f"CAST(:{name} AS {types[col]})")
            values.append(f"({', '.join(slots)})")

        con.run(
            f"INSERT INTO {tbl(spec['table'])} AS t ({', '.join(cols)}) "
            f"VALUES {', '.join(values)} {conflict}",
            **params,
        )
        written += len(batch)

    return written


def log_quota(con, run_id, phase):
    """Best effort: record a quota reading. Never fails the request."""
    try:
        body = api_get("/v1/quota")
        con.run(
            f"INSERT INTO {tbl('datadive_quota_log')} (run_id, phase, captured_at, raw) "
            f"VALUES (:r, :p, now(), CAST(:raw AS jsonb)) ON CONFLICT DO NOTHING",
            r=run_id, p=phase, raw=json.dumps(body),
        )
    except Exception as error:  # noqa: BLE001
        log("quota_log_failed", phase=phase, error=str(error))


def write_s3_copy(run_id, dataset, rows, endpoint, params):
    """Raw audit copy for the loader. Best effort, after the DB commit."""
    if not rows:
        return
    try:
        now = utc_now()
        key = (f"{S3_PREFIX}/{dataset}/dt={now.date().isoformat()}/"
               f"datadive_{SOURCE}_{run_id}.jsonl")
        body = "\n".join(
            json.dumps({**row, "_endpoint": endpoint, "_request_params": params},
                       default=str, ensure_ascii=False)
            for row in rows
        )
        _client("s3").put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"),
                       ContentType="application/x-ndjson")
        log("s3_copy", dataset=dataset, rows=len(rows), s3_key=key)
    except Exception as error:  # noqa: BLE001
        log("s3_copy_failed", dataset=dataset, error=str(error))


# ============================================================
# SNAPSHOT REGISTRY
# ============================================================

def reg_get(con, dataset, entity):
    rows = query(
        con,
        f"SELECT source_version, fetched_at, refreshing_until "
        f"FROM {tbl('datadive_snapshot_registry')} "
        f"WHERE dataset = :d AND entity_key = :e",
        d=dataset, e=entity,
    )
    return rows[0] if rows else None


def reg_claim(con, dataset, entity):
    """True if this request may refresh; False if another one is already on it."""
    rows = con.run(
        f"INSERT INTO {tbl('datadive_snapshot_registry')} AS r "
        f"(dataset, entity_key, refreshing_until) "
        f"VALUES (:d, :e, now() + make_interval(secs => :s)) "
        f"ON CONFLICT (dataset, entity_key) DO UPDATE "
        f"SET refreshing_until = EXCLUDED.refreshing_until "
        f"WHERE r.refreshing_until IS NULL OR r.refreshing_until < now() "
        f"RETURNING dataset",
        d=dataset, e=entity, s=REFRESH_LOCK_SECONDS,
    )
    return bool(rows)


def reg_finish(con, dataset, entity, source_version):
    con.run(
        f"UPDATE {tbl('datadive_snapshot_registry')} "
        f"SET source_version = :v, fetched_at = now(), refreshing_until = NULL "
        f"WHERE dataset = :d AND entity_key = :e",
        d=dataset, e=entity, v=source_version,
    )


def reg_release(con, dataset, entity):
    try:
        con.run(
            f"UPDATE {tbl('datadive_snapshot_registry')} SET refreshing_until = NULL "
            f"WHERE dataset = :d AND entity_key = :e",
            d=dataset, e=entity,
        )
    except Exception as error:  # noqa: BLE001
        log("registry_release_failed", error=str(error))


# ============================================================
# REFRESH ORCHESTRATION
# ============================================================

def refresh(con, dataset, entity, needs_pull, pull):
    """
    needs_pull: (bool, reason) already decided by the caller
    pull(run_id) -> list of (table_dataset, rows, endpoint, params) + source_version
    Returns a status dict for the response.
    """
    status = {"entity": entity, "pulled": False, "stale": False, "reason": needs_pull[1]}

    if not needs_pull[0]:
        return status

    if con is _conn and not _conn_writable:
        # The warehouse login in use cannot write datadive_* tables:
        # answer from the stored copy instead of failing the call.
        status.update(stale=True, reason=(
            f"{needs_pull[1]}; refresh skipped (this server's warehouse login is "
            f"read-only for {SCHEMA}.datadive_* tables), showing the stored copy"))
        return status

    if not reg_claim(con, dataset, entity):
        status["reason"] = "another request is refreshing this data"
        return status

    run_id = f"{utc_now():%Y%m%dT%H%M%SZ}-app-{uuid.uuid4().hex[:8]}"
    log_quota(con, run_id, "before")

    try:
        batches, version = pull(run_id)
    except Exception as error:  # noqa: BLE001 - fall back to stored data
        reg_release(con, dataset, entity)
        log("refresh_failed", dataset=dataset, entity=entity, error=str(error))
        status.update(stale=True, reason=f"Data Dive refresh failed: {str(error)[:200]}")
        return status

    try:
        con.run("START TRANSACTION")
        for table_dataset, rows, _, _ in batches:
            upsert(con, table_dataset, rows)
        reg_finish(con, dataset, entity, version)
        con.run("COMMIT")
    except Exception as error:  # noqa: BLE001
        con.run("ROLLBACK")
        reg_release(con, dataset, entity)
        log("warehouse_write_failed", dataset=dataset, error=str(error))
        status.update(stale=True, reason=f"Saving to the warehouse failed: {str(error)[:200]}")
        return status

    for table_dataset, rows, endpoint, params in batches:
        write_s3_copy(run_id, table_dataset, rows, endpoint, params)

    log_quota(con, run_id, "after")

    status.update(pulled=True, reason="refreshed from Data Dive",
                  rows_pulled=sum(len(b[1]) for b in batches))
    return status


def ttl_decision(reg, ttl_hours, force):
    """Shared rule for TTL datasets."""
    age = age_hours(reg["fetched_at"]) if reg else None
    if force:
        if age is not None and age * 60 < FORCE_COOLDOWN_MINUTES:
            return False, f"refreshed {age * 60:.0f} min ago (force cooldown)"
        return True, "force_refresh"
    if age is None:
        return True, "never loaded"
    if age >= ttl_hours:
        return True, f"older than {ttl_hours:g}h"
    return False, f"fresh ({age:.1f}h old)"


def meta(run_id):
    return {"_run_id": run_id, "_source": SOURCE, "_fetched_at": iso(utc_now())}


# ============================================================
# NICHES
# ============================================================

def niche_dive_date(niche):
    value = pick(niche, "latestResearchDate", "lastDiveDate", "diveDate",
                 "researchDate", "lastResearchedAt", "researchedAt", "updatedAt")
    try:
        return to_date(value)
    except ValueError:
        return None


def niche_row(niche, run_id):
    return {
        "niche_id": str(pick(niche, "nicheId", "id")),
        "marketplace": niche.get("marketplace"),
        "latest_dive_date": niche_dive_date(niche),
        "raw": niche,
        **meta(run_id),
    }


def pull_niche_list(run_id):
    endpoint = "/v1/niches"
    rows = [niche_row(n, run_id) for n in api_paged(endpoint, item_keys=("niches",))]
    return [("niches", rows, endpoint, {})], f"count={len(rows)}"


def current_niche(con, niche_id, force):
    """
    The niche's row with an up-to-date dive date. Refreshes the niche list
    when it is older than NICHE_CHECK_MINUTES (or on force).
    """
    reg = reg_get(con, "niches", "all")
    age = age_hours(reg["fetched_at"]) if reg else None
    stale_list = age is None or age * 60 >= NICHE_CHECK_MINUTES or force
    status = refresh(con, "niches", "all", (stale_list, "dive-date check"), pull_niche_list)

    rows = query(
        con,
        f"SELECT niche_id, marketplace, latest_dive_date, raw "
        f"FROM {tbl('datadive_niches')} WHERE niche_id = :n",
        n=str(niche_id),
    )
    return (rows[0] if rows else None), status


def niche_item_rows(dataset, body, niche_id, dive_date, run_id):
    rows = []

    if dataset == "niche_keywords":
        for item in items_of(body, "keywords"):
            rows.append({"keyword": pick(item, "keyword", "searchTerm", "term", "phrase"),
                         "raw": item})
    elif dataset == "niche_roots":
        for item in items_of(body, "roots", "keywordRoots"):
            rows.append({"root": pick(item, "root", "keywordRoot", "term", "word"),
                         "raw": item})
    elif dataset == "niche_competitors":
        for item in items_of(body, "competitors", "asins"):
            rows.append({"asin": pick(item, "asin"), "raw": item})
    elif dataset == "ranking_juice":
        for item in items_of(body, "competitors", "listings", "competitorScores"):
            rows.append({"asin": pick(item, "asin"), "raw": item})
        # The niche-level breakdown (current vs optimized listing) as one row
        rows.append({"asin": "__niche__", "raw": body})

    for row in rows:
        row.update(niche_id=str(niche_id), dive_date=dive_date, **meta(run_id))
    return rows


def handle_niche_dataset(con, dataset, args, force):
    niche_id = args.get("niche_id")
    if not niche_id:
        raise ToolError("niche_id is required for this dataset.")

    niche, list_status = current_niche(con, niche_id, force)
    if niche is None:
        raise ToolError(f"Niche {niche_id} was not found in Data Dive.")

    dive_date = to_date(niche["latest_dive_date"]) or utc_now().date()
    version = f"dive_date={dive_date.isoformat()}"
    entity = f"niche_id={niche_id}"

    reg = reg_get(con, dataset, entity)
    if force:
        decision = ttl_decision(reg, 0, True)
    elif reg is None or reg["source_version"] != version:
        decision = (True, "new dive date" if reg else "never loaded")
    else:
        decision = (False, "dive date unchanged")

    endpoint = NICHE_DATASETS[dataset].format(id=urllib.parse.quote(str(niche_id), safe=""))

    def pull(run_id):
        body = api_get(endpoint)
        rows = niche_item_rows(dataset, body, niche_id, dive_date, run_id)
        return [(dataset, rows, endpoint, {})], version

    status = refresh(con, dataset, entity, decision, pull)
    status["stale"] = status["stale"] or list_status["stale"]

    spec = TABLES[dataset]
    item_col = {"niche_keywords": "keyword", "niche_roots": "root"}.get(dataset, "asin")
    rows = query(
        con,
        f"SELECT {item_col}, dive_date, raw, _fetched_at FROM {tbl(spec['table'])} "
        f"WHERE niche_id = :n AND dive_date = ("
        f"  SELECT max(dive_date) FROM {tbl(spec['table'])} WHERE niche_id = :n) "
        f"ORDER BY {item_col}",
        n=str(niche_id),
    )

    served_dive = rows[0]["dive_date"] if rows else None
    redive = bool(served_dive and (utc_now().date() - served_dive).days > REDIVE_AFTER_DAYS)

    return rows, status, {
        "source_version": f"dive_date={served_dive}" if served_dive else None,
        "redive_recommended": redive,
        "niche": {"niche_id": niche["niche_id"], "marketplace": niche["marketplace"],
                  "latest_dive_date": niche["latest_dive_date"]},
    }


def handle_niches(con, args, force):
    reg = reg_get(con, "niches", "all")
    status = refresh(con, "niches", "all",
                     ttl_decision(reg, NICHE_LIST_TTL_HOURS, force), pull_niche_list)
    filters, params = [], {}
    if args.get("marketplace"):
        filters.append("marketplace = :m")
        params["m"] = args["marketplace"]
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = query(
        con,
        f"SELECT niche_id, marketplace, latest_dive_date, raw, _fetched_at "
        f"FROM {tbl('datadive_niches')} {where} "
        f"ORDER BY latest_dive_date DESC NULLS LAST",
        **params,
    )
    return rows, status, {}


# ============================================================
# ALERTS
# ============================================================

def alert_row(alert_type, alert, run_id):
    return {
        "alert_type": alert_type,
        "alert_id": str(pick(alert, "id", "alertId")),
        "asin": pick(alert, "asin"),
        "seller_id": pick(alert, "sellerId", "seller_id"),
        "marketplace": pick(alert, "marketplace"),
        "last_alerted_at": pick(alert, "lastAlertedAt"),
        "resolved_at": pick(alert, "resolvedAt"),
        "wasted_spend": pick(alert, "wastedSpend"),
        "raw": alert,
        **meta(run_id),
    }


def handle_alerts(con, args, force):
    alert_type = args.get("alert_type")
    if alert_type and alert_type not in ALERT_ENDPOINTS:
        raise ToolError(f"alert_type must be one of {list(ALERT_ENDPOINTS)}.")
    types = [alert_type] if alert_type else list(ALERT_ENDPOINTS)

    statuses = []
    for a_type in types:
        entity = f"alert_type={a_type}"
        reg = reg_get(con, "alerts", entity)
        endpoint = ALERT_ENDPOINTS[a_type]

        def pull(run_id, a_type=a_type, endpoint=endpoint, reg=reg):
            params = {"status": "all"}
            if reg and reg["fetched_at"]:
                # Incremental: changes since the last refresh, with an hour of overlap
                params["updatedSince"] = iso(reg["fetched_at"] - dt.timedelta(hours=1))
            rows = [alert_row(a_type, a, run_id)
                    for a in api_paged(endpoint, params, item_keys=("alerts",))]
            return [("alerts", rows, endpoint, params)], f"rows={len(rows)}"

        statuses.append(refresh(con, "alerts", entity,
                                ttl_decision(reg, ALERT_TTL_HOURS, force), pull))

    filters = ["alert_type = ANY(CAST(:alert_types AS text[]))"]
    params = {"alert_types": "{" + ",".join(types) + "}"}
    status_filter = args.get("alert_status", "active")
    if status_filter == "active":
        filters.append("resolved_at IS NULL")
    elif status_filter == "resolved":
        filters.append("resolved_at IS NOT NULL")
    for col in ("asin", "seller_id", "marketplace"):
        if args.get(col):
            filters.append(f"{col} = :{col}")
            params[col] = args[col]

    rows = query(
        con,
        f"SELECT alert_type, alert_id, asin, seller_id, marketplace, last_alerted_at, "
        f"resolved_at, wasted_spend, raw, _fetched_at FROM {tbl('datadive_alerts')} "
        f"WHERE {' AND '.join(filters)} "
        f"ORDER BY wasted_spend DESC NULLS LAST, last_alerted_at DESC NULLS LAST",
        **params,
    )
    return rows, merge_status(statuses), {}


# ============================================================
# RANK DAILY (gap fill)
# ============================================================

def date_window(args, default_days):
    end = to_date(args.get("end_date")) or utc_now().date()
    start = to_date(args.get("start_date")) or end - dt.timedelta(days=default_days - 1)
    if start > end:
        raise ToolError("start_date cannot be after end_date.")
    if (end - start).days + 1 > MAX_WINDOW_DAYS:
        raise ToolError(f"Date range is limited to {MAX_WINDOW_DAYS} days.")
    return start, end


def handle_rank_daily(con, args, force):
    start, end = date_window(args, 7)

    filters, params = [], {}
    if args.get("radar_id"):
        filters.append("radar_id = :radar_id")
        params["radar_id"] = str(args["radar_id"])
    elif args.get("asin"):
        filters.append("asin = :asin")
        params["asin"] = args["asin"]
        if args.get("marketplace"):
            filters.append("marketplace = :marketplace")
            params["marketplace"] = args["marketplace"]
    else:
        raise ToolError("Give a radar_id or an asin for rank data.")

    radars = query(
        con,
        f"SELECT radar_id, asin, marketplace FROM {tbl('datadive_rank_radars')} "
        f"WHERE {' AND '.join(filters)} ORDER BY radar_id LIMIT {MAX_RADARS_PER_CALL}",
        **params,
    )
    if not radars:
        raise ToolError("No Rank Radar found in the warehouse for that radar_id / asin. "
                        "Radars load with the daily run.")

    statuses = []
    for radar in radars:
        radar_id = radar["radar_id"]
        entity = f"radar_id={radar_id}"
        reg = reg_get(con, "rank_daily", entity)

        loaded = query(
            con,
            f"SELECT max(rank_date) AS last_date FROM {tbl('datadive_rank_daily')} "
            f"WHERE radar_id = :r",
            r=radar_id,
        )[0]["last_date"]

        if force:
            decision = ttl_decision(reg, 0, True)
        elif loaded and loaded >= end:
            decision = (False, "all requested days loaded")
        else:
            decision = ttl_decision(reg, RANK_TTL_HOURS, False)
            if not decision[0]:
                decision = (False, f"{decision[1]}; newer days not available yet")

        pull_start = max(start, loaded - dt.timedelta(days=2)) if loaded and not force else start
        endpoint = f"/v1/niches/rank-radars/{urllib.parse.quote(str(radar_id), safe='')}"

        def pull(run_id, radar=radar, endpoint=endpoint, pull_start=pull_start):
            p = {"startDate": pull_start.isoformat(), "endDate": end.isoformat()}
            rows = []
            for kw in api_paged(endpoint, p, item_keys=("keywords",)):
                for day in kw.get("ranks") or []:
                    rows.append({
                        "radar_id": radar["radar_id"], "keyword_id": kw.get("id"),
                        "rank_date": day.get("date"), "asin": radar["asin"],
                        "marketplace": radar["marketplace"], "keyword": kw.get("keyword"),
                        "search_volume": kw.get("searchVolume"),
                        "relevancy": kw.get("relevancy"),
                        "organic_rank": day.get("organicRank"),
                        "sponsored_rank": day.get("sponsoredRank"),
                        "impression_rank": day.get("impressionRank"),
                        **meta(run_id),
                    })
            return [("rank_daily", rows, endpoint, p)], f"through={end.isoformat()}"

        statuses.append(refresh(con, "rank_daily", entity, decision, pull))

    q_params = {"ids": "{" + ",".join(str(r["radar_id"]) for r in radars) + "}",
                "s": start, "e": end}
    keyword_filter = ""
    if args.get("keyword"):
        keyword_filter = "AND keyword ILIKE :kw"
        q_params["kw"] = f"%{args['keyword']}%"

    rows = query(
        con,
        f"SELECT radar_id, asin, marketplace, keyword, search_volume, rank_date, "
        f"organic_rank_clean AS organic_rank, outside_top_100, sponsored_rank, "
        f"impression_rank, _fetched_at FROM {tbl('datadive_v_rank_daily')} "
        f"WHERE radar_id = ANY(CAST(:ids AS text[])) "
        f"AND rank_date BETWEEN CAST(:s AS date) AND CAST(:e AS date) {keyword_filter} "
        f"ORDER BY rank_date DESC, search_volume DESC NULLS LAST",
        **q_params,
    )
    return rows, merge_status(statuses), {"window": {"start_date": start, "end_date": end}}


# ============================================================
# LISTING CHANGES (top-up)
# ============================================================

def handle_listing_changes(con, args, force):
    seller_id = args.get("seller_id")
    if not seller_id:
        raise ToolError("seller_id is required for listing changes.")
    start, end = date_window(args, 7)

    marketplaces = [args["marketplace"]] if args.get("marketplace") else [
        r["marketplace"] for r in query(
            con,
            f"SELECT DISTINCT marketplace FROM {tbl('datadive_listing_changes')} "
            f"WHERE seller_id = :s",
            s=seller_id,
        )
    ]
    if not marketplaces:
        raise ToolError("Give a marketplace (e.g. US) for this seller.")

    statuses = []
    for marketplace in marketplaces:
        entity = f"seller_id={seller_id}|marketplace={marketplace}"
        reg = reg_get(con, "listing_changes", entity)
        endpoint = (f"/v1/sellers/{urllib.parse.quote(seller_id, safe='')}"
                    f"/marketplaces/{urllib.parse.quote(marketplace, safe='')}/listing-changes")

        last = reg["fetched_at"].date() if reg and reg["fetched_at"] else None
        pull_start = max(start, last - dt.timedelta(days=1)) if last and not force else start

        def pull(run_id, marketplace=marketplace, endpoint=endpoint, pull_start=pull_start):
            p = {"startDate": pull_start.isoformat(), "endDate": end.isoformat()}
            rows = [{
                "seller_id": seller_id, "marketplace": marketplace,
                "asin": pick(c, "asin"),
                "changed_at": pick(c, "changedAt", "changed_at", "date", "detectedAt"),
                "change_type": pick(c, "changeType", "change_type", "type"),
                "content_type": pick(c, "contentType", "content_type", "field"),
                "raw": c, **meta(run_id),
            } for c in api_paged(endpoint, p, item_keys=("changes", "listingChanges"))]
            return [("listing_changes", rows, endpoint, p)], f"through={end.isoformat()}"

        statuses.append(refresh(con, "listing_changes", entity,
                                ttl_decision(reg, LISTING_TTL_HOURS, force), pull))

    filters = ["seller_id = :s", "marketplace = ANY(CAST(:m AS text[]))",
               "changed_at::date BETWEEN CAST(:start AS date) AND CAST(:end AS date)"]
    params = {"s": seller_id, "m": "{" + ",".join(marketplaces) + "}",
              "start": start, "end": end}
    if args.get("asin"):
        filters.append("asin = :asin")
        params["asin"] = args["asin"]

    rows = query(
        con,
        f"SELECT seller_id, marketplace, asin, changed_at, change_type, content_type, "
        f"raw, _fetched_at FROM {tbl('datadive_listing_changes')} "
        f"WHERE {' AND '.join(filters)} ORDER BY changed_at DESC",
        **params,
    )
    return rows, merge_status(statuses), {"window": {"start_date": start, "end_date": end}}


# ============================================================
# WAREHOUSE-ONLY DATASETS + QUOTA
# ============================================================

def handle_seller_profiles(con, args, force):
    rows = query(con, f"SELECT profile_id, seller_id, marketplace, raw, _fetched_at "
                      f"FROM {tbl('datadive_seller_profiles')} ORDER BY seller_id")
    return rows, {"pulled": False, "stale": False,
                  "reason": "loaded by the daily schedule"}, {}


def handle_rank_radars(con, args, force):
    filters, params = [], {}
    for col in ("asin", "marketplace"):
        if args.get(col):
            filters.append(f"{col} = :{col}")
            params[col] = args[col]
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = query(con, f"SELECT radar_id, asin, marketplace, status, raw, _fetched_at "
                      f"FROM {tbl('datadive_rank_radars')} {where} ORDER BY asin",
                 **params)
    return rows, {"pulled": False, "stale": False,
                  "reason": "loaded by the daily schedule"}, {}


def handle_quota(con, args, force):
    try:
        return [{"captured_at": iso(utc_now()), "quota": api_get("/v1/quota")}], \
            {"pulled": True, "stale": False, "reason": "live reading"}, {}
    except RuntimeError as error:
        rows = query(con, f"SELECT captured_at, raw AS quota FROM {tbl('datadive_quota_log')} "
                          f"ORDER BY captured_at DESC LIMIT 1")
        return rows, {"pulled": False, "stale": True,
                      "reason": f"live call failed: {str(error)[:200]}"}, {}


def merge_status(statuses):
    return {
        "pulled": any(s["pulled"] for s in statuses),
        "stale": any(s["stale"] for s in statuses),
        "reason": "; ".join(f"{s['entity']}: {s['reason']}" for s in statuses),
    }


HANDLERS = {
    "niches": handle_niches,
    "alerts": handle_alerts,
    "rank_daily": handle_rank_daily,
    "listing_changes": handle_listing_changes,
    "seller_profiles": handle_seller_profiles,
    "rank_radars": handle_rank_radars,
    "quota": handle_quota,
}


# ============================================================
# ENTRY POINT
# ============================================================

def handle_datadive_tool(args, conn=None):
    """
    Run one `datadive_data` tool call. Returns a JSON-serialisable dict.

    args: the tool input from the LLM, e.g.
        {"dataset": "niche_keywords", "niche_id": "abc123"}
        {"dataset": "rank_daily", "asin": "B0XXXX", "start_date": "2026-09-01"}
        {"dataset": "alerts", "alert_type": "blind_spend", "force_refresh": true}
    conn: optional pg8000.native.Connection if the app already has one.
    """
    args = dict(args or {})
    dataset = args.get("dataset")
    force = bool(args.get("force_refresh", False))
    max_rows = max(1, min(int(args.get("max_rows") or DEFAULT_MAX_ROWS), HARD_MAX_ROWS))

    log("tool_call", args=args)

    if dataset not in DATASETS:
        return {"error": f"Unknown dataset {dataset!r}. Use one of {DATASETS}."}

    con = conn or get_connection()

    try:
        if dataset in NICHE_DATASETS:
            rows, status, extra = handle_niche_dataset(con, dataset, args, force)
        else:
            rows, status, extra = HANDLERS[dataset](con, args, force)
    except ToolError as error:
        return {"dataset": dataset, "error": str(error)}

    snapshot_at = max((r.get("_fetched_at") for r in rows if r.get("_fetched_at")),
                      default=None)
    for row in rows:
        row.pop("_fetched_at", None)

    response = {
        "dataset": dataset,
        "served_from": "datadive" if status["pulled"] else "dw",
        "snapshot_at": iso(snapshot_at) if isinstance(snapshot_at, dt.datetime) else snapshot_at,
        "source_version": extra.pop("source_version", None),
        "stale": status["stale"],
        "redive_recommended": extra.pop("redive_recommended", False),
        "freshness_note": status["reason"],
        "row_count": len(rows),
        "truncated": len(rows) > max_rows,
        "rows": rows[:max_rows],
        **extra,
    }

    log("tool_result", dataset=dataset, served_from=response["served_from"],
        stale=response["stale"], row_count=response["row_count"])

    return jsonable(response)
