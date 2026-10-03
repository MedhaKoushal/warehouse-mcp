"""
apify_tools.py: the Apify tools the SellerOS assistant's LLM can call.

This is one data source among several (your own DB, Triple Whale, Data Dive, ...).
Everything here is prefixed so it can sit beside the other sources:
  APIFY_TOOLS           add to the combined tool list you send the model
  handle_apify_tool()   route any tool call whose name is in APIFY_TOOL_NAMES here
  APIFY_PROMPT_SECTION  add to your main system prompt
  LLM tool names        apify_amazon_product, apify_amazon_offers, apify_amazon_search,
                        apify_amazon_reviews, apify_social_posts, apify_web_research,
                        get_apify_results

Flow for every tool call:
  1. Validate what the LLM sent (ASINs, URLs, counts). Partner comes from the session (ctx).
  2. Amazon product / offers / reviews: look in Postgres (schema apify) for a snapshot
     newer than the freshness limit. Use it for every ASIN that has one.
  3. For anything missing, start (or join) an Apify run through apify_gateway.
  4. Wait up to APIFY_WAIT_SECS. A finished run is saved to Postgres, then read back.
     If it is still going, return status "running" + run_id. The run keeps going on
     Apify; it is saved when anyone asks again (or by apify_gateway.reconcile()).
  5. Return small, clean rows to the LLM, always with fetched_at.

Freshness defaults (the LLM sets max_age_hours=0 for "right now"):
  product 24h, offers 6h, reviews 7 days, search / social / web always live.
"""
import json
import logging
import os
import re
from urllib.parse import urlparse

import apify_gateway as gw

log = logging.getLogger(__name__)

WAIT_SECS = int(os.environ.get("APIFY_WAIT_SECS", 90))
FRESH_HOURS = {"product": 24, "offers": 6, "reviews": 168}
ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")
SOCIAL_DOMAINS = {
    "instagram": ("instagram.com",), "tiktok": ("tiktok.com",),
    "facebook": ("facebook.com", "fb.com"), "x": ("x.com", "twitter.com"),
    "youtube": ("youtube.com", "youtu.be"),
}


class ApifyToolError(Exception):
    """Bad arguments from the LLM; the message goes back so it can fix the call."""


# =============================================================== tool schemas (Anthropic format)

_ASINS = {"type": "array", "items": {"type": "string", "pattern": "^[A-Z0-9]{10}$"},
          "minItems": 1, "maxItems": 20, "description": "Amazon ASINs, e.g. B0C1234567"}
_MAX_AGE = {"type": "integer", "minimum": 0, "maximum": 720,
            "description": "Reuse stored data younger than this many hours. "
                           "Use 0 when the user wants live / right now / current data."}

APIFY_TOOLS = [
    {"name": "apify_amazon_product",
     "description": "Live Amazon product page via Apify: price, stock, star rating, review count "
                    "and bullet points for ASINs. Uses stored data when fresh, otherwise scrapes "
                    "live (costs money, can take a minute or two). For keyword or rank history "
                    "use the Data Dive tools; for your own sales use your own data tools.",
     "input_schema": {"type": "object", "properties": {
         "asins": _ASINS, "max_age_hours": {**_MAX_AGE, "default": 24}}, "required": ["asins"]}},
    {"name": "apify_amazon_offers",
     "description": "Live Amazon offers via Apify: Buy Box seller, competing offers and stock "
                    "for ASINs. Use for Buy Box, hijacker or offer-price questions.",
     "input_schema": {"type": "object", "properties": {
         "asins": _ASINS,
         "max_offers": {"type": "integer", "minimum": 1, "maximum": 20, "default": 10},
         "max_age_hours": {**_MAX_AGE, "default": 6}}, "required": ["asins"]}},
    {"name": "apify_amazon_search",
     "description": "Live Amazon search page via Apify: what a search shows right now for "
                    "keywords, with positions (organic and sponsored). Always live. For search "
                    "volume or keyword rank history use the Data Dive tools.",
     "input_schema": {"type": "object", "properties": {
         "terms": {"type": "array", "items": {"type": "string", "maxLength": 120},
                   "minItems": 1, "maxItems": 5},
         "per_term": {"type": "integer", "minimum": 1, "maximum": 30, "default": 30}},
         "required": ["terms"]}},
    {"name": "apify_amazon_reviews",
     "description": "Amazon review text via Apify: count, average rating, star breakdown and "
                    "recent sample reviews for ASINs. Pulls new reviews only if not pulled "
                    "within max_age_hours.",
     "input_schema": {"type": "object", "properties": {
         "asins": {**_ASINS, "maxItems": 5},
         "per_asin": {"type": "integer", "minimum": 10, "maximum": 400, "default": 100},
         "max_age_hours": {**_MAX_AGE, "default": 168}}, "required": ["asins"]}},
    {"name": "apify_social_posts",
     "description": "Public posts via Apify from a brand's Instagram, TikTok, Facebook, X or "
                    "YouTube, with text and engagement. Pass full profile or page URLs. For ad "
                    "spend or store performance use the Triple Whale tools.",
     "input_schema": {"type": "object", "properties": {
         "platform": {"type": "string", "enum": list(SOCIAL_DOMAINS)},
         "urls": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 5},
         "per_source": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20}},
         "required": ["platform", "urls"]}},
    {"name": "apify_web_research",
     "description": "Web content via Apify for brand copy. 'website' crawls a brand site (urls); "
                    "'google_search' returns Google results (queries); 'rag_browser' searches "
                    "and reads top pages for one question (one query).",
     "input_schema": {"type": "object", "properties": {
         "mode": {"type": "string", "enum": ["website", "google_search", "rag_browser"]},
         "urls": {"type": "array", "items": {"type": "string"}, "maxItems": 3},
         "queries": {"type": "array", "items": {"type": "string", "maxLength": 200}, "maxItems": 5},
         "max_pages": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20}},
         "required": ["mode"]}},
    {"name": "get_apify_results",
     "description": "Results of Apify scrapes that were still running earlier. Pass the "
                    "run_ids a previous apify_* tool returned with status 'running'.",
     "input_schema": {"type": "object", "properties": {
         "run_ids": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 5}},
         "required": ["run_ids"]}},
]

APIFY_TOOL_NAMES = {t["name"] for t in APIFY_TOOLS}

# Add this to your main system prompt; it is not a whole prompt on its own.
APIFY_PROMPT_SECTION = """Apify tools (names start with apify_ or are get_apify_results) fetch live public
Amazon pages, social posts and web pages, and store them as snapshots.
- Prefer stored data. Set max_age_hours to 0 only when the user wants live data.
- Every row has fetched_at. Always tell the user how fresh the data is.
- When a result includes a live run with cost_usd, mention the cost briefly.
- If a tool returns status "running", tell the user it is still fetching. When they come
  back or ask again, call get_apify_results with the run_ids.
- If the daily Apify budget is reached, say so and answer from stored data only.
- If a run has a warning (partial results, possible free-tier review limit), tell the user.
- Never invent numbers that are not in tool results."""


# =============================================================== helpers

def _cut(text, n):
    if not isinstance(text, str):
        return text
    return text if len(text) <= n else text[:n].rstrip() + "…"


def _int_arg(args, key, default, lo, hi):
    try:
        v = int(args.get(key, default))
    except (TypeError, ValueError):
        raise ApifyToolError(f"{key} must be a whole number")
    return max(lo, min(v, hi))


def _asins(args, limit):
    asins = list(dict.fromkeys(str(a).strip().upper() for a in args.get("asins") or []))
    bad = [a for a in asins if not ASIN_RE.match(a)]
    if not asins:
        raise ApifyToolError("asins is required")
    if bad:
        raise ApifyToolError(f"not valid ASINs: {bad}")
    if len(asins) > limit:
        raise ApifyToolError(f"at most {limit} ASINs per call")
    return asins


def _urls(args, domains=None, limit=5):
    urls = [str(u).strip() for u in args.get("urls") or []]
    if not urls:
        raise ApifyToolError("urls is required")
    if len(urls) > limit:
        raise ApifyToolError(f"at most {limit} urls per call")
    for u in urls:
        p = urlparse(u)
        host = (p.hostname or "").lower()
        if p.scheme not in ("http", "https") or not host:
            raise ApifyToolError(f"not a full URL: {u}")
        if domains and not any(host == d or host.endswith("." + d) for d in domains):
            raise ApifyToolError(f"{u} is not on {', '.join(domains)}")
    return urls


def _run_info(row):
    info = {"run_id": row["run_id"], "status": row["status"], "items": row["items"],
            "cost_usd": row["cost_usd"], "fetched_at": row["finished_at"]}
    warnings = []
    if row["status"] != "SUCCEEDED":
        warnings.append(f"Run ended {row['status']}; results may be partial.")
    if row.get("warning"):
        warnings.append(row["warning"])
    if warnings:
        info["warning"] = " ".join(warnings)
    return info


def _running(run_ids):
    return {"status": "running", "run_ids": run_ids,
            "message": "Live scrape still running. Tell the user it is in progress; "
                       "call get_apify_results with these run_ids when they ask again."}


def _trim_offer(o):
    if not isinstance(o, dict):
        return None
    price, seller = o.get("price"), o.get("seller")
    if isinstance(seller, dict):
        seller = seller.get("name")
    return {"seller": o.get("sellerName") or seller,
            "price": price.get("value") if isinstance(price, dict) else price,
            "condition": o.get("condition"),
            "fba_or_prime": o.get("isFulfilledByAmazon", o.get("isPrime"))}


# =============================================================== reading Postgres

SELECTS = {
    "product_pages": """asin, left(title, 200) AS title, brand, price, currency, rating,
                        review_count, in_stock, stock_text, bullets, fetched_at""",
    "offers": "asin, buy_box_seller, buy_box_price, in_stock, offers, fetched_at",
    "search_results": """term, position, sponsored, asin, left(title, 150) AS title, price,
                         rating, review_count, fetched_at""",
    "social_posts": """platform, url, left(post_text, 400) AS post_text, posted_at, likes,
                       comments, views, fetched_at""",
    "web_pages": """source, url, title, left(page_text, 800) AS page_text, google_query,
                    google_results, fetched_at""",
}
ORDER = {"product_pages": "item_index", "offers": "item_index",
         "search_results": "term, position NULLS LAST, item_index",
         "social_posts": "posted_at DESC NULLS LAST", "web_pages": "item_index"}
ROW_LIMIT = {"product_pages": 20, "offers": 20, "search_results": 100,
             "social_posts": 60, "web_pages": 15}


def _shape(table, r):
    """Final small touches per row before it goes to the LLM."""
    if table == "product_pages":
        r["bullets"] = [_cut(b, 200) for b in (r.get("bullets") or [])[:5] if isinstance(b, str)]
    elif table == "offers":
        r["offers"] = [o for o in map(_trim_offer, (r.get("offers") or [])[:10]) if o]
    elif table == "web_pages":
        g = r.pop("google_results", None)
        if g:
            r["google_results"] = [{"title": x.get("title"), "url": x.get("url"),
                                    "snippet": _cut(x.get("description"), 250)}
                                   for x in g[:10] if isinstance(x, dict)]
    return r


def rows_for_run(table, run_id):
    if run_id in getattr(gw, "_MEMORY_ROWS", {}):
        return [_shape(table, dict(r)) for r in gw._MEMORY_ROWS[run_id][:ROW_LIMIT[table]]]
    limit = ROW_LIMIT[table]
    rows = gw.q(f"SELECT {SELECTS[table]} FROM apify.{table} "
                f"WHERE run_id = %s ORDER BY {ORDER[table]} LIMIT %s", (run_id, limit))
    return [_shape(table, r) for r in rows]


def fresh_snapshots(table, partner, asins, hours):
    """Newest row per ASIN younger than `hours`."""
    if hours <= 0:
        return {}
    rows = gw.q(f"""
        SELECT DISTINCT ON (asin) {SELECTS[table]}
        FROM apify.{table}
        WHERE partner = %s AND asin = ANY(%s)
          AND fetched_at >= now() - make_interval(hours => %s)
        ORDER BY asin, fetched_at DESC""", (partner, asins, hours))
    return {r["asin"]: _shape(table, r) for r in rows}


def review_summary(partner, asins, samples=15):
    summary = {r["asin"]: r for r in gw.q(
        "SELECT * FROM apify.v_review_summary WHERE partner = %s AND asin = ANY(%s)",
        (partner, asins))}
    sample_rows = gw.q("""
        SELECT * FROM (
          SELECT asin, rating, left(title, 120) AS title, left(body, 400) AS body,
                 review_date, verified,
                 row_number() OVER (PARTITION BY asin ORDER BY fetched_at DESC, id DESC) AS rn
          FROM apify.reviews WHERE partner = %s AND asin = ANY(%s)) t
        WHERE rn <= %s ORDER BY asin, rn""", (partner, asins, samples))
    out = {}
    for a in asins:
        s = summary.get(a)
        out[a] = {
            "reviews_stored": s["reviews"] if s else 0,
            "average_rating": s["avg_rating"] if s else None,
            "star_distribution": {k: s[f"stars_{k}"] for k in "54321"} if s else {},
            "latest_fetched_at": s["latest_fetched_at"] if s else None,
            "sample": [{k: r[k] for k in ("rating", "title", "body", "review_date", "verified")}
                       for r in sample_rows if r["asin"] == a],
        }
    return out


def recently_pulled_reviews(partner, asins, hours):
    if hours <= 0:
        return set()
    rows = gw.q("""
        SELECT DISTINCT a.asin
        FROM apify.runs r
        CROSS JOIN LATERAL jsonb_array_elements_text(r.request -> 'asins') AS a(asin)
        WHERE r.partner = %s AND r.operation = 'reviews' AND r.status = 'SUCCEEDED'
          AND r.saved_at IS NOT NULL
          AND r.started_at >= now() - make_interval(hours => %s)
          AND a.asin = ANY(%s)""", (partner, hours, asins))
    return {r["asin"] for r in rows}


# =============================================================== tool handlers

def _amazon_snapshot(op, table, args, ctx, extra=None):
    asins = _asins(args, 20)
    hours = _int_arg(args, "max_age_hours", FRESH_HOURS[op], 0, 720)
    partner = ctx["partner"]

    cached = fresh_snapshots(table, partner, asins, hours)
    rows = list(cached.values())
    missing = [a for a in asins if a not in cached]
    if not missing:
        return {"source": "stored", "rows": rows}

    started = gw.start_or_join(partner, op, {"asins": missing, "domain": ctx.get("domain", "www.amazon.com"),
                                             **(extra or {})})
    saved = gw.wait_for_runs([started["run_id"]], WAIT_SECS)[started["run_id"]]
    if saved is None:
        return {**_running([started["run_id"]]), "rows_from_storage": rows}

    rows += rows_for_run(table, started["run_id"])
    found = {r.get("asin") for r in rows}
    out = {"source": "stored+live" if cached else "live", "rows": rows, "run": _run_info(saved)}
    not_found = [a for a in asins if a not in found]
    if not_found:
        out["not_found"] = not_found
    return out


def tool_product(args, ctx):
    return _amazon_snapshot("product", "product_pages", args, ctx)


def tool_offers(args, ctx):
    return _amazon_snapshot("offers", "offers", args, ctx,
                            {"max_offers": _int_arg(args, "max_offers", 10, 1, 20)})


def tool_search(args, ctx):
    terms = list(dict.fromkeys(str(t).strip() for t in args.get("terms") or [] if str(t).strip()))
    if not terms or len(terms) > 5:
        raise ApifyToolError("give 1 to 5 search terms")
    per = _int_arg(args, "per_term", 30, 1, 30)
    # One run per term so every row knows exactly which term found it.
    runs = [gw.start_or_join(ctx["partner"], "search",
                             {"terms": [t], "per_term": per,
                              "domain": ctx.get("domain", "www.amazon.com")}) for t in terms]
    saved = gw.wait_for_runs([r["run_id"] for r in runs], WAIT_SECS)
    rows, runs_done, still = [], [], []
    for rid, row in saved.items():
        if row:
            rows += rows_for_run("search_results", rid)
            runs_done.append(_run_info(row))
        else:
            still.append(rid)
    out = {"source": "live", "rows": rows, "runs": runs_done}
    if still:
        out.update(_running(still))
    return out


def tool_reviews(args, ctx):
    asins = _asins(args, 5)
    hours = _int_arg(args, "max_age_hours", FRESH_HOURS["reviews"], 0, 720)
    partner = ctx["partner"]
    to_pull = [a for a in asins if a not in recently_pulled_reviews(partner, asins, hours)]
    out = {"source": "stored"}
    if to_pull:
        started = gw.start_or_join(partner, "reviews",
                                   {"asins": to_pull, "domain": ctx.get("domain", "www.amazon.com"),
                                    "per_asin": _int_arg(args, "per_asin", 100, 10, 400)})
        saved = gw.wait_for_runs([started["run_id"]], WAIT_SECS)[started["run_id"]]
        if saved is None:
            return {**_running([started["run_id"]]),
                    "stored_so_far": review_summary(partner, asins)}
        out.update(source="stored+live", run=_run_info(saved))
    # A new run holds only new reviews (dedupe), so always answer from everything stored.
    out["by_asin"] = review_summary(partner, asins)
    return out


def _live(op, table, payload, ctx):
    started = gw.start_or_join(ctx["partner"], op, payload)
    saved = gw.wait_for_runs([started["run_id"]], WAIT_SECS)[started["run_id"]]
    if saved is None:
        return _running([started["run_id"]])
    return {"source": "live", "rows": rows_for_run(table, started["run_id"]),
            "run": _run_info(saved)}


def tool_social(args, ctx):
    platform = args.get("platform")
    if platform not in SOCIAL_DOMAINS:
        raise ApifyToolError(f"platform must be one of {list(SOCIAL_DOMAINS)}")
    return _live(platform, "social_posts",
                 {"urls": _urls(args, SOCIAL_DOMAINS[platform]),
                  "per_source": _int_arg(args, "per_source", 20, 1, 50)}, ctx)


def tool_web(args, ctx):
    mode = args.get("mode")
    if mode == "website":
        payload = {"urls": _urls(args, limit=3), "max_pages": _int_arg(args, "max_pages", 20, 1, 50)}
    elif mode in ("google_search", "rag_browser"):
        queries = [str(q).strip() for q in args.get("queries") or [] if str(q).strip()]
        if not queries:
            raise ApifyToolError("queries is required for this mode")
        if mode == "rag_browser" and len(queries) != 1:
            raise ApifyToolError("rag_browser takes exactly one query")
        payload = {"queries": queries[:5]}
    else:
        raise ApifyToolError("mode must be website, google_search or rag_browser")
    return _live(mode, "web_pages", payload, ctx)


def tool_get_results(args, ctx):
    run_ids = [str(r).strip() for r in args.get("run_ids") or []][:5]
    rows = gw.q("SELECT run_id, doc_table, request FROM apify.runs "
                "WHERE run_id = ANY(%s) AND partner = %s", (run_ids, ctx["partner"]))
    known = {r["run_id"]: r for r in rows}           # other partners' runs are simply not found
    if not known:
        raise ApifyToolError("no runs with those ids for this account")
    saved = gw.wait_for_runs(list(known), 20)
    out = {"results": [], "still_running": []}
    for rid, row in saved.items():
        if not row:
            out["still_running"].append(rid)
            continue
        table = known[rid]["doc_table"]
        res = {"run": _run_info(row), "table": table}
        if table == "reviews":
            res["by_asin"] = review_summary(ctx["partner"], known[rid]["request"].get("asins") or [])
        else:
            res["rows"] = rows_for_run(table, rid)
        out["results"].append(res)
    return out


APIFY_HANDLERS = {
    "apify_amazon_product": tool_product,
    "apify_amazon_offers": tool_offers,
    "apify_amazon_search": tool_search,
    "apify_amazon_reviews": tool_reviews,
    "apify_social_posts": tool_social,
    "apify_web_research": tool_web,
    "get_apify_results": tool_get_results,
}


def handle_apify_tool(name, args, ctx=None):
    """ctx comes from your login session, e.g. {"partner": "rolling-sands", "domain": "www.amazon.com"}."""
    ctx = dict(ctx or {})
    if not ctx.get("partner"):
        ctx["partner"] = os.environ.get("APIFY_DEFAULT_PARTNER") or getattr(gw, "get_setting", lambda k, d: d)("APIFY_DEFAULT_PARTNER", "selleros")
    if not ctx.get("domain"):
        ctx["domain"] = getattr(gw, "get_setting", lambda k, d: d)("APIFY_DEFAULT_DOMAIN", "www.amazon.com")

    fn = APIFY_HANDLERS.get(name)
    if not fn:
        return {"error": f"Unknown tool {name}."}
    try:
        return fn(args or {}, ctx)
    except ApifyToolError as e:
        return {"error": str(e)}
    except gw.ApifyBudgetExceeded as e:
        who = "this account's" if e.scope == "partner" else "the overall"
        return {"error": f"{who} daily Apify budget is used up (${e.spent:.2f} of ${e.cap:.2f}). "
                         "Answer from stored data and tell the user live data is available "
                         "again tomorrow (UTC), or an admin can raise the cap."}
    except gw.ApifyError as e:
        log.warning("Apify error in %s: %s", name, e)
        return {"error": f"Apify error: {e}"}
    except Exception as e:
        log.exception("tool %s failed", name)
        return {"error": f"Apify data service error: {e}. Tell the user and suggest trying again."}


# =============================================================== combining with other sources (example)

def apify_chat_example(client, messages, ctx, main_prompt, other_tools=(), other_handler=None,
                       model="claude-sonnet-5-5"):
    """How to plug Apify in beside your other sources (own DB, Triple Whale, Data Dive).
    client = anthropic.Anthropic(); main_prompt = your assistant's own system prompt;
    other_tools / other_handler = the other sources' tool list and router.
    `messages` is updated in place so the conversation can continue."""
    system = main_prompt + "\n\n" + APIFY_PROMPT_SECTION
    tools = list(other_tools) + APIFY_TOOLS
    while True:
        resp = client.messages.create(model=model, max_tokens=2000, system=system,
                                      tools=tools, messages=messages)
        messages.append({"role": "assistant", "content": resp.content})
        if resp.stop_reason != "tool_use":
            return "".join(b.text for b in resp.content if b.type == "text")
        results = []
        for block in resp.content:
            if block.type != "tool_use":
                continue
            if block.name in APIFY_TOOL_NAMES:
                out = handle_apify_tool(block.name, block.input, ctx)
            elif other_handler:
                out = other_handler(block.name, block.input, ctx)
            else:
                out = {"error": f"No handler for tool {block.name}."}
            results.append({"type": "tool_result", "tool_use_id": block.id,
                            "content": json.dumps(out, default=str, ensure_ascii=False),
                            "is_error": "error" in out})
        messages.append({"role": "user", "content": results})
