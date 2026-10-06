"""
merchantspring_tools.py: FastMCP tools for MerchantSpring API integration.
Implements:
  - Channels & tags discovery
  - Search Query Performance (SQP) for ASINs and keywords
  - Advertising (campaigns, ad groups, keywords, products)
  - Profitability (store and product P&L)
  - Sales & advertising reporting
  - Order search and ASIN store lookup
"""
import calendar
from datetime import datetime, timezone, timedelta
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from merchantspring_client import merchantspring_client, MerchantSpringAPIError, get_ms_setting

logger = logging.getLogger("selleros_mcp.merchantspring_tools")

# In-memory channel cache
_merchantspring_channels_cache: Optional[List[Dict[str, Any]]] = None

class AmbiguousChannelError(Exception):
    def __init__(self, message: str, candidates: List[str]):
        self.message = message
        self.candidates = candidates
        super().__init__(message)

def _resolve_sqp_date_epochs(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prior_from_date: Optional[str] = None,
    prior_to_date: Optional[str] = None
) -> Tuple[int, int, int, int]:
    """
    Resolves Unix epoch timestamps (in seconds) for MerchantSpring SQP & report queries.
    If dates are omitted, defaults to the last completed calendar month and its prior comparison period.
    """
    now = datetime.now(timezone.utc)

    def parse_to_epoch(d_str: Any, is_end: bool = False) -> int:
        if isinstance(d_str, (int, float)):
            return int(d_str)
        s = str(d_str).strip()
        if s.isdigit():
            return int(s)
        if len(s) == 7 and s[4] == "-":
            year, month = int(s[:4]), int(s[5:7])
            if is_end:
                last_day = calendar.monthrange(year, month)[1]
                dt = datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)
            else:
                dt = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
            return int(dt.timestamp())
        dt = datetime.fromisoformat(s)
        if is_end:
            dt = dt.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        else:
            dt = dt.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
        return int(dt.timestamp())

    if not from_date or not to_date:
        first_of_current = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        last_of_prior = first_of_current - timedelta(seconds=1)
        first_of_prior = datetime(last_of_prior.year, last_of_prior.month, 1, tzinfo=timezone.utc)

        last_of_prior_prior = first_of_prior - timedelta(seconds=1)
        first_of_prior_prior = datetime(last_of_prior_prior.year, last_of_prior_prior.month, 1, tzinfo=timezone.utc)

        from_epoch = int(first_of_prior.timestamp())
        to_epoch = int(last_of_prior.timestamp())
        prior_from_epoch = int(first_of_prior_prior.timestamp())
        prior_to_epoch = int(last_of_prior_prior.timestamp())
    else:
        from_epoch = parse_to_epoch(from_date, is_end=False)
        to_epoch = parse_to_epoch(to_date, is_end=True)
        if prior_from_date and prior_to_date:
            prior_from_epoch = parse_to_epoch(prior_from_date, is_end=False)
            prior_to_epoch = parse_to_epoch(prior_to_date, is_end=True)
        else:
            duration = to_epoch - from_epoch
            prior_to_epoch = from_epoch - 1
            prior_from_epoch = prior_to_epoch - duration

    return from_epoch, to_epoch, prior_from_epoch, prior_to_epoch

def _sanitize_ms_sort_key(sort_key: Optional[str]) -> str:
    if not sort_key:
        return "cost"
    mapping = {
        "spend": "cost",
        "sales": "attributed_sales",
        "ad_spend": "cost",
        "ad_sales": "attributed_sales",
        "cpc": "cost_per_click",
        "ctr": "click_through_rate",
        "roas": "roas",
        "acos": "acos",
        "clicks": "clicks",
        "impressions": "impressions",
    }
    return mapping.get(sort_key.lower().strip(), sort_key)

async def _resolve_merchantspring_channel(
    channel_id: Optional[str] = None,
    channel_name: Optional[str] = None,
    merchant_id: Optional[str] = None,
    country: Optional[str] = None
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Resolves (channelId, merchantId, displayName) for MerchantSpring API calls.
    Uses in-memory cache to avoid repeated POST /channels queries.
    Raises AmbiguousChannelError if multiple channels match and cannot be distinguished.
    """
    global _merchantspring_channels_cache
    if channel_id and merchant_id:
        return str(channel_id), str(merchant_id), channel_name or str(channel_id)

    try:
        if not _merchantspring_channels_cache:
            raw = await merchantspring_client.post("/channels", json_body={})
            if isinstance(raw, list):
                _merchantspring_channels_cache = raw
    except Exception as e:
        logger.warning(f"Failed to fetch MerchantSpring channels for resolution: {e}")

    channels = _merchantspring_channels_cache or []

    # 1. Match by channel_id directly
    if channel_id:
        for c in channels:
            if str(c.get("channelId")) == str(channel_id):
                return str(c.get("channelId")), str(c.get("merchantId")), c.get("displayName")
        return None, None, None

    # 2. Match by channel_name
    if channel_name:
        n_lower = channel_name.strip().lower()

        # Step 2a: Check exact matches first
        exact_matches = [
            c for c in channels
            if str(c.get("displayName", "")).strip().lower() == n_lower
        ]
        if exact_matches:
            if country:
                c_upper = country.strip().upper()
                exact_matches = [
                    c for c in exact_matches
                    if str(c.get("countryCode", "")).upper() == c_upper
                ]
            if len(exact_matches) == 1:
                target = exact_matches[0]
                return str(target.get("channelId")), str(target.get("merchantId")), target.get("displayName")
            if len(exact_matches) > 1:
                names = [f"{c.get('displayName')} ({c.get('countryCode', 'Unknown')})" for c in exact_matches]
                raise AmbiguousChannelError(
                    f"Multiple stores match '{channel_name}': {', '.join(names)}. Please specify the country.",
                    names
                )

        # Step 2b: Check partial / substring matches
        partial_matches = [
            c for c in channels
            if n_lower in str(c.get("displayName", "")).lower()
            or n_lower in str(c.get("channelId", ""))
            or n_lower in str(c.get("merchantId", "")).lower()
        ]
        if country and partial_matches:
            c_upper = country.strip().upper()
            partial_matches = [
                c for c in partial_matches
                if str(c.get("countryCode", "")).upper() == c_upper
            ]

        if len(partial_matches) == 1:
            target = partial_matches[0]
            return str(target.get("channelId")), str(target.get("merchantId")), target.get("displayName")
        elif len(partial_matches) > 1:
            names = [f"{c.get('displayName')} ({c.get('countryCode', 'Unknown')})" for c in partial_matches]
            raise AmbiguousChannelError(
                f"Multiple stores match '{channel_name}': {', '.join(names)}. Please specify the exact store or country.",
                names
            )

    return None, None, None


# ==============================================================================
# Domain 1: Channels & Tags Tools
# ==============================================================================

async def get_merchantspring_channels(
    marketplace: Optional[str] = None,
    country: Optional[str] = None,
    channel_name: Optional[str] = None
) -> str:
    """
    Get connected sales channels/stores from MerchantSpring API via POST /channels.
    Filter results by specifying marketplace (e.g. 'amazon'), country (e.g. 'USA'), or channel_name (e.g. 'Herbalogic').
    Returns channel names, marketplaces, country codes, advertising connection status, and channel IDs.
    """
    filter_dict = {}
    if marketplace:
        mps = [m.strip().lower() for m in marketplace.split(",") if m.strip()]
        if mps:
            filter_dict["marketplaces"] = mps
    if country:
        codes = []
        for c in country.split(","):
            c_clean = c.strip().upper()
            if c_clean in ("US", "USA", "UNITED STATES"):
                codes.append("USA")
            elif c_clean in ("CA", "CAN", "CANADA"):
                codes.append("CAN")
            elif c_clean in ("UK", "GB", "GBR", "UNITED KINGDOM"):
                codes.append("GBR")
            elif c_clean:
                codes.append(c_clean)
        if codes:
            filter_dict["countryCodes"] = codes

    body = {"filter": filter_dict} if filter_dict else {}

    try:
        raw_data = await merchantspring_client.post("/channels", json_body=body)
        if isinstance(raw_data, list):
            channel_list = raw_data
            if channel_name:
                c_name_lower = channel_name.strip().lower()
                channel_list = [
                    c for c in channel_list
                    if c_name_lower in str(c.get("displayName", "")).lower()
                    or c_name_lower in str(c.get("channelId", ""))
                    or c_name_lower in str(c.get("merchantId", "")).lower()
                ]

            formatted_channels = [
                {
                    "channel_name": c.get("displayName") or "Unknown",
                    "marketplace": c.get("marketplace"),
                    "country": c.get("countryCode"),
                    "advertising_connected": c.get("hasAdvertisingConnected"),
                    "channel_id": str(c.get("channelId")),
                    "merchant_id": c.get("merchantId"),
                }
                for c in channel_list
            ]
            return json.dumps({"total": len(formatted_channels), "data": formatted_channels}, indent=2)

        return json.dumps(raw_data, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_merchantspring_tags(
    page_size: int = 25,
    page_index: int = 0
) -> str:
    """
    Retrieve all tags across all caller channels via GET /tags.
    Useful for organizing and discovering stores by category, group, or brand.
    """
    params = {
        "pageSize": str(min(page_size, 100)),
        "pageIndex": str(page_index)
    }
    try:
        data = await merchantspring_client.get("/tags", params=params)
        return json.dumps(data, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ==============================================================================
# Domain 2: Search Query Performance (SQP) Tools
# ==============================================================================

async def get_search_query_performance_asins(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    asins: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prior_from_date: Optional[str] = None,
    prior_to_date: Optional[str] = None,
    currency: str = "USD",
    timezone: Optional[str] = None,
    page_size: int = 25,
    page_index: int = 0,
    sort_key: str = "searchQueryVolume",
    sort_order: str = "desc"
) -> str:
    """
    Retrieve Search Query Performance (SQP) data at the ASIN level (Amazon only) from MerchantSpring via GET /search-query-performance/asins.
    Returns aggregated search query volume, impressions, impression share, clicks, click share, CTR,
    cart adds, purchases, purchase share, conversion rate (CVR), and revenue per ASIN, compared to a prior period.
    Pass store_name / channel_name (e.g. 'Herbalogic') or channel_id.
    Dates default to the last completed calendar month if omitted.
    """
    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name, merchant_id=merchant_id
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid or not mid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    f_epoch, t_epoch, pf_epoch, pt_epoch = _resolve_sqp_date_epochs(
        from_date, to_date, prior_from_date, prior_to_date
    )

    params: Dict[str, Any] = {
        "channelId": cid,
        "merchantId": mid,
        "fromDate": str(f_epoch),
        "toDate": str(t_epoch),
        "priorFromDate": str(pf_epoch),
        "priorToDate": str(pt_epoch),
        "timezone": timezone or get_ms_setting("DEFAULT_TIMEZONE", "UTC"),
        "currency": currency,
        "pageSize": min(page_size, 50),
        "pageIndex": page_index,
        "sortKey": sort_key,
        "sortOrder": sort_order
    }
    if asins:
        asins_list = [a.strip() for a in asins.split(",") if a.strip()]
        if len(asins_list) == 1:
            params["asins"] = asins_list[0]
        else:
            params["asins"] = asins_list

    try:
        res = await merchantspring_client.get("/search-query-performance/asins", params=params)
        raw_items = res.get("data", []) if isinstance(res, dict) else []

        formatted_items = []
        for item in raw_items:
            cur = item.get("current", {}) or {}
            pri = item.get("prior", {}) or {}
            formatted_items.append({
                "asin": item.get("asin"),
                "search_query_volume": cur.get("searchQueryVolume"),
                "impressions": cur.get("impressions"),
                "impression_share": round(cur.get("impressionShare", 0), 2) if cur.get("impressionShare") is not None else None,
                "clicks": cur.get("clicks"),
                "click_share": round(cur.get("clickShare", 0), 2) if cur.get("clickShare") is not None else None,
                "ctr": round(cur.get("ctr", 0), 2) if cur.get("ctr") is not None else None,
                "cart_adds": cur.get("cartAdds"),
                "purchases": cur.get("purchases"),
                "purchase_share": round(cur.get("purchaseShare", 0), 2) if cur.get("purchaseShare") is not None else None,
                "cvr": round(cur.get("cvr", 0), 2) if cur.get("cvr") is not None else None,
                "revenue": cur.get("currentRevenue"),
                "prior_impressions": pri.get("impressions"),
                "prior_clicks": pri.get("clicks"),
                "prior_purchases": pri.get("purchases"),
                "prior_revenue": pri.get("currentRevenue")
            })

        output = {
            "store_name": resolved_name,
            "total": len(formatted_items),
            "data": formatted_items,
            "meta": res.get("meta") if isinstance(res, dict) else None
        }
        return json.dumps(output, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_search_query_performance_keywords(
    asin: str,
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    search_text: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prior_from_date: Optional[str] = None,
    prior_to_date: Optional[str] = None,
    currency: str = "USD",
    timezone: Optional[str] = None,
    page_size: int = 25,
    page_index: int = 0,
    sort_key: str = "searchQueryVolume",
    sort_order: str = "desc"
) -> str:
    """
    Retrieve Search Query Performance (SQP) data at the keyword/search query level for a single ASIN (Amazon only) via GET /search-query-performance/keywords.
    Returns metrics per search query (search query volume, impressions, clicks, click share, CTR, cart adds, purchases, purchase share, CVR, revenue).
    Pass asin (e.g. 'B0714GKJ12') and store_name / channel_name (e.g. 'Herbalogic').
    Dates default to the last completed calendar month if omitted.
    """
    if not asin:
        return json.dumps({"error": "asin parameter is required for keyword-level SQP.", "status_code": 400})

    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name, merchant_id=merchant_id
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid or not mid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    f_epoch, t_epoch, pf_epoch, pt_epoch = _resolve_sqp_date_epochs(
        from_date, to_date, prior_from_date, prior_to_date
    )

    params: Dict[str, Any] = {
        "asin": asin.strip(),
        "channelId": cid,
        "merchantId": mid,
        "fromDate": str(f_epoch),
        "toDate": str(t_epoch),
        "priorFromDate": str(pf_epoch),
        "priorToDate": str(pt_epoch),
        "timezone": timezone or get_ms_setting("DEFAULT_TIMEZONE", "UTC"),
        "currency": currency,
        "pageSize": min(page_size, 50),
        "pageIndex": page_index,
        "sortKey": sort_key,
        "sortOrder": sort_order
    }
    if search_text:
        params["searchText"] = search_text.strip()

    try:
        res = await merchantspring_client.get("/search-query-performance/keywords", params=params)
        raw_items = res.get("data", []) if isinstance(res, dict) else []

        formatted_items = []
        for item in raw_items:
            cur = item.get("current", {}) or {}
            pri = item.get("prior", {}) or {}
            formatted_items.append({
                "search_query": item.get("searchQuery"),
                "search_query_volume": cur.get("searchQueryVolume"),
                "impressions": cur.get("impressions"),
                "impression_share": round(cur.get("impressionShare", 0), 2) if cur.get("impressionShare") is not None else None,
                "clicks": cur.get("clicks"),
                "click_share": round(cur.get("clickShare", 0), 2) if cur.get("clickShare") is not None else None,
                "ctr": round(cur.get("ctr", 0), 2) if cur.get("ctr") is not None else None,
                "cart_adds": cur.get("cartAdds"),
                "purchases": cur.get("purchases"),
                "purchase_share": round(cur.get("purchaseShare", 0), 2) if cur.get("purchaseShare") is not None else None,
                "cvr": round(cur.get("cvr", 0), 2) if cur.get("cvr") is not None else None,
                "revenue": cur.get("currentRevenue"),
                "prior_impressions": pri.get("impressions"),
                "prior_clicks": pri.get("clicks"),
                "prior_purchases": pri.get("purchases"),
                "prior_revenue": pri.get("currentRevenue")
            })

        output = {
            "asin": asin,
            "store_name": resolved_name,
            "total": len(formatted_items),
            "data": formatted_items,
            "meta": res.get("meta") if isinstance(res, dict) else None
        }
        return json.dumps(output, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ==============================================================================
# Domain 3: Advertising Tools
# ==============================================================================

async def get_ms_campaigns(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    status: Optional[str] = None,
    currency: str = "USD",
    page_size: int = 50,
    page_index: int = 0,
    sort_key: str = "cost",
    sort_order: str = "desc"
) -> str:
    """
    Get advertising campaigns from MerchantSpring API via GET /advertising/campaigns.
    Returns campaign names, spend, sales, impressions, clicks, CPC, CTR, ROAS, and status.
    Pass store_name / channel_name (e.g. 'Herbalogic') or channel_id.
    """
    sort_key = _sanitize_ms_sort_key(sort_key)
    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name, merchant_id=merchant_id
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    f_epoch, t_epoch, _, _ = _resolve_sqp_date_epochs(from_date, to_date)
    params: Dict[str, Any] = {
        "channelId": cid,
        "fromDate": str(f_epoch),
        "toDate": str(t_epoch),
        "currency": currency,
        "pageSize": min(page_size, 100),
        "pageIndex": page_index,
        "sortKey": sort_key,
        "sortOrder": sort_order
    }
    if status:
        params["status"] = status

    try:
        data = await merchantspring_client.get("/advertising/campaigns", params=params)
        return json.dumps({"store_name": resolved_name, "result": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_ms_ad_groups(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    status: Optional[str] = None,
    currency: str = "USD",
    page_size: int = 50,
    page_index: int = 0,
    sort_key: str = "cost",
    sort_order: str = "desc"
) -> str:
    """
    Get advertising ad groups from MerchantSpring via GET /advertising/adGroups.
    Returns ad group metrics (spend, sales, impressions, clicks, CPC).
    Filter by campaign_id or store_name.
    """
    sort_key = _sanitize_ms_sort_key(sort_key)
    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    f_epoch, t_epoch, _, _ = _resolve_sqp_date_epochs(from_date, to_date)
    params: Dict[str, Any] = {
        "channelId": cid,
        "fromDate": str(f_epoch),
        "toDate": str(t_epoch),
        "currency": currency,
        "pageSize": min(page_size, 100),
        "pageIndex": page_index,
        "sortKey": sort_key,
        "sortOrder": sort_order
    }
    if campaign_id:
        params["campaignId"] = campaign_id
    if status:
        params["status"] = status

    try:
        data = await merchantspring_client.get("/advertising/adGroups", params=params)
        return json.dumps({"store_name": resolved_name, "result": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_ms_keywords(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    ad_group_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    status: Optional[str] = None,
    currency: str = "USD",
    page_size: int = 50,
    page_index: int = 0,
    sort_key: str = "cost",
    sort_order: str = "desc"
) -> str:
    """
    Get keyword metrics and performance from MerchantSpring via GET /advertising/keywords.
    Returns keyword text, match type, spend, sales, impressions, clicks, CTR, ROAS.
    """
    sort_key = _sanitize_ms_sort_key(sort_key)
    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    f_epoch, t_epoch, _, _ = _resolve_sqp_date_epochs(from_date, to_date)
    params: Dict[str, Any] = {
        "channelId": cid,
        "fromDate": str(f_epoch),
        "toDate": str(t_epoch),
        "currency": currency,
        "pageSize": min(page_size, 100),
        "pageIndex": page_index,
        "sortKey": sort_key,
        "sortOrder": sort_order
    }
    if campaign_id:
        params["campaignId"] = campaign_id
    if ad_group_id:
        params["adGroupId"] = ad_group_id
    if status:
        params["status"] = status

    try:
        data = await merchantspring_client.get("/advertising/keywords", params=params)
        return json.dumps({"store_name": resolved_name, "result": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_ms_products(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    product_sku: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    currency: str = "USD",
    page_size: int = 50,
    page_index: int = 0,
    sort_key: str = "cost",
    sort_order: str = "desc"
) -> str:
    """
    Get product advertising metrics from MerchantSpring via GET /advertising/products.
    Returns spend, sales, and conversions broken down by advertised product/SKU.
    """
    sort_key = _sanitize_ms_sort_key(sort_key)
    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    f_epoch, t_epoch, _, _ = _resolve_sqp_date_epochs(from_date, to_date)
    params: Dict[str, Any] = {
        "channelId": cid,
        "fromDate": str(f_epoch),
        "toDate": str(t_epoch),
        "currency": currency,
        "pageSize": min(page_size, 100),
        "pageIndex": page_index,
        "sortKey": sort_key,
        "sortOrder": sort_order
    }
    if product_sku:
        params["productSku"] = product_sku

    try:
        data = await merchantspring_client.get("/advertising/products", params=params)
        return json.dumps({"store_name": resolved_name, "result": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ==============================================================================
# Domain 4: Profitability Tools
# ==============================================================================

async def get_store_profit_and_loss(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    profitability_view: str = "settled",
    include_tax: bool = False,
    include_cogs_refunds: bool = False,
    timezone: Optional[str] = None
) -> str:
    """
    Retrieve Store-level Profit and Loss (P&L) via POST /profitability/storeProfitAndLoss.
    READ-ONLY: Calculates revenue, gross profit, net margin, marketplace fees, COGS, and ad spend.
    profitability_view: 'settled' (cash basis) or 'accrual'.
    """
    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name, merchant_id=merchant_id
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid or not mid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    f_epoch, t_epoch, _, _ = _resolve_sqp_date_epochs(from_date, to_date)
    body: Dict[str, Any] = {
        "channelId": cid,
        "merchantId": mid,
        "reportOptions": {
            "fromDate": f_epoch,
            "toDate": t_epoch,
            "timezone": timezone or get_ms_setting("DEFAULT_TIMEZONE", "UTC"),
            "includeTax": include_tax,
            "profitabilityView": profitability_view,
            "includeCogsRefunds": include_cogs_refunds
        }
    }

    try:
        data = await merchantspring_client.post("/profitability/storeProfitAndLoss", json_body=body)
        return json.dumps({"store_name": resolved_name, "data": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_product_profit_and_loss(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    seller_skus: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    profitability_view: str = "settled",
    include_tax: bool = False,
    include_cogs_refunds: bool = False,
    page_size: int = 25,
    page_index: int = 0
) -> str:
    """
    Retrieve Product-level Profit and Loss (P&L) via POST /profitability/productProfitAndLoss.
    READ-ONLY: Calculates item revenue, gross profit, product margins, and unit COGS.
    Pass seller_skus as comma-separated list (up to 50 SKUs) or leave empty for all products.
    """
    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name, merchant_id=merchant_id
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid or not mid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    f_epoch, t_epoch, _, _ = _resolve_sqp_date_epochs(from_date, to_date)
    report_options: Dict[str, Any] = {
        "fromDate": f_epoch,
        "toDate": t_epoch,
        "includeTax": include_tax,
        "profitabilityView": profitability_view,
        "includeCogsRefunds": include_cogs_refunds,
        "pageSize": min(page_size, 50),
        "pageIndex": page_index
    }
    if seller_skus:
        skus_list = [s.strip() for s in seller_skus.split(",") if s.strip()]
        if skus_list:
            report_options["sellerSkus"] = skus_list[:50]

    body: Dict[str, Any] = {
        "channelId": cid,
        "merchantId": mid,
        "reportOptions": report_options
    }

    try:
        data = await merchantspring_client.post("/profitability/productProfitAndLoss", json_body=body)
        return json.dumps({"store_name": resolved_name, "data": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ==============================================================================
# Domain 5: Reports Tools
# ==============================================================================

async def get_sales_by_period(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prior_from_date: Optional[str] = None,
    prior_to_date: Optional[str] = None,
    timezone: Optional[str] = None,
    include_tax: bool = False,
    page_size: int = 50,
    page_index: int = 0
) -> str:
    """
    Get aggregated sales over time (daily, weekly, monthly) via POST /reports/view/salesByPeriod.
    READ-ONLY: Compares current period performance against prior period.
    """
    try:
        cid, _, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    f_epoch, t_epoch, pf_epoch, pt_epoch = _resolve_sqp_date_epochs(
        from_date, to_date, prior_from_date, prior_to_date
    )

    body: Dict[str, Any] = {
        "fromDate": f_epoch,
        "toDate": t_epoch,
        "priorFromDate": pf_epoch,
        "priorToDate": pt_epoch,
        "timezone": timezone or get_ms_setting("DEFAULT_TIMEZONE", "UTC"),
        "includeTax": include_tax,
        "pageSize": min(page_size, 100),
        "pageIndex": page_index
    }
    if cid:
        body["filter"] = {"channels": [cid]}

    try:
        data = await merchantspring_client.post("/reports/view/salesByPeriod", json_body=body)
        return json.dumps({"store_name": resolved_name, "data": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_sales_by_product(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prior_from_date: Optional[str] = None,
    prior_to_date: Optional[str] = None,
    timezone: Optional[str] = None,
    search_text: Optional[str] = None,
    include_no_sales: bool = False,
    include_tax: bool = False,
    page_size: int = 50,
    page_index: int = 0,
    sort_key: str = "totalSales",
    sort_order: str = "desc"
) -> str:
    """
    Get sales metrics broken down by individual product/SKU via POST /reports/view/salesByProduct.
    READ-ONLY: Compares product units, sales, and order counts against prior period.
    """
    try:
        cid, _, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    f_epoch, t_epoch, pf_epoch, pt_epoch = _resolve_sqp_date_epochs(
        from_date, to_date, prior_from_date, prior_to_date
    )

    body: Dict[str, Any] = {
        "fromDate": f_epoch,
        "toDate": t_epoch,
        "priorFromDate": pf_epoch,
        "priorToDate": pt_epoch,
        "timezone": timezone or get_ms_setting("DEFAULT_TIMEZONE", "UTC"),
        "includeTax": include_tax,
        "includeNoSales": include_no_sales,
        "pageSize": min(page_size, 100),
        "pageIndex": page_index,
        "sortKey": sort_key,
        "sortOrder": sort_order
    }
    if search_text:
        body["searchText"] = search_text.strip()
    if cid:
        body["filter"] = {"channels": [cid]}

    try:
        data = await merchantspring_client.post("/reports/view/salesByProduct", json_body=body)
        return json.dumps({"store_name": resolved_name, "data": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_sales_by_channel(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prior_from_date: Optional[str] = None,
    prior_to_date: Optional[str] = None,
    marketplace: Optional[str] = None,
    country: Optional[str] = None,
    timezone: Optional[str] = None,
    include_tax: bool = False,
    page_size: int = 50,
    page_index: int = 0
) -> str:
    """
    Get cross-channel sales overview via POST /reports/view/salesByChannel.
    READ-ONLY: Compares sales, orders, and units across connected marketplaces and stores.
    """
    f_epoch, t_epoch, pf_epoch, pt_epoch = _resolve_sqp_date_epochs(
        from_date, to_date, prior_from_date, prior_to_date
    )

    filter_dict = {}
    if marketplace:
        filter_dict["marketplaces"] = [m.strip().lower() for m in marketplace.split(",") if m.strip()]
    if country:
        filter_dict["countries"] = [c.strip().upper() for c in country.split(",") if c.strip()]

    body: Dict[str, Any] = {
        "fromDate": f_epoch,
        "toDate": t_epoch,
        "priorFromDate": pf_epoch,
        "priorToDate": pt_epoch,
        "timezone": timezone or get_ms_setting("DEFAULT_TIMEZONE", "UTC"),
        "includeTax": include_tax,
        "pageSize": min(page_size, 100),
        "pageIndex": page_index
    }
    if filter_dict:
        body["filter"] = filter_dict

    try:
        data = await merchantspring_client.post("/reports/view/salesByChannel", json_body=body)
        return json.dumps({"data": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_advertising_by_channels(
    currency: str = "USD",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    prior_from_date: Optional[str] = None,
    prior_to_date: Optional[str] = None,
    timezone: Optional[str] = None,
    page_size: int = 25,
    page_index: int = 0
) -> str:
    """
    Get cross-channel advertising performance via POST /reports/view/advertisingByChannels.
    READ-ONLY: Aggregates spend, sales, and ROAS across all connected advertising accounts.
    """
    f_epoch, t_epoch, pf_epoch, pt_epoch = _resolve_sqp_date_epochs(
        from_date, to_date, prior_from_date, prior_to_date
    )

    body: Dict[str, Any] = {
        "currentCurrency": currency,
        "reportOptions": {
            "fromDate": f_epoch,
            "toDate": t_epoch,
            "priorFromDate": pf_epoch,
            "priorToDate": pt_epoch,
            "timezone": timezone or get_ms_setting("DEFAULT_TIMEZONE", "UTC")
        },
        "pageSize": min(page_size, 100),
        "pageIndex": page_index
    }

    try:
        data = await merchantspring_client.post("/reports/view/advertisingByChannels", json_body=body)
        return json.dumps({"data": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ==============================================================================
# Domain 6: Orders & Catalog Tools
# ==============================================================================

async def find_asin_store(asin: str) -> str:
    """
    Identify which seller store/channel owns an Amazon ASIN across connected MerchantSpring channels.
    Use this whenever the user asks which store an ASIN belongs to, or asks about a product/ASIN without naming a store.
    Returns the store name, channel ID, brand, product title, SKU, and price.
    """
    clean_asin = (asin or "").strip().upper()
    if not clean_asin or len(clean_asin) != 10:
        return json.dumps({
            "error": "A valid 10-character Amazon ASIN is required (e.g. 'B075Q3L8JR').",
            "status_code": 400
        })

    try:
        channels = await merchantspring_client.post("/channels", json_body={})
    except Exception as e:
        return json.dumps({"error": f"Failed to retrieve channels: {e}", "status_code": 500})

    if not isinstance(channels, list):
        return json.dumps({"error": "No channels found.", "status_code": 404})

    amazon_channels = [c for c in channels if c.get("marketplace") == "amazon"]

    for c in amazon_channels:
        cid = str(c.get("channelId"))
        mid = str(c.get("merchantId"))
        dname = c.get("displayName")
        try:
            res = await merchantspring_client.get(
                "/products/amazon",
                params={"channelId": cid, "merchantId": mid, "asin": clean_asin}
            )
            if res and isinstance(res, dict) and "data" in res:
                prod_data = res.get("data", {}).get("data", {}) or res.get("data", {})
                summaries = prod_data.get("summaries", {}) or {}
                seller_listings = prod_data.get("sellerListings", []) or []
                if not seller_listings and not summaries.get("itemName"):
                    continue

                sku = seller_listings[0].get("sku") if seller_listings else None
                price = None
                if seller_listings and seller_listings[0].get("offers"):
                    price_info = seller_listings[0]["offers"][0].get("price", {})
                    if price_info.get("amount"):
                        price = f"${price_info.get('amount')} {price_info.get('currency', 'USD')}"

                return json.dumps({
                    "store_name": dname,
                    "channel_id": cid,
                    "asin": clean_asin,
                    "brand": summaries.get("brand"),
                    "product_title": summaries.get("itemName"),
                    "sku": sku,
                    "price": price,
                    "status": "found"
                }, indent=2)
        except Exception:
            continue

    return json.dumps({
        "error": f"ASIN '{clean_asin}' was not found in any connected MerchantSpring Amazon store.",
        "status_code": 404
    })

async def get_amazon_orders(
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    order_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    date_field: str = "createdAt",
    page_size: int = 50
) -> str:
    """
    Get Amazon orders from MerchantSpring via POST /orders/amazon.
    READ-ONLY: Retrieve order numbers, purchase dates, status, order total, and item details.
    """
    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name, merchant_id=merchant_id
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid or not mid:
        return json.dumps({
            "error": "Could not identify channelId and merchantId. Please specify a valid store_name or channel_id.",
            "status_code": 400
        })

    body: Dict[str, Any] = {
        "channelId": cid,
        "merchantId": mid,
        "pageSize": min(page_size, 100),
        "dateField": date_field
    }
    if order_id:
        body["orderId"] = order_id.strip()

    if from_date and to_date:
        f_epoch, t_epoch, _, _ = _resolve_sqp_date_epochs(from_date, to_date)
        body["fromDate"] = f_epoch
        body["toDate"] = t_epoch

    try:
        data = await merchantspring_client.post("/orders/amazon", json_body=body)
        return json.dumps({"store_name": resolved_name, "data": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})

async def get_amazon_products(
    asin: str,
    store_name: Optional[str] = None,
    channel_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    merchant_id: Optional[str] = None
) -> str:
    """
    Get Amazon product catalog details from MerchantSpring via GET /products/amazon.
    READ-ONLY: Returns ASIN, title, price, buy box status, and catalog attributes.
    An ASIN (10 characters, e.g. 'B0714GKJ12') and store_name/channel_id are required.
    """
    clean_asin = (asin or "").strip().upper()
    if not clean_asin or len(clean_asin) != 10:
        return json.dumps({
            "error": "A valid 10-character Amazon Standard Identification Number (ASIN, e.g. 'B0714GKJ12') is required to look up product attributes and Buy Box status.",
            "status_code": 400
        })

    try:
        cid, mid, resolved_name = await _resolve_merchantspring_channel(
            channel_id=channel_id, channel_name=channel_name or store_name, merchant_id=merchant_id
        )
    except AmbiguousChannelError as e:
        return json.dumps({
            "status": "ambiguous",
            "error": e.message,
            "candidates": e.candidates,
            "status_code": 400
        }, indent=2)

    if not cid or not mid:
        return json.dumps({
            "error": f"Store '{channel_name or store_name or ''}' was not found. Please specify which store you want to query (e.g. Herbalogic, Rolling Sands).",
            "status_code": 400
        })

    params: Dict[str, Any] = {
        "channelId": cid,
        "merchantId": mid,
        "asin": clean_asin
    }

    try:
        data = await merchantspring_client.get("/products/amazon", params=params)
        return json.dumps({"store_name": resolved_name, "data": data}, indent=2)
    except MerchantSpringAPIError as e:
        return json.dumps({"error": e.message, "status_code": e.status_code, "details": e.details})
    except Exception as e:
        return json.dumps({"error": str(e)})
