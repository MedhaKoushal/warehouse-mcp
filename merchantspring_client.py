"""
merchantspring_client.py: HTTP Client for MerchantSpring API.
Supports configuration via Secrets Manager, environment variables, or config.json.
"""
import logging
import os
import httpx
from typing import Any, Dict, Optional

logger = logging.getLogger("selleros_mcp.merchantspring_client")

# Defaults
DEFAULT_BASE_URL = "https://mm-api.merchantspring.io"
DEFAULT_TIMEOUT = 30.0

_api_key: Optional[str] = None
_base_url: Optional[str] = None

def get_ms_setting(key: str, default: Any = None) -> Any:
    """Helper to retrieve configuration value from env or server._cfg."""
    if key in os.environ and os.environ[key].strip() != "":
        return os.environ[key].strip()
    try:
        from server import get_setting
        return get_setting(key, default)
    except Exception:
        return default

class MerchantSpringAPIError(Exception):
    def __init__(self, status_code: int, message: str, details: Optional[Any] = None):
        self.status_code = status_code
        self.message = message
        self.details = details
        super().__init__(f"MerchantSpring API Error ({status_code}): {message}")

class MerchantSpringClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        self._custom_base_url = base_url
        self._custom_api_key = api_key

    @property
    def base_url(self) -> str:
        if self._custom_base_url:
            return self._custom_base_url.rstrip("/")
        val = get_ms_setting("MERCHANTSPRING_API_BASE_URL", DEFAULT_BASE_URL)
        return str(val).rstrip("/")

    @property
    def api_key(self) -> str:
        if self._custom_api_key:
            return self._custom_api_key
        # Check MERCHANTSPRING_API_KEY first, fallback to hardcoded default if needed
        val = get_ms_setting("MERCHANTSPRING_API_KEY", "")
        return str(val).strip()

    def _get_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "SellerOS-Warehouse-MCP/1.0"
        }
        key = self.api_key
        if key:
            headers["x-api-key"] = key
        return headers

    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{endpoint}" if endpoint.startswith("/") else f"{self.base_url}/{endpoint}"
        headers = self._get_headers()

        cleaned_params = {}
        if params:
            for k, v in params.items():
                if v is not None:
                    cleaned_params[k] = v

        timeout_sec = float(get_ms_setting("API_TIMEOUT_SECONDS", DEFAULT_TIMEOUT))
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            try:
                response = await client.get(url, headers=headers, params=cleaned_params)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (401, 403):
                    error_detail = response.json() if response.content else ""
                    logger.error(f"MerchantSpring Auth/Permission failed ({response.status_code}) for endpoint {endpoint}: {error_detail}")
                    raise MerchantSpringAPIError(
                        status_code=response.status_code,
                        message="Authentication or permission error from MerchantSpring API. Check API key and permissions.",
                        details=error_detail
                    )
                elif response.status_code == 400:
                    error_detail = response.json() if response.content else "Bad Request"
                    logger.error(f"MerchantSpring Bad Request for endpoint {endpoint}: {error_detail}")
                    raise MerchantSpringAPIError(
                        status_code=400,
                        message="Invalid parameters sent to MerchantSpring API",
                        details=error_detail
                    )
                elif response.status_code == 422:
                    error_detail = response.json() if response.content else "Validation Error"
                    logger.error(f"Validation error for MerchantSpring endpoint {endpoint}: {error_detail}")
                    raise MerchantSpringAPIError(
                        status_code=422,
                        message="Invalid parameters sent to MerchantSpring API",
                        details=error_detail
                    )
                else:
                    logger.error(f"MerchantSpring API Error {response.status_code} for endpoint {endpoint}")
                    raise MerchantSpringAPIError(
                        status_code=response.status_code,
                        message=f"MerchantSpring API returned status code {response.status_code}"
                    )
            except httpx.RequestError as e:
                logger.error(f"HTTP Request failed for MerchantSpring endpoint {endpoint}: {str(e)}")
                raise MerchantSpringAPIError(
                    status_code=500,
                    message=f"Unable to reach MerchantSpring API: {type(e).__name__}"
                )

    async def post(self, endpoint: str, json_body: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{endpoint}" if endpoint.startswith("/") else f"{self.base_url}/{endpoint}"
        headers = self._get_headers()
        
        cleaned_body = {}
        if json_body:
            for k, v in json_body.items():
                if v is not None:
                    cleaned_body[k] = v

        timeout_sec = float(get_ms_setting("API_TIMEOUT_SECONDS", DEFAULT_TIMEOUT))
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            try:
                response = await client.post(url, headers=headers, json=cleaned_body)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (401, 403):
                    logger.error(f"MerchantSpring Authentication failed ({response.status_code}) for endpoint {endpoint}")
                    raise MerchantSpringAPIError(
                        status_code=response.status_code,
                        message="Authentication failed. Please check MERCHANTSPRING_API_KEY in configuration."
                    )
                elif response.status_code == 422:
                    error_detail = response.json() if response.content else "Validation Error"
                    logger.error(f"Validation error for MerchantSpring endpoint {endpoint}: {error_detail}")
                    raise MerchantSpringAPIError(
                        status_code=422,
                        message="Invalid parameters sent to MerchantSpring API",
                        details=error_detail
                    )
                else:
                    logger.error(f"MerchantSpring API Error {response.status_code} for endpoint {endpoint}")
                    raise MerchantSpringAPIError(
                        status_code=response.status_code,
                        message=f"MerchantSpring API returned status code {response.status_code}"
                    )
            except httpx.RequestError as e:
                logger.error(f"HTTP Request failed for MerchantSpring endpoint {endpoint}: {str(e)}")
                raise MerchantSpringAPIError(
                    status_code=500,
                    message=f"Unable to reach MerchantSpring API: {type(e).__name__}"
                )

merchantspring_client = MerchantSpringClient()
