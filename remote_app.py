"""
remote_app.py: ASGI application exposing SellerOS Warehouse MCP via Streamable HTTP for AWS ECS.

Endpoints:
  - GET /health: Unauthenticated health check for AWS Application Load Balancer Target Group.
  - GET /: Service info and available endpoints.
  - GET/POST /mcp: Streamable HTTP endpoint for MCP clients (Claude, Cursor, Antigravity).

Authentication:
  - Header: Authorization: Bearer <key>
  - Header: X-API-Key: <key>
  - Query parameter: ?key=<key> (Required for Claude Web / Desktop "Add custom connector" modal)
"""
from __future__ import annotations

import hmac
import logging
import os
import sys

# Ensure remote mode is default when loaded as ASGI app
os.environ.setdefault("DEPLOY_MODE", "remote")
os.environ.setdefault("AUTO_START_TUNNEL", "false")

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from server import mcp, get_allowed_api_keys

log = logging.getLogger("selleros-warehouse.remote")

class ApiKeyAuthMiddleware(BaseHTTPMiddleware):
    """Timing-safe API key authentication middleware for remote MCP requests."""
    async def dispatch(self, request: Request, call_next):
        # 1. Allow ALB health checks and root ping without authentication
        if request.url.path in ("/health", "/healthz", "/ping", "/"):
            return await call_next(request)

        allowed_keys = get_allowed_api_keys()

        # 2. If allowed keys are configured, enforce authentication
        if allowed_keys:
            key = None

            # Check Authorization: Bearer header
            auth_header = request.headers.get("Authorization")
            if auth_header:
                if auth_header.lower().startswith("bearer "):
                    key = auth_header[7:].strip()
                else:
                    key = auth_header.strip()
            # Check X-API-Key header
            elif "X-API-Key" in request.headers:
                key = request.headers["X-API-Key"].strip()
            # Check URL query parameters (?key=... or ?token=...)
            elif "key" in request.query_params:
                key = request.query_params["key"].strip()
            elif "token" in request.query_params:
                key = request.query_params["token"].strip()

            # Timing-safe validation
            if not key or not any(hmac.compare_digest(key, ak) for ak in allowed_keys):
                return JSONResponse(
                    {
                        "error": "Unauthorized",
                        "message": "API key is missing or invalid. Provide via 'Authorization: Bearer <key>' header or '?key=<key>' parameter."
                    },
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"}
                )

        return await call_next(request)

# Configure FastMCP transport security for custom ALB domain
try:
    from mcp.server.transport_security import TransportSecurityMiddleware, TransportSecuritySettings

    # Allow custom ALB host header by overriding transport security validation
    async def _noop_validate(self, request, is_post=False):
        return None

    TransportSecurityMiddleware.validate_request = _noop_validate
    mcp.settings.transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
        allowed_hosts=["*", "mcp.app.simpliworks.io", "mcp.app.simpliworks.io:*"],
        allowed_origins=["*"]
    )
except Exception as e:
    log.warning(f"Could not configure TransportSecuritySettings: {e}")

# ALB health check route
@mcp.custom_route("/health", methods=["GET"])
async def health_endpoint(request: Request):
    return JSONResponse({
        "status": "healthy",
        "service": "selleros-warehouse-mcp",
        "transport": "streamable-http"
    })

# Root ping endpoint
@mcp.custom_route("/", methods=["GET"])
async def root_endpoint(request: Request):
    return JSONResponse({
        "service": "selleros-warehouse-mcp",
        "mcp_endpoint": "/mcp",
        "health_endpoint": "/health"
    })

# Initialize Streamable HTTP Starlette application
app = mcp.streamable_http_app()
app.add_middleware(ApiKeyAuthMiddleware)
