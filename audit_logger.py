"""
audit_logger.py: Structured audit logging for SellerOS Warehouse MCP.
Captures user prompts, tool arguments, caller identity (API key / IP), execution latency, and outcomes.
Logs in single-line JSON format to sys.stderr for AWS CloudWatch and local compatibility.
"""
from __future__ import annotations

import asyncio
import contextvars
from datetime import datetime, timezone
import functools
import inspect
import json
import logging
import os
import sys
import time
import types
from typing import Annotated, Any, Callable, Dict, List, Literal, Optional, Set, Tuple, Union
from pydantic import Field

# Configure dedicated audit logger writing to sys.stderr (never sys.stdout, to protect stdio JSON-RPC)
logger = logging.getLogger("mcp.audit")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

# Context variables for caller identity (set by HTTP middleware or local caller)
_ctx_client_key: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("ctx_client_key", default=None)
_ctx_client_ip: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("ctx_client_ip", default=None)

def set_audit_context(client_key: Optional[str] = None, client_ip: Optional[str] = None):
    """Set the current request client key and IP in contextvars."""
    if client_key is not None:
        _ctx_client_key.set(client_key)
    if client_ip is not None:
        _ctx_client_ip.set(client_ip)

def get_audit_context() -> tuple[Optional[str], Optional[str]]:
    """Retrieve the current request client key and IP."""
    return _ctx_client_key.get(), _ctx_client_ip.get()

def mask_key(key: Optional[str]) -> str:
    """Mask sensitive API keys for safe audit logging."""
    if not key:
        return "anonymous"
    s = str(key).strip()
    if len(s) <= 8:
        return "****"
    return f"{s[:4]}...{s[-4:]}"

def _sanitize_for_audit(val: Any, max_len: int = 1500) -> Any:
    """Truncate or sanitize arguments to avoid log bloat and leak of secrets."""
    if isinstance(val, (int, float, bool)) or val is None:
        return val
    if isinstance(val, str):
        if len(val) > max_len:
            return val[:max_len] + f"... [truncated {len(val) - max_len} chars]"
        return val
    if isinstance(val, dict):
        sanitized = {}
        for k, v in val.items():
            if any(secret_term in str(k).lower() for secret_term in ("password", "secret", "token", "auth")):
                sanitized[k] = "******"
            else:
                sanitized[k] = _sanitize_for_audit(v, max_len=max_len)
        return sanitized
    if isinstance(val, (list, tuple)):
        if len(val) > 50:
            return [_sanitize_for_audit(x, max_len=max_len) for x in val[:50]] + [f"... [truncated {len(val)-50} items]"]
        return [_sanitize_for_audit(x, max_len=max_len) for x in val]
    return str(val)

def log_audit_event(
    tool_name: str,
    user_prompt: Optional[str],
    arguments: Dict[str, Any],
    duration_ms: float,
    status: str,
    summary: Optional[str] = None,
    error: Optional[str] = None,
):
    """Emits a structured JSON audit event to CloudWatch (sys.stderr)."""
    client_key, client_ip = get_audit_context()

    # Filter out internal user_prompt from arguments dict in the log for cleaner presentation
    logged_args = {k: v for k, v in arguments.items() if k != "user_prompt"}

    record = {
        "audit": True,
        "event": "mcp_tool_invocation",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool": tool_name,
        "user_prompt": user_prompt if (user_prompt and str(user_prompt).strip()) else "[not provided by caller]",
        "client_key": mask_key(client_key),
        "client_ip": client_ip or "local",
        "duration_ms": round(duration_ms, 2),
        "status": status,
        "arguments": _sanitize_for_audit(logged_args),
    }

    if summary:
        record["summary"] = summary
    if error:
        record["error"] = str(error)

    # Output as single-line JSON for easy AWS CloudWatch Log Insights querying
    json_line = json.dumps(record, default=str)
    logger.info(f"[AUDIT] {json_line}")

    # Optional file logging if AUDIT_LOG_FILE is specified
    audit_file = os.environ.get("AUDIT_LOG_FILE")
    if audit_file:
        try:
            with open(audit_file, "a", encoding="utf-8") as f:
                f.write(json_line + "\n")
        except Exception:
            pass

USER_PROMPT_DESCRIPTION = (
    "The verbatim question, prompt, or request from the user that triggered this tool call. "
    "Always provide the user's actual prompt for audit and compliance logging."
)

def audit_tool(fn: Callable) -> Callable:
    """
    Decorator that injects 'user_prompt' into the tool's signature/schema and logs execution.
    Works for both synchronous and asynchronous FastMCP tools.
    """
    try:
        sig = inspect.signature(fn, eval_str=True)
    except (TypeError, NameError):
        sig = inspect.signature(fn)
    had_user_prompt = "user_prompt" in sig.parameters

    if not had_user_prompt:
        param = inspect.Parameter(
            "user_prompt",
            inspect.Parameter.KEYWORD_ONLY,
            default="",
            annotation=Annotated[str, Field(default="", description=USER_PROMPT_DESCRIPTION)]
        )
        new_params = list(sig.parameters.values()) + [param]
        new_sig = sig.replace(parameters=new_params)
    else:
        new_sig = sig

    if inspect.iscoroutinefunction(fn):
        async def async_wrapper(*args, **kwargs):
            t0 = time.time()
            prompt = kwargs.get("user_prompt", "")
            if not had_user_prompt:
                call_kwargs = {k: v for k, v in kwargs.items() if k != "user_prompt"}
            else:
                call_kwargs = kwargs

            tool_name = fn.__name__
            status = "SUCCESS"
            error_str = None
            summary = None

            try:
                res = await fn(*args, **call_kwargs)
                if isinstance(res, dict):
                    if "error" in res:
                        status = "ERROR"
                        error_str = str(res["error"])
                    elif "row_count" in res:
                        summary = f"{res['row_count']} rows"
                    elif "table_count" in res:
                        summary = f"{res['table_count']} tables"
                elif isinstance(res, list):
                    summary = f"{len(res)} items"
                return res
            except Exception as e:
                status = "ERROR"
                error_str = f"{type(e).__name__}: {str(e)}"
                raise
            finally:
                duration_ms = (time.time() - t0) * 1000
                log_audit_event(
                    tool_name=tool_name,
                    user_prompt=prompt,
                    arguments=kwargs,
                    duration_ms=duration_ms,
                    status=status,
                    summary=summary,
                    error=error_str,
                )
        w = functools.update_wrapper(async_wrapper, fn)
        w.__signature__ = new_sig
        return w
    else:
        def sync_wrapper(*args, **kwargs):
            t0 = time.time()
            prompt = kwargs.get("user_prompt", "")
            if not had_user_prompt:
                call_kwargs = {k: v for k, v in kwargs.items() if k != "user_prompt"}
            else:
                call_kwargs = kwargs

            tool_name = fn.__name__
            status = "SUCCESS"
            error_str = None
            summary = None

            try:
                res = fn(*args, **call_kwargs)
                if isinstance(res, dict):
                    if "error" in res:
                        status = "ERROR"
                        error_str = str(res["error"])
                    elif "row_count" in res:
                        summary = f"{res['row_count']} rows"
                    elif "table_count" in res:
                        summary = f"{res['table_count']} tables"
                elif isinstance(res, list):
                    summary = f"{len(res)} items"
                return res
            except Exception as e:
                status = "ERROR"
                error_str = f"{type(e).__name__}: {str(e)}"
                raise
            finally:
                duration_ms = (time.time() - t0) * 1000
                log_audit_event(
                    tool_name=tool_name,
                    user_prompt=prompt,
                    arguments=kwargs,
                    duration_ms=duration_ms,
                    status=status,
                    summary=summary,
                    error=error_str,
                )
        w = functools.update_wrapper(sync_wrapper, fn)
        w.__signature__ = new_sig
        return w
