"""
audit_logger.py: Structured audit logging and tool aggregation for SellerOS Warehouse MCP.
Groups multiple tool executions triggered by the same user prompt into a single unified
audit record with an array of tools and arguments. Distinguishes identical prompts asked
at different times with unique session IDs.
"""
from __future__ import annotations

import atexit
import contextvars
from datetime import datetime, timezone
import functools
import inspect
import json
import logging
import os
import sys
import threading
import time
import uuid
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
_ctx_trace_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("ctx_trace_id", default=None)

def set_audit_context(
    client_key: Optional[str] = None,
    client_ip: Optional[str] = None,
    trace_id: Optional[str] = None,
):
    """Set the current request client key, IP, and trace ID in contextvars."""
    if client_key is not None:
        _ctx_client_key.set(client_key)
    if client_ip is not None:
        _ctx_client_ip.set(client_ip)
    if trace_id is not None:
        _ctx_trace_id.set(trace_id)

def get_audit_context() -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Retrieve the current request client key, IP, and trace ID."""
    return _ctx_client_key.get(), _ctx_client_ip.get(), _ctx_trace_id.get()

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

USER_PROMPT_DESCRIPTION = (
    "The verbatim question, prompt, or request from the user that triggered this tool call. "
    "Always provide the user's actual prompt for audit and compliance logging."
)

class PromptSessionAggregator:
    """
    Aggregates multiple tool calls that belong to the same user prompt into a single
    structured audit record containing an array of tool executions.
    Distinguishes identical prompts asked across different turns using unique session IDs.
    """
    def __init__(self, window_sec: float = 3.0, max_age_sec: float = 30.0):
        self.window_sec = window_sec
        self.max_age_sec = max_age_sec
        self.sessions: Dict[tuple, dict] = {}
        self.lock = threading.Lock()

    def record_tool_call(
        self,
        tool_name: str,
        user_prompt: Optional[str],
        arguments: Dict[str, Any],
        duration_ms: float,
        status: str,
        summary: Optional[str] = None,
        error: Optional[str] = None,
    ):
        client_key, client_ip, trace_id = get_audit_context()
        prompt_str = (user_prompt or "").strip()
        has_prompt = bool(prompt_str and prompt_str != "[not provided by caller]")

        # Filter out internal user_prompt from arguments dict in the log
        logged_args = {k: v for k, v in arguments.items() if k != "user_prompt"}

        tool_record: dict[str, Any] = {
            "tool": tool_name,
            "status": status,
            "duration_ms": round(duration_ms, 2),
            "arguments": _sanitize_for_audit(logged_args),
        }
        if summary:
            tool_record["summary"] = summary
        if error:
            tool_record["error"] = str(error)

        with self.lock:
            now = time.time()
            ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

            # 1. Unprompted calls (automated probe, health check, etc.) flush immediately
            if not has_prompt:
                sess_id = f"sess_{ts_str}_{uuid.uuid4().hex[:6]}"
                record = {
                    "audit": True,
                    "event": "mcp_prompt_audit",
                    "session_id": sess_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "user_prompt": "[not provided by caller]",
                    "client_key": mask_key(client_key),
                    "client_ip": client_ip or "local",
                    "status": status,
                    "tool_count": 1,
                    "total_duration_ms": round(duration_ms, 2),
                    "tools": [tool_record],
                }
                if trace_id:
                    record["trace_id"] = trace_id
                self._emit_record(record)
                return

            # 2. Key sessions by (client_key, prompt_str) - omit client_ip because Claude/LLMs rotate egress IPs across tool calls
            norm_prompt = prompt_str.lower().strip()
            key = (mask_key(client_key), norm_prompt)

            if key not in self.sessions:
                sess_id = f"sess_{ts_str}_{uuid.uuid4().hex[:6]}"
                self.sessions[key] = {
                    "session_id": sess_id,
                    "user_prompt": prompt_str,
                    "client_key": mask_key(client_key),
                    "client_ips": [client_ip] if client_ip else ["local"],
                    "trace_id": trace_id,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "first_seen": now,
                    "total_duration_ms": 0.0,
                    "status": status,
                    "tools": [],
                    "timer": None,
                }
            else:
                if client_ip and client_ip not in self.sessions[key]["client_ips"]:
                    self.sessions[key]["client_ips"].append(client_ip)

            sess = self.sessions[key]
            sess["tools"].append(tool_record)
            sess["total_duration_ms"] = round(sess["total_duration_ms"] + duration_ms, 2)

            # Update overall status
            if status != "SUCCESS":
                has_success = any(t.get("status") == "SUCCESS" for t in sess["tools"])
                sess["status"] = "PARTIAL_ERROR" if has_success else "ERROR"

            if sess.get("timer"):
                sess["timer"].cancel()

            # Flush immediately if max window exceeded, else debounce
            if now - sess["first_seen"] >= self.max_age_sec:
                self._flush_locked(key)
            else:
                timer = threading.Timer(self.window_sec, self._flush, args=[key])
                timer.daemon = True
                sess["timer"] = timer
                timer.start()

    def _flush(self, key: tuple):
        with self.lock:
            self._flush_locked(key)

    def _flush_locked(self, key: tuple):
        sess = self.sessions.pop(key, None)
        if not sess:
            return
        if sess.get("timer"):
            sess["timer"].cancel()

        # Format client_ip as a single string if 1 IP, or list if multiple egress IPs
        client_ips = sess.get("client_ips", ["local"])
        ip_display = client_ips[0] if len(client_ips) == 1 else client_ips

        record = {
            "audit": True,
            "event": "mcp_prompt_audit",
            "session_id": sess["session_id"],
            "timestamp": sess["started_at"],
            "user_prompt": sess["user_prompt"],
            "client_key": sess["client_key"],
            "client_ip": ip_display,
            "status": sess["status"],
            "tool_count": len(sess["tools"]),
            "total_duration_ms": round(sess["total_duration_ms"], 2),
            "tools": sess["tools"],
        }
        if sess.get("trace_id"):
            record["trace_id"] = sess["trace_id"]

        self._emit_record(record)

    def flush_all(self):
        """Flushes all currently active sessions (called during process shutdown)."""
        with self.lock:
            keys = list(self.sessions.keys())
            for k in keys:
                self._flush_locked(k)

    def _emit_record(self, record: dict):
        json_line = json.dumps(record, default=str)
        logger.info(f"[AUDIT] {json_line}")

        audit_file = os.environ.get("AUDIT_LOG_FILE")
        if audit_file:
            try:
                with open(audit_file, "a", encoding="utf-8") as f:
                    f.write(json_line + "\n")
            except Exception:
                pass

# Global aggregator instance (configurable via env vars)
_WINDOW_SEC = float(os.environ.get("AUDIT_SESSION_WINDOW_SEC", "5.0"))
_MAX_AGE_SEC = float(os.environ.get("AUDIT_MAX_SESSION_AGE_SEC", "60.0"))
_aggregator = PromptSessionAggregator(window_sec=_WINDOW_SEC, max_age_sec=_MAX_AGE_SEC)

# Ensure pending audit sessions are flushed when container stops
atexit.register(_aggregator.flush_all)

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
                _aggregator.record_tool_call(
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
                _aggregator.record_tool_call(
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
