from __future__ import annotations
import atexit
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
import boto3
import psycopg
from psycopg.rows import dict_row

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp.server.mcpserver import MCPServer as FastMCP

# Initialize FastMCP / MCPServer
mcp = FastMCP("selleros-warehouse")

# Helper function to load configuration file
def load_config() -> dict:
    """Loads configuration with search hierarchy for frozen .exe and dev mode."""
    candidates = []
    if os.getenv("CONFIG_FILE"):
        candidates.append(Path(os.environ["CONFIG_FILE"]))

    # Next to current executable (when packaged into .exe)
    candidates.append(Path(sys.executable).parent / "config.json")

    # Next to this source file
    candidates.append(Path(__file__).parent / "config.json")

    # Inside PyInstaller bundle (_MEIPASS)
    if hasattr(sys, "_MEIPASS"):
        candidates.append(Path(sys._MEIPASS) / "config.json")

    # Fallback to current working directory
    candidates.append(Path.cwd() / "config.json")

    for p in candidates:
        if p.exists() and p.is_file():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                sys.stderr.write(f"Warning: Failed to load config at {p}: {e}\n")

    return {}

_cfg = load_config()

def get_setting(key: str, default: any = None) -> any:
    """Get setting from environment variable first, then config.json, then default."""
    if key in os.environ and os.environ[key].strip() != "":
        val = os.environ[key]
        if isinstance(default, int):
            return int(val)
        if isinstance(default, list):
            return [s.strip() for s in val.split(",") if s.strip()]
        if isinstance(default, bool):
            return val.lower() in ("true", "1", "yes")
        return val
    return _cfg.get(key, default)

# Configuration settings
AWS_PROFILE       = get_setting("AWS_PROFILE", "selleros")
REGION            = get_setting("AWS_REGION", "us-east-1")
SSM_TARGET        = get_setting("SSM_TARGET_INSTANCE_ID", "i-03680992802022b55")
AUTO_START_TUNNEL = get_setting("AUTO_START_TUNNEL", True)
LOCAL_HOST        = get_setting("DB_LOCAL_HOST", "127.0.0.1")
LOCAL_PORT        = get_setting("DB_LOCAL_PORT", 55434)
RDS_HOST          = get_setting("DB_RDS_HOST", "selleros-warehouse.cvvtiac72c6q.us-east-1.rds.amazonaws.com")
RDS_PORT          = get_setting("DB_RDS_PORT", 5432)
DB_NAME           = get_setting("DB_NAME", "warehouse")
DB_USER           = get_setting("DB_USER", "swdw")
DB_PASSWORD       = get_setting("DB_PASSWORD")
CA_BUNDLE         = get_setting("RDS_CA_BUNDLE")
ALLOWED_SCHEMAS_RAW = get_setting("ALLOWED_SCHEMAS", ["dw", "ops"])

if isinstance(ALLOWED_SCHEMAS_RAW, str):
    ALLOWED_SCHEMAS = tuple(s.strip() for s in ALLOWED_SCHEMAS_RAW.split(",") if s.strip())
else:
    ALLOWED_SCHEMAS = tuple(ALLOWED_SCHEMAS_RAW)

MAX_ROWS          = get_setting("MAX_ROWS", 1000)
STATEMENT_MS      = get_setting("STATEMENT_TIMEOUT_MS", 30000)

_tunnel_process: subprocess.Popen | None = None
_tunnel_log_file = None

def _get_tunnel_log_path() -> Path:
    log_dir = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))) / "SellerOS" / "WarehouseMCP"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "ssm_tunnel.log"

def _read_recent_tunnel_logs(max_lines: int = 15) -> str:
    log_path = _get_tunnel_log_path()
    if not log_path.exists():
        return ""
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            return "".join(lines[-max_lines:]).strip()
    except Exception:
        return ""

def _find_aws_cli_path() -> str | None:
    path = shutil.which("aws")
    if path:
        return path
    candidates = [
        r"C:\Program Files\Amazon\AWSCLIV2\aws.exe",
        r"C:\Program Files (x86)\Amazon\AWSCLIV2\aws.exe",
        str(Path.home() / "AppData" / "Local" / "Programs" / "Amazon" / "AWSCLIV2" / "aws.exe"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None

def _is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    """Checks whether the specified TCP host and port are accepting connections."""
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def _kill_stale_processes_on_port(port: int = LOCAL_PORT):
    """Kills any stale processes on the tunnel port or orphaned SSM tunnels."""
    if os.name == "nt":
        try:
            # Check for any process holding the local port
            res = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 f"Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess"],
                capture_output=True, text=True, timeout=5
            )
            for line in res.stdout.strip().splitlines():
                line = line.strip()
                if line and line.isdigit():
                    pid = int(line)
                    if pid != os.getpid():
                        subprocess.run(["taskkill", "/F", "/PID", str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

def _terminate_tunnel():
    """Terminates the background SSM tunnel if spawned by this process."""
    global _tunnel_process, _tunnel_log_file
    if _tunnel_process is not None:
        try:
            sys.stderr.write("[selleros-warehouse] Stopping background SSM tunnel...\n")
            sys.stderr.flush()
            _tunnel_process.terminate()
            _tunnel_process.wait(timeout=3)
        except Exception:
            try:
                _tunnel_process.kill()
            except Exception:
                pass
        _tunnel_process = None
    if _tunnel_log_file is not None:
        try:
            _tunnel_log_file.close()
        except Exception:
            pass
        _tunnel_log_file = None

atexit.register(_terminate_tunnel)

def _ensure_ssm_tunnel(max_attempts: int = 3, timeout_per_attempt: int = 25):
    """Checks if local SSM port is open; if not, automatically launches the AWS SSM tunnel with retry logic."""
    global _tunnel_process, _tunnel_log_file

    # Check if port is already listening
    if _is_port_open(LOCAL_HOST, LOCAL_PORT, timeout=0.5):
        return

    if not AUTO_START_TUNNEL:
        raise ConnectionError(
            f"Cannot connect to {LOCAL_HOST}:{LOCAL_PORT}. SSM Tunnel is not running and AUTO_START_TUNNEL is disabled."
        )

    if not SSM_TARGET:
        raise ValueError("SSM_TARGET_INSTANCE_ID not configured in config.json.")

    aws_cmd = _find_aws_cli_path()
    if not aws_cmd:
        raise FileNotFoundError(
            "AWS CLI (aws.exe) not found. Please ensure AWS CLI v2 is installed."
        )

    log_path = _get_tunnel_log_path()

    for attempt in range(1, max_attempts + 1):
        # Terminate any dead or partial tunnel from previous try
        _terminate_tunnel()

        # Re-check port in case it just became ready
        if _is_port_open(LOCAL_HOST, LOCAL_PORT, timeout=0.5):
            return

        if attempt > 1:
            _kill_stale_processes_on_port(LOCAL_PORT)
            time.sleep(1.0)

        sys.stderr.write(
            f"[selleros-warehouse] Starting SSM tunnel (attempt {attempt}/{max_attempts}) "
            f"to {SSM_TARGET} on port {LOCAL_PORT}...\n"
        )
        sys.stderr.flush()

        cmd = [
            aws_cmd,
            "ssm",
            "start-session",
            "--target", SSM_TARGET,
            "--document-name", "AWS-StartPortForwardingSessionToRemoteHost",
            "--parameters", f"host=['{RDS_HOST}'],portNumber=['{RDS_PORT}'],localPortNumber=['{LOCAL_PORT}']",
            "--region", REGION or "us-east-1",
        ]
        if AWS_PROFILE:
            cmd.extend(["--profile", AWS_PROFILE])

        creation_flags = 0
        if os.name == "nt":
            creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)

        try:
            _tunnel_log_file = open(log_path, "w", encoding="utf-8")
            _tunnel_process = subprocess.Popen(
                cmd,
                stdout=_tunnel_log_file,
                stderr=_tunnel_log_file,
                creationflags=creation_flags,
            )
        except Exception as e:
            if attempt == max_attempts:
                raise RuntimeError(f"Failed to start AWS SSM tunnel process: {e}")
            time.sleep(1.5)
            continue

        # Wait up to timeout_per_attempt for port to open
        start_time = time.time()
        while time.time() - start_time < timeout_per_attempt:
            if _is_port_open(LOCAL_HOST, LOCAL_PORT, timeout=0.5):
                sys.stderr.write(f"[selleros-warehouse] SSM tunnel connected successfully on attempt {attempt}.\n")
                sys.stderr.flush()
                return

            if _tunnel_process.poll() is not None:
                err_text = _read_recent_tunnel_logs(10)
                sys.stderr.write(
                    f"[selleros-warehouse] Attempt {attempt} failed: SSM process exited early "
                    f"(code {_tunnel_process.returncode}). Log: {err_text}\n"
                )
                sys.stderr.flush()
                break

            time.sleep(0.5)

        if attempt < max_attempts:
            sys.stderr.write(f"[selleros-warehouse] Port {LOCAL_PORT} not open yet. Retrying in 2 seconds...\n")
            sys.stderr.flush()
            time.sleep(2)

    _terminate_tunnel()
    recent_logs = _read_recent_tunnel_logs(15)
    log_detail = f"\nSSM Error Log:\n{recent_logs}\nLog File: {log_path}" if recent_logs else f"See {log_path}"
    raise TimeoutError(
        f"Timed out after {max_attempts} attempts waiting for SSM tunnel to open port {LOCAL_PORT} on {LOCAL_HOST}. {log_detail}"
    )

def _connect() -> psycopg.Connection:
    """Connects to PostgreSQL over SSM tunnel using DB_PASSWORD or fresh AWS IAM token."""
    if not RDS_HOST or not DB_NAME or not DB_USER:
        raise ValueError("Missing required database configuration (DB_RDS_HOST, DB_NAME, DB_USER) in config.json or environment variables.")

    # Ensure background SSM tunnel is running
    _ensure_ssm_tunnel()

    if DB_PASSWORD:
        token = DB_PASSWORD
    else:
        # Mint IAM auth token via boto3
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=REGION) if AWS_PROFILE else boto3.Session(region_name=REGION)
        rds_client = session.client("rds")
        token = rds_client.generate_db_auth_token(
            DBHostname=RDS_HOST, Port=RDS_PORT, DBUsername=DB_USER, Region=REGION
        )

    conn = psycopg.connect(
        host=RDS_HOST,          # Checked against SSL cert
        hostaddr=LOCAL_HOST,    # Actual network target (SSM Tunnel)
        port=LOCAL_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=token,
        sslmode="verify-full" if CA_BUNDLE else "require",
        sslrootcert=CA_BUNDLE,
        row_factory=dict_row,
        connect_timeout=15,
    )
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
        cur.execute(f"SET statement_timeout = {STATEMENT_MS}")
        cur.execute("SET idle_in_transaction_session_timeout = 60000")
    conn.commit()
    return conn

_shared_conn: psycopg.Connection | None = None

def _cleanup_idle_swdw_connections(conn: psycopg.Connection):
    """Terminates old idle connections owned by swdw to prevent connection exhaustion."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE usename = current_user
                  AND pid <> pg_backend_pid()
                  AND state = 'idle'
                  AND state_change < now() - interval '20 seconds';
            """)
        conn.commit()
    except Exception:
        pass

def _get_connection(max_retries: int = 4) -> psycopg.Connection:
    """Returns a single reusable, healthy database connection.
    Prevents role connection exhaustion and retries automatically if slots are full."""
    global _shared_conn
    _ensure_ssm_tunnel()

    if _shared_conn is not None:
        try:
            if not _shared_conn.closed:
                # Fast liveness probe
                with _shared_conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                return _shared_conn
        except Exception:
            try:
                _shared_conn.close()
            except Exception:
                pass
            _shared_conn = None

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            conn = _connect()
            _cleanup_idle_swdw_connections(conn)
            _shared_conn = conn
            return _shared_conn
        except psycopg.OperationalError as e:
            last_error = e
            err_str = str(e).lower()
            if "too many connections" in err_str or "closed the connection" in err_str:
                wait_time = 2 * attempt
                sys.stderr.write(
                    f"[selleros-warehouse] Connection limit reached for role '{DB_USER}'. "
                    f"Waiting {wait_time}s for slot to release (attempt {attempt}/{max_retries})...\n"
                )
                sys.stderr.flush()
                time.sleep(wait_time)
                continue
            raise

    raise last_error

def _close_shared_conn():
    global _shared_conn
    if _shared_conn is not None:
        try:
            _shared_conn.close()
        except Exception:
            pass
        _shared_conn = None

atexit.register(_close_shared_conn)

_WRITE = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|"
    r"vacuum|analyze|call|do|merge|reindex|lock|comment|refresh|set|reset|"
    r"begin|commit|rollback|savepoint|prepare|execute|listen|notify)\b",
    re.IGNORECASE,
)

def _guard(sql: str) -> str:
    """Validates that query is strictly read-only single statement SELECT/WITH."""
    q = sql.strip().rstrip(";").strip()
    if not q:
        raise ValueError("Empty query.")
    if ";" in q:
        raise ValueError("One statement only — remove internal semicolons.")
    if not re.match(r"^\s*(select|with)\b", q, re.IGNORECASE):
        raise ValueError("Only SELECT or WITH queries permitted.")
    if _WRITE.search(q):
        raise ValueError("Write/session keyword detected.")
    return q

@mcp.tool()
def list_tables_and_schema(schema: str | None = None) -> dict:
    """Inspect allowed schema tables and column signatures."""
    targets = [schema] if schema else list(ALLOWED_SCHEMAS)
    if any(s not in ALLOWED_SCHEMAS for s in targets):
        raise ValueError(f"Schema not allowed. Allowed: {list(ALLOWED_SCHEMAS)}")

    conn = _get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.is_nullable
            FROM information_schema.columns c
            JOIN information_schema.tables t 
              ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE c.table_schema = ANY(%s) AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """, (targets,))
        rows = cur.fetchall()

    tables: dict[str, dict] = {}
    for r in rows:
        key = f"{r['table_schema']}.{r['table_name']}"
        tables.setdefault(key, {"schema": r["table_schema"], "table": r["table_name"], "columns": []})
        tables[key]["columns"].append({"name": r["column_name"], "type": r["data_type"], "nullable": r["is_nullable"] == "YES"})
    return {"table_count": len(tables), "tables": list(tables.values())}

@mcp.tool()
def run_metric_query(sql: str, max_rows: int = 200) -> dict:
    """Execute a single read-only analytical SQL query."""
    q = _guard(sql)
    limit = max(1, min(max_rows, MAX_ROWS))
    conn = _get_connection()
    with conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchmany(limit)
        truncated = cur.fetchone() is not None
        cols = [d.name for d in cur.description] if cur.description else []
    return {"columns": cols, "row_count": len(rows), "truncated": truncated, "rows": [dict(r) for r in rows]}

# =============================================================== Apify Web & Social Scraping Tools

@mcp.tool()
def apify_amazon_product(asins: list[str], max_age_hours: int = 24, partner: str | None = None, domain: str | None = None) -> dict:
    """Live Amazon product page via Apify: price, stock, star rating, review count and bullet points for ASINs."""
    from apify_tools import handle_apify_tool
    ctx = {}
    if partner: ctx["partner"] = partner
    if domain: ctx["domain"] = domain
    return handle_apify_tool("apify_amazon_product", {"asins": asins, "max_age_hours": max_age_hours}, ctx)

@mcp.tool()
def apify_amazon_offers(asins: list[str], max_offers: int = 10, max_age_hours: int = 6, partner: str | None = None, domain: str | None = None) -> dict:
    """Live Amazon offers via Apify: Buy Box seller, competing offers and stock for ASINs."""
    from apify_tools import handle_apify_tool
    ctx = {}
    if partner: ctx["partner"] = partner
    if domain: ctx["domain"] = domain
    return handle_apify_tool("apify_amazon_offers", {"asins": asins, "max_offers": max_offers, "max_age_hours": max_age_hours}, ctx)

@mcp.tool()
def apify_amazon_search(terms: list[str], per_term: int = 30, partner: str | None = None, domain: str | None = None) -> dict:
    """Live Amazon search page via Apify: keywords results and organic/sponsored positions."""
    from apify_tools import handle_apify_tool
    ctx = {}
    if partner: ctx["partner"] = partner
    if domain: ctx["domain"] = domain
    return handle_apify_tool("apify_amazon_search", {"terms": terms, "per_term": per_term}, ctx)

@mcp.tool()
def apify_amazon_reviews(asins: list[str], per_asin: int = 100, max_age_hours: int = 168, partner: str | None = None, domain: str | None = None) -> dict:
    """Amazon review text via Apify: count, average rating, star breakdown and recent sample reviews for ASINs."""
    from apify_tools import handle_apify_tool
    ctx = {}
    if partner: ctx["partner"] = partner
    if domain: ctx["domain"] = domain
    return handle_apify_tool("apify_amazon_reviews", {"asins": asins, "per_asin": per_asin, "max_age_hours": max_age_hours}, ctx)

@mcp.tool()
def apify_social_posts(platform: str, urls: list[str], per_source: int = 20, partner: str | None = None) -> dict:
    """Public posts via Apify from Instagram, TikTok, Facebook, X (Twitter), or YouTube with engagement metrics. Pass full profile or post URLs."""
    from apify_tools import handle_apify_tool
    ctx = {}
    if partner: ctx["partner"] = partner
    return handle_apify_tool("apify_social_posts", {"platform": platform, "urls": urls, "per_source": per_source}, ctx)

@mcp.tool()
def apify_web_research(mode: str, urls: list[str] | None = None, queries: list[str] | None = None, max_pages: int = 20, partner: str | None = None) -> dict:
    """Web content via Apify. 'website' crawls a site (urls); 'google_search' returns Google search results (queries); 'rag_browser' searches and reads top pages."""
    from apify_tools import handle_apify_tool
    ctx = {}
    if partner: ctx["partner"] = partner
    payload = {"mode": mode, "max_pages": max_pages}
    if urls: payload["urls"] = urls
    if queries: payload["queries"] = queries
    return handle_apify_tool("apify_web_research", payload, ctx)

@mcp.tool()
def get_apify_results(run_ids: list[str], partner: str | None = None) -> dict:
    """Check status and retrieve results of earlier Apify scrapes that returned status 'running'."""
    from apify_tools import handle_apify_tool
    ctx = {}
    if partner: ctx["partner"] = partner
    return handle_apify_tool("get_apify_results", {"run_ids": run_ids}, ctx)

if __name__ == "__main__":
    if "--install" in sys.argv:
        from installer import run_installation
        run_installation()
        sys.exit(0)
    elif "--test" in sys.argv:
        print("[*] Testing SellerOS Warehouse MCP connection...")
        try:
            print("[*] Ensuring SSM tunnel...")
            _ensure_ssm_tunnel()
            print("[*] Connecting to PostgreSQL database...")
            with _connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT current_user, current_database(), version();")
                info = cur.fetchone()
                print(f"[+] Success! Connected as '{info['current_user']}' to DB '{info['current_database']}'.")
                print(f"[+] PostgreSQL Version: {info['version'][:55]}...")
            print("[*] Testing list_tables_and_schema tool...")
            res = list_tables_and_schema()
            print(f"[+] Found {res['table_count']} tables across allowed schemas: {list(ALLOWED_SCHEMAS)}")
            for t in res["tables"][:5]:
                print(f"    - {t['schema']}.{t['table']} ({len(t['columns'])} columns)")
            if len(res["tables"]) > 5:
                print(f"    ... and {len(res['tables']) - 5} more.")

            print("[*] Testing Apify tools configuration...")
            from apify_gateway import get_apify_token
            tok = get_apify_token()
            if tok:
                print(f"[+] Apify Token is configured ({tok[:4]}...{tok[-3:]})")
            else:
                print("[!] Apify Token: not set (set APIFY_TOKEN in config.json or env to run live scrapes)")
            print("[+] Registered Apify tools: apify_amazon_product, apify_amazon_offers, apify_amazon_search, apify_amazon_reviews, apify_social_posts, apify_web_research, get_apify_results")

            print("\n[+] ALL CHECKS PASSED! Ready for Claude Desktop & Antigravity IDE.")
        except Exception as e:
            print(f"[-] Connection test failed: {e}")
            sys.exit(1)
        sys.exit(0)
    elif "--server" in sys.argv or not sys.stdin.isatty():
        # Spawned by Claude Desktop, Antigravity IDE, or with --server flag
        mcp.run()
    else:
        # Double-clicked directly by user in Windows File Explorer
        from installer import run_installation
        run_installation()
        sys.exit(0)
