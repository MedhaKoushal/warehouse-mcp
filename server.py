from __future__ import annotations
import json
import os
import re
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

# Helper function to load configuration file (Piyush Sir's directive)
def load_config() -> dict:
    """Loads configuration from config.json with fallback to environment variables."""
    config_file_path = os.getenv("CONFIG_FILE", str(Path(__file__).parent / "config.json"))
    cfg = {}
    if os.path.exists(config_file_path):
        try:
            with open(config_file_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load config file at {config_file_path}: {e}")
    return cfg

_cfg = load_config()

def get_setting(key: str, default: any = None) -> any:
    """Get setting from environment variable first, then config.json, then default."""
    if key in os.environ:
        val = os.environ[key]
        if isinstance(default, int):
            return int(val)
        if isinstance(default, list):
            return [s.strip() for s in val.split(",") if s.strip()]
        return val
    return _cfg.get(key, default)

# Configuration settings
AWS_PROFILE  = get_setting("AWS_PROFILE")
REGION       = get_setting("AWS_REGION")
LOCAL_HOST   = get_setting("DB_LOCAL_HOST")
LOCAL_PORT   = get_setting("DB_LOCAL_PORT")
RDS_HOST     = get_setting("DB_RDS_HOST")
RDS_PORT     = get_setting("DB_RDS_PORT")
DB_NAME      = get_setting("DB_NAME")
DB_USER      = get_setting("DB_USER")
CA_BUNDLE    = get_setting("RDS_CA_BUNDLE")
ALLOWED_SCHEMAS_RAW = get_setting("ALLOWED_SCHEMAS")

if isinstance(ALLOWED_SCHEMAS_RAW, str):
    ALLOWED_SCHEMAS = tuple(s.strip() for s in ALLOWED_SCHEMAS_RAW.split(",") if s.strip())
else:
    ALLOWED_SCHEMAS = tuple(ALLOWED_SCHEMAS_RAW)

DB_PASSWORD  = get_setting("DB_PASSWORD")
MAX_ROWS     = get_setting("MAX_ROWS", 1000)
STATEMENT_MS = get_setting("STATEMENT_TIMEOUT_MS", 30000)

def _connect() -> psycopg.Connection:
    """Connects to PostgreSQL over SSM tunnel using DB_PASSWORD or fresh AWS IAM token."""
    if not RDS_HOST or not DB_NAME or not DB_USER:
        raise ValueError("Missing required database configuration (DB_RDS_HOST, DB_NAME, DB_USER) in config.json or environment variables.")
    if DB_PASSWORD:
        token = DB_PASSWORD
    else:
        # Setup AWS Boto3 Session using AWS_PROFILE if specified
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=REGION) if AWS_PROFILE else boto3.Session(region_name=REGION)
        rds_client = session.client("rds")

        # IAM auth tokens expire after 15 minutes; mint fresh per call
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
        connect_timeout=10,
    )
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
        cur.execute(f"SET statement_timeout = {STATEMENT_MS}")
        cur.execute("SET idle_in_transaction_session_timeout = 60000")
    conn.commit()
    return conn

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

    with _connect() as conn, conn.cursor() as cur:
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
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchmany(limit)
        truncated = cur.fetchone() is not None
        cols = [d.name for d in cur.description] if cur.description else []
    return {"columns": cols, "row_count": len(rows), "truncated": truncated, "rows": [dict(r) for r in rows]}

if __name__ == "__main__":
    mcp.run()
