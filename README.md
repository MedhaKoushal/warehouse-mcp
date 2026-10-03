# SellerOS Warehouse MCP Server (`warehouse-mcp`)

A read-only Model Context Protocol (MCP) server for querying the SellerOS PostgreSQL Data Warehouse safely using AWS IAM database authentication routed through an AWS SSM tunnel.

## Key Features
- **Config File Integration**: All connection parameters, AWS profiles, and limits are managed in `config.json` (as instructed by Piyush Sir).
- **Dynamic IAM Authentication**: Automatically generates fresh, temporary 15-minute AWS IAM database auth tokens using `boto3`.
- **SSM Tunnel Support**: Connects to the local SSM tunnel target (`127.0.0.1:55434`) while validating against the official RDS SSL hostname certificate.
- **Strict Read-Only Guardrails**: Blocks non-SELECT/WITH queries, write keywords, multi-statement injection, and enforces PostgreSQL read-only session transactions and statement timeouts.

---

## Architecture Overview

```
[ Antigravity IDE / MCP Client ]
               │
               ▼ (stdio)
  [ server.py (FastMCP) ] ◄── Read settings from config.json
               │
   1. Mint IAM Token via boto3 (profile: "selleros")
   2. Connect to 127.0.0.1:55434
               ▼
[ AWS SSM Tunnel (aws ssm start-session...) ]  <-- Must be running in background
               │
               ▼ (Port Forwarding via AWS VPC)
  [ AWS RDS PostgreSQL (dw, ops) ]
```

---

## Setup Guide

### Step 1: AWS CLI & SSM Plugin Setup
Ensure you have the AWS CLI and Session Manager Plugin installed:
1. **AWS CLI**: Download from [AWS CLI Windows Installer](https://awscli.amazonaws.com/AWSCLIV2.msi)
2. **SSM Plugin**: Install Session Manager plugin ([AWS SSM Plugin Guide](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html))
3. **AWS Profile Configuration**:
   ```bash
   aws configure --profile selleros
   ```
   (Provide Access Key, Secret Key, Region `us-east-1`, Output `json`)

4. **Verify Identity**:
   ```bash
   aws sts get-caller-identity --profile selleros
   ```

---

### Step 2: Open the SSM Tunnel
Before starting the MCP Server or connecting your AI client, open the SSM Tunnel in a terminal window and leave it running:

**Windows (PowerShell / CMD):**
```powershell
aws ssm start-session --target i-03680992802022b55 --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters "host=['selleros-warehouse.cvvtiac72c6q.us-east-1.rds.amazonaws.com'],portNumber=['5432'],localPortNumber=['55434']" --region us-east-1 --profile selleros
```

You will see: `Waiting for connections...`. Keep this terminal window open.

---

### Step 3: Virtual Environment & Dependencies
Create a virtual environment and install the required dependencies:

```bash
cd warehouse-mcp
python -m venv .venv

# On Windows:
.venv\Scripts\activate

# Install dependencies:
pip install -r requirements.txt
```

---

### Step 4: Config File (`config.json`)
Settings are configured in `warehouse-mcp/config.json`:

```json
{
  "AWS_PROFILE": "selleros",
  "AWS_REGION": "us-east-1",
  "DB_RDS_HOST": "selleros-warehouse.cvvtiac72c6q.us-east-1.rds.amazonaws.com",
  "DB_LOCAL_HOST": "127.0.0.1",
  "DB_LOCAL_PORT": 55434,
  "DB_RDS_PORT": 5432,
  "DB_NAME": "warehouse",
  "DB_USER": "mcp_reader",
  "RDS_CA_BUNDLE": null,
  "ALLOWED_SCHEMAS": ["dw", "ops"],
  "MAX_ROWS": 1000,
  "STATEMENT_TIMEOUT_MS": 30000
}
```

---

### Step 5: Connecting to Antigravity IDE / MCP Clients

#### Option A: Antigravity IDE (Recommended)
We have configured `.agents/mcp_config.json` directly in the project root!

Or, you can put it in your global config (`C:\Users\Admin\.gemini\config\mcp_config.json`):

```json
{
  "mcpServers": {
    "selleros-warehouse": {
      "command": "C:/Users/Admin/Documents/GitHub/SellerOS/warehouse-mcp/.venv/Scripts/python.exe",
      "args": ["C:/Users/Admin/Documents/GitHub/SellerOS/warehouse-mcp/server.py"]
    }
  }
}
```

#### Option B: Claude Desktop
Edit `%APPDATA%\Claude\claude_desktop_config.json` using the exact same JSON format as above.

*(Notice: Credentials & parameters do not need to be passed here because `server.py` reads `config.json` automatically!)*

---

## Exposed MCP Tools

1. `list_tables_and_schema(schema: str | None = None)`
   - Lists allowed tables (`dw`, `ops`) and their column names, data types, and nullability.

2. `run_metric_query(sql: str, max_rows: int = 200)`
   - Executes a single read-only analytical SQL query with safeguards against modification statements.

3. `apify_social_posts(platform: str, urls: list[str], per_source: int = 20)`
   - Scrapes public posts with engagement from Instagram, TikTok, Facebook, X (Twitter), or YouTube.

4. `apify_amazon_product(asins: list[str], max_age_hours: int = 24)`
   - Live Amazon product page via Apify: price, stock, star rating, review count, and bullet points.

5. `apify_amazon_offers(asins: list[str], max_offers: int = 10, max_age_hours: int = 6)`
   - Live Amazon offers via Apify: Buy Box seller, competing offers and stock.

6. `apify_amazon_search(terms: list[str], per_term: int = 30)`
   - Live Amazon search results with keyword ranking (organic and sponsored).

7. `apify_amazon_reviews(asins: list[str], per_asin: int = 100, max_age_hours: int = 168)`
   - Amazon reviews via Apify: review count, average rating, star breakdown, and review text.

8. `apify_web_research(mode: str, urls: list[str] | None = None, queries: list[str] | None = None)`
   - Web crawler, Google search, and RAG web browser via Apify.

9. `get_apify_results(run_ids: list[str])`
   - Checks status and retrieves results of scrapes that returned status `running`.

---

## Claude Scraper Chat (`claude_chat.py`)

You can chat with Claude directly in your terminal to scrape Instagram or Amazon pages:

```bash
# Automated verification test
python claude_chat.py --test

# Interactive chat loop
python claude_chat.py

# Single prompt execution
python claude_chat.py --prompt "Please scrape this Instagram page: https://www.instagram.com/nikestore"
```

Set `"APIFY_TOKEN"` and `"ANTHROPIC_API_KEY"` in `config.json` to enable live scrapes and Anthropic API inference.

