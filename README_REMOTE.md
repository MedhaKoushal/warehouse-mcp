# Connecting to Remote SellerOS Warehouse MCP

The SellerOS Warehouse MCP server is deployed on AWS and accessible at:
```text
https://mcp.app.simpliworks.io/mcp
```

Team members no longer need to install Python, AWS CLI, AWS Session Manager, or run any `.exe` files locally.

---

## 1. Claude Web / Claude Desktop (Add Custom Connector)

1. Open **Claude** (web or desktop app).
2. Go to **Settings** → **Connectors** → **Add custom connector**.
3. Fill in:
   - **Name:** `SellerOS Warehouse`
   - **MCP server URL:**
     ```text
     https://mcp.app.simpliworks.io/mcp?key=YOUR_TEAM_KEY
     ```
4. Click **Continue**.

Claude will connect immediately and make all analytical and scraping tools available in your conversations.

---

## 2. Cursor (`mcp.json`)

Add this entry to your `mcp.json` (Settings → Features → MCP):

```json
{
  "mcpServers": {
    "selleros-warehouse": {
      "url": "https://mcp.app.simpliworks.io/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TEAM_KEY"
      }
    }
  }
}
```

---

## 3. Antigravity IDE / VS Code

In your `mcp.json` or Antigravity configuration:

```json
{
  "mcpServers": {
    "selleros-warehouse": {
      "url": "https://mcp.app.simpliworks.io/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TEAM_KEY"
      }
    }
  }
}
```

---

## 4. Claude Code CLI

Run this command in your terminal:

```bash
claude mcp add --transport http selleros https://mcp.app.simpliworks.io/mcp --header "Authorization: Bearer YOUR_TEAM_KEY"
```

---

## Available Tools

Once connected, the following tools are available to the AI:

### Database & Analytics Tools
- `list_tables_and_schema`: Inspect warehouse tables, column names, data types, and nullability across allowed schemas (`apify`, `audit`, `datadive`, `dw`, `ops`, `public`, `ref`, `stg`, `triplewhale`).
- `run_metric_query`: Execute read-only analytical SQL queries (`SELECT`, `WITH`) directly against the warehouse.

### Apify Scrapers & Data Tools
- `apify_amazon_product`: Live Amazon product page scrape (price, stock, rating, bullet points).
- `apify_amazon_offers`: Live Buy Box seller, competing offers, and stock.
- `apify_amazon_search`: Keyword search positions (organic and sponsored).
- `apify_amazon_reviews`: Full customer review scrape with rating breakdown and verified purchase status.
- `apify_social_posts`: Scrape social posts from Instagram, TikTok, Facebook, X (Twitter), YouTube.
- `apify_web_research`: Crawl web pages, Google search results, or fetch top pages via RAG browser.
- `get_apify_results`: Retrieve asynchronous scraper runs.
