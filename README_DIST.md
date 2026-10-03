# 🚀 SellerOS Warehouse MCP - 1-Click Setup Guide

A complete zero-terminal, zero-Python setup guide for team members.

---

## ⚡ What Does This Do?
This installs the **SellerOS Warehouse MCP** on your PC so that AI tools (**Claude Desktop**, **Antigravity IDE**, **Cursor**) can query the SellerOS PostgreSQL data warehouse directly.

* **No Python needed** (Python is bundled in the `.exe`).
* **No PostgreSQL needed** (The database is in AWS Cloud, all drivers are inside the `.exe`).
* **No PowerShell commands needed** (The background AWS SSM tunnel opens and closes automatically).
* **No config file editing needed** (The installer configures Claude Desktop and Antigravity automatically).

---

## 📥 How to Install (For Non-Tech Team Members)

### Step 1: Download
Get `selleros-warehouse.exe` from the `dist/` folder or shared team drive.

### Step 2: Run the 1-Click Installer
Open Command Prompt or PowerShell once (or double-click) and run:
```cmd
selleros-warehouse.exe --install
```
*(Or double-click the installer shortcut).*

* The installer copies the application into `%LOCALAPPDATA%\SellerOS\WarehouseMCP\`.
* It detects your **Claude Desktop** and **Antigravity IDE** setups and automatically configures the MCP server.
* If AWS credentials are missing, a simple dialog will ask for your **AWS Access Key** and **Secret Key**.

### Step 3: Restart Your AI App
Close and reopen **Claude Desktop** or **Antigravity IDE**.
That's it! You will now see the `selleros-warehouse` tools:
* `list_tables_and_schema`
* `run_metric_query`

---

## 🛠️ Testing Your Connection (Optional)
To verify your connection at any time without opening an AI app:
```cmd
selleros-warehouse.exe --test
```
You will see:
```text
[+] Success! Connected as 'swdw' to DB 'warehouse'.
[+] Found 18 tables across allowed schemas: ['dw', 'ops']
[+] ALL CHECKS PASSED! Ready for Claude Desktop & Antigravity IDE.
```

---

## ⚙️ Configuration (`config.json`)
The settings file is located at:
`%LOCALAPPDATA%\SellerOS\WarehouseMCP\config.json`

```json
{
  "AWS_PROFILE": "selleros",
  "AWS_REGION": "us-east-1",
  "SSM_TARGET_INSTANCE_ID": "i-03680992802022b55",
  "AUTO_START_TUNNEL": true,
  "DB_RDS_HOST": "selleros-warehouse.cvvtiac72c6q.us-east-1.rds.amazonaws.com",
  "DB_LOCAL_HOST": "127.0.0.1",
  "DB_LOCAL_PORT": 55434,
  "DB_RDS_PORT": 5432,
  "DB_NAME": "warehouse",
  "DB_USER": "swdw",
  "DB_PASSWORD": "6efc885a9a4f06d287e8455685add4e6",
  "RDS_CA_BUNDLE": null,
  "ALLOWED_SCHEMAS": ["dw", "ops"],
  "MAX_ROWS": 1000,
  "STATEMENT_TIMEOUT_MS": 30000
}
```
