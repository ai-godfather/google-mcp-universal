# Google MCP Universal — Agent Instructions

> These rules apply to any Claude agent working on this plugin codebase.
> Read this file FIRST before making any changes.

---

## 1. Project Overview

This is **google-mcp-universal** — an open-source Claude Desktop plugin for managing Google Ads accounts via MCP (Model Context Protocol).

- **Repo**: https://github.com/ai-godfather/google-mcp-universal
- **Author**: God_FatherAI (https://x.com/God_FatherAI)
- **License**: MIT
- **Architecture**: FastMCP server (Python) communicating via stdio with Claude Desktop

### File Structure

```
google-mcp-universal/
├── .claude-plugin/
│   └── plugin.json            # Plugin metadata (name, version, author)
├── skills/
│   └── google-mcp-universal/  # ← skill folder name MUST match plugin name
│       ├── SKILL.md            # Skill definition (triggers, instructions)
│       ├── google_ads_mcp.py   # Main MCP server entry point
│       ├── accounts_config.py  # Config loader (config.json → _config dict)
│       ├── batch_optimizer.py  # Batch tools (56 tools, ~16K lines)
│       ├── batch_analytics.py  # Analytics & reporting tools
│       ├── batch_intelligence.py # Keyword intelligence & categorization
│       ├── batch_db.py         # SQLite persistence layer
│       ├── mcp_campaigns.py    # Campaign CRUD tools
│       ├── mcp_ads_and_assets.py # Ad & asset management tools
│       ├── mcp_keywords.py     # Keyword tools
│       ├── mcp_merchant.py     # Merchant Center tools
│       ├── mcp_shopping.py     # Shopping & PMax tools
│       ├── mcp_reporting.py    # Reporting tools
│       ├── mcp_targeting.py    # Geo, device, schedule tools
│       ├── mcp_pagespeed.py    # PageSpeed Insights tools
│       └── pagespeed_fixes.py  # Shopify theme fix patterns
├── commands/                   # 27 slash commands (.md files)
├── dat/BLACKLIST/gMerchant/    # Runtime blacklist data (gitignored)
├── config.example.json         # Template config — user copies to config.json
├── .env.example                # Template env vars — user copies to .env
├── setup_account.py            # Interactive setup wizard
├── install.sh                  # macOS/Linux auto-installer
├── requirements.txt            # Python dependencies
├── README.md                   # Main documentation
├── README_INSTALL_MCP_WINDOWS.txt  # Windows install guide
└── LICENSE                     # MIT
```

---

## 2. CRITICAL RULES — Universal Plugin Hygiene

This plugin is **universal and commercially distributable**. It MUST NOT contain any account-specific, brand-specific, or company-specific data.

### NEVER hardcode:

- Real customer IDs (any 10-digit Google Ads account numbers)
- Real Merchant Center IDs (any numeric sub-account IDs)
- Real campaign IDs (any numeric campaign identifiers)
- API tokens, secrets, refresh tokens
- Domain names of specific stores (any real shop URLs)
- Brand codes tied to specific companies (any company-specific abbreviations)
- File paths to specific user machines (any `/Users/...` or `C:\Users\...` paths)
- Email addresses of specific people
- Company names of any real businesses

### ALWAYS use:

- `CUSTOMER_ID` variable (loaded from config via `accounts_config.py`)
- `_config.get('section', {}).get('key', 'default')` for all configurable values
- `_get_endpoint('name', '')` for external API URLs
- `os.getenv('VAR_NAME', '')` for environment variables
- `MERCHANT_IDS`, `DOMAINS`, `COUNTRY_CONFIG` dicts (loaded from config)
- Generic placeholders in examples: `YOUR_CUSTOMER_ID`, `YOUR_CAMPAIGN_ID`, `us.yourshop.com`
- Generic brand codes: `MAIN`, `OUTLET`, `PREMIUM` (user configures via `brand_codes` in config.json)

### Before every commit/release, verify:

```bash
# Must return ZERO results:
grep -rn "FORBIDDEN_WORD_1\|FORBIDDEN_WORD_2\|REAL_CUSTOMER_ID\|REAL_MCC_ID" \
  --include="*.py" --include="*.md" --include="*.json" --include="*.txt" --include="*.sh" .
# Replace FORBIDDEN_WORD_* with any company/brand names that should not appear.
# Replace REAL_CUSTOMER_ID / REAL_MCC_ID with actual IDs to check for.
```

---

## 3. Configuration Architecture

All user-specific values flow through a single config chain:

```
.env file → environment variables → accounts_config.py → _config dict → all modules
config.json → accounts_config.py → _config dict → all modules
```

- `accounts_config.py` is the SOLE config loader. Never load config elsewhere.
- `_config = load_config()` is called once at module level in `batch_optimizer.py`
- Other modules import config indirectly through shared variables:
  - `CUSTOMER_ID`, `MERCHANT_IDS`, `DOMAINS`, `COUNTRY_CONFIG`, `XML_FEEDS`, `NP_FEEDS`
- Endpoints use pattern: `_get_endpoint('name', '')` with empty string fallback

---

## 4. Versioning & Release Process

### Version format: `MAJOR.MINOR.PATCH`

- **MAJOR** (1.x.x → 2.0.0): Breaking config.json schema changes, removed tools
- **MINOR** (1.0.x → 1.1.0): New tools, new commands, new config options
- **PATCH** (1.0.0 → 1.0.1): Bug fixes, doc updates, cleanup

### When creating a new version:

1. **Update version** in `.claude-plugin/plugin.json` → `"version": "X.Y.Z"`
2. **Update version** in SKILL.md description if referenced
3. **Run full verification**:
   ```bash
   # Syntax check all Python files
   find skills/ -name "*.py" -exec python3 -c "
   import sys
   for f in sys.argv[1:]:
       with open(f) as fh: compile(fh.read(), f, 'exec')
       print(f'OK: {f}')
   " {} +

   # Hardcoded data check (must return zero)
   grep -rn "REAL_COMPANY\|REAL_CUSTOMER_ID" \
     --include="*.py" --include="*.md" --include="*.json" .
   ```
4. **Build the .plugin file**:
   ```bash
   zip -r google-mcp-universal-vX.Y.Z.plugin . \
     -x "*.pyc" "*__pycache__*" ".git/*" "*.DS_Store" "*.backup" \
     "*.plugin" "batch_state.db*" "config.json" ".env"
   ```
5. **Save versioned builds** in a separate directory:
   ```
   releases/
   ├── google-mcp-universal-v1.0.0.plugin
   ├── google-mcp-universal-v1.0.1.plugin
   └── google-mcp-universal-v1.1.0.plugin
   ```
6. **Tag in git**: `git tag v1.0.1 && git push --tags`

### Files to NEVER include in .plugin:

- `config.json` (user credentials)
- `.env` (user secrets)
- `batch_state.db` / `*.db-wal` / `*.db-journal` (runtime data)
- `*.backup` files
- `__pycache__/` directories
- `.git/` directory
- `.DS_Store`, `Thumbs.db`

---

## 5. Post-Installation Checklist

After installing the plugin for development, the agent should:

1. **Check Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Create config.json** from template:
   ```bash
   python setup_account.py
   # Or manually: copy config.example.json → config.json
   ```

3. **Create .env** from template:
   ```bash
   # Copy .env.example → .env and fill in OAuth credentials
   ```

4. **Verify MCP server starts**:
   ```bash
   python skills/google-mcp-universal/google_ads_mcp.py
   # Should start FastMCP server on stdio (no errors)
   ```

5. **Test basic connectivity** (in Claude Desktop):
   ```
   /ads-health
   /ads-quota
   ```

---

## 6. Adding New Tools

### MCP Tool (in a `mcp_*.py` file):

```python
@mcp_app.tool()
async def my_new_tool(request: MyRequest) -> dict:
    """Tool description shown in Claude."""
    # Implementation
    return {"status": "success", "data": result}
```

### Batch Tool (in `batch_optimizer.py`):

```python
class MyBatchRequest(BaseModel):
    customer_id: str = Field(default=CUSTOMER_ID, description="Customer ID (from config)")
    country_code: str = Field(description="Country code (e.g. DE, PL)")

@mcp_app.tool()
async def batch_my_tool(request: MyBatchRequest) -> dict:
    """Batch tool description."""
    return {"status": "success"}
```

**Rules for new tools:**
- Always use `CUSTOMER_ID` as default for `customer_id` field
- Always load account-specific data from `_config`, never hardcode
- Add error handling with descriptive messages
- Return `{"status": "success/error", ...}` format
- Add the tool to the appropriate section in SKILL.md

### New Slash Command (in `commands/`):

Create `commands/ads-my-command.md`:
```markdown
---
description: Short description of what this command does
---

# My Command

## Steps

1. First call `tool_name` with parameters...
2. Then analyze results...
3. Present findings to user...
```

---

## 7. Adding New Config Options

When adding a new configurable value:

1. Add it to `config.example.json` with a descriptive `_comment`
2. Add the env var to `.env.example` with explanation
3. Load it in the appropriate module via `_config.get('section', {}).get('key', 'default')`
4. Document it in README.md
5. If it's a required field, add to `setup_account.py` wizard

---

## 8. Testing

### Syntax verification:
```bash
find skills/ -name "*.py" -exec python3 -c "
import sys
for f in sys.argv[1:]:
    with open(f) as fh: compile(fh.read(), f, 'exec')
    print(f'OK: {f}')
" {} +
```

### Config validation:
```bash
python setup_account.py --validate
```

### Integration test (requires real Google Ads account):
```
/ads-health          # Basic connectivity
/ads-quota           # API quota check
/ads-report          # Campaign performance
```

---

## 9. Database Schema

SQLite database `batch_state.db` is auto-created by `batch_db.py` on first use.

Tables:
- `product_setup` — tracks which assets exist per product/campaign
- `operation_log` — audit trail of all API actions
- `gaql_cache` — avoids redundant Google Ads API calls (TTL: 5 min)
- `xml_feed_cache` — avoids re-downloading product feeds (TTL: 1 hour)
- `keyword_intelligence` — cross-campaign keyword performance data

The database is gitignored and never included in the plugin package.
To reset: delete `batch_state.db` — it recreates automatically.

---

## 10. Common Patterns

### Reading config values:
```python
# Good:
customer_id = _config.get('account', {}).get('customer_id', '0000000000')
merchants = _config.get('merchant_center', {}).get('merchants', {})
domain = DOMAINS.get(country_code, '')

# Bad:
customer_id = "1234567890"  # NEVER hardcode real IDs
```

### Endpoint loading:
```python
# Good:
url = _get_endpoint('image_finder', '')
if not url:
    return {"status": "error", "message": "image_finder endpoint not configured"}

# Bad:
url = "https://some-specific-api.example.com/images"  # NEVER hardcode
```

### Brand codes:
```python
# Good — reads from config:
brand_codes = _config.get('brand_codes', {})
brand = brand_codes.get(domain_pattern, 'MAIN')

# Bad — hardcoded brand:
if "somebrand" in domain: return "XYZ"  # NEVER hardcode brand mapping
```

---

## 11. Troubleshooting Guide for Developers

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ModuleNotFoundError: fastmcp` | Dependencies missing | `pip install -r requirements.txt` |
| `config not loaded` | No config.json | `python setup_account.py` |
| `CUSTOMER_ID is 0000000000` | config.json missing customer_id | Edit config.json |
| `RefreshError` | OAuth token expired | Regenerate refresh token |
| `batch_state.db locked` | Multiple MCP instances | Kill duplicate processes |
| Tools not showing in Claude | Wrong path in claude_desktop_config.json | Point to `skills/google-mcp-universal/google_ads_mcp.py` |
| Plugin name collision | Old plugin with same skill name | Uninstall old version first |
