# Universal Google Ads Manager for Claude Code

A comprehensive, enterprise-grade Google Ads management plugin for Claude Code and Cowork. Automate campaign creation, ad optimization, asset management, and performance analysis across multiple countries with 186 tools, 51 slash commands and 2 skills.

> **Installing?** Follow **[INSTALL.md](INSTALL.md)** — a step-by-step runbook an AI assistant can execute on a clean computer (virtualenv, credentials, `verify_install.py`, Claude Code / Claude Desktop registration, troubleshooting). The sections below are background reference.

## What's new in 1.1.0 / 1.1.1

- **1.1.1** — `google_ads_get_account_summary`, `google_ads_get_campaign_details` and `google_ads_get_performance_report` queried metrics that the current Google Ads API does not have (`average_position`, `conversion_rate`); they now use `conversions_from_interactions_rate`.

- **Reviewed Search delivery** — `google_ads_create_reviewed_search_campaign`, `google_ads_set_exact_campaign_conversion_goal` and `google_ads_optimize_reviewed_search` create and tune Search campaigns PAUSED, validate-only by default, with naming/destination rules from `config.json` (`search_launch`). New skill **google-ads-search-launch** guides research → owner review → delivery.
- **E-commerce engine** — Producthero-style labelizer (`ecom_labelizer_run/apply/deploy/verify`), supplemental label feeds, IF-THEN feed rules, PMax architecture, catalog comparison and SEO title optimizer.
- **Shopping coverage monitor** — `batch_shopping_coverage_audit` and `batch_shopping_fix_bids` (stores in `shopping_coverage.stores`).
- **Campaign config tracker** and **new-products roadmap** — `batch_sync_campaign_config`, `batch_campaign_config_dashboard`, `batch_np_roadmap_dashboard/update`.
- **Offline conversions** — `google_ads_upload_click_conversions` (gclid / gbraid / wbraid).
- **Search-terms analyzers** — offline CLI scripts for harvest, negatives, country priority and channel arbitrage from "Search terms report" CSV exports.
- **Fixes** — keyword-intelligence tools (`batch_keyword_research`, `batch_mine_country_keywords`, …) load again; all account-specific defaults now come from `config.json`; outbound HTTPS keeps certificate verification.
- 15 new slash commands (51 total). All new `config.json` sections are optional — see `config.example.json`.

## Features

### Core Capabilities
- **186 Integrated Tools** — Full Google Ads API coverage for Search, Shopping, Performance Max, and Display campaigns
- **27 Slash Commands** — Quick access to common workflows: `/ads-health`, `/ads-audit`, `/ads-report`, and more
- **Batch Optimizer** — Process hundreds of products in parallel with automatic asset generation, image handling, and error recovery
- **AI Ad Copy Generation** — Bring your own OpenAI/Anthropic key for intelligent, localized ad copy (fallback to templates)
- **Multi-Country Support** — Built-in configurations for 30+ countries with localized templates, feeds, and merchant IDs
- **Product Dashboard** — Real-time visibility into setup progress, asset completeness, and error tracking
- **Keyword Intelligence** — Mine converting keywords across all campaigns, map to products, build category libraries
- **Shopping Campaign Optimization** — Bid automation, product grouping, exclusions, and cross-country cloning
- **Performance Analytics** — Location, device, time-of-day, and keyword spend analysis with trend detection
- **Guardrail Validation** — Automatic eligibility checks, disapproval detection, and remediation workflows

### Advanced Features
- **Market Intelligence Pre-warming** — Gather trends, competitor ads, and keyword data before batch setup
- **Image Processing** — Automatic validation, cropping, enhancement via GPT-4o, and upload to Google Ads
- **API Quota Monitoring** — Track daily quota usage with health checks and warnings
- **Job Queuing** — Async processing with progress polling and automatic resume-on-interrupt
- **Changelog & Audit Trail** — Track all changes, fixes, and decisions with persistent logging
- **Demo Mode** — Safe testing without API calls

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Google Cloud Project Setup](#1-google-cloud-project-setup)
3. [Enable APIs](#2-enable-required-apis)
4. [OAuth 2.0 Credentials](#3-create-oauth-20-credentials)
5. [Google Ads Developer Token](#4-google-ads-developer-token)
6. [Generate Refresh Token](#5-generate-refresh-token)
7. [Google Merchant Center Setup](#6-google-merchant-center-setup)
8. [Plugin Installation](#7-plugin-installation)
9. [Configuration](#8-configuration)
10. [Verify Installation](#9-verify-installation)
11. [AI Ad Copy Setup (Optional)](#10-ai-ad-copy-generation-optional)
12. [Slash Commands Reference](#slash-commands-reference)
13. [Architecture Overview](#architecture-overview)
14. [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before you begin, you need:

- A **Google Ads account** (any type — even a free account works for initial setup)
- A **Google Cloud Platform (GCP) account** — [console.cloud.google.com](https://console.cloud.google.com)
- **Python 3.10+** installed on your system
- **Claude Code** or **Cowork** (Claude desktop app)
- Optionally: a **Google Merchant Center** account (for Shopping campaigns)
- Optionally: an **OpenAI API key** (for AI-generated ad copy)

---

## 1. Google Cloud Project Setup

### 1.1 Create a New GCP Project

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Click the project dropdown in the top bar → **"New Project"**
3. Enter project details:
   - **Project name**: `google-ads-plugin` (or anything descriptive)
   - **Organization**: your org or "No organization"
   - **Location**: leave default
4. Click **"Create"**
5. Wait ~30 seconds for the project to be created
6. **Select the new project** from the project dropdown

> Save your **Project ID** — you'll need it later. It's shown on the project dashboard and looks like `google-ads-plugin-123456`.

### 1.2 Enable Billing (Required for API access)

Even though the Google Ads API itself is free, GCP requires a billing account to enable APIs:

1. Go to **Billing** in the left sidebar (or [billing page](https://console.cloud.google.com/billing))
2. Click **"Link a billing account"**
3. If you don't have one, click **"Create billing account"** and follow the wizard
4. Link it to your project

> The Google Ads API and Merchant Center API have no per-call charges. You only pay for GCP services if you use them (Cloud Functions, BigQuery, etc.), which this plugin does NOT require.

---

## 2. Enable Required APIs

You need to enable 3 APIs in your GCP project:

### Via Google Cloud Console (GUI)

1. Go to **APIs & Services → Library** ([link](https://console.cloud.google.com/apis/library))
2. Search for and enable each of these:

| API | Search Term | Direct Link |
|-----|-------------|-------------|
| **Google Ads API** | "Google Ads API" | [Enable](https://console.cloud.google.com/apis/library/googleads.googleapis.com) |
| **Content API for Shopping** | "Content API for Shopping" | [Enable](https://console.cloud.google.com/apis/library/content.googleapis.com) |
| **PageSpeed Insights API** | "PageSpeed Insights" | [Enable](https://console.cloud.google.com/apis/library/pagespeedonline.googleapis.com) |

3. For each API: click **"Enable"** button → wait for confirmation

### Via gcloud CLI (Alternative)

```bash
# Set your project
gcloud config set project YOUR_PROJECT_ID

# Enable APIs
gcloud services enable googleads.googleapis.com
gcloud services enable content.googleapis.com
gcloud services enable pagespeedonline.googleapis.com
```

### Verify APIs are Enabled

Go to **APIs & Services → Dashboard** → you should see all 3 APIs listed with "Enabled" status.

---

## 3. Create OAuth 2.0 Credentials

The plugin uses OAuth 2.0 to authenticate with Google Ads and Merchant Center APIs. You need to create a **Desktop application** OAuth client.

### 3.1 Configure OAuth Consent Screen

1. Go to **APIs & Services → OAuth consent screen** ([link](https://console.cloud.google.com/apis/credentials/consent))
2. Select **User Type**:
   - Choose **"External"** (works for any Google account)
   - Click **"Create"**
3. Fill in the form:
   - **App name**: `Google Ads Plugin` (or your brand name)
   - **User support email**: your email
   - **Developer contact**: your email
4. Click **"Save and Continue"**
5. **Scopes** page — click **"Add or Remove Scopes"** and add:
   - `https://www.googleapis.com/auth/adwords` (Google Ads)
   - `https://www.googleapis.com/auth/content` (Merchant Center)
   - Or simply type `adwords` and `content` in the filter
6. Click **"Update"** → **"Save and Continue"**
7. **Test users** page — click **"Add Users"** → add your Gmail/Google Workspace email
8. Click **"Save and Continue"** → **"Back to Dashboard"**

> **Important**: while the consent screen is in **"Testing"** mode, Google **expires refresh tokens after 7 days**. After adding yourself as a test user, click **Publish app → In production**. Unverified apps show a warning at sign-in (*Advanced → Go to app*), which is fine for your own account; Google verification is only needed for distributing the app to other organisations.

### 3.2 Create OAuth Client ID

1. Go to **APIs & Services → Credentials** ([link](https://console.cloud.google.com/apis/credentials))
2. Click **"+ Create Credentials"** → **"OAuth client ID"**
3. Select **Application type**: **"Desktop app"**
4. **Name**: `Google Ads Plugin Desktop Client`
5. Click **"Create"**
6. A popup shows your credentials:
   - **Client ID**: `123456789-abcdef.apps.googleusercontent.com`
   - **Client Secret**: `<your-client-secret>`
7. Click **"Download JSON"** → save as `client_secret.json` (backup)
8. **Copy both values** — you'll need them in step 5

> **Save these securely!** The Client Secret is like a password. Never commit it to git or share publicly.

---

## 4. Google Ads Developer Token

The Developer Token is required to access the Google Ads API. It's tied to your Google Ads **Manager (MCC) account**.

### 4.1 If You Have an MCC Account

1. Sign in to [Google Ads](https://ads.google.com)
2. Navigate to your **Manager account** (MCC)
3. Go to **Tools & Settings** (wrench icon) → **Setup** → **API Center**
4. If you don't see "API Center", you may need to first request access:
   - Fill out the [API access request form](https://developers.google.com/google-ads/api/docs/get-started/dev-token)
5. Your **Developer Token** is displayed (looks like: `ABcDeFgHiJkLmNoPqRs`)
6. **Access level** will be one of:
   - **Test Account** — limited to test accounts, no real data
   - **Basic Access** — full read/write, up to 15,000 operations/day
   - **Standard Access** — higher quotas

### 4.2 If You Don't Have an MCC Account

1. Go to [Google Ads Manager Accounts](https://ads.google.com/home/tools/manager-accounts/)
2. Click **"Create a manager account"**
3. Fill in your business details
4. Once created, follow step 4.1 above

### 4.3 Developer Token Access Levels

| Level | Read | Write | Daily Quota | How to Get |
|-------|------|-------|-------------|------------|
| **Test** | Yes (test accounts only) | Yes (test accounts only) | 15,000 | Automatic |
| **Basic** | Yes | Yes | 15,000 | Apply via API Center |
| **Standard** | Yes | Yes | Custom (higher) | Apply after Basic approval |

> **For this plugin**: Basic Access is sufficient. Apply for it in the API Center. Approval usually takes 1-3 business days. You can start testing with Test Access immediately.

### 4.4 Apply for Basic Access

1. In **API Center**, click **"Apply for Basic Access"**
2. Fill in the form:
   - **Product type**: "Internal tools (not public-facing)"
   - **API usage**: Describe your use: "Automated campaign management, batch product setup, performance reporting"
   - **Implementation details**: "MCP plugin for Claude Code, managing campaigns via Google Ads API"
3. Submit and wait for approval email (1-3 business days)

---

## 5. Generate Refresh Token

The refresh token lets the plugin call Google Ads (and Merchant Center) without signing in again. The bundled script runs Google's sign-in in your browser and writes the token straight into `.env` (permissions 600) — it is never printed:

```bash
.venv/bin/python generate_refresh_token.py --client-secrets ~/Downloads/client_secret_XXXX.json
```

- Use the **Desktop app** client JSON from step 3.2.
- Sign in with the Google account that has access to the Google Ads account (and Merchant Center); approve.
- `--no-browser` prints the URL instead (open it on the same computer); `--ads-only` skips the Merchant Center scope.
- If Google returns no refresh token, remove the app at https://myaccount.google.com/permissions and run it again.

**Alternative — OAuth Playground:** only works with a **Web application** client that has `https://developers.google.com/oauthplayground` as an authorized redirect URI. In the Playground settings tick *Use your own OAuth credentials*, authorize the scopes `https://www.googleapis.com/auth/adwords` and `https://www.googleapis.com/auth/content`, exchange the code, and put the refresh token into `.env` as `GOOGLE_ADS_REFRESH_TOKEN`.

---

## 6. Google Merchant Center Setup

Google Merchant Center is required only if you want to manage **Shopping campaigns** or **Performance Max campaigns** with product feeds. Skip this section if you only use Search campaigns.

### 6.1 Create a Merchant Center Account

1. Go to [Google Merchant Center](https://merchants.google.com)
2. Click **"Get Started"** or sign in with your Google account
3. Fill in business details:
   - **Business name**: your company name
   - **Country**: your primary market
   - **Website URL**: your shop domain (e.g., `https://www.yourshop.com`)
4. Verify and claim your website (follow the on-screen instructions — options include HTML tag, Google Analytics, Google Tag Manager, or DNS record)

### 6.2 Link Merchant Center to Google Ads

1. In Merchant Center, go to **Settings** → **Linked accounts** (or **Tools → Linked accounts** in newer UI)
2. Click **"Link account"** → **"Google Ads"**
3. Enter your **Google Ads Customer ID** (10-digit number, e.g., `123-456-7890`)
4. Click **"Send link request"**
5. Go to **Google Ads** → **Tools & Settings** → **Setup** → **Linked accounts** → **Google Merchant Center**
6. **Approve** the pending link request

### 6.3 Note Your Merchant Center IDs

You'll need the Merchant Center account ID(s) for the plugin config:

1. In Merchant Center, your **Account ID** is shown in the top-right corner or in **Settings → Account info**
2. If you have a **Multi-client account (MCA)** with sub-accounts per country:
   - Go to **Settings → Sub-accounts**
   - Note each sub-account's ID and country

| What | Where to Find | Example |
|------|---------------|---------|
| **Main Account ID** | Top-right in MC dashboard | `123456789` |
| **Sub-account IDs** | Settings → Sub-accounts | US: `111222333`, DE: `444555666` |
| **MCA ID** (if multi-client) | Settings → Account info | `987654321` |

### 6.4 Content API for Shopping — Authentication

The plugin uses the **Content API for Shopping** (enabled in step 2) to read products, check statuses, and manage feeds. Authentication uses the **same OAuth credentials** from step 3 — no additional setup needed.

The Refresh Token generated in step 5 already includes the `content` scope, so it works for both Google Ads API and Merchant Center API.

### 6.5 Product Feeds

If you use Shopify, WooCommerce, or similar platforms, set up product feeds in Merchant Center:

1. Go to **Products → Feeds** in Merchant Center
2. Click **"+"** to add a new feed
3. Configure:
   - **Country**: target market
   - **Language**: matching language
   - **Feed type**: "Scheduled fetch" (recommended for Shopify/WooCommerce)
   - **Feed URL**: your XML/RSS product feed URL
   - **Fetch schedule**: daily or more frequent
4. Save and trigger the first fetch
5. Note the **Feed URL** — you'll add it to `config.json`

---

## 7. Plugin Installation & MCP Server Setup

Full runbook: **[INSTALL.md](INSTALL.md)**. In short:

### 7.1 Code and dependencies (virtualenv)

```bash
git clone https://github.com/ai-godfather/google-mcp-universal.git ~/google-mcp-universal
cd ~/google-mcp-universal
bash install.sh --no-wizard            # macOS/Linux: creates .venv, installs requirements.txt, runs verify_install.py
```

Windows: `py -3.13 -m venv .venv` then `.venv\Scripts\python -m pip install -r requirements.txt`. Python 3.12–3.14 is supported. Never `pip install` into the system Python — MCP configs must point at `.venv/bin/python` (Windows: `.venv\Scripts\python.exe`).

### 7.2 Claude Code

```bash
claude mcp add --scope user --transport stdio google-ads -- "$PWD/.venv/bin/python" "$PWD/skills/google-mcp-universal/google_ads_mcp.py"
claude mcp list                       # google-ads … ✔ Connected
```

Optional slash commands and skills (Claude Code plugin):

```bash
claude plugin marketplace add ai-godfather/google-mcp-universal
claude plugin install google-mcp-universal@ai-godfather
```

Commands are namespaced, e.g. `/google-mcp-universal:ads-health`. The plugin does not start the MCP server — register it as above.

### 7.3 Claude Desktop

Add the server to `claude_desktop_config.json` (Settings → Developer → Edit Config; macOS `~/Library/Application Support/Claude/`, Windows `%APPDATA%\Claude\`, Linux `~/.config/Claude/`), then quit and reopen Claude Desktop:

```json
{
  "mcpServers": {
    "google-ads": {
      "command": "/Users/NAME/google-mcp-universal/.venv/bin/python",
      "args": ["/Users/NAME/google-mcp-universal/skills/google-mcp-universal/google_ads_mcp.py"]
    }
  }
}
```

No `env` block is needed: the server reads `.env` and `config.json` from the plugin folder itself. `bash install.sh --register-claude-desktop` writes this entry for you (with a backup).

### 7.4 Run the server by hand (debugging)

```bash
.venv/bin/python skills/google-mcp-universal/google_ads_mcp.py     # stdio MCP server; Ctrl+C to stop
.venv/bin/python verify_install.py --live                           # handshake + read-only API call
```

---

## 8. Configuration

### 8.1 Environment Variables Reference

Credentials live in **`.env` in the plugin folder** (copy `.env.example`, `chmod 600`). The server loads it itself; variables set in an MCP client config or your shell take precedence. `generate_refresh_token.py` fills the OAuth values for you.

```bash
# === REQUIRED ===
export GOOGLE_ADS_DEVELOPER_TOKEN="<your-developer-token>"
export GOOGLE_ADS_CLIENT_ID="123456789-abcdef.apps.googleusercontent.com"
export GOOGLE_ADS_CLIENT_SECRET="<your-client-secret>"
export GOOGLE_ADS_REFRESH_TOKEN="<your-refresh-token>"
export GOOGLE_ADS_CUSTOMER_ID="1234567890"

# === OPTIONAL (for MCC accounts) ===
export GOOGLE_ADS_LOGIN_CUSTOMER_ID="9876543210"

# === OPTIONAL (for Merchant Center — uses same OAuth by default) ===
# Only set if you need a DIFFERENT refresh token for Merchant Center:
# export MERCHANT_CENTER_REFRESH_TOKEN="1//0eYYYYYYYYYYYYY"

# === OPTIONAL (for AI ad copy generation) ===
export OPENAI_API_KEY="sk-..."

# === OPTIONAL (for PageSpeed Insights — higher quota) ===
export PSI_API_KEY="AIzaSy..."
```

### 8.2 config.json (Account-Specific Settings)

Run the interactive wizard or copy the example:

```bash
# Interactive wizard (recommended)
python setup_account.py

# Or manual setup
cp config.example.json config.json
```

Edit `config.json` with your account details:

```json
{
  "account": {
    "customer_id": "1234567890",
    "mcc_id": "9876543210",
    "alias": "my-brand",
    "company_name": "Your Company Name",
    "brand_name": "Your Brand",
    "industry": "e-commerce, retail"
  },

  "merchant_center": {
    "mca_ids": ["123456789"],
    "merchant_ids": {
      "US": "111222333",
      "UK": "444555666",
      "DE": "777888999"
    }
  },

  "domains": {
    "US": "us.yourshop.com",
    "UK": "uk.yourshop.com",
    "DE": "de.yourshop.com"
  },

  "markets": ["US", "UK", "DE"],

  "xml_feeds": {
    "US": "https://us.yourshop.com/collections/all.atom",
    "UK": "https://uk.yourshop.com/collections/all.atom",
    "DE": "https://de.yourshop.com/collections/all.atom"
  },

  "ai_copy": {
    "provider": "openai",
    "model": "gpt-4o",
    "fallback_to_templates": true
  },

  "campaign_defaults": {
    "daily_budget_micros": 5000000,
    "bidding_strategy": "MAXIMIZE_CONVERSION_VALUE",
    "target_roas": 1.6,
    "search_network": false,
    "display_network": false
  }
}
```

### 8.3 Configuration Priority

The plugin loads settings in this order (later overrides earlier):

1. **Demo defaults** (built-in, if no config.json found)
2. **config.json** (from plugin directory or `~/.google-ads-plugin/config.json`)
3. **Environment variables** (override specific fields from config.json)

### 8.4 All Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `GOOGLE_ADS_DEVELOPER_TOKEN` | **Yes** | Your Google Ads API developer token |
| `GOOGLE_ADS_CLIENT_ID` | **Yes** | OAuth 2.0 Client ID from GCP |
| `GOOGLE_ADS_CLIENT_SECRET` | **Yes** | OAuth 2.0 Client Secret from GCP |
| `GOOGLE_ADS_REFRESH_TOKEN` | **Yes** | OAuth 2.0 Refresh Token (generated in step 5) |
| `GOOGLE_ADS_CUSTOMER_ID` | **Yes** | Your Google Ads Customer ID (10 digits, no dashes) |
| `GOOGLE_ADS_LOGIN_CUSTOMER_ID` | If MCC | Your MCC (Manager) Account ID — required if accessing via MCC |
| `MERCHANT_CENTER_REFRESH_TOKEN` | No | Separate refresh token for MC (defaults to `GOOGLE_ADS_REFRESH_TOKEN`) |
| `OPENAI_API_KEY` | No | For AI ad copy generation |
| `ANTHROPIC_API_KEY` | No | Alternative AI provider |
| `PSI_API_KEY` | No | Google PageSpeed Insights API key (for higher quota) |
| `GOOGLE_ADS_PLUGIN_CONFIG` | No | Custom path to config.json |

---

## 9. Verify Installation

```bash
.venv/bin/python verify_install.py --live
```

It checks Python and packages, `config.json`, the credential variables (names only), starts the server over stdio exactly like Claude does, lists the tools (186) and runs one read-only query (`SELECT customer.id, customer.descriptive_name, customer.currency_code FROM customer`). Every `[FAIL]` line names what to fix; error-specific hints are in [INSTALL.md → Troubleshooting](INSTALL.md#9-troubleshooting).

Inside Claude, the tool **`google_ads_setup_status`** reports what the running server sees (config file, `.env`, customer ID, missing variables) without calling Google. With the plugin installed, `/google-mcp-universal:ads-health` and `/google-mcp-universal:ads-quota` are good first commands.

---

## 10. AI Ad Copy Generation (Optional)

The plugin can generate high-quality, localized RSA headlines and descriptions using AI. This is optional — without it, the plugin uses built-in templates.

### 10.1 OpenAI Setup (Recommended)

1. Go to [OpenAI Platform](https://platform.openai.com)
2. Create an account or sign in
3. Go to **API Keys** → **"Create new secret key"**
4. Copy the key (starts with `sk-`)
5. Add to environment:
   ```bash
   export OPENAI_API_KEY="sk-..."
   ```
6. Add credits to your OpenAI account (minimum $5 recommended)

**Cost**: ~$0.01-0.03 per product (GPT-4o). 100 products ≈ $1-3.

### 10.2 Anthropic Setup (Alternative)

1. Go to [Anthropic Console](https://console.anthropic.com)
2. Create an API key
3. Set in config:
   ```json
   {
     "ai_copy": {
       "provider": "anthropic",
       "anthropic_api_key": "sk-ant-...",
       "model": "claude-sonnet-4-20250514"
     }
   }
   ```

### 10.3 How AI Copy Works

When batch-setting up products:
1. Plugin fetches product data from XML feed (name, price, description, URL)
2. Queries Google Ads for winning asset patterns (BEST/GOOD performance labels)
3. Gathers keyword intelligence (converting keywords, ROI data)
4. Sends everything to your AI provider with a localized prompt
5. AI generates 3 RSA variants per product:
   - **RSA A**: Generic messaging from winning patterns
   - **RSA B**: Keyword-focused with dynamic insertion
   - **RSA C**: Brand/trust with pinned HEADLINE_1
6. Each variant: 15 headlines + 4 descriptions (Google's maximum)
7. Results cached in SQLite for instant reuse

If AI is disabled or fails, the plugin falls back to template-based copy with localized fillers.

---

## Slash Commands Reference

Installed through the Claude Code plugin (INSTALL.md, step 6.3), every command carries the plugin prefix: `/google-mcp-universal:ads-health`. A selection of the 51 commands:

| Command | Arguments | Description |
|---------|-----------|-------------|
| `/ads-health` | — | Quick health check: quota, guardrails, eligibility |
| `/ads-audit` | `{CC} {campaign_id}` | Full campaign audit: sync, missing, guardrails |
| `/ads-report` | `{CC} {campaign_id} {days}` | Performance report with trends |
| `/ads-quota` | — | API quota usage breakdown |
| `/ads-setup-country` | `{CC} {campaign_id}` | Full country setup pipeline |
| `/ads-setup-product` | `{CC} {campaign_id} {handles}` | Setup specific products |
| `/ads-cleanup` | `{CC} {campaign_id}` | Pause stale products |
| `/ads-eligibility` | `{CC} {campaign_id}` | Fix disapproved ads/assets |
| `/ads-guardrails` | `{CC} {campaign_id}` | Deep quality validation |
| `/ads-keywords` | `{CC} {handle}` | Keyword research for product |
| `/ads-search-terms` | `{CC} {campaign_id} {days}` | Search terms analysis |
| `/ads-keyword-trends` | `{CC} [{campaign_id}] [{days}]` | Keyword spend trend analysis |
| `/ads-blacklist` | `{CC}` | Image variant A/B test |
| `/ads-winning-patterns` | `{CC}` | Best-performing ad patterns |
| `/ads-compare` | `{id1} {id2} {days}` | Compare two campaigns |
| `/ads-global-dashboard` | — | Cross-country dashboard |
| `/ads-loc-device-time` | `{CC} [{days}]` | Location + Device + Time analysis |
| `/ads-fix-images` | `{CC} {campaign_id}` | Fix missing images |
| `/ads-fix-empty-groups` | `[{CC}] [{campaign_id}]` | Fix ad groups without ads |
| `/ads-fix-negative-conflicts` | `{campaign_id}` | Resolve negative keyword conflicts |
| `/ads-merchant-gc` | `[{merchant_id}] [{CC}]` | Merchant Center cleanup |
| `/ads-shopping-optimize` | `{CC} {campaign_id}` | Shopping bid optimization |
| `/ads-shopping-clone` | `{source_id} {target_CC}` | Clone Shopping campaign |
| `/ads-pagespeed` | `{CC\|URL} [scan\|analyze]` | PageSpeed audit |
| `/ads-copy-audit` | `{CC} [{campaign_id}]` | RSA copy quality audit |
| `/ads-domain-migration` | `{CC} {id} {src} {tgt}` | Migrate ads between domains |
| `/ads-labelizer` · `-apply` · `-deploy` · `-verify` | `{CC}` | Hero / Sidekick / Villain / Zombie product segmentation and its campaigns |
| `/ads-shopping-coverage` · `/ads-shopping-fix-bids` | `[{CC}]` | Shopping coverage audit and catch-all bid repair |
| `/ads-setup-shopping-np` · `/ads-np-roadmap` | `{CC}` | New-products Shopping / PMax rollout |
| `/ads-config-dashboard` · `/ads-sync-config` | `[{campaign_id}]` | Local campaign-settings tracker |

---

## Architecture Overview

### Core Modules

| File | Lines | Purpose |
|------|-------|---------|
| `batch_optimizer.py` | ~17,000 | Main batch engine: product setup, campaign audits, error recovery, campaign config tracker |
| `batch_analytics.py` | ~3,300 | Performance analysis, keyword trends, location/device/time |
| `batch_db.py` | ~3,400 | SQLite database layer |
| `batch_intelligence.py` | ~1,800 | Keyword mining, ROI calculation, category analysis |
| `batch_shopping_coverage.py` | ~930 | Shopping coverage audit and bid repair |
| `google_ads_mcp.py` | ~1,600 | MCP server, tool registration, API client, `google_ads_setup_status` |
| `accounts_config.py` | ~280 | Universal config loader from config.json + env vars |
| `mcp_*.py` (12 files) | ~9,000 | Modular tool implementations (incl. e-com engine, reviewed Search delivery, conversions) |
| `search_terms_*.py`, `st_*.py` | ~3,200 | Offline search-terms analyzers (CLI) |
| `pagespeed_fixes.py` | ~570 | Shopify template optimization |

### Data Persistence

- **batch_state.db** — SQLite database created at runtime, stores product status, asset performance, keyword intelligence, operation logs
- **config.json** — Your account configuration (not committed to git)

---

## Troubleshooting

### Google Cloud / API Issues

| Problem | Solution |
|---------|----------|
| "API not enabled" | Go to GCP Console → APIs & Services → Library → enable the API |
| "Billing not enabled" | Link a billing account to your GCP project |
| "Application not verified" warning | Expected for your own unverified app: *Advanced → Go to app* |
| "Access blocked" | Add your account as a test user, or publish the consent screen |
| Refresh token stops working after ~7 days | Consent screen still in *Testing*: publish it (In production) and run `generate_refresh_token.py` again |
| "invalid_grant" error | The refresh token expired or was revoked — run `generate_refresh_token.py` again |
| MCC vs direct account confusion | Set `GOOGLE_ADS_LOGIN_CUSTOMER_ID` to your MCC ID, `GOOGLE_ADS_CUSTOMER_ID` to the managed account |

### Plugin Issues

| Problem | Solution |
|---------|----------|
| "No such tool" after install | Claude Code: new session or `/mcp`; Claude Desktop: quit and reopen. Check `claude mcp list` |
| Server fails to start | The MCP config must point at `.venv/bin/python` (Windows `.venv\Scripts\python.exe`), not the system Python |
| Credentials not found | Call `google_ads_setup_status`; `.env` must be in the plugin folder (next to `verify_install.py`) |
| Config not loading | Check `config.json` exists in plugin root or set `GOOGLE_ADS_PLUGIN_CONFIG` env var |
| AI copy not working | Verify `OPENAI_API_KEY` is set and has credits |
| Batch setup stalls | Check `/ads-quota` — you may have hit the 15,000 daily API limit |
| "CUSTOMER_NOT_FOUND" | Remove dashes from Customer ID: use `1234567890` not `123-456-7890` |

### Getting Your IDs — Quick Reference

| What You Need | Where to Find It |
|---------------|------------------|
| **Customer ID** | Google Ads → top-right corner (10 digits, e.g., `123-456-7890`) |
| **MCC ID** | Google Ads Manager account → top-right corner |
| **Developer Token** | Google Ads → Tools → API Center |
| **OAuth Client ID** | GCP Console → APIs → Credentials → OAuth 2.0 Client IDs |
| **OAuth Client Secret** | Same place as Client ID |
| **Refresh Token** | `generate_refresh_token.py` (step 5) |
| **Merchant Center ID** | Merchant Center → Settings → Account info |
| **Campaign ID** | Google Ads → Campaigns → click campaign → ID in URL bar |
| **PSI API Key** | GCP Console → APIs → Credentials → API Keys → Create |

---

## License

MIT License — free to use, modify, and distribute with attribution.

## Author

**God_FatherAI** — [x.com/God_FatherAI](https://x.com/God_FatherAI)

## Support

- **Issues & Bugs** — Open a GitHub issue
- **Feature Requests** — GitHub Discussions
- **Contact** — [x.com/God_FatherAI](https://x.com/God_FatherAI)

---

**Version:** 1.1.1 | **Last Updated:** October 2026 | **Status:** Production Ready
