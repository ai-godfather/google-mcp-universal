# Installing Google Ads MCP Universal — step by step

This guide is written so that an **AI assistant (Claude Code or Claude Desktop) can install the plugin for a user on a clean computer**. Steps only a person can do (signing in to Google, copying a token from a Google screen) are marked **👤 user**. Everything else the assistant can run itself.

Verified on macOS with Python 3.12 and 3.14 for release 1.1.0 (fresh virtualenv, `claude mcp list` → ✔ Connected, 186 tools).

---

## 0. What gets installed

| Part | What it gives | Needed for |
|------|---------------|-----------|
| **MCP server** `skills/google-mcp-universal/google_ads_mcp.py` (Python, stdio) | 186 Google Ads / Merchant Center / PageSpeed tools | Claude Code **and** Claude Desktop — required |
| **Claude Code plugin** (`.claude-plugin/`, `commands/`, `skills/`) | 51 slash commands + 2 skills (`google-mcp-universal`, `google-ads-search-launch`) | Claude Code only — optional |

Where things live (all inside the cloned folder, called **PLUGIN_ROOT** below):

| File | Contains | Committed? |
|------|----------|-----------|
| `.venv/` | Python virtualenv with the dependencies | no |
| `.env` | **secrets**: developer token, OAuth client ID/secret, refresh token, optional API keys (chmod 600) | no (gitignored) |
| `config.json` | account settings: customer ID, MCC ID, markets, domains, Merchant Center IDs (no secrets) | no (gitignored) |

The server reads `PLUGIN_ROOT/.env` and `PLUGIN_ROOT/config.json` itself, wherever Claude starts it from, so **Claude configs never need to contain secrets**.

### Rules for the assistant

- Never ask the user to paste a secret into the chat, never print `.env`, `config.json` values that are secrets, or the OAuth client JSON. Let the user type secrets into `.env` themselves or into the hidden prompts of the scripts.
- Keep `.env` at permission 600 and never commit it.
- After installation, all mutating tools still default to dry-run / validate-only: show the user what would change and wait for an explicit "yes" before applying anything to a live account.

---

## 1. Prerequisites

| Item | Check | Install |
|------|-------|---------|
| git | `git --version` | macOS: `xcode-select --install` · Windows: git-scm.com |
| Python **3.12, 3.13 or 3.14** | `python3 --version` (Windows: `py -0`) | macOS: `brew install python@3.13` · Ubuntu: `sudo apt install python3.12 python3.12-venv` · Windows: python.org (tick "Add to PATH") |
| Claude Code and/or Claude Desktop | `claude --version` | claude.com/download |

**👤 user** must have: a Google account with access to the Google Ads account (Standard or Admin), ideally a manager (MCC) account for the developer token, and — optional — Merchant Center access.

---

## 2. Get the code and the dependencies

macOS / Linux:

```bash
git clone https://github.com/ai-godfather/google-mcp-universal.git ~/google-mcp-universal
cd ~/google-mcp-universal
bash install.sh --no-wizard        # creates .venv, installs requirements.txt, runs verify_install.py
```

Windows (PowerShell):

```powershell
git clone https://github.com/ai-godfather/google-mcp-universal.git $HOME\google-mcp-universal
cd $HOME\google-mcp-universal
py -3.13 -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python verify_install.py
```

Expected at this point: `[OK] MCP tools/list — 186 tools`. The `[FAIL]` lines for config.json and credentials are normal until steps 3–4 are done.

> Below, `PY` means the virtualenv interpreter: `~/google-mcp-universal/.venv/bin/python` (macOS/Linux) or `$HOME\google-mcp-universal\.venv\Scripts\python.exe` (Windows). Always use it — the system Python has no dependencies.

---

## 3. Google credentials

| Value | Where it comes from | Who | Stored in |
|-------|--------------------|-----|-----------|
| Developer token | Google Ads → Tools → **API Center** (in the manager/MCC account) | 👤 user | `.env` `GOOGLE_ADS_DEVELOPER_TOKEN` |
| OAuth client (Desktop app) | Google Cloud Console → Credentials → Download JSON | 👤 user downloads, assistant passes the file path | `.env` `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET` (written by the script) |
| Refresh token | `generate_refresh_token.py` + 👤 Google sign-in in the browser | assistant runs, 👤 user signs in | `.env` `GOOGLE_ADS_REFRESH_TOKEN` (written by the script, never printed) |
| Customer ID | Google Ads, top right (`123-456-7890`) | user tells the assistant (not secret) | `config.json` `account.customer_id` |
| Manager (MCC) ID | only if the user's login reaches the account through a manager | user tells the assistant | `config.json` `account.mcc_id` (or `.env` `GOOGLE_ADS_LOGIN_CUSTOMER_ID`) |

### 3.1 Google Cloud project and APIs (👤 user, ~5 min)

1. https://console.cloud.google.com/ → create or pick a project.
2. Enable: **Google Ads API**, **Content API for Shopping** (Merchant Center tools) and optionally **PageSpeed Insights API** — APIs & Services → Library.

### 3.2 OAuth consent screen (👤 user)

1. APIs & Services → **OAuth consent screen** → User type **External** (or **Internal** for a Google Workspace organisation).
2. Add scopes `https://www.googleapis.com/auth/adwords` and `https://www.googleapis.com/auth/content`.
3. Add the user's Google account as a test user, then **Publish app → In production**.
   **Why:** while the screen is in *Testing*, Google expires refresh tokens after 7 days. For an unverified app Google shows a warning at sign-in: *Advanced → Go to (app)* is fine for your own account.

### 3.3 OAuth client (👤 user)

APIs & Services → **Credentials** → Create credentials → **OAuth client ID** → Application type **Desktop app** → Create → **Download JSON**. Note where the file is saved (e.g. `~/Downloads/client_secret_….json`) and give the assistant **only the path**.

### 3.4 Developer token (👤 user)

Google Ads (manager account recommended) → Tools → **API Center** → copy the developer token.
- *Test access* works only with Google Ads **test** accounts. For a real account apply for **Basic access** (API Center → Apply; usually 1–3 business days).
- The user adds it to `PLUGIN_ROOT/.env` (create from the template if needed):

```bash
cd ~/google-mcp-universal && [ -f .env ] || cp .env.example .env; chmod 600 .env
open -e .env        # macOS (Linux: nano .env · Windows: notepad .env) → GOOGLE_ADS_DEVELOPER_TOKEN=…
```

### 3.5 Refresh token (assistant runs, 👤 user signs in)

```bash
PY generate_refresh_token.py --client-secrets ~/Downloads/client_secret_XXXX.json
```

A browser tab opens on the user's computer. **👤 user** signs in with the Google account that has access to Google Ads (and Merchant Center), accepts, and returns. The script writes the client ID, client secret and refresh token into `.env` (chmod 600) and prints only "Saved …".
- No browser (SSH/remote): add `--no-browser` and open the printed URL **on the same computer**.
- Ads only, no Merchant Center: add `--ads-only`.
- Run in an interactive terminal, the script also asks (hidden input) for a missing developer token and MCC ID.

---

## 4. Account settings (`config.json`)

The user tells the assistant the non-secret values; the assistant runs:

```bash
PY setup_account.py --customer-id 1234567890 --mcc-id 9876543210 \
    --markets US,DE --domain US=us.example.com --domain DE=de.example.com \
    --merchant US=111111111 --merchant DE=333333333 \
    --brand "Your Brand" --company "Your Company"
PY setup_account.py --validate
```

Or interactively: `PY setup_account.py`. Leave out `--mcc-id` when the login has direct access.

Optional sections (edit `config.json`; templates in `config.example.json`): `xml_feeds`, `np_feeds`, `country_config`, `campaign_defaults`, `search_launch` (reviewed Search delivery rules), `shopping_coverage.stores`, `backend_conversions`, `endpoints.labelizer_push`, `whitelist`, `search_terms.brand_tokens`, `np_roadmap`, `global_dashboard`, `store_feeds` / `country_domains` / `country_to_repo` / `pagespeed.no_repo_countries` (PageSpeed). Everything works without them; the related tools explain what to add when they need it.

---

## 5. Verify with a live call

```bash
PY verify_install.py --live
```

Expected last lines:

```
[OK] MCP tools/list — 186 tools (expected ≥ 186)
[OK] server sees its setup — status=ready, config=…/config.json, .env=…/.env, customer=1234567890, missing=[]
[OK] Google Ads live query — {"id": "1234567890", "descriptive_name": "…", "currency_code": "…"}
RESULT: ALL CHECKS PASSED
```

The live query is read-only (`SELECT customer.id, … FROM customer LIMIT 1`). If it fails, the script prints a hint — see [Troubleshooting](#9-troubleshooting).

---

## 6. Connect Claude

### 6.1 Claude Code (CLI or the desktop app's Code tab)

macOS / Linux (absolute paths, user scope = available in every project):

```bash
cd ~/google-mcp-universal
claude mcp add --scope user --transport stdio google-ads -- "$PWD/.venv/bin/python" "$PWD/skills/google-mcp-universal/google_ads_mcp.py"
claude mcp list            # google-ads: … - ✔ Connected
```

Windows (PowerShell):

```powershell
cd $HOME\google-mcp-universal
claude mcp add --scope user --transport stdio google-ads -- "$PWD\.venv\Scripts\python.exe" "$PWD\skills\google-mcp-universal\google_ads_mcp.py"
claude mcp list
```

(`bash install.sh --register-claude-code` does the same.) Start a new session; `/mcp` shows the server and its tools (named `mcp__google-ads__…`). Remove with `claude mcp remove google-ads -s user`.

### 6.2 Claude Desktop (chat app)

Edit the config file (Claude Desktop → Settings → Developer → *Edit Config*), add the server, then **fully quit and reopen** Claude Desktop:

| OS | File |
|----|------|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| Linux | `~/.config/Claude/claude_desktop_config.json` |

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

Windows paths use double backslashes: `"C:\\Users\\NAME\\google-mcp-universal\\.venv\\Scripts\\python.exe"`. Keep any other servers already in `mcpServers`. On macOS/Linux `bash install.sh --register-claude-desktop` adds the entry (and keeps a backup). No `env` block is needed — the server reads `.env`.

### 6.3 Slash commands and skills (Claude Code plugin, optional)

```bash
claude plugin marketplace add ai-godfather/google-mcp-universal
claude plugin install google-mcp-universal@ai-godfather
claude plugin list          # google-mcp-universal@ai-godfather — ✔ enabled
```

Inside a session the same works with `/plugin marketplace add ai-godfather/google-mcp-universal` and `/plugin install google-mcp-universal@ai-godfather`. Commands are namespaced, e.g. `/google-mcp-universal:ads-health`. The plugin only adds commands and skills — the MCP server from 6.1 is still required. Update later with `claude plugin marketplace update ai-godfather` and `claude plugin update google-mcp-universal@ai-godfather`.

---

## 7. First use

Ask Claude:

1. "Call `google_ads_setup_status`" → `status: ready`, no `missing_required`.
2. "Run GAQL `SELECT campaign.id, campaign.name, campaign.status FROM campaign LIMIT 10`" (tool `google_ads_execute_gaql`).
3. With the plugin: `/google-mcp-universal:ads-health` or `/google-mcp-universal:ads-report`.

---

## 8. Updating

```bash
cd ~/google-mcp-universal && git pull
PY -m pip install -r requirements.txt
PY verify_install.py --live
```

Then restart the client: Claude Code — new session (or `/mcp` → reconnect); Claude Desktop — quit and reopen. Plugin: `claude plugin marketplace update ai-godfather && claude plugin update google-mcp-universal@ai-godfather`.

---

## 9. Troubleshooting

| Symptom / error text | Cause | Fix |
|----------------------|-------|-----|
| `tools/list` fails, "server exited", `ModuleNotFoundError` | MCP config points at a Python without the dependencies | Use the **virtualenv** interpreter path (`.venv/bin/python` / `.venv\Scripts\python.exe`) |
| `missing_required` in `google_ads_setup_status`, "Missing … credentials" | `.env` absent, empty, or not in PLUGIN_ROOT | Put `.env` next to `verify_install.py`; fill it (step 3) |
| `invalid_grant` | Refresh token expired/revoked — consent screen still in *Testing* (7-day expiry) or the user removed access | Publish the consent screen (3.2), run `generate_refresh_token.py` again |
| `invalid_client` / `unauthorized_client` | Client ID/secret don't match the client that issued the token | Regenerate with the right JSON (3.3 → 3.5) |
| "No refresh token returned" | Google reused an earlier consent | Remove the app at https://myaccount.google.com/permissions, run the script again |
| `DEVELOPER_TOKEN_NOT_APPROVED` | Test-level token used on a real account | Apply for Basic access, or test with a Google Ads test account |
| `DEVELOPER_TOKEN_PROHIBITED` | Token belongs to a different Google Cloud project | Use an OAuth client from the token's project |
| `USER_PERMISSION_DENIED` | Access goes through a manager account but no MCC ID is set | `--mcc-id` in `setup_account.py` (or `GOOGLE_ADS_LOGIN_CUSTOMER_ID` in `.env`) |
| `CUSTOMER_NOT_FOUND` | Wrong customer ID | 10 digits without dashes in `config.json` |
| Merchant Center `403` / insufficient scopes | Token created with `--ads-only`, or no MC access | Re-run the generator without `--ads-only`; user needs Merchant Center access |
| PageSpeed tools fail | No API key | `PSI_API_KEY=` in `.env` (Google Cloud → Credentials → API key) |
| Claude Desktop shows no tools | JSON syntax error or app not fully restarted | Validate the JSON, quit (Cmd+Q / tray → Quit) and reopen; logs: `~/Library/Logs/Claude/mcp*.log` (macOS), `%APPDATA%\Claude\logs` (Windows) |
| `externally-managed-environment` from pip | Installing into the system Python | Use the virtualenv (step 2) |

Still stuck: run `PY verify_install.py --live` and read the first `[FAIL]` line — each one names the file or setting to fix.
