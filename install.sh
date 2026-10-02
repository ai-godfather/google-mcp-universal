#!/usr/bin/env bash
# Google Ads MCP Universal — installer for macOS / Linux.
#
# Creates .venv in this folder, installs requirements.txt, optionally runs the account wizard,
# optionally registers the MCP server in Claude Code and/or Claude Desktop, then runs verify_install.py.
# Secrets are never written into Claude configs: the server reads them from .env in this folder.
#
# Usage:
#   bash install.sh                              # venv + dependencies + wizard (if no config.json) + check
#   bash install.sh --register-claude-code       # also: claude mcp add --scope user google-ads …
#   bash install.sh --register-claude-desktop    # also: add "google-ads" to claude_desktop_config.json (backup kept)
#   bash install.sh --no-wizard --python python3.12
#
# Repo: https://github.com/ai-godfather/google-mcp-universal — full guide: INSTALL.md
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER="$ROOT/skills/google-mcp-universal/google_ads_mcp.py"
VENV="$ROOT/.venv"
PYTHON_ARG=""
REG_CODE=0
REG_DESKTOP=0
WIZARD=1

while [ $# -gt 0 ]; do
  case "$1" in
    --register-claude-code) REG_CODE=1 ;;
    --register-claude-desktop) REG_DESKTOP=1 ;;
    --no-wizard) WIZARD=0 ;;
    --python) PYTHON_ARG="${2:?--python needs a value}"; shift ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done

echo "== Google Ads MCP Universal — installer"
echo "   folder: $ROOT"

# 1. Python 3.12–3.14
pick_python() {
  for c in ${PYTHON_ARG:-} python3.13 python3.12 python3.14 python3; do
    if command -v "$c" >/dev/null 2>&1 && "$c" -c 'import sys; sys.exit(0 if (3,12) <= sys.version_info[:2] < (3,15) else 1)' 2>/dev/null; then
      command -v "$c"; return 0
    fi
  done
  return 1
}
if ! PY="$(pick_python)"; then
  echo "[ERROR] Python 3.12, 3.13 or 3.14 is required."
  echo "        macOS: brew install python@3.13   ·   Ubuntu/Debian: sudo apt install python3.12 python3.12-venv"
  exit 1
fi
echo "[1/5] Python: $PY ($("$PY" -c 'import sys; print(sys.version.split()[0])'))"

# 2. Virtual environment + dependencies (never pip-installs into the system Python)
if [ ! -x "$VENV/bin/python" ]; then
  "$PY" -m venv "$VENV"
fi
"$VENV/bin/python" -m pip install --quiet --upgrade pip
"$VENV/bin/python" -m pip install --quiet -r "$ROOT/requirements.txt"
echo "[2/5] Dependencies installed in $VENV"

# 3. Account settings (config.json) — interactive only on a real terminal
if [ -f "$ROOT/config.json" ]; then
  echo "[3/5] config.json exists — keeping it ($VENV/bin/python setup_account.py --validate to check)"
elif [ "$WIZARD" -eq 1 ] && [ -t 0 ]; then
  echo "[3/5] Running the account wizard…"
  "$VENV/bin/python" "$ROOT/setup_account.py"
else
  echo "[3/5] No config.json yet — create it with: $VENV/bin/python $ROOT/setup_account.py  (or --customer-id … flags)"
fi

# 4. MCP registration (command + script path only; credentials stay in .env)
if [ "$REG_CODE" -eq 1 ]; then
  if command -v claude >/dev/null 2>&1; then
    if claude mcp get google-ads >/dev/null 2>&1; then
      echo "[4/5] Claude Code already has a 'google-ads' server — remove it first: claude mcp remove google-ads -s user"
    else
      claude mcp add --scope user --transport stdio google-ads -- "$VENV/bin/python" "$SERVER"
      echo "[4/5] Registered in Claude Code (user scope). Check with: claude mcp list"
    fi
  else
    echo "[4/5] 'claude' CLI not found — install Claude Code, then run:"
    echo "      claude mcp add --scope user --transport stdio google-ads -- \"$VENV/bin/python\" \"$SERVER\""
  fi
fi
if [ "$REG_DESKTOP" -eq 1 ]; then
  case "$(uname -s)" in
    Darwin) DESKTOP_CFG="$HOME/Library/Application Support/Claude/claude_desktop_config.json" ;;
    *) DESKTOP_CFG="${XDG_CONFIG_HOME:-$HOME/.config}/Claude/claude_desktop_config.json" ;;
  esac
  mkdir -p "$(dirname "$DESKTOP_CFG")"
  [ -f "$DESKTOP_CFG" ] && cp "$DESKTOP_CFG" "$DESKTOP_CFG.backup-$(date +%Y%m%d%H%M%S)"
  DESKTOP_CFG="$DESKTOP_CFG" VENV_PY="$VENV/bin/python" SERVER="$SERVER" "$VENV/bin/python" - <<'PYEOF'
import json, os
path = os.environ["DESKTOP_CFG"]
cfg = json.load(open(path, encoding="utf-8")) if os.path.exists(path) and os.path.getsize(path) else {}
cfg.setdefault("mcpServers", {})["google-ads"] = {"command": os.environ["VENV_PY"], "args": [os.environ["SERVER"]]}
with open(path, "w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2)
print(f"[4/5] Registered in Claude Desktop: {path} — quit and reopen Claude Desktop")
PYEOF
fi
if [ "$REG_CODE" -eq 0 ] && [ "$REG_DESKTOP" -eq 0 ]; then
  echo "[4/5] Not registered in any Claude client (use --register-claude-code / --register-claude-desktop, see INSTALL.md)"
fi

# 5. Check (no Google API call; add --live after credentials are in .env)
echo "[5/5] Checking the installation…"
"$VENV/bin/python" "$ROOT/verify_install.py" || true

echo
echo "Next:"
echo "  • OAuth + refresh token: $VENV/bin/python $ROOT/generate_refresh_token.py --client-secrets <downloaded client JSON>"
echo "  • Developer token:       add GOOGLE_ADS_DEVELOPER_TOKEN=… to $ROOT/.env (chmod 600)"
echo "  • Live check:            $VENV/bin/python $ROOT/verify_install.py --live"
