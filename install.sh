#!/bin/bash
# Google Ads Universal MCP — Auto Install Script for macOS/Linux
# Run from the plugin folder: bash install.sh
#
# Author: God_FatherAI (https://x.com/God_FatherAI)
# Repo:   https://github.com/ai-godfather/google-mcp-universal

set -e

echo "=========================================="
echo "  Google Ads Universal MCP — Installer"
echo "=========================================="
echo ""

# 1. Check Python
if ! command -v python3 &>/dev/null; then
    echo "[ERROR] Python 3 is not installed."
    echo "  Install from https://www.python.org/downloads/"
    exit 1
fi

PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "[OK] Python $PY_VER detected"

# 2. Install dependencies
echo ""
echo "[1/4] Installing Python dependencies..."
pip3 install fastmcp google-ads python-dotenv "pydantic>=2.0" google-api-python-client google-auth httpx

# 3. Get absolute path to this directory
MCP_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILLS_DIR="$MCP_DIR/skills/google-ads-manager"
echo "[2/4] Plugin directory: $MCP_DIR"

# 4. Run interactive config setup (if no config.json yet)
if [ ! -f "$MCP_DIR/config.json" ]; then
    echo ""
    echo "[3/4] No config.json found — running setup wizard..."
    python3 "$MCP_DIR/setup_account.py"
else
    echo "[3/4] config.json already exists — skipping setup."
    echo "  (Run 'python3 setup_account.py' to reconfigure)"
fi

# 5. Configure Claude Desktop (macOS)
CONFIG_DIR="$HOME/Library/Application Support/Claude"
CONFIG_FILE="$CONFIG_DIR/claude_desktop_config.json"

echo ""
echo "[4/4] Configuring Claude Desktop..."

mkdir -p "$CONFIG_DIR"

# Backup existing config
if [ -f "$CONFIG_FILE" ]; then
    cp "$CONFIG_FILE" "$CONFIG_FILE.backup"
    echo "  Backed up existing config to claude_desktop_config.json.backup"
fi

python3 -c "
import json, os

config_file = '$CONFIG_FILE'
mcp_dir = '$SKILLS_DIR'

if os.path.exists(config_file):
    with open(config_file) as f:
        config = json.load(f)
else:
    config = {}

if 'mcpServers' not in config:
    config['mcpServers'] = {}

# Read env vars from config.json if present
plugin_config = '$MCP_DIR/config.json'
env_vars = {}
if os.path.exists(plugin_config):
    with open(plugin_config) as f:
        pc = json.load(f)
    acct = pc.get('account', {})
    if acct.get('customer_id'):
        env_vars['GOOGLE_ADS_CUSTOMER_ID'] = acct['customer_id']
    if acct.get('mcc_id'):
        env_vars['GOOGLE_ADS_LOGIN_CUSTOMER_ID'] = acct['mcc_id']
    if acct.get('developer_token'):
        env_vars['GOOGLE_ADS_DEVELOPER_TOKEN'] = acct['developer_token']
    env_vars['GOOGLE_ADS_PLUGIN_CONFIG'] = plugin_config

server_entry = {
    'command': 'python3',
    'args': [f'{mcp_dir}/google_ads_mcp.py']
}
if env_vars:
    server_entry['env'] = env_vars

config['mcpServers']['google-ads'] = server_entry

with open(config_file, 'w') as f:
    json.dump(config, f, indent=2)
print('  Config updated successfully')
"

echo ""
echo "=========================================="
echo "  SUCCESS! Google Ads Universal MCP installed."
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Set up Google Ads API OAuth credentials"
echo "     (see README_INSTALL_MCP_WINDOWS.txt for details)"
echo "  2. Edit config.json with your account details"
echo "  3. Restart Claude Desktop to activate"
echo ""
echo "Test commands in Claude Desktop:"
echo "  /ads-health"
echo "  /ads-report"
echo "  \"List my Google Ads campaigns\""
echo "=========================================="
