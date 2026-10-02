#!/usr/bin/env python3
"""Verify a Google Ads MCP Universal installation end to end.

Checks, in order:
  1. Python version and required packages (run this with the SAME interpreter your MCP config uses).
  2. Configuration: config.json location, customer ID, credential variables (names only, values are never printed).
  3. MCP handshake: starts the server over stdio exactly like Claude does (cwd "/"), sends initialize + tools/list.
  4. --live: one read-only Google Ads query (customer id, name, currency) through the server's GAQL tool.

Usage:
  .venv/bin/python verify_install.py            # steps 1-3 (no Google API call)
  .venv/bin/python verify_install.py --live     # also step 4 (needs credentials)

Exit code 0 when every requested check passes.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SERVER = ROOT / "skills" / "google-mcp-universal" / "google_ads_mcp.py"
REQUIRED_ENV = ["GOOGLE_ADS_DEVELOPER_TOKEN", "GOOGLE_ADS_CLIENT_ID", "GOOGLE_ADS_CLIENT_SECRET", "GOOGLE_ADS_REFRESH_TOKEN"]
OPTIONAL_ENV = ["GOOGLE_ADS_CUSTOMER_ID", "GOOGLE_ADS_LOGIN_CUSTOMER_ID", "MERCHANT_CENTER_REFRESH_TOKEN",
                "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "PSI_API_KEY", "REPOS_PATH", "GOOGLE_ADS_PLUGIN_CONFIG"]
PACKAGES = [("fastmcp", "fastmcp"), ("google.ads.googleads", "google-ads"), ("googleapiclient", "google-api-python-client"),
            ("google.auth", "google-auth"), ("google_auth_oauthlib", "google-auth-oauthlib"), ("pydantic", "pydantic"),
            ("httpx", "httpx"), ("dotenv", "python-dotenv")]
EXPECTED_MIN_TOOLS = 186
CLIENT_ENV = dict(os.environ)  # what the MCP client would pass; the server must find .env on its own
HINTS = {
    "DEVELOPER_TOKEN_NOT_APPROVED": "The developer token has only test access: apply for Basic access in Google Ads API Center, or use a test account.",
    "DEVELOPER_TOKEN_PROHIBITED": "The developer token belongs to another Google Cloud project than the OAuth client.",
    "USER_PERMISSION_DENIED": "Set GOOGLE_ADS_LOGIN_CUSTOMER_ID to the manager (MCC) account that owns this customer, or grant the OAuth user access.",
    "CUSTOMER_NOT_FOUND": "Check account.customer_id in config.json (10 digits, no dashes).",
    "invalid_grant": "The refresh token is expired or revoked (Testing-mode consent screens expire tokens after 7 days): run generate_refresh_token.py again.",
    "unauthorized_client": "The refresh token was created with a different OAuth client ID/secret.",
    "invalid_client": "GOOGLE_ADS_CLIENT_ID / GOOGLE_ADS_CLIENT_SECRET do not match an existing OAuth client: copy them again from Google Cloud → APIs & Services → Credentials.",
    "Missing": "A required credential is missing: check .env next to verify_install.py or the env block of your MCP config.",
    "NOT_ADS_USER": "The Google account used for OAuth has no Google Ads access.",
}

ok_all = True


def report(ok, label, detail=""):
    global ok_all
    ok_all &= bool(ok)
    print(f"[{'OK' if ok else 'FAIL'}] {label}" + (f" — {detail}" if detail else ""))


def check_python_and_packages():
    v = sys.version_info
    report((3, 12) <= (v.major, v.minor) < (3, 15), "Python version", f"{v.major}.{v.minor}.{v.micro} at {sys.executable} (supported: 3.12–3.14)")
    from importlib import metadata, import_module
    for module, dist in PACKAGES:
        try:
            import_module(module)
            report(True, f"package {dist}", metadata.version(dist))
        except Exception as e:  # noqa: BLE001 - report any import problem
            report(False, f"package {dist}", f"{type(e).__name__}: {e} → run: {sys.executable} -m pip install -r {ROOT / 'requirements.txt'}")


def check_config():
    try:
        from dotenv import load_dotenv
        env_file = ROOT / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            mode = oct(env_file.stat().st_mode & 0o777)
            report(True, ".env file", f"{env_file} (permissions {mode}{'' if mode in ('0o600', '0o400') else ' — consider chmod 600'})")
        else:
            print(f"[INFO] no .env file at {env_file} (fine if the MCP client config passes the variables)")
    except Exception as e:  # noqa: BLE001
        report(False, ".env loading", str(e))
    sys.path.insert(0, str(SERVER.parent))
    try:
        import accounts_config as ac
        cfg = ac.load_config(force_reload=True)
        path = ac._find_config_path()
        if path:
            report(True, "config.json", str(path))
        else:
            report(False, "config.json", f"not found — run: {sys.executable} {ROOT / 'setup_account.py'} (demo mode is active)")
        cid = ac.get_customer_id().replace("-", "")
        report(cid.isdigit() and len(cid) == 10, "customer ID", cid or "missing (account.customer_id or GOOGLE_ADS_CUSTOMER_ID)")
        if cfg.get("_demo_mode"):
            print("[INFO] demo configuration in use: tools load, but account-specific defaults are empty")
    except Exception as e:  # noqa: BLE001
        report(False, "config loader", f"{type(e).__name__}: {e}")
    for name in REQUIRED_ENV:
        report(bool(os.environ.get(name)), f"env {name}", "set" if os.environ.get(name) else "missing")
    for name in OPTIONAL_ENV:
        print(f"[INFO] env {name}: {'set' if os.environ.get(name) else 'not set (optional)'}")


class StdioClient:
    def __init__(self, python):
        env = dict(CLIENT_ENV)
        # Claude starts servers from an arbitrary working directory; reproduce that.
        self.proc = subprocess.Popen([python, str(SERVER)], cwd="/", env=env, stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        self.stderr = []
        threading.Thread(target=lambda: self.stderr.extend(self.proc.stderr), daemon=True).start()
        self.next_id = 0

    def request(self, method, params=None, timeout=90):
        self.next_id += 1
        rid = self.next_id
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}}) + "\n")
        self.proc.stdin.flush()
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                if self.proc.poll() is not None:
                    raise RuntimeError("server exited: " + "".join(self.stderr[-15:]))
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                continue  # stray non-protocol output
            if msg.get("id") == rid:
                if "error" in msg:
                    raise RuntimeError(json.dumps(msg["error"]))
                return msg.get("result", {})
        raise TimeoutError(f"no answer to {method} within {timeout}s")

    def notify(self, method):
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method}) + "\n")
        self.proc.stdin.flush()

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.terminate()
            self.proc.wait(timeout=10)
        except Exception:  # noqa: BLE001
            self.proc.kill()


def check_server(live):
    client = StdioClient(sys.executable)
    try:
        init = client.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                             "clientInfo": {"name": "verify_install", "version": "1"}}, timeout=120)
        report(True, "MCP initialize", f"server {init.get('serverInfo', {}).get('name', '?')}, protocol {init.get('protocolVersion', '?')}")
        client.notify("notifications/initialized")
        names, cursor = [], None
        while True:
            res = client.request("tools/list", {"cursor": cursor} if cursor else {})
            names += [t["name"] for t in res.get("tools", [])]
            cursor = res.get("nextCursor")
            if not cursor:
                break
        report(len(names) >= EXPECTED_MIN_TOOLS, "MCP tools/list", f"{len(names)} tools (expected ≥ {EXPECTED_MIN_TOOLS})")
        res = client.request("tools/call", {"name": "google_ads_setup_status", "arguments": {}}, timeout=60)
        st = res.get("structuredContent") or {}
        if not st:
            try:
                st = json.loads("".join(c.get("text", "") for c in res.get("content", []) if c.get("type") == "text"))
            except ValueError:
                st = {}
        st = st.get("result", st) if isinstance(st, dict) else {}
        report(st.get("status") == "ready", "server sees its setup",
               f"status={st.get('status')}, config={st.get('config_file')}, .env={st.get('dotenv_file')}, "
               f"customer={st.get('customer_id')}, missing={st.get('missing_required')}")
        if live:
            sys.path.insert(0, str(SERVER.parent))
            import accounts_config as ac
            cid = ac.get_customer_id().replace("-", "")
            query = "SELECT customer.id, customer.descriptive_name, customer.currency_code, customer.time_zone FROM customer LIMIT 1"
            res = client.request("tools/call", {"name": "google_ads_execute_gaql",
                                                "arguments": {"request": {"customer_id": cid, "query": query, "page_size": 1}}}, timeout=120)
            text = "".join(c.get("text", "") for c in res.get("content", []) if c.get("type") == "text")
            data = res.get("structuredContent") or {}
            if not data:
                try:
                    data = json.loads(text)
                except ValueError:
                    data = {"raw": text[:500]}
            data = data.get("result", data) if isinstance(data, dict) else data
            if isinstance(data, dict) and data.get("status") == "success":
                row = (data.get("rows") or data.get("results") or [{}])[0]
                report(True, "Google Ads live query", json.dumps(row.get("customer", row), ensure_ascii=False)[:300])
            else:
                msg = json.dumps(data, ensure_ascii=False)[:600]
                hint = next((h for k, h in HINTS.items() if k in msg), "See INSTALL.md → Troubleshooting.")
                report(False, "Google Ads live query", f"{msg}\n       hint: {hint}")
    except Exception as e:  # noqa: BLE001
        report(False, "MCP server", f"{type(e).__name__}: {e}")
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true", help="also run one read-only Google Ads query")
    args = parser.parse_args()
    print(f"Google Ads MCP Universal — install check ({ROOT})\n")
    check_python_and_packages()
    check_config()
    check_server(args.live)
    print("\nRESULT:", "ALL CHECKS PASSED" if ok_all else "SOME CHECKS FAILED")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
