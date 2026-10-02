#!/usr/bin/env python3
"""Create the OAuth refresh token for Google Ads (and Merchant Center) and store it in .env.

The token is written straight into the .env file next to this script (permissions 600) and is NEVER printed,
so an assistant can run this for the user without seeing any secret.

Before running:
  1. In Google Cloud Console create an OAuth client of type "Desktop app" and download its JSON
     (APIs & Services → Credentials → your client → Download JSON).
  2. Publish the OAuth consent screen ("In production"); in "Testing" mode Google expires refresh tokens after 7 days.

Usage:
  .venv/bin/python generate_refresh_token.py --client-secrets ~/Downloads/client_secret_XXXX.json
  .venv/bin/python generate_refresh_token.py               # reuses GOOGLE_ADS_CLIENT_ID/SECRET already in .env
  .venv/bin/python generate_refresh_token.py --no-browser  # print the sign-in URL instead of opening a browser
  .venv/bin/python generate_refresh_token.py --ads-only    # skip the Merchant Center (content) scope

A browser window opens; sign in with the Google account that has access to the Google Ads account
(and Merchant Center), approve, and return to the terminal.
"""
import argparse
import getpass
import json
import os
import stat
import sys
from pathlib import Path

# Windows consoles and pipes default to a legacy code page (cp1252); print UTF-8 everywhere.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
SCOPE_ADS = "https://www.googleapis.com/auth/adwords"
SCOPE_CONTENT = "https://www.googleapis.com/auth/content"


def read_env(path):
    lines, values = [], {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            lines.append(line)
            if "=" in line and not line.lstrip().startswith("#"):
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip().strip('"').strip("'")
    return lines, values


def write_env(path, lines, updates):
    """Replace or append KEY=VALUE lines; keep comments and unrelated keys; chmod 600."""
    seen, out = set(), []
    for line in lines:
        key = line.partition("=")[0].strip() if "=" in line and not line.lstrip().startswith("#") else None
        if key in updates:
            out.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            out.append(line)
    for key, value in updates.items():
        if key not in seen:
            out.append(f"{key}={value}")
    path.touch(exist_ok=True)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    path.write_text("\n".join(out).rstrip("\n") + "\n", encoding="utf-8")
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def client_from_json(path):
    data = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    section = data.get("installed") or data.get("web")
    if not section:
        sys.exit("The JSON is not an OAuth client file (expected an 'installed' or 'web' section).")
    if "web" in data:
        print("Note: this is a 'Web application' client. A 'Desktop app' client is recommended; a web client "
              "needs http://localhost:<port>/ registered as an authorized redirect URI.")
    return section["client_id"], section["client_secret"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--client-secrets", help="OAuth client JSON downloaded from Google Cloud (Desktop app)")
    parser.add_argument("--no-browser", action="store_true", help="print the sign-in URL instead of opening a browser")
    parser.add_argument("--port", type=int, default=0, help="local redirect port (default: any free port)")
    parser.add_argument("--ads-only", action="store_true", help="request only the Google Ads scope")
    parser.add_argument("--timeout", type=int, default=600, help="seconds to wait for the browser sign-in (default 600)")
    args = parser.parse_args()

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        sys.exit(f"Missing package google-auth-oauthlib. Run: {sys.executable} -m pip install -r {ROOT / 'requirements.txt'}")

    lines, current = read_env(ENV_PATH)
    if args.client_secrets:
        client_id, client_secret = client_from_json(args.client_secrets)
    else:
        client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID") or current.get("GOOGLE_ADS_CLIENT_ID", "")
        client_secret = os.environ.get("GOOGLE_ADS_CLIENT_SECRET") or current.get("GOOGLE_ADS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        sys.exit("No OAuth client: pass --client-secrets <downloaded JSON> (or put GOOGLE_ADS_CLIENT_ID/SECRET in .env).")

    scopes = [SCOPE_ADS] if args.ads_only else [SCOPE_ADS, SCOPE_CONTENT]
    flow = InstalledAppFlow.from_client_config(
        {"installed": {"client_id": client_id, "client_secret": client_secret,
                       "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                       "token_uri": "https://oauth2.googleapis.com/token",
                       "redirect_uris": ["http://localhost"]}},
        scopes=scopes)
    print("Opening Google sign-in. Use the Google account that has access to the Google Ads account"
          + ("" if args.ads_only else " and Merchant Center") + ".")
    try:
        creds = flow.run_local_server(
            port=args.port, open_browser=not args.no_browser, timeout_seconds=args.timeout,
            authorization_prompt_message="If no browser opened, visit this URL on THIS computer:\n{url}\n",
            success_message="Authorization complete. You can close this tab and return to the terminal.",
            access_type="offline", prompt="consent")
    except Exception as e:  # noqa: BLE001 - surface any OAuth/browser problem plainly
        sys.exit(f"Sign-in failed: {type(e).__name__}: {e}")
    if not creds or not creds.refresh_token:
        sys.exit("Google returned no refresh token. Remove the app at https://myaccount.google.com/permissions and run again.")

    updates = {"GOOGLE_ADS_CLIENT_ID": client_id, "GOOGLE_ADS_CLIENT_SECRET": client_secret,
               "GOOGLE_ADS_REFRESH_TOKEN": creds.refresh_token}
    # Ask for the remaining credentials only on an interactive terminal; values are typed hidden.
    interactive = sys.stdin.isatty()
    if not (os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or current.get("GOOGLE_ADS_DEVELOPER_TOKEN")):
        token = getpass.getpass("Google Ads developer token (hidden, Enter to skip): ").strip() if interactive else ""
        updates["GOOGLE_ADS_DEVELOPER_TOKEN"] = token
    if "GOOGLE_ADS_LOGIN_CUSTOMER_ID" not in current and interactive:
        mcc = input("Manager (MCC) account ID used for access, digits only (Enter if none): ").strip().replace("-", "")
        if mcc:
            updates["GOOGLE_ADS_LOGIN_CUSTOMER_ID"] = mcc
    write_env(ENV_PATH, lines, updates)

    print(f"Saved OAuth client and refresh token to {ENV_PATH} (permissions 600; values not shown).")
    if not updates.get("GOOGLE_ADS_DEVELOPER_TOKEN", current.get("GOOGLE_ADS_DEVELOPER_TOKEN")):
        print("Still missing: GOOGLE_ADS_DEVELOPER_TOKEN — the user adds it to .env (Google Ads → Tools → API Center).")
    print(f"Next: {sys.executable} {ROOT / 'verify_install.py'} --live")


if __name__ == "__main__":
    main()
