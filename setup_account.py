#!/usr/bin/env python3
"""
Google Ads Universal Plugin — Account Setup Wizard
==================================================

Creates config.json (account settings, NOT secrets) next to this script.
Secrets (developer token, OpenAI key) are written to .env (permissions 600), never to config.json.
OAuth client + refresh token: run generate_refresh_token.py.

Usage:
    python setup_account.py                      # interactive wizard
    python setup_account.py --validate           # validate existing config.json
    python setup_account.py --demo               # create a demo config.json
    python setup_account.py --customer-id 1234567890 --markets US,DE \\
        --domain US=us.example.com --domain DE=de.example.com \\
        --merchant US=111111111 --mcc-id 9876543210 --brand "VitaBoost" --company "Acme Ltd"
                                                 # non-interactive (for assistants); no secrets on the command line
"""

import argparse
import getpass
import json
import sys
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent
CONFIG_PATH = PLUGIN_DIR / "config.json"
EXAMPLE_PATH = PLUGIN_DIR / "config.example.json"
sys.path.insert(0, str(PLUGIN_DIR))
from generate_refresh_token import ENV_PATH, read_env, write_env  # noqa: E402  (same folder)


def load_example() -> dict:
    """Load config.example.json as template."""
    if EXAMPLE_PATH.exists():
        with open(EXAMPLE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def prompt(question: str, default: str = "", required: bool = False) -> str:
    """Prompt user for input with optional default."""
    suffix = f" [{default}]" if default else ""
    suffix += " (required)" if required and not default else ""
    while True:
        answer = input(f"  {question}{suffix}: ").strip()
        if not answer and default:
            return default
        if not answer and required:
            print("    ⚠ This field is required.")
            continue
        return answer


def prompt_yn(question: str, default: bool = True) -> bool:
    """Yes/No prompt."""
    suffix = " [Y/n]" if default else " [y/N]"
    answer = input(f"  {question}{suffix}: ").strip().lower()
    if not answer:
        return default
    return answer in ("y", "yes", "1", "true")


def save_secret(key: str, value: str):
    """Store a secret in .env (chmod 600). Never echoes the value."""
    if not value:
        return
    lines, _ = read_env(ENV_PATH)
    write_env(ENV_PATH, lines, {key: value})
    print(f"    → {key} saved to {ENV_PATH} (value not shown)")


def finish(config: dict):
    config.pop("_comment", None)
    config.pop("_instructions", None)
    config["_demo_mode"] = False
    config.setdefault("account", {}).pop("developer_token", None)   # secrets live in .env only
    config.setdefault("ai_copy", {}).pop("openai_api_key", None)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"  ✓ Config saved to: {CONFIG_PATH}")
    print()
    print("  Next steps:")
    print(f"  1. OAuth + refresh token:  {sys.executable} {PLUGIN_DIR / 'generate_refresh_token.py'} --client-secrets <client JSON>")
    print(f"  2. Check everything:       {sys.executable} {PLUGIN_DIR / 'verify_install.py'} --live")
    print("  3. Register the MCP server in Claude (see INSTALL.md, step 6)")
    print()


def setup_interactive():
    """Run interactive setup wizard."""
    print("\n" + "=" * 60)
    print("  Google Ads Universal Plugin — Setup Wizard")
    print("=" * 60)
    print()
    print("  This wizard creates config.json for your account.")
    print("  Press Enter to accept defaults shown in [brackets]. Secrets are typed hidden and saved to .env.")
    print()

    config = load_example()

    # --- Account ---
    print("─── ACCOUNT DETAILS ───")
    config["account"]["customer_id"] = prompt(
        "Google Ads Customer ID (e.g., 1234567890)", required=True
    ).replace("-", "")
    config["account"]["mcc_id"] = prompt(
        "MCC (Manager) Account ID used for access (leave empty if none)", default=""
    ).replace("-", "")
    config["account"]["company_name"] = prompt("Company name", default="My Company")
    config["account"]["brand_name"] = prompt("Brand name", default="My Brand")
    config["account"]["industry"] = prompt("Industry", default="e-commerce")
    config["account"]["alias"] = config["account"]["brand_name"].lower().replace(" ", "-")
    _, env_now = read_env(ENV_PATH)
    if not env_now.get("GOOGLE_ADS_DEVELOPER_TOKEN"):
        save_secret("GOOGLE_ADS_DEVELOPER_TOKEN",
                    getpass.getpass("  Google Ads API developer token (hidden, Enter to skip): ").strip())

    # --- Markets ---
    print("\n─── MARKETS ───")
    markets_str = prompt(
        "Active markets (comma-separated country codes, e.g., US,UK,DE)",
        default="US"
    )
    config["markets"] = [m.strip().upper() for m in markets_str.split(",") if m.strip()]

    # --- Domains ---
    print("\n─── SHOP DOMAINS ───")
    config["domains"] = {}
    for cc in config["markets"]:
        domain = prompt(f"  Shop domain for {cc}", default=f"{cc.lower()}.yourshop.com")
        config["domains"][cc] = domain

    # --- Merchant Center ---
    print("\n─── MERCHANT CENTER (optional) ───")
    if prompt_yn("Do you use Google Merchant Center?", default=True):
        config["merchant_center"]["merchant_ids"] = {}
        for cc in config["markets"]:
            mid = prompt(f"  Merchant ID for {cc} (empty to skip)")
            if mid:
                config["merchant_center"]["merchant_ids"][cc] = mid
    else:
        config["merchant_center"] = {"mca_ids": [], "merchant_ids": {}}

    # --- XML Feeds ---
    print("\n─── XML PRODUCT FEEDS (optional) ───")
    config["xml_feeds"] = {}
    if prompt_yn("Do you have XML product feeds?", default=False):
        for cc in config["markets"]:
            feed = prompt(f"  XML feed URL for {cc} (empty to skip)")
            if feed:
                config["xml_feeds"][cc] = feed

    # --- AI Copy ---
    print("\n─── AI AD COPY GENERATION (optional) ───")
    config["ai_copy"]["provider"] = "openai"
    config["ai_copy"]["fallback_to_templates"] = True
    if not env_now.get("OPENAI_API_KEY"):
        key = getpass.getpass("  OpenAI API key for AI ad copy (hidden, Enter = templates only): ").strip()
        save_secret("OPENAI_API_KEY", key)
        if key:
            config["ai_copy"]["model"] = prompt("OpenAI model", default=config["ai_copy"].get("model", "gpt-4o"))

    print(f"\n─── SAVING to {CONFIG_PATH} ───")
    finish(config)


def setup_from_args(args):
    """Non-interactive setup from command-line values (IDs and domains are not secrets)."""
    config = load_example()
    acc = config.setdefault("account", {})
    acc["customer_id"] = args.customer_id.replace("-", "")
    acc["mcc_id"] = (args.mcc_id or "").replace("-", "")
    acc["company_name"] = args.company or acc.get("company_name", "My Company")
    acc["brand_name"] = args.brand or acc.get("brand_name", "My Brand")
    acc["industry"] = args.industry or acc.get("industry", "e-commerce")
    acc["alias"] = acc["brand_name"].lower().replace(" ", "-")
    markets = [m.strip().upper() for m in (args.markets or "").split(",") if m.strip()]
    config["markets"] = markets
    config["domains"] = dict(pair.split("=", 1) for pair in args.domain)
    config["domains"] = {k.upper(): v for k, v in config["domains"].items()}
    mc = config.setdefault("merchant_center", {})
    mc["merchant_ids"] = {k.upper(): v for k, v in (pair.split("=", 1) for pair in args.merchant)}
    mc["mca_ids"] = [x for x in (args.mca_id or [])]
    config["xml_feeds"] = {k.upper(): v for k, v in (pair.split("=", 1) for pair in args.xml_feed)}
    if not markets:
        config["markets"] = sorted(set(config["domains"]) | set(mc["merchant_ids"]))
    finish(config)


def setup_demo():
    """Create demo config."""
    config = load_example()
    config["_demo_mode"] = True
    config["account"]["customer_id"] = ""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print(f"✓ Demo config created at: {CONFIG_PATH}")
    print("  Edit config.json to add your real account details.")


def validate_config():
    """Validate existing config.json."""
    if not CONFIG_PATH.exists():
        print("✗ No config.json found. Run: python setup_account.py")
        return False

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    errors = []
    warnings = []

    cid = str(config.get("account", {}).get("customer_id", ""))
    if not cid:
        errors.append("account.customer_id is empty — plugin cannot connect to Google Ads")
    elif not cid.replace("-", "").isdigit() or len(cid.replace("-", "")) != 10:
        errors.append(f"account.customer_id '{cid}' must be 10 digits")

    if not config.get("markets"):
        warnings.append("No markets defined — add country codes to 'markets' array")

    if not config.get("domains"):
        warnings.append("No domains defined — needed for batch product setup")

    if config.get("account", {}).get("developer_token") or config.get("ai_copy", {}).get("openai_api_key"):
        warnings.append("config.json contains secrets — move them to .env (GOOGLE_ADS_DEVELOPER_TOKEN, OPENAI_API_KEY)")

    _, env_now = read_env(ENV_PATH)
    for key in ("GOOGLE_ADS_DEVELOPER_TOKEN", "GOOGLE_ADS_CLIENT_ID", "GOOGLE_ADS_CLIENT_SECRET", "GOOGLE_ADS_REFRESH_TOKEN"):
        if not env_now.get(key):
            warnings.append(f"{key} not in .env (fine only if your MCP client config sets it)")

    if errors:
        print("✗ ERRORS:")
        for e in errors:
            print(f"  - {e}")
    if warnings:
        print("⚠ WARNINGS:")
        for w in warnings:
            print(f"  - {w}")
    if not errors and not warnings:
        print("✓ Config is valid!")

    return len(errors) == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--validate", action="store_true", help="validate existing config.json and .env")
    parser.add_argument("--demo", action="store_true", help="create a demo config.json")
    parser.add_argument("--customer-id", help="Google Ads customer ID (10 digits) — enables non-interactive mode")
    parser.add_argument("--mcc-id", help="manager (MCC) account ID used for access")
    parser.add_argument("--company", help="company name")
    parser.add_argument("--brand", help="brand name")
    parser.add_argument("--industry", help="industry (default e-commerce)")
    parser.add_argument("--markets", help="comma-separated country codes, e.g. US,DE")
    parser.add_argument("--domain", action="append", default=[], metavar="CC=HOST", help="shop domain per country (repeatable)")
    parser.add_argument("--merchant", action="append", default=[], metavar="CC=ID", help="Merchant Center ID per country (repeatable)")
    parser.add_argument("--mca-id", action="append", default=[], metavar="ID", help="Merchant Center multi-client account ID (repeatable)")
    parser.add_argument("--xml-feed", action="append", default=[], metavar="CC=URL", help="product feed URL per country (repeatable)")
    args = parser.parse_args()

    if args.validate:
        sys.exit(0 if validate_config() else 1)
    elif args.demo:
        setup_demo()
    elif args.customer_id:
        setup_from_args(args)
    else:
        setup_interactive()


if __name__ == "__main__":
    main()
