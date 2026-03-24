#!/usr/bin/env python3
"""
Google Ads Universal Plugin — Interactive Account Setup Wizard
==============================================================

Run this script to configure the plugin for your Google Ads account.
It creates config.json with your account details.

Usage:
    python setup_account.py              # Interactive setup
    python setup_account.py --validate   # Validate existing config
    python setup_account.py --demo       # Create demo config
"""

import json
import os
import sys
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent
CONFIG_PATH = PLUGIN_DIR / "config.json"
EXAMPLE_PATH = PLUGIN_DIR / "config.example.json"


def load_example() -> dict:
    """Load config.example.json as template."""
    if EXAMPLE_PATH.exists():
        with open(EXAMPLE_PATH, "r") as f:
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


def setup_interactive():
    """Run interactive setup wizard."""
    print("\n" + "=" * 60)
    print("  Google Ads Universal Plugin — Setup Wizard")
    print("=" * 60)
    print()
    print("  This wizard will create config.json for your account.")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    config = load_example()

    # --- Account ---
    print("─── ACCOUNT DETAILS ───")
    config["account"]["customer_id"] = prompt(
        "Google Ads Customer ID (e.g., 1234567890)", required=True
    )
    config["account"]["mcc_id"] = prompt(
        "MCC (Manager) Account ID (leave empty if none)", default=""
    )
    config["account"]["company_name"] = prompt(
        "Company name", default="My Company"
    )
    config["account"]["brand_name"] = prompt(
        "Brand name", default="My Brand"
    )
    config["account"]["industry"] = prompt(
        "Industry", default="e-commerce"
    )
    config["account"]["developer_token"] = prompt(
        "Google Ads API Developer Token", default=""
    )
    config["account"]["alias"] = config["account"]["brand_name"].lower().replace(" ", "-")

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
    print("\n─── AI AD COPY GENERATION ───")
    config["ai_copy"]["provider"] = "openai"
    config["ai_copy"]["openai_api_key"] = prompt(
        "OpenAI API Key (for AI ad copy, empty to use templates only)"
    )
    if not config["ai_copy"]["openai_api_key"]:
        config["ai_copy"]["fallback_to_templates"] = True
        print("    → Will use template-based ad copy (no AI)")
    else:
        config["ai_copy"]["fallback_to_templates"] = True
        config["ai_copy"]["model"] = prompt("OpenAI model", default="gpt-4o")

    # --- Cleanup ---
    config.pop("_comment", None)
    config.pop("_instructions", None)
    config["_demo_mode"] = False

    # --- Save ---
    print(f"\n─── SAVING to {CONFIG_PATH} ───")
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print(f"  ✓ Config saved to: {CONFIG_PATH}")
    print()
    print("  Next steps:")
    print("  1. Install the plugin in Claude Code / Cowork")
    print("  2. Use /ads-health to verify your account connection")
    print("  3. Use /ads-report to see campaign performance")
    print()


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

    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    errors = []
    warnings = []

    cid = config.get("account", {}).get("customer_id", "")
    if not cid:
        errors.append("account.customer_id is empty — plugin cannot connect to Google Ads")
    elif not cid.replace("-", "").isdigit():
        errors.append(f"account.customer_id '{cid}' is not numeric")

    if not config.get("markets"):
        warnings.append("No markets defined — add country codes to 'markets' array")

    if not config.get("domains"):
        warnings.append("No domains defined — needed for batch product setup")

    ai = config.get("ai_copy", {})
    if not ai.get("openai_api_key") and not ai.get("anthropic_api_key"):
        warnings.append("No AI API key — will use template-based ad copy only")

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


if __name__ == "__main__":
    if "--validate" in sys.argv:
        validate_config()
    elif "--demo" in sys.argv:
        setup_demo()
    else:
        setup_interactive()
