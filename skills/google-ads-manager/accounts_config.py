"""
Universal Google Ads Plugin — Account Configuration Layer
=========================================================

This module provides a universal configuration system that loads account details
from config.json instead of hardcoded values. Any Google Ads account can be
configured by editing config.json or running the setup wizard.

Configuration file location (checked in order):
1. ./config.json (plugin directory)
2. ~/.google-ads-plugin/config.json (user home)
3. $GOOGLE_ADS_PLUGIN_CONFIG env var

Environment variables (override config.json):
- GOOGLE_ADS_CUSTOMER_ID — Google Ads customer ID
- GOOGLE_ADS_MCC_ID — MCC (Manager) account ID
- GOOGLE_ADS_DEVELOPER_TOKEN — API developer token
- OPENAI_API_KEY — For AI ad copy generation (optional)
- ANTHROPIC_API_KEY — Alternative AI provider (optional)
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# CONFIG FILE DISCOVERY
# ---------------------------------------------------------------------------

_PLUGIN_DIR = Path(__file__).resolve().parent.parent.parent  # google-ads-plugin/
_CONFIG_SEARCH_PATHS = [
    _PLUGIN_DIR / "config.json",
    Path.home() / ".google-ads-plugin" / "config.json",
]

_cached_config: Optional[Dict[str, Any]] = None


def _find_config_path() -> Optional[Path]:
    """Find the first existing config file."""
    env_path = os.environ.get("GOOGLE_ADS_PLUGIN_CONFIG")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
    for p in _CONFIG_SEARCH_PATHS:
        if p.exists():
            return p
    return None


def load_config(force_reload: bool = False) -> Dict[str, Any]:
    """Load config from JSON file with env var overrides."""
    global _cached_config
    if _cached_config is not None and not force_reload:
        return _cached_config

    config_path = _find_config_path()
    if config_path:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    else:
        config = _get_demo_config()

    # Environment variable overrides
    env_overrides = {
        "GOOGLE_ADS_CUSTOMER_ID": ("account", "customer_id"),
        "GOOGLE_ADS_MCC_ID": ("account", "mcc_id"),
        "GOOGLE_ADS_DEVELOPER_TOKEN": ("account", "developer_token"),
        "OPENAI_API_KEY": ("ai_copy", "openai_api_key"),
        "ANTHROPIC_API_KEY": ("ai_copy", "anthropic_api_key"),
    }
    for env_key, (section, field) in env_overrides.items():
        val = os.environ.get(env_key)
        if val:
            config.setdefault(section, {})[field] = val

    _cached_config = config
    return config


# ---------------------------------------------------------------------------
# CONVENIENCE ACCESSORS
# ---------------------------------------------------------------------------


def get_customer_id() -> str:
    """Get the primary Google Ads customer ID."""
    cfg = load_config()
    return cfg.get("account", {}).get("customer_id", "")


def get_mcc_id() -> str:
    """Get the MCC (Manager) account ID."""
    cfg = load_config()
    return cfg.get("account", {}).get("mcc_id", "")


def get_company_name() -> str:
    cfg = load_config()
    return cfg.get("account", {}).get("company_name", "My Company")


def get_brand_name() -> str:
    cfg = load_config()
    return cfg.get("account", {}).get("brand_name", "My Brand")


def get_industry() -> str:
    cfg = load_config()
    return cfg.get("account", {}).get("industry", "e-commerce")


def get_merchant_ids() -> Dict[str, str]:
    """Get country → Merchant Center ID mapping."""
    cfg = load_config()
    return cfg.get("merchant_center", {}).get("merchant_ids", {})


def get_merchant_id(country_code: str) -> Optional[str]:
    """Get Merchant Center ID for a specific country."""
    return get_merchant_ids().get(country_code.upper())


def get_domains() -> Dict[str, str]:
    """Get country → shop domain mapping."""
    cfg = load_config()
    return cfg.get("domains", {})


def get_domain(country_code: str) -> Optional[str]:
    """Get shop domain for a specific country."""
    return get_domains().get(country_code.upper())


def get_markets() -> List[str]:
    """Get list of active market country codes."""
    cfg = load_config()
    return cfg.get("markets", [])


def get_country_config() -> Dict[str, Any]:
    """Get per-country ad copy templates and locale config."""
    cfg = load_config()
    return cfg.get("country_config", {})


def get_np_feeds() -> Dict[str, Any]:
    """Get NEW_PRODUCTS feed configuration per country."""
    cfg = load_config()
    return cfg.get("np_feeds", {})


def get_xml_feeds() -> Dict[str, str]:
    """Get main XML feed URLs per country."""
    cfg = load_config()
    return cfg.get("xml_feeds", {})


def get_ai_config() -> Dict[str, Any]:
    """Get AI copy generation settings."""
    cfg = load_config()
    return cfg.get("ai_copy", {})


def get_campaign_defaults() -> Dict[str, Any]:
    """Get default campaign settings (budget, bidding, etc.)."""
    cfg = load_config()
    return cfg.get("campaign_defaults", {
        "daily_budget_micros": 5_000_000,
        "bidding_strategy": "MAXIMIZE_CONVERSION_VALUE",
        "target_roas": 1.6,
        "search_network": False,
        "display_network": False,
    })


def get_endpoints() -> Dict[str, str]:
    """Get external API endpoint URLs (image finder, etc.)."""
    cfg = load_config()
    return cfg.get("endpoints", {})


def is_demo_mode() -> bool:
    """Check if running with demo config (no real account configured)."""
    cfg = load_config()
    return cfg.get("_demo_mode", False)


# ---------------------------------------------------------------------------
# ACCOUNTS DICT (backward compatibility with multi-account structure)
# ---------------------------------------------------------------------------


def get_accounts() -> Dict[str, Dict[str, Any]]:
    """Get all configured accounts (primary + secondary)."""
    cfg = load_config()
    accounts = {}

    # Primary account
    primary = cfg.get("account", {})
    if primary.get("customer_id"):
        alias = primary.get("alias", "primary")
        accounts[alias] = {
            "customer_id": primary["customer_id"],
            "mcc_id": primary.get("mcc_id", ""),
            "company_name": primary.get("company_name", ""),
            "brand_name": primary.get("brand_name", ""),
            "industry": primary.get("industry", ""),
        }

    # Additional accounts
    for acc in cfg.get("additional_accounts", []):
        alias = acc.get("alias", acc.get("customer_id", "unknown"))
        accounts[alias] = acc

    return accounts


# ---------------------------------------------------------------------------
# DEMO CONFIG (used when no config.json found)
# ---------------------------------------------------------------------------


def _get_demo_config() -> Dict[str, Any]:
    """Return demo configuration for first-run experience."""
    return {
        "_demo_mode": True,
        "_demo_notice": (
            "This is a DEMO configuration. To use with your real Google Ads account, "
            "run: python setup_account.py — or edit config.json manually."
        ),
        "account": {
            "customer_id": "",
            "mcc_id": "",
            "alias": "demo",
            "company_name": "Acme Online Store",
            "brand_name": "VitaBoost",
            "industry": "e-commerce, retail",
            "developer_token": "",
        },
        "merchant_center": {
            "mca_ids": [],
            "merchant_ids": {},
        },
        "domains": {},
        "markets": [],
        "xml_feeds": {},
        "np_feeds": {},
        "country_config": {},
        "ai_copy": {
            "provider": "openai",
            "openai_api_key": "",
            "anthropic_api_key": "",
            "model": "gpt-4o",
            "fallback_to_templates": True,
        },
        "campaign_defaults": {
            "daily_budget_micros": 5_000_000,
            "bidding_strategy": "MAXIMIZE_CONVERSION_VALUE",
            "target_roas": 1.6,
            "search_network": False,
            "display_network": False,
        },
        "endpoints": {
            "image_finder": "",
            "batch_process_images": "",
            "ad_generator": "",
            "batch_initialize": "",
            "market_intel_warmup": "",
        },
    }
