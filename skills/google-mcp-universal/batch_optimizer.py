"""
Universal Google Ads Batch Optimizer — High-level MCP tools for mass Search campaign optimization.

Adds batch processing tools to the Google Ads MCP server:
- batch_setup_products: Create ad groups + RSAs + extensions + images for multiple products at once
- batch_generate_ad_copy: Generate localized ad copy via Claude Sonnet 4.5 API
- batch_audit_campaign: Audit campaign completeness (which products have what assets)

Usage: Import and call register_batch_tools(mcp_instance) from google_ads_mcp.py
"""

import os
import json
import xml.etree.ElementTree as ET
import ssl
import socket
import urllib.request
import urllib.parse
import time
import re
import random
import threading
import uuid
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

import logging

from batch_db import BatchDB
from accounts_config import load_config

_config = load_config()
_endpoints = _config.get('endpoints', {})

# ---------------------------------------------------------------------------
# Central debug logger for batch optimizer
# ---------------------------------------------------------------------------
_log = logging.getLogger("batch_optimizer")
_log.setLevel(logging.DEBUG)
if not _log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"
    ))
    _log.addHandler(_handler)

# ---------------------------------------------------------------------------
# Debug store — accumulates debug info per-session, visible via batch_dashboard
# ---------------------------------------------------------------------------

# Structured debug categories — use these constants instead of ad-hoc strings
class DebugCat:
    """Structured debug categories for _debug() calls."""
    WORKER = "worker"                # batch_setup_all background worker lifecycle
    IMAGES_WORKER = "images_worker"  # batch_add_images_only background worker
    SYNC = "sync"                    # sync_from_audit, sync_from_api
    ASSETS = "assets"                # asset scanning, ad_group_asset queries
    ASSET_SCAN = "asset_scan"        # detailed asset type parsing
    ASSET_TYPE = "asset_type_parse"  # enum/int → string conversion
    IMAGES = "images"                # image fetch, upload, validation
    AI_COPY = "ai_copy"              # AI ad copy generation (PHP endpoint)
    AD_COPY_GEN = "ad_copy_gen"      # local template-based copy generation
    GUARDRAILS = "guardrails"        # guardrail validation checks
    ELIGIBILITY = "eligibility"      # ad/asset eligibility (approval status)
    KEYWORDS = "keyword_intel"       # keyword research, category keywords
    PERF_DATA = "perf_data"          # performance data gathering (GAQL)
    WINNING = "winning_patterns"     # headline/description pattern analysis
    AD_GROUPS = "ad_groups"          # ad group creation/management
    CHANGELOG = "changelog"          # changelog operations
    CACHE = "cache"                  # cache operations (GAQL, feed, session)
    EXCEPTION = "exception"          # generic exception handler (fallback)
    HEALTH = "health"                # health check diagnostics

_debug_store: Dict[str, list] = {}

def _debug(category: str, msg: str, level: str = "info"):
    """Store debug message for later retrieval + log to stderr.
    Use DebugCat.* constants for category parameter."""
    if category not in _debug_store:
        _debug_store[category] = []
    entry = f"{time.strftime('%H:%M:%S')} {msg}"
    _debug_store[category].append(entry)
    # Keep max 50 per category
    if len(_debug_store[category]) > 50:
        _debug_store[category] = _debug_store[category][-50:]
    getattr(_log, level, _log.info)(msg)

# ---------------------------------------------------------------------------
# Background job registry for async batch_setup_all
# ---------------------------------------------------------------------------
_background_jobs: Dict[str, Dict[str, Any]] = {}
_background_jobs_lock = threading.Lock()
_stop_flags: Dict[str, bool] = {}  # job_id -> True to request stop

# ---------------------------------------------------------------------------
# Smart Rate Limit Manager — adaptive throttling with token bucket awareness
# ---------------------------------------------------------------------------
class RateLimitManager:
    """
    Tracks API mutations, parses Retry-After delays, and provides adaptive
    throttling to prevent 429 errors instead of reacting to them.

    v3.4.4: Replaces fixed 1.5s delays with dynamic throttling based on
    actual API response patterns.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._mutation_count = 0           # Total mutations this session
        self._mutations_this_hour = 0      # Mutations in current hour window
        self._hour_window_start = time.time()
        self._last_429_at: Optional[float] = None  # Timestamp of last 429
        self._retry_after_seconds: int = 0          # Parsed from "Retry in X seconds"
        self._consecutive_429s = 0
        self._base_delay = 1.5             # Starting delay between products (seconds)
        self._current_delay = 1.5          # Adaptive delay (increases on 429s)
        self._max_delay = 30.0             # Max delay between products
        self._pause_until: float = 0       # Timestamp: don't process until this time
        self._hourly_soft_limit = 800      # Start throttling at this many mutations/hour
        self._hourly_hard_limit = 1200     # Stop processing at this limit

    def record_mutation(self, count: int = 1):
        """Record API mutation(s). Called after each successful API write."""
        with self._lock:
            self._mutation_count += count
            now = time.time()
            # Reset hourly window if >1h elapsed
            if now - self._hour_window_start > 3600:
                self._mutations_this_hour = 0
                self._hour_window_start = now
            self._mutations_this_hour += count

    def record_429(self, error_msg: str = ""):
        """Record a 429 rate limit hit. Parses Retry-After if present."""
        with self._lock:
            self._last_429_at = time.time()
            self._consecutive_429s += 1

            # Parse "Retry in X seconds" from error message
            retry_seconds = self._parse_retry_seconds(error_msg)
            if retry_seconds > 0:
                self._retry_after_seconds = retry_seconds
                self._pause_until = time.time() + retry_seconds
                _debug("rate_limit", f"429 received. Retry-After: {retry_seconds}s. "
                       f"Pausing until {time.strftime('%H:%M:%S', time.localtime(self._pause_until))}", "warning")
            else:
                # No explicit retry time — use exponential backoff
                backoff = min(60 * (2 ** (self._consecutive_429s - 1)), 3600)
                self._pause_until = time.time() + backoff
                _debug("rate_limit", f"429 received (no Retry-After). Backoff: {backoff}s. "
                       f"Consecutive 429s: {self._consecutive_429s}", "warning")

            # Increase adaptive delay
            self._current_delay = min(self._current_delay * 2, self._max_delay)

    def record_success(self):
        """Record a successful API call. Gradually reduces throttling."""
        with self._lock:
            self._consecutive_429s = 0
            # Slowly reduce delay back toward base (decay factor 0.9)
            self._current_delay = max(self._base_delay, self._current_delay * 0.9)

    def _parse_retry_seconds(self, error_msg: str) -> int:
        """Extract seconds from 'Retry in X seconds' or 'retry_delay_seconds: X'."""
        import re
        # Pattern: "Retry in 13232 seconds"
        m = re.search(r'[Rr]etry\s+in\s+(\d+)\s+second', error_msg)
        if m:
            return int(m.group(1))
        # Pattern: "retry_delay_seconds: 13232"
        m = re.search(r'retry_delay_seconds[:\s]+(\d+)', error_msg)
        if m:
            return int(m.group(1))
        return 0

    def should_pause(self) -> tuple:
        """Check if processing should pause. Returns (should_pause, wait_seconds, reason)."""
        with self._lock:
            now = time.time()

            # Check explicit pause (from 429 Retry-After)
            if now < self._pause_until:
                wait = self._pause_until - now
                return (True, wait, f"Rate limited. Resume in {int(wait)}s "
                        f"({time.strftime('%H:%M:%S', time.localtime(self._pause_until))})")

            # Check hourly hard limit
            if self._mutations_this_hour >= self._hourly_hard_limit:
                wait = 3600 - (now - self._hour_window_start)
                if wait > 0:
                    return (True, wait, f"Hourly mutation limit ({self._hourly_hard_limit}) reached. "
                            f"Wait {int(wait)}s for new window.")

            return (False, 0, "")

    def get_delay(self) -> float:
        """Get current recommended delay between products."""
        with self._lock:
            # If approaching hourly soft limit, increase delay
            if self._mutations_this_hour >= self._hourly_soft_limit:
                throttle_factor = 1 + (self._mutations_this_hour - self._hourly_soft_limit) / 200
                return min(self._current_delay * throttle_factor, self._max_delay)
            return self._current_delay

    def get_stats(self) -> dict:
        """Get current rate limit stats for monitoring."""
        with self._lock:
            return {
                "total_mutations": self._mutation_count,
                "mutations_this_hour": self._mutations_this_hour,
                "hourly_window_start": time.strftime('%H:%M:%S', time.localtime(self._hour_window_start)),
                "consecutive_429s": self._consecutive_429s,
                "current_delay": round(self._current_delay, 2),
                "is_paused": time.time() < self._pause_until,
                "pause_until": time.strftime('%H:%M:%S', time.localtime(self._pause_until)) if self._pause_until > time.time() else None,
                "last_retry_after": self._retry_after_seconds,
            }

# Global rate limit manager instance
_rate_limiter = RateLimitManager()

# ---------------------------------------------------------------------------
# Language contamination markers — known words that ONLY belong to one language.
# Used by _validate_rsa_language() to detect cross-language contamination.
# ---------------------------------------------------------------------------
_LANGUAGE_MARKERS: Dict[str, List[str]] = {
    "RO": ["Comandă", "Cumpără", "România", "Site Oficial", "Livrare Rapidă",
           "Plata la", "Produs Original", "Recenzii", "Ofertă", "Calitate"],
    "TR": ["Sipariş", "Hemen", "Kapıda Ödeme", "Resmi Site", "Hızlı Kargo",
           "Türkiye", "Onaylı Ürün", "Sipariş Ver", "Oferta", "Kalite"],
    "PL": ["Zamów", "Oficjalny Sklep", "Wysyłka", "Płatność Przy",
           "Opinie", "Gwarancja", "Oferta", "Jakość"],
    "HU": ["Rendelje", "Hivatalos Bolt", "Szállítás", "Utánvéttel",
           "Vélemények", "Minőség", "Ajánlat"],
    "FR": ["Commandez", "Site Officiel", "Livraison Rapide", "Paiement",
           "Avis Clients", "Qualité", "Offre"],
    "DE": ["Bestellen", "Offizieller Shop", "Schnelle Lieferung", "Nachnahme",
           "Bewertungen", "Qualität", "Angebot"],
    "IT": ["Ordina", "Sito Ufficiale", "Consegna Rapida", "Pagamento",
           "Recensioni", "Qualità", "Offerta"],
    "ES": ["Comprar", "Tienda Oficial", "Envío Rápido", "Pago Contra",
           "Opiniones", "Calidad", "Oferta"],
    "BG": ["Поръчай", "Бърза Доставка", "Официален", "Качество", "Оферта"],
    "GR": ["Αποστολή", "Επίσημο", "Παραγγείλτε", "Ποιότητα", "Προσφορά"],
}


def _validate_rsa_language(headlines: List[str], country_code: str) -> Optional[str]:
    """
    Validate that RSA headlines don't contain foreign-language contamination.
    Returns None if clean, or a string describing the contamination if found.

    Checks all headline texts against known language markers for OTHER countries.
    If >2 headlines match a foreign language, flags as contaminated.
    """
    cc = country_code.upper()
    foreign_hits: Dict[str, int] = {}  # foreign_country -> count of matches

    for hl_text in headlines:
        for lang_cc, markers in _LANGUAGE_MARKERS.items():
            if lang_cc == cc:
                continue  # Skip own language
            for marker in markers:
                if marker in hl_text:
                    foreign_hits[lang_cc] = foreign_hits.get(lang_cc, 0) + 1
                    break  # One match per headline per language is enough

    # If >2 headlines match any single foreign language, it's contaminated
    for lang_cc, count in foreign_hits.items():
        if count >= 2:
            return f"Language contamination: {count} headlines match {lang_cc} markers"

    return None


# Singleton DB instance (reused across calls within same MCP process)
_batch_db: Optional[BatchDB] = None

def _get_db() -> BatchDB:
    """Get or create the singleton BatchDB instance."""
    global _batch_db
    if _batch_db is None:
        _batch_db = BatchDB()
    return _batch_db

# ---------------------------------------------------------------------------
# Image Finder endpoint configuration
# ---------------------------------------------------------------------------
def _get_endpoint(name, default=''):
    """Get endpoint URL from config with fallback."""
    return _endpoints.get(name, default)

IMAGE_FINDER_BASE_URL = _get_endpoint('image_finder', "")
BATCH_PROCESS_IMAGES_URL = _get_endpoint('batch_process_images', "")
IMAGE_FINDER_TIMEOUT = 15  # seconds
IMAGE_MAX_PER_AD_GROUP = 3  # max images to add per ad group
IMAGE_MIN_EXISTING = 2  # skip if ad group already has >= this many ENABLED images

# SSL context for image downloads (some CDNs have cert issues)
_img_ssl_ctx = ssl.create_default_context()
_img_ssl_ctx.check_hostname = False
_img_ssl_ctx.verify_mode = ssl.CERT_NONE

# ---------------------------------------------------------------------------
# Retry helper with exponential backoff (handles 429 rate limits)
# ---------------------------------------------------------------------------
MAX_RETRIES = 4
RETRY_BASE_DELAY = 5  # seconds

def _retry_with_backoff(fn, *args, max_retries=MAX_RETRIES, **kwargs):
    """
    Call fn(*args, **kwargs) with exponential backoff on transient errors.
    Catches 429 rate-limit and TRANSIENT_ERROR from Google Ads API.

    v3.4.4: Integrates with RateLimitManager for adaptive delays + jitter.
    Increased max_retries to 4. Adds random jitter to prevent thundering herd.
    """
    import random
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            result = fn(*args, **kwargs)
            _rate_limiter.record_success()
            return result
        except Exception as e:
            err_str = str(e).upper()
            is_rate_limit = (
                "429" in err_str
                or "RATE_EXCEEDED" in err_str
                or "RESOURCE_TEMPORARILY_EXHAUSTED" in err_str
            )
            is_retryable = (
                is_rate_limit
                or "TRANSIENT_ERROR" in err_str
                or "DEADLINE_EXCEEDED" in err_str
                or "INTERNAL_ERROR" in err_str
            )

            if is_rate_limit:
                _rate_limiter.record_429(str(e))
                # Check if Retry-After is very long (>120s) — don't retry inline, re-raise
                pause_check, wait_s, reason = _rate_limiter.should_pause()
                if wait_s > 120:
                    _debug("rate_limit", f"Retry-After too long ({int(wait_s)}s), re-raising for job-level handling", "warning")
                    raise

            if not is_retryable or attempt >= max_retries:
                raise

            # Exponential backoff with jitter (±25%)
            base_delay = RETRY_BASE_DELAY * (2 ** attempt)
            jitter = base_delay * 0.25 * (2 * random.random() - 1)  # ±25%
            delay = max(1, base_delay + jitter)
            _debug("retry", f"Attempt {attempt+1}/{max_retries} failed: {str(e)[:100]}. Retrying in {delay:.1f}s")
            time.sleep(delay)
            last_exc = e
    raise last_exc  # Should never reach here

# ---------------------------------------------------------------------------
# Error classification for smart retry decisions
# ---------------------------------------------------------------------------
class ErrorCategory:
    """Classifies Google Ads API errors for retry/skip decisions."""
    TRANSIENT = "transient"       # Retry after delay (429, SSL, DEADLINE_EXCEEDED)
    PERMANENT = "permanent"       # Skip + log (INVALID_ARGUMENT, policy violation)
    RESOURCE_LIMIT = "resource_limit"  # Change strategy (too many assets)
    QUOTA = "quota"               # Stop all processing (daily limit)

def classify_error(error: Exception) -> dict:
    """
    Classify an error into a category with recommended action.
    Returns: {"category": str, "retryable": bool, "action": str, "detail": str}
    """
    err_str = str(error).upper()

    # Quota exhaustion — stop everything
    if "QUOTA" in err_str or "BILLING" in err_str:
        return {"category": ErrorCategory.QUOTA, "retryable": False,
                "action": "stop_all", "detail": "API quota or billing issue"}

    # Resource limits — skip this asset type, continue with others
    if "RESOURCE_LIMIT" in err_str or "TOO_MANY" in err_str:
        return {"category": ErrorCategory.RESOURCE_LIMIT, "retryable": False,
                "action": "skip_asset_type", "detail": "Campaign asset limit reached"}

    # Transient — retry with backoff
    if any(k in err_str for k in ["429", "RATE_EXCEEDED", "RESOURCE_TEMPORARILY_EXHAUSTED",
                                    "TRANSIENT_ERROR", "DEADLINE_EXCEEDED", "INTERNAL_ERROR",
                                    "SSL", "HANDSHAKE", "CONNECT", "TIMEOUT"]):
        return {"category": ErrorCategory.TRANSIENT, "retryable": True,
                "action": "retry_with_backoff", "detail": "Transient error, will retry"}

    # Permanent — don't retry, log and move on
    if any(k in err_str for k in ["INVALID_ARGUMENT", "POLICY_VIOLATION", "NOT_FOUND",
                                    "PERMISSION_DENIED", "AUTHENTICATION"]):
        return {"category": ErrorCategory.PERMANENT, "retryable": False,
                "action": "skip_and_log", "detail": "Permanent error, cannot retry"}

    # Unknown — treat as permanent to be safe
    return {"category": ErrorCategory.PERMANENT, "retryable": False,
            "action": "skip_and_log", "detail": f"Unknown error type"}

def _preflight_resource_check(campaign_id: str, country_code: str) -> dict:
    """
    Pre-flight check: query campaign asset counts to predict resource limit issues.
    Returns dict with asset type -> {"count": N, "limit": M, "available": M-N, "warning": bool}

    Known Google Ads limits per campaign:
    - Callouts: ~20 per campaign (varies)
    - Sitelinks: ~20 per ad group
    - Promotions: ~20 per campaign
    - Structured snippets: ~20 per campaign
    """
    ASSET_LIMITS = {
        "CALLOUT": 20,
        "SITELINK": 20,  # per ad group, but campaign-level also has limits
        "PROMOTION": 20,
        "STRUCTURED_SNIPPET": 20,
    }

    try:
        from google_ads_mcp import _execute_gaql

        # Count assets per type using a single GAQL query
        # campaign.id MUST be in SELECT when used in WHERE (GAQL requirement)
        query = (
            f"SELECT campaign.id, asset.type "
            f"FROM campaign_asset "
            f"WHERE campaign.id = {campaign_id} "
            f"AND campaign_asset.status = 'ENABLED'"
        )
        results = _execute_gaql(query)

        # Count by type
        type_counts = {}
        for row in results:
            try:
                asset_type = str(row.asset.type_).replace("AssetType.", "").upper()
                # Handle enum int values
                ASSET_TYPE_MAP = {
                    "2": "TEXT", "3": "IMAGE", "4": "MEDIA_BUNDLE",
                    "5": "YOUTUBE_VIDEO", "8": "BOOK_ON_GOOGLE",
                }
                if asset_type.isdigit():
                    asset_type = ASSET_TYPE_MAP.get(asset_type, asset_type)
            except Exception as e:
                _debug("asset_type_parse", f"Failed parsing asset type: {e}", "warning")
                continue
            type_counts[asset_type] = type_counts.get(asset_type, 0) + 1

        report = {}
        for asset_type, limit in ASSET_LIMITS.items():
            count = type_counts.get(asset_type, 0)
            available = max(0, limit - count)
            report[asset_type] = {
                "count": count,
                "limit": limit,
                "available": available,
                "warning": available < 5,
                "full": available == 0,
            }

        return {"status": "success", "assets": report, "total_campaign_assets": sum(type_counts.values())}
    except Exception as e:
        return {"status": "error", "message": str(e)[:200], "assets": {}}

# ---------------------------------------------------------------------------
# XML Feed URLs per country — loaded from config.json
# ---------------------------------------------------------------------------
XML_FEEDS: Dict[str, Dict[str, Any]] = _config.get('xml_feeds', {})


# ---------------------------------------------------------------------------
# NEW_PRODUCTS Feed URLs per country — loaded from config.json
# ---------------------------------------------------------------------------
NP_FEEDS: Dict[str, Dict[str, list]] = _config.get('np_feeds', {})

# ---------------------------------------------------------------------------
# Brand mapping: domain pattern → brand code (for campaign naming)
# Campaign name format: {CC} | {Country} | {Type} | NEW_PRODUCTS | {Brand}
# ---------------------------------------------------------------------------
def _domain_to_brand(domain: str) -> str:
    """Map shop domain to brand code for campaign naming."""
    d = domain.lower()
    if "vogler" in d:
        return "VGL"
    elif "dobra-oferta" in d:
        return "DO"
    elif "herbalhaus" in d:
        return "HBS"
    return "HLE"  # default fallback


def _get_np_shops_for_country(country_code: str, feed_category: str = "bluewinston") -> list:
    """Get list of NEW_PRODUCTS shops for a country.
    
    Args:
        country_code: ISO 2-letter code (e.g., "HR")
        feed_category: "bluewinston" (Search) or "google" (Shopping)
    
    Returns: List of dicts with domain, feed_url, server, instance_id, brand
    """
    cc = country_code.upper()
    if cc not in NP_FEEDS:
        return []
    feeds = NP_FEEDS[cc].get(feed_category, [])
    result = []
    for f in feeds:
        entry = dict(f)
        entry["brand"] = _domain_to_brand(f["domain"])
        result.append(entry)
    return result


def _resolve_np_feed_url(country_code: str, shop_domain: str = None) -> str:
    """Resolve NP_FEEDS BW feed URL for a country (optionally filtered by shop_domain).

    Returns the first matching BW feed URL, or empty string if not found.
    """
    shops = _get_np_shops_for_country(country_code, "bluewinston")
    if not shops:
        return ""
    if shop_domain:
        for s in shops:
            if shop_domain.lower() in s["domain"].lower():
                return s["feed_url"]
        return ""
    # No filter — return first BW feed
    return shops[0]["feed_url"] if shops else ""


def _load_np_feed_handles(country_code: str, shop_domain: str = None) -> list:
    """Load product handles from NP_FEEDS BW feeds for a country.

    Args:
        country_code: ISO 2-letter code
        shop_domain: Optional filter (e.g., 'www.yourstore.com')

    Returns: List of product handles from BW feed(s)
    """
    cc = country_code.upper()
    shops = _get_np_shops_for_country(cc, "bluewinston")
    if not shops:
        return []

    all_handles = []
    for shop in shops:
        if shop_domain and shop_domain.lower() not in shop["domain"].lower():
            continue
        feed_url = shop["feed_url"]
        if not feed_url:
            continue
        try:
            raw_products = _fetch_xml_feed_cached(cc, feed_url)
            handles = [p["handle"] for p in raw_products if p.get("handle")]
            all_handles.extend(handles)
            _debug("np_feed", f"Loaded {len(handles)} handles from NP feed {shop['domain']}")
        except Exception as e:
            _debug("np_feed", f"Failed to load NP feed {feed_url}: {e}", "warning")

    return all_handles


# English country names for campaign naming convention
COUNTRY_NAMES_EN: Dict[str, str] = {
    "AE": "UAE", "AL": "Albania", "AM": "Armenia", "AR": "Argentina",
    "AT": "Austria", "AU": "Australia", "AZ": "Azerbaijan", "BA": "Bosnia",
    "BD": "Bangladesh", "BE": "Belgium", "BG": "Bulgaria", "BH": "Bahrain",
    "BJ": "Benin", "BO": "Bolivia", "BR": "Brazil", "CA": "Canada",
    "CH": "Switzerland", "CI": "Ivory Coast", "CL": "Chile", "CO": "Colombia",
    "CR": "Costa Rica", "CY": "Cyprus", "CZ": "Czechia", "DE": "Germany",
    "DK": "Denmark", "DZ": "Algeria", "EC": "Ecuador", "EE": "Estonia",
    "EG": "Egypt", "ES": "Spain", "FR": "France", "GA": "Gabon",
    "GE": "Georgia", "GN": "Guinea", "GR": "Greece", "GT": "Guatemala",
    "HN": "Honduras", "HR": "Croatia", "HU": "Hungary", "ID": "Indonesia",
    "IE": "Ireland", "IN": "India", "IT": "Italy", "JP": "Japan",
    "KE": "Kenya", "KG": "Kyrgyzstan", "KR": "South Korea", "KZ": "Kazakhstan",
    "LB": "Lebanon", "LK": "Sri Lanka", "LT": "Lithuania", "LU": "Luxembourg",
    "LV": "Latvia", "MA": "Morocco", "MD": "Moldova", "ME": "Montenegro",
    "MK": "North Macedonia", "MX": "Mexico", "MY": "Malaysia", "NG": "Nigeria",
    "NL": "Netherlands", "NO": "Norway", "NZ": "New Zealand", "OM": "Oman",
    "PA": "Panama", "PE": "Peru", "PH": "Philippines", "PK": "Pakistan",
    "PL": "Poland", "PT": "Portugal", "QA": "Qatar", "RO": "Romania",
    "RS": "Serbia", "RU": "Russia", "SA": "Saudi Arabia", "SE": "Sweden",
    "SG": "Singapore", "SI": "Slovenia", "SK": "Slovakia", "SN": "Senegal",
    "SV": "El Salvador", "TH": "Thailand", "TJ": "Tajikistan", "TR": "Turkey",
    "TZ": "Tanzania", "UA": "Ukraine", "UG": "Uganda", "UK": "United Kingdom",
    "US": "United States", "UZ": "Uzbekistan", "VN": "Vietnam", "XK": "Kosovo",
    "ZA": "South Africa",
}


def _get_np_campaign_name(country_code: str, campaign_type: str, brand: str, update_date: str = None) -> str:
    """Generate NEW_PRODUCTS campaign name using standard convention.

    Format: {CC} | {Country} | {Type} | NEW_PRODUCTS | {Brand} | CLAUDE_MCP - last updated: {YYYY-MM-DD}

    Args:
        country_code: "HR", "IT", etc.
        campaign_type: "Search" or "Shopping"
        brand: "HLE", "VGL", "HLP", etc.
        update_date: Override date (YYYY-MM-DD). Defaults to today.

    Returns: e.g. "DE | Germany | Search | NEW_PRODUCTS | HLE | CLAUDE_MCP - last updated: 2026-03-12"
    """
    cc = country_code.upper()
    country_name = COUNTRY_NAMES_EN.get(cc, XML_FEEDS.get(cc, {}).get("name", cc))
    if not update_date:
        update_date = time.strftime("%Y-%m-%d")
    return f"{cc} | {country_name} | {campaign_type} | NEW_PRODUCTS | {brand} | CLAUDE_MCP - last updated: {update_date}"


# Language code → full language name mapping
LANG_NAMES: Dict[str, str] = {
    "AR": "Arabic", "SQ": "Albanian", "AM": "Armenian", "AZ": "Azerbaijani",
    "BS": "Bosnian", "BN": "Bengali", "BG": "Bulgarian", "CS": "Czech",
    "DA": "Danish", "DE": "German", "EL": "Greek", "EN": "English",
    "ES": "Spanish", "ET": "Estonian", "FR": "French", "HI": "Hindi",
    "HR": "Croatian", "HU": "Hungarian", "ID": "Indonesian", "IT": "Italian",
    "JA": "Japanese", "KA": "Georgian", "KO": "Korean", "LT": "Lithuanian",
    "LV": "Latvian", "MK": "Macedonian", "NL": "Dutch", "NO": "Norwegian",
    "PL": "Polish", "PT": "Portuguese", "RO": "Romanian", "RU": "Russian",
    "SI": "Sinhala", "SK": "Slovak", "SL": "Slovenian", "SR": "Serbian",
    "SV": "Swedish", "SW": "Swahili", "TG": "Tajik", "TH": "Thai",
    "TR": "Turkish", "UK": "Ukrainian", "UR": "Urdu", "UZ": "Uzbek",
    "VI": "Vietnamese",
}

# Language code → ISO 639-1 for Google Ads promotion_asset language_code
LANG_ISO: Dict[str, str] = {
    "AR": "ar", "SQ": "sq", "AM": "hy", "AZ": "az", "BS": "bs", "BN": "bn",
    "BG": "bg", "CS": "cs", "DA": "da", "DE": "de", "EL": "el", "EN": "en",
    "ES": "es", "ET": "et", "FR": "fr", "HI": "hi", "HR": "hr", "HU": "hu",
    "ID": "id", "IT": "it", "JA": "ja", "KA": "ka", "KO": "ko", "LT": "lt",
    "LV": "lv", "MK": "mk", "NL": "nl", "NO": "nb", "PL": "pl", "PT": "pt",
    "RO": "ro", "RU": "ru", "SI": "si", "SK": "sk", "SL": "sl", "SR": "sr",
    "SV": "sv", "SW": "sw", "TG": "tg", "TH": "th", "TR": "tr", "UK": "uk",
    "UR": "ur", "UZ": "uz", "VI": "vi",
}

# Merchant Center IDs — loaded from config.json (or demo defaults)
MERCHANT_IDS: Dict[str, str] = _config.get('merchant_center', {}).get('merchants', {})

# Domain mapping — loaded from config.json (or demo defaults)
DOMAINS: Dict[str, str] = _config.get('domains', {})

# ---------------------------------------------------------------------------
# Country-specific ad copy configuration (fully localized templates)
# Countries NOT listed here use auto-generated templates based on language
# Loads from config.json with built-in defaults for template-based generation
# ---------------------------------------------------------------------------

# Define built-in defaults (contains 824 lines of localization data)
_COUNTRY_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "RO": {
        "language": "Romanian", "language_code": "ro", "currency": "Lei", "currency_code": "RON",
        "default_keyword": "Produs",
        "callout_templates": ["Preț {price} Lei", "Ofertă Specială", "Plata la Livrare", "Livrare Rapidă"],
        "sitelink_templates": [
            {"link_text": "Comandă Acum", "desc1": "Comandă {product} Original", "desc2": "Preț special {price} Lei"},
            {"link_text": "Recenzii", "desc1": "Citește părerile clienților", "desc2": "Recenzii verificate"},
            {"link_text": "Detalii", "desc1": "Informații complete", "desc2": "Specificații produs"},
            {"link_text": "Contact", "desc1": "Ai întrebări?", "desc2": "Echipa noastră te ajută"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "Comandă {product}", "Site Oficial", "Livrare Rapidă",
            "Produs Verificat", "Calitate Garantată", "Plata la Livrare", "Produs Original",
            "Ofertă Specială", "Cumpără Acum", "Comandă Acum Online",
            "Recenzii Pozitive", "Preț Bun", "Stock Disponibil",
        ],
        "filler_descriptions": [
            "{product} — comandă de pe site-ul oficial acum.",
            "Livrare rapidă în toată România. Plata la livrare disponibilă.",
            "Produs original, calitate garantată. Produs verificat.",
            "Comandă {product} azi. Satisfacție garantată.",
        ],
    },
    "TR": {
        "language": "Turkish", "language_code": "tr", "currency": "TL", "currency_code": "TRY",
        "default_keyword": "Ürün",
        "callout_templates": ["Fiyat {price} TL", "Özel Teklif", "Kapıda Ödeme", "Hızlı Kargo"],
        "sitelink_templates": [
            {"link_text": "Sipariş Ver", "desc1": "{product} Original Sipariş", "desc2": "Özel fiyat {price} TL"},
            {"link_text": "Yorumlar", "desc1": "Müşteri yorumlarını oku", "desc2": "Doğrulanmış yorumlar"},
            {"link_text": "Detaylar", "desc1": "Ürün bilgileri", "desc2": "Teknik özellikler"},
            {"link_text": "İletişim", "desc1": "Sorularınız mı var?", "desc2": "Ekibimiz yardımcı olur"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Orijinal", "Sipariş {product}", "Resmi Site", "Hızlı Kargo",
            "Onaylı Ürün", "Kalite Garantisi", "Kapıda Ödeme", "Ürün Orijinal",
            "Özel Teklif", "Hemen Sipariş Ver", "Olumlu Yorumlar",
            "İyi Fiyat", "Stok Mevcut",
        ],
        "filler_descriptions": [
            "{product} — resmi siteden hemen sipariş verin.",
            "Türkiye geneli hızlı kargo. Kapıda ödeme imkanı.",
            "Orijinal ürün, kalite garantili. Onaylı ürün.",
            "{product} sipariş edin. Memnuniyet garantisi.",
        ],
    },
    "PL": {
        "language": "Polish", "language_code": "pl", "currency": "zł", "currency_code": "PLN",
        "default_keyword": "Produkt",
        "callout_templates": ["Cena {price} zł", "Oferta Specjalna", "Płatność przy Odbiorze", "Szybka Wysyłka"],
        "sitelink_templates": [
            {"link_text": "Zamów Teraz", "desc1": "Zamów {product} Oryginał", "desc2": "Cena specjalna {price} zł"},
            {"link_text": "Opinie", "desc1": "Przeczytaj opinie klientów", "desc2": "Zweryfikowane recenzje"},
            {"link_text": "Szczegóły", "desc1": "Informacje o produkcie", "desc2": "Specyfikacja"},
            {"link_text": "Kontakt", "desc1": "Masz pytania?", "desc2": "Nasz zespół pomoże"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Oryginał", "Kup {product}", "Oficjalny Sklep", "Szybka Wysyłka",
            "Produkt Zweryfikowany", "Gwarancja Jakości", "Płatność Przy Odbiorze", "Produkt Oryginalny",
            "Oferta Specjalna", "Zamów Teraz Online", "Pozytywne Opinie",
            "Dobra Cena", "Dostępny",
        ],
        "filler_descriptions": [
            "{product} — zamów ze sklepu oficjalnego już teraz.",
            "Szybka wysyłka w całej Polsce. Płatność przy odbiorze.",
            "Oryginalny produkt, gwarancja jakości. Produkt zweryfikowany.",
            "Zamów {product} dziś. Gwarancja satysfakcji.",
        ],
    },
    "PT": {
        "language": "Portuguese", "language_code": "pt", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Produto",
        "callout_templates": ["Preço {price} €", "Oferta Especial", "Pagamento na Entrega", "Entrega Rápida"],
        "sitelink_templates": [
            {"link_text": "Encomendar Agora", "desc1": "Peça {product} Original", "desc2": "Preço especial {price} €"},
            {"link_text": "Avaliações", "desc1": "Leia as avaliações", "desc2": "Opiniões verificadas"},
            {"link_text": "Detalhes", "desc1": "Informações do produto", "desc2": "Especificações"},
            {"link_text": "Contato", "desc1": "Tem dúvidas?", "desc2": "A nossa equipa ajuda"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "Encomendar {product}", "Loja Oficial", "Entrega Rápida",
            "Produto Verificado", "Qualidade Garantida", "Pagamento na Entrega", "Produto Original",
            "Oferta Especial", "Encomende Agora", "Avaliações Positivas",
            "Bom Preço", "Disponível",
        ],
        "filler_descriptions": [
            "{product} — encomende na loja oficial agora.",
            "Entrega rápida em todo o país. Pagamento na entrega.",
            "Produto original, qualidade garantida. Produto verificado.",
            "Encomende {product} hoje. Satisfação garantida.",
        ],
    },
    "HU": {
        "language": "Hungarian", "language_code": "hu", "currency": "Ft", "currency_code": "HUF",
        "default_keyword": "Termék",
        "callout_templates": ["Ár {price} Ft", "Különleges Ajánlat", "Utánvétes Fizetés", "Gyors Szállítás"],
        "sitelink_templates": [
            {"link_text": "Rendelés Most", "desc1": "Rendelj {product} Eredetit", "desc2": "Akciós ár {price} Ft"},
            {"link_text": "Vélemények", "desc1": "Olvasd el a véleményeket", "desc2": "Ellenőrzött értékelések"},
            {"link_text": "Részletek", "desc1": "Termék információk", "desc2": "Specifikációk"},
            {"link_text": "Kapcsolat", "desc1": "Kérdésed van?", "desc2": "Csapatunk segít"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Eredeti", "Rendelje {product}", "Hivatalos Bolt", "Gyors Szállítás",
            "Ellenőrzött Termék", "Minőséggarancia", "Utánvéttel", "Termék Eredeti",
            "Különleges Ajánlat", "Rendelje Most Online", "Pozitív Vélemények",
            "Jó Ár", "Elérhető",
        ],
        "filler_descriptions": [
            "{product} — rendelje a hivatalos boltból most.",
            "Gyors szállítás egész Magyarországon. Utánvéttel is.",
            "Eredeti termék, minőség garantált. Ellenőrzött termék.",
            "Rendelje meg a {product} terméket ma. Elégedettség garantált.",
        ],
    },
    "FR": {
        "language": "French", "language_code": "fr", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Produit",
        "callout_templates": ["Prix {price} €", "Offre Spéciale", "Paiement à la Livraison", "Livraison Rapide"],
        "sitelink_templates": [
            {"link_text": "Commander", "desc1": "Commandez {product} Original", "desc2": "Prix spécial {price} €"},
            {"link_text": "Avis Clients", "desc1": "Lisez les avis clients", "desc2": "Avis vérifiés"},
            {"link_text": "Détails", "desc1": "Informations produit", "desc2": "Spécifications"},
            {"link_text": "Contact", "desc1": "Des questions?", "desc2": "Notre équipe vous aide"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "Commander {product}", "Site Officiel", "Livraison Rapide",
            "Produit Vérifié", "Qualité Garantie", "Paiement à Réception", "Produit Original",
            "Offre Spéciale", "Commandez Maintenant", "Avis Positifs",
            "Bon Prix", "Disponible",
        ],
        "filler_descriptions": [
            "{product} — commandez sur le site officiel maintenant.",
            "Livraison rapide dans toute la France. Paiement à la livraison.",
            "Produit original, qualité garantie. Produit vérifié.",
            "Commandez {product} aujourd'hui. Satisfaction garantie.",
        ],
    },
    "DE": {
        "language": "German", "language_code": "de", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Produkt",
        "callout_templates": ["Preis {price} €", "Sonderangebot", "Zahlung bei Lieferung", "Schnelle Lieferung"],
        "sitelink_templates": [
            {"link_text": "Jetzt Bestellen", "desc1": "Bestellen Sie {product}", "desc2": "Sonderpreis {price} €"},
            {"link_text": "Bewertungen", "desc1": "Kundenbewertungen lesen", "desc2": "Verifizierte Bewertungen"},
            {"link_text": "Details", "desc1": "Produktinformation", "desc2": "Spezifikationen"},
            {"link_text": "Kontakt", "desc1": "Fragen?", "desc2": "Unser Team hilft Ihnen"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "{product} Kaufen", "Offizieller Shop", "Schnelle Lieferung",
            "Geprüftes Produkt", "Qualitätsgarantie", "Nachnahme Möglich", "Produkt Original",
            "Sonderangebot", "Jetzt Online Bestellen", "Positive Bewertungen",
            "Guter Preis", "Verfügbar",
        ],
        "filler_descriptions": [
            "{product} — jetzt im offiziellen Shop bestellen.",
            "Schnelle Lieferung in ganz Deutschland. Nachnahme möglich.",
            "Original Produkt, Qualität garantiert. Geprüftes Produkt.",
            "Bestellen Sie {product} heute. Zufriedenheit garantiert.",
        ],
    },
    "IT": {
        "language": "Italian", "language_code": "it", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Prodotto",
        "callout_templates": ["Prezzo {price} €", "Offerta Speciale", "Pagamento alla Consegna", "Spedizione Rapida"],
        "sitelink_templates": [
            {"link_text": "Ordina Ora", "desc1": "Ordina {product} Originale", "desc2": "Prezzo speciale {price} €"},
            {"link_text": "Recensioni", "desc1": "Leggi le recensioni", "desc2": "Recensioni verificate"},
            {"link_text": "Dettagli", "desc1": "Informazioni prodotto", "desc2": "Specifiche"},
            {"link_text": "Contatto", "desc1": "Hai domande?", "desc2": "Il nostro team ti aiuta"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Originale", "Ordina {product}", "Sito Ufficiale", "Consegna Rapida",
            "Prodotto Verificato", "Qualità Garantita", "Pagamento alla Consegna", "Prodotto Originale",
            "Offerta Speciale", "Ordina Ora Online", "Recensioni Positive",
            "Buon Prezzo", "Disponibile",
        ],
        "filler_descriptions": [
            "{product} — ordina dal sito ufficiale adesso.",
            "Consegna rapida in tutta Italia. Pagamento alla consegna.",
            "Prodotto originale, qualità garantita. Prodotto verificato.",
            "Ordina {product} oggi. Soddisfazione garantita.",
        ],
    },
    "ES": {
        "language": "Spanish", "language_code": "es", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Producto",
        "callout_templates": ["Precio {price} €", "Oferta Especial", "Pago Contra Reembolso", "Envío Rápido"],
        "sitelink_templates": [
            {"link_text": "Pedir Ahora", "desc1": "Pide {product} Original", "desc2": "Precio especial {price} €"},
            {"link_text": "Opiniones", "desc1": "Lee las opiniones", "desc2": "Opiniones verificadas"},
            {"link_text": "Detalles", "desc1": "Información del producto", "desc2": "Especificaciones"},
            {"link_text": "Contacto", "desc1": "¿Tienes preguntas?", "desc2": "Nuestro equipo te ayuda"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "Comprar {product}", "Tienda Oficial", "Envío Rápido",
            "Producto Verificado", "Calidad Garantizada", "Pago Contra Reembolso", "Producto Original",
            "Oferta Especial", "Compra Ahora Online", "Opiniones Positivas",
            "Buen Precio", "Disponible",
        ],
        "filler_descriptions": [
            "{product} — compra en la tienda oficial ahora.",
            "Envío rápido en toda España. Pago contra reembolso.",
            "Producto original, calidad garantizada. Producto verificado.",
            "Compra {product} hoy. Satisfacción garantizada.",
        ],
    },
    "CZ": {
        "language": "Czech", "language_code": "cs", "currency": "Kč", "currency_code": "CZK",
        "default_keyword": "Produkt",
        "callout_templates": ["Cena {price} Kč", "Speciální Nabídka", "Platba na Dobírku", "Rychlé Doručení"],
        "sitelink_templates": [
            {"link_text": "Objednat Nyní", "desc1": "Objednejte {product}", "desc2": "Speciální cena {price} Kč"},
            {"link_text": "Recenze", "desc1": "Přečtěte si recenze", "desc2": "Ověřené recenze"},
            {"link_text": "Detaily", "desc1": "Informace o produktu", "desc2": "Specifikace"},
            {"link_text": "Kontakt", "desc1": "Máte dotazy?", "desc2": "Náš tým vám pomůže"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Originál", "Kupte {product}", "Oficiální Obchod", "Rychlé Doručení",
            "Ověřený Produkt", "Záruka Kvality", "Platba na Dobírku", "Produkt Originál",
            "Speciální Nabídka", "Objednejte Nyní Online", "Pozitivní Recenze",
            "Dobrá Cena", "Dostupné",
        ],
        "filler_descriptions": [
            "{product} — objednejte z oficiálního obchodu nyní.",
            "Rychlé doručení po celé České republice. Platba na dobírku.",
            "Originální produkt, kvalita garantována. Ověřený produkt.",
            "Objednejte {product} dnes. Záruka spokojenosti.",
        ],
    },
    "SK": {
        "language": "Slovak", "language_code": "sk", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Produkt",
        "callout_templates": ["Cena {price} €", "Špeciálna Ponuka", "Platba na Dobierku", "Rýchle Doručenie"],
        "sitelink_templates": [
            {"link_text": "Objednať Teraz", "desc1": "Objednajte {product}", "desc2": "Špeciálna cena {price} €"},
            {"link_text": "Recenzie", "desc1": "Prečítajte si recenzie", "desc2": "Overené recenzie"},
            {"link_text": "Detaily", "desc1": "Informácie o produkte", "desc2": "Špecifikácie"},
            {"link_text": "Kontakt", "desc1": "Máte otázky?", "desc2": "Náš tím vám pomôže"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Originál", "Kúpte {product}", "Oficiálny Obchod", "Rýchle Doručenie",
            "Overený Produkt", "Záruka Kvality", "Platba na Dobierku", "Produkt Originál",
            "Špeciálna Ponuka", "Objednajte Teraz Online", "Pozitívne Recenzie",
            "Dobrá Cena", "Dostupné",
        ],
        "filler_descriptions": [
            "{product} — objednajte z oficiálneho obchodu teraz.",
            "Rýchle doručenie po celom Slovensku. Platba na dobierku.",
            "Originálny produkt, kvalita garantovaná. Overený produkt.",
            "Objednajte {product} dnes. Záruka spokojnosti.",
        ],
    },
    "BG": {
        "language": "Bulgarian", "language_code": "bg", "currency": "лв", "currency_code": "BGN",
        "default_keyword": "Продукт",
        "callout_templates": ["Цена {price} лв", "Специална Оферта", "Плащане при Доставка", "Бърза Доставка"],
        "sitelink_templates": [
            {"link_text": "Поръчай Сега", "desc1": "Поръчай {product}", "desc2": "Специална цена {price} лв"},
            {"link_text": "Отзиви", "desc1": "Прочетете отзивите", "desc2": "Проверени отзиви"},
            {"link_text": "Детайли", "desc1": "Информация за продукта", "desc2": "Спецификации"},
            {"link_text": "Контакт", "desc1": "Имате въпроси?", "desc2": "Нашият екип помага"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Оригинал", "Поръчай {product}", "Официален Магазин", "Бърза Доставка",
            "Проверен Продукт", "Гарантирано Качество", "Плащане при Доставка", "Продукт Оригинал",
            "Специална Оферта", "Поръчай Сега Онлайн", "Положителни Отзиви",
            "Добра Цена", "Наличен",
        ],
        "filler_descriptions": [
            "{product} — поръчай от официалния магазин сега.",
            "Бърза доставка в цяла България. Плащане при доставка.",
            "Оригинален продукт, качество гарантирано. Проверен продукт.",
            "Поръчай {product} днес. Гарантирана удовлетвореност.",
        ],
    },
    "GR": {
        "language": "Greek", "language_code": "el", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Προϊόν",
        "callout_templates": ["Τιμή {price} €", "Ειδική Προσφορά", "Αντικαταβολή", "Γρήγορη Αποστολή"],
        "sitelink_templates": [
            {"link_text": "Παραγγείλτε", "desc1": "Παραγγείλτε {product}", "desc2": "Ειδική τιμή {price} €"},
            {"link_text": "Κριτικές", "desc1": "Διαβάστε κριτικές", "desc2": "Επαληθευμένες κριτικές"},
            {"link_text": "Λεπτομέρειες", "desc1": "Πληροφορίες προϊόντος", "desc2": "Προδιαγραφές"},
            {"link_text": "Επικοινωνία", "desc1": "Έχετε ερωτήσεις;", "desc2": "Η ομάδα μας βοηθά"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Αυθεντικό", "Παραγγείλτε {product}", "Επίσημο Κατάστημα", "Γρήγορη Αποστολή",
            "Επαληθευμένο Προϊόν", "Εγγυημένη Ποιότητα", "Αντικαταβολή", "Προϊόν Αυθεντικό",
            "Ειδική Προσφορά", "Παραγγείλτε Online", "Θετικές Κριτικές",
            "Καλή Τιμή", "Διαθέσιμο",
        ],
        "filler_descriptions": [
            "{product} — παραγγείλτε από το επίσημο κατάστημα τώρα.",
            "Γρήγορη αποστολή σε όλη την Ελλάδα. Αντικαταβολή διαθέσιμη.",
            "Αυθεντικό προϊόν, ποιότητα εγγυημένη. Επαληθευμένο προϊόν.",
            "Παραγγείλτε {product} σήμερα. Εγγυημένη ικανοποίηση.",
        ],
    },
    "HR": {
        "language": "Croatian", "language_code": "hr", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Proizvod",
        "callout_templates": ["Cijena {price} €", "Posebna Ponuda", "Plaćanje Pouzećem", "Brza Dostava"],
        "sitelink_templates": [
            {"link_text": "Naruči Sada", "desc1": "Naruči {product} Original", "desc2": "Posebna cijena {price} €"},
            {"link_text": "Recenzije", "desc1": "Pročitajte recenzije", "desc2": "Verificirane recenzije"},
            {"link_text": "Detaljno", "desc1": "Informacije o proizvodu", "desc2": "Specifikacije"},
            {"link_text": "Kontakt", "desc1": "Imate pitanja?", "desc2": "Naš tim vam pomaže"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "Naruči {product}", "Službena Trgovina", "Brza Dostava",
            "Provjereni Proizvod", "Zajamčena Kvaliteta", "Plaćanje Pouzećem", "Proizvod Original",
            "Posebna Ponuda", "Naruči Sada Online", "Pozitivne Recenzije",
            "Dobra Cijena", "Dostupno",
        ],
        "filler_descriptions": [
            "{product} — naručite iz službene trgovine sada.",
            "Brza dostava diljem Hrvatske. Plaćanje pouzećem.",
            "Originalni proizvod, kvaliteta zajamčena. Provjereni proizvod.",
            "Naručite {product} danas. Zadovoljstvo zajamčeno.",
        ],
    },
    "ME": {
        "language": "Serbian", "language_code": "sr", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Proizvod",
        "callout_templates": ["Cena {price} €", "Posebna Ponuda", "Plaćanje Pouzećem", "Brza Dostava"],
        "sitelink_templates": [
            {"link_text": "Poruči Sada", "desc1": "Poruči {product} Original", "desc2": "Posebna cena {price} €"},
            {"link_text": "Recenzije", "desc1": "Pročitaj utiske kupaca", "desc2": "Proverena mišljenja"},
            {"link_text": "Detaljno", "desc1": "Informacije o proizvodu", "desc2": "Specifikacije"},
            {"link_text": "Kontakt", "desc1": "Imaš pitanja?", "desc2": "Naš tim pomaže"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "Poruči {product}", "Zvanična Prodavnica", "Brza Dostava",
            "Proveren Proizvod", "Garantovan Kvalitet", "Plaćanje Pouzećem", "Proizvod Original",
            "Posebna Ponuda", "Poruči Online", "Pozitivne Recenzije",
            "Dobra Cena", "Dostupno",
        ],
        "filler_descriptions": [
            "{product} — poruči sa zvaničnog sajta odmah.",
            "Brza dostava širom zemlje. Plaćanje pouzećem.",
            "Originalni proizvod, kvalitet garantovan. Proveren proizvod.",
            "Poruči {product} danas. Zadovoljstvo zagarantovano.",
        ],
    },
    "SI": {
        "language": "Slovenian", "language_code": "sl", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Proizvod",
        "callout_templates": ["Cena {price} €", "Posebna Ponudba", "Plačilo po Povzetju", "Hitra Dostava"],
        "sitelink_templates": [
            {"link_text": "Naroči Zdaj", "desc1": "Naroči {product} Original", "desc2": "Posebna cena {price} €"},
            {"link_text": "Mnenja", "desc1": "Preberite mnenja", "desc2": "Preverjena mnenja"},
            {"link_text": "Detaljno", "desc1": "Informacije o proizvodu", "desc2": "Specifikacije"},
            {"link_text": "Kontakt", "desc1": "Imate vprašanja?", "desc2": "Naša ekipa pomaga"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "Naročite {product}", "Uradna Trgovina", "Hitra Dostava",
            "Preverjen Izdelek", "Zagotovljena Kakovost", "Plačilo po Povzetju", "Proizvod Original",
            "Posebna Ponudba", "Naročite Zdaj Online", "Pozitivna Mnenja",
            "Dobra Cena", "Dostopno",
        ],
        "filler_descriptions": [
            "{product} — naročite iz uradne trgovine zdaj.",
            "Hitra dostava po vsej Sloveniji. Plačilo po povzetju.",
            "Originalni proizvod, kakovost zagotovljena. Preverjen izdelek.",
            "Naročite {product} danes. Zadovoljstvo zagotovljeno.",
        ],
    },
    "LT": {
        "language": "Lithuanian", "language_code": "lt", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Produktas",
        "callout_templates": ["Kaina {price} €", "Speciali Pasiūlymas", "Mokėjimas Pristatymo Metu", "Greitas Pristatymas"],
        "sitelink_templates": [
            {"link_text": "Užsisakyti Dabar", "desc1": "Užsisakykite {product}", "desc2": "Speciali kaina {price} €"},
            {"link_text": "Atsiliepimai", "desc1": "Skaitykite atsiliepimus", "desc2": "Patikrinti atsiliepimai"},
            {"link_text": "Detalės", "desc1": "Produkto informacija", "desc2": "Specifikacijos"},
            {"link_text": "Kontaktai", "desc1": "Turite klausimų?", "desc2": "Mūsų komanda padės"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Originalus", "Užsisakykite {product}", "Oficiali Parduotuvė", "Greitas Pristatymas",
            "Patikrintas Produktas", "Kokybės Garantija", "Mokėjimas Pristatymo Metu", "Produktas Originalus",
            "Speciali Pasiūlymas", "Užsisakykite Dabar", "Teigiami Atsiliepimai",
            "Gera Kaina", "Prieinamas",
        ],
        "filler_descriptions": [
            "{product} — užsisakykite iš oficialios parduotuvės dabar.",
            "Greitas pristatymas visoje Lietuvoje. Mokėjimas pristatymo metu.",
            "Originalus produktas, kokybė garantuota. Patikrintas produktas.",
            "Užsisakykite {product} šiandien. Pasitenkinimas garantuotas.",
        ],
    },
    "EE": {
        "language": "Estonian", "language_code": "et", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Toode",
        "callout_templates": ["Hind {price} €", "Eripakkumine", "Maksmine Tarnimisel", "Kiire Tarne"],
        "sitelink_templates": [
            {"link_text": "Telli Nüüd", "desc1": "Telli {product} Originaal", "desc2": "Eripakkumine {price} €"},
            {"link_text": "Arvustused", "desc1": "Loe arvustusi", "desc2": "Kontrollitud arvustused"},
            {"link_text": "Üksikasjad", "desc1": "Toote teave", "desc2": "Spetsifikatsioonid"},
            {"link_text": "Kontakt", "desc1": "Küsimusi?", "desc2": "Meie meeskond aitab"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Originaal", "Telli {product}", "Ametlik Pood", "Kiire Tarne",
            "Kontrollitud Toode", "Kvaliteedi Garantii", "Maksmine Tarnimisel", "Toode Originaal",
            "Eripakkumine", "Telli Kohe Veebist", "Positiivsed Arvustused",
            "Hea Hind", "Saadaval",
        ],
        "filler_descriptions": [
            "{product} — telli ametlikust poest kohe.",
            "Kiire tarne üle kogu Eesti. Maksmine tarnimisel.",
            "Originaaltoode, kvaliteet garanteeritud. Kontrollitud toode.",
            "Telli {product} täna. Rahulolu garanteeritud.",
        ],
    },
    "LV": {
        "language": "Latvian", "language_code": "lv", "currency": "€", "currency_code": "EUR",
        "default_keyword": "Produkts",
        "callout_templates": ["Cena {price} €", "Īpašais Piedāvājums", "Apmaksa Piegādē", "Ātra Piegāde"],
        "sitelink_templates": [
            {"link_text": "Pasūtīt Tagad", "desc1": "Pasūtiet {product}", "desc2": "Īpašā cena {price} €"},
            {"link_text": "Atsauksmes", "desc1": "Lasiet atsauksmes", "desc2": "Pārbaudītas atsauksmes"},
            {"link_text": "Detaļas", "desc1": "Produkta informācija", "desc2": "Specifikācijas"},
            {"link_text": "Kontakti", "desc1": "Ir jautājumi?", "desc2": "Mūsu komanda palīdzēs"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Oriģināls", "Pasūtiet {product}", "Oficiālais Veikals", "Ātra Piegāde",
            "Pārbaudīts Produkts", "Kvalitātes Garantija", "Apmaksa Piegādē", "Produkts Oriģināls",
            "Īpašais Piedāvājums", "Pasūtiet Tagad Tiešsaistē", "Pozitīvas Atsauksmes",
            "Laba Cena", "Pieejams",
        ],
        "filler_descriptions": [
            "{product} — pasūtiet no oficiālā veikala tagad.",
            "Ātra piegāde visā Latvijā. Apmaksa piegādē.",
            "Oriģināls produkts, kvalitāte garantēta. Pārbaudīts produkts.",
            "Pasūtiet {product} šodien. Apmierinātība garantēta.",
        ],
    },
    "UA": {
        "language": "Ukrainian", "language_code": "uk", "currency": "₴", "currency_code": "UAH",
        "default_keyword": "Продукт",
        "callout_templates": ["Ціна {price} ₴", "Спеціальна Пропозиція", "Оплата при Доставці", "Швидка Доставка"],
        "sitelink_templates": [
            {"link_text": "Замовити Зараз", "desc1": "Замовте {product}", "desc2": "Спеціальна ціна {price} ₴"},
            {"link_text": "Відгуки", "desc1": "Читайте відгуки", "desc2": "Перевірені відгуки"},
            {"link_text": "Деталі", "desc1": "Інформація про продукт", "desc2": "Специфікації"},
            {"link_text": "Контакти", "desc1": "Маєте питання?", "desc2": "Наша команда допоможе"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Оригінал", "Замовте {product}", "Офіційний Магазин", "Швидка Доставка",
            "Перевірений Продукт", "Гарантія Якості", "Оплата при Доставці", "Продукт Оригінал",
            "Спеціальна Пропозиція", "Замовте Зараз Онлайн", "Позитивні Відгуки",
            "Гарна Ціна", "Доступно",
        ],
        "filler_descriptions": [
            "{product} — замовте з офіційного магазину зараз.",
            "Швидка доставка по всій Україні. Оплата при доставці.",
            "Оригінальний продукт, якість гарантована. Перевірений продукт.",
            "Замовте {product} сьогодні. Задоволення гарантоване.",
        ],
    },
    "MK": {
        "language": "Macedonian", "language_code": "mk", "currency": "ден", "currency_code": "MKD",
        "default_keyword": "Производ",
        "callout_templates": ["Цена {price} ден", "Специјална Понуда", "Плаќање при Достава", "Брза Достава"],
        "sitelink_templates": [
            {"link_text": "Порачај Сега", "desc1": "Порачај {product} Оригинал", "desc2": "Специјална цена {price} ден"},
            {"link_text": "Рецензии", "desc1": "Прочитајте рецензии", "desc2": "Проверени мислења"},
            {"link_text": "Детали", "desc1": "Информации за производот", "desc2": "Спецификации"},
            {"link_text": "Контакт", "desc1": "Имате прашања?", "desc2": "Нашиот тим помага"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Оригинал", "Порачај {product}", "Официјална Продавница", "Брза Достава",
            "Проверен Производ", "Гарантиран Квалитет", "Плаќање при Достава", "Производ Оригинал",
            "Специјална Понуда", "Порачај Онлајн", "Позитивни Рецензии",
            "Добра Цена", "Достапно",
        ],
        "filler_descriptions": [
            "{product} — порачај од официјалната продавница сега.",
            "Брза достава низ целата земја. Плаќање при достава.",
            "Оригинален производ, квалитет гарантиран. Проверен производ.",
            "Порачај {product} денес. Задоволство гарантирано.",
        ],
    },
    "RS": {
        "language": "Serbian", "language_code": "sr", "currency": "din.", "currency_code": "RSD",
        "default_keyword": "Proizvod",
        "callout_templates": ["Cena {price} din.", "Posebna Ponuda", "Plaćanje Pouzećem", "Brza Dostava"],
        "sitelink_templates": [
            {"link_text": "Poruči Sada", "desc1": "Poruči {product} Original", "desc2": "Posebna cena {price} din."},
            {"link_text": "Recenzije", "desc1": "Pročitaj utiske kupaca", "desc2": "Proverena mišljenja"},
            {"link_text": "Detaljno", "desc1": "Informacije o proizvodu", "desc2": "Specifikacije"},
            {"link_text": "Kontakt", "desc1": "Imaš pitanja?", "desc2": "Naš tim pomaže"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Original", "Poruči {product}", "Zvanična Prodavnica", "Brza Dostava",
            "Proveren Proizvod", "Garantovan Kvalitet", "Plaćanje Pouzećem", "Proizvod Original",
            "Posebna Ponuda", "Poruči Online", "Pozitivne Recenzije",
            "Dobra Cena", "Dostupno",
        ],
        "filler_descriptions": [
            "{product} — poruči sa zvaničnog sajta odmah.",
            "Brza dostava širom Srbije. Plaćanje pouzećem.",
            "Originalni proizvod, kvalitet garantovan. Proveren proizvod.",
            "Poruči {product} danas. Zadovoljstvo zagarantovano.",
        ],
    },
    "AL": {
        "language": "Albanian", "language_code": "sq", "currency": "Lekë", "currency_code": "ALL",
        "default_keyword": "Produkt",
        "callout_templates": ["Çmim {price} Lekë", "Ofertë Speciale", "Pagesë në Dorëzim", "Dërgesë e Shpejtë"],
        "sitelink_templates": [
            {"link_text": "Porosit Tani", "desc1": "Porosit {product} Origjinal", "desc2": "Çmim special {price} Lekë"},
            {"link_text": "Vlerësime", "desc1": "Lexoni vlerësimet", "desc2": "Vlerësime të verifikuara"},
            {"link_text": "Detaje", "desc1": "Informacioni i produktit", "desc2": "Specifikacione"},
            {"link_text": "Kontakt", "desc1": "Keni pyetje?", "desc2": "Ekipi ynë ndihmon"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
        "filler_headlines": [
            "{product} Origjinal", "Porosit {product}", "Dyqani Zyrtar", "Dërgesë e Shpejtë",
            "Produkt i Verifikuar", "Cilësi e Garantuar", "Pagesë në Dorëzim", "Produkt Origjinal",
            "Ofertë Speciale", "Porosit Tani Online", "Vlerësime Pozitive",
            "Çmim i Mirë", "Në Dispozicion",
        ],
        "filler_descriptions": [
            "{product} — porosit nga dyqani zyrtar tani.",
            "Dërgesë e shpejtë në të gjithë vendin. Pagesë në dorëzim.",
            "Produkt origjinal, cilësi e garantuar. Produkt i verifikuar.",
            "Porosit {product} sot. Kënaqësia e garantuar.",
        ],
    },
    # English-language markets (EN) — shared template
    "EN": {
        "language": "English", "language_code": "en", "currency": "$", "currency_code": "USD",
        "default_keyword": "Product",
        "callout_templates": ["Price {price}", "Special Offer", "Cash on Delivery", "Fast Shipping"],
        "sitelink_templates": [
            {"link_text": "Order Now", "desc1": "Order {product} Original", "desc2": "Special price {price}"},
            {"link_text": "Reviews", "desc1": "Read customer reviews", "desc2": "Verified reviews"},
            {"link_text": "Details", "desc1": "Product information", "desc2": "Specifications"},
            {"link_text": "Contact", "desc1": "Have questions?", "desc2": "Our team can help"},
        ],
        "promotion_percent_off": 50000, "snippet_header": "Types",
    },
}

# Load from config.json with fallback to built-in defaults
COUNTRY_CONFIG: Dict[str, Dict[str, Any]] = {}

# Merge built-in defaults
for _cc, _config_val in _COUNTRY_DEFAULTS.items():
    COUNTRY_CONFIG[_cc] = _config_val.copy()

# Override with config-loaded values (if any)
for _cc, _config_val in _config.get("country_config", {}).items():
    if _cc in COUNTRY_CONFIG:
        COUNTRY_CONFIG[_cc].update(_config_val)
    else:
        COUNTRY_CONFIG[_cc] = _config_val

# English-speaking countries that share the EN template
_EN_COUNTRIES = ["AU", "CA", "UK", "US", "NZ", "MY", "PH", "NG", "KE", "UG", "ZA", "SG", "IE", "IN"]
for _cc in _EN_COUNTRIES:
    if _cc not in COUNTRY_CONFIG:
        COUNTRY_CONFIG[_cc] = COUNTRY_CONFIG["EN"].copy()

# Spanish-speaking LATAM countries share ES template
_ES_COUNTRIES = ["AR", "CL", "CO", "CR", "EC", "GT", "HN", "MX", "PA", "PE", "SV", "BO"]
for _cc in _ES_COUNTRIES:
    if _cc not in COUNTRY_CONFIG:
        COUNTRY_CONFIG[_cc] = COUNTRY_CONFIG["ES"].copy()

# French-speaking countries share FR template
_FR_COUNTRIES = ["BE", "LU", "CI", "SN", "GA", "GN", "BJ"]
for _cc in _FR_COUNTRIES:
    if _cc not in COUNTRY_CONFIG:
        COUNTRY_CONFIG[_cc] = COUNTRY_CONFIG["FR"].copy()

# German-speaking countries share DE template
for _cc in ["AT", "CH"]:
    if _cc not in COUNTRY_CONFIG:
        COUNTRY_CONFIG[_cc] = COUNTRY_CONFIG["DE"].copy()

# Romanian-speaking Moldova shares RO template
if "MD" not in COUNTRY_CONFIG:
    COUNTRY_CONFIG["MD"] = COUNTRY_CONFIG["RO"].copy()


# ---------------------------------------------------------------------------
# Anchor sitelink templates — catchy marketing phrases as URL anchors/params.
# All URLs point to the SAME product page but with #anchor or ?page=x params,
# making the ad look like it has rich sub-pages. Google Ads accepts these URLs.
#
# Activated via asset_types=["anchor_sitelinks"] — NOT included by default.
# Placeholders: {product} → product name, {price} → price, {url} → product URL
# ---------------------------------------------------------------------------
ANCHOR_SITELINK_TEMPLATES: Dict[str, List[Dict[str, str]]] = {
    "ro": [
        {"link_text": "Livrare Gratuită", "desc1": "Transport gratuit în toată România", "desc2": "Plata la livrare disponibilă", "url_suffix": "#livrare-gratuita"},
        {"link_text": "Original Din România", "desc1": "Produs verificat și original", "desc2": "Comandă de pe site-ul oficial", "url_suffix": "?page=original#verificat-romania"},
        {"link_text": "Ingrediente Naturale", "desc1": "Formulă 100% naturală", "desc2": "Fără chimicale sau aditivi", "url_suffix": "?page=ingrediente#formula-naturala"},
        {"link_text": "Ofertă -{discount}% Azi", "desc1": "Preț redus: {price} {currency}", "desc2": "Ofertă valabilă doar online", "url_suffix": "?page=oferta#reducere-{discount}"},
    ],
    "tr": [
        {"link_text": "Ücretsiz Kargo", "desc1": "Türkiye geneli ücretsiz kargo", "desc2": "Kapıda ödeme imkanı", "url_suffix": "#ucretsiz-kargo"},
        {"link_text": "Orijinal Ürün", "desc1": "Doğrulanmış orijinal ürün", "desc2": "Resmi satış noktası", "url_suffix": "?page=orijinal#dogrulanmis"},
        {"link_text": "Doğal İçerik", "desc1": "%100 doğal formül", "desc2": "Kimyasal katkı içermez", "url_suffix": "?page=icerik#dogal-formul"},
        {"link_text": "%{discount} İndirim Bugün", "desc1": "İndirimli fiyat: {price} {currency}", "desc2": "Sadece online geçerli", "url_suffix": "?page=indirim#fiyat-{discount}"},
    ],
    "pl": [
        {"link_text": "Darmowa Wysyłka", "desc1": "Wysyłka gratis w całej Polsce", "desc2": "Płatność przy odbiorze", "url_suffix": "#darmowa-wysylka"},
        {"link_text": "Oryginalny Produkt", "desc1": "Produkt zweryfikowany", "desc2": "Zamów ze strony oficjalnej", "url_suffix": "?page=oryginal#zweryfikowany"},
        {"link_text": "Naturalne Składniki", "desc1": "Formuła 100% naturalna", "desc2": "Bez chemii i dodatków", "url_suffix": "?page=skladniki#formula-naturalna"},
        {"link_text": "Oferta -{discount}% Dziś", "desc1": "Cena obniżona: {price} {currency}", "desc2": "Oferta tylko online", "url_suffix": "?page=oferta#rabat-{discount}"},
    ],
    "hu": [
        {"link_text": "Ingyenes Szállítás", "desc1": "Ingyenes szállítás egész Magyaro.", "desc2": "Utánvétes fizetés lehetséges", "url_suffix": "#ingyenes-szallitas"},
        {"link_text": "Eredeti Termék", "desc1": "Hitelesített eredeti termék", "desc2": "Rendelés a hivatalos oldalról", "url_suffix": "?page=eredeti#hitelesitett"},
        {"link_text": "Prémium Minőség", "desc1": "Garantált minőségi termék", "desc2": "Hivatalos forgalmazó", "url_suffix": "?page=minoseg#premium"},
        {"link_text": "Akció -{discount}% Ma", "desc1": "Kedvezményes ár: {price} {currency}", "desc2": "Csak online érvényes", "url_suffix": "?page=akcio#kedvezmeny-{discount}"},
    ],
    "fr": [
        {"link_text": "Livraison Gratuite", "desc1": "Livraison offerte en France", "desc2": "Paiement à la livraison", "url_suffix": "#livraison-gratuite"},
        {"link_text": "Produit Original", "desc1": "Produit vérifié et original", "desc2": "Commandez sur le site officiel", "url_suffix": "?page=original#verifie"},
        {"link_text": "Qualité Premium", "desc1": "Produit de qualité supérieure", "desc2": "Distributeur officiel", "url_suffix": "?page=qualite#premium"},
        {"link_text": "Offre -{discount}% Auj.", "desc1": "Prix réduit : {price} {currency}", "desc2": "Offre valable en ligne", "url_suffix": "?page=offre#reduction-{discount}"},
    ],
    "de": [
        {"link_text": "Kostenloser Versand", "desc1": "Gratis Versand in Deutschland", "desc2": "Zahlung bei Lieferung möglich", "url_suffix": "#kostenloser-versand"},
        {"link_text": "Originalprodukt", "desc1": "Verifiziertes Originalprodukt", "desc2": "Bestellen Sie im offiziellen Shop", "url_suffix": "?page=original#verifiziert"},
        {"link_text": "Premium Qualität", "desc1": "Produkt höchster Qualität", "desc2": "Offizieller Vertrieb", "url_suffix": "?page=qualitaet#premium"},
        {"link_text": "Angebot -{discount}%", "desc1": "Reduziert: {price} {currency}", "desc2": "Nur online verfügbar", "url_suffix": "?page=angebot#rabatt-{discount}"},
    ],
    "it": [
        {"link_text": "Spedizione Gratuita", "desc1": "Spedizione gratis in tutta Italia", "desc2": "Pagamento alla consegna", "url_suffix": "#spedizione-gratuita"},
        {"link_text": "Prodotto Originale", "desc1": "Prodotto verificato e originale", "desc2": "Ordina dal sito ufficiale", "url_suffix": "?page=originale#verificato"},
        {"link_text": "Qualità Premium", "desc1": "Prodotto di qualità superiore", "desc2": "Distributore ufficiale", "url_suffix": "?page=qualita#premium"},
        {"link_text": "Offerta -{discount}% Oggi", "desc1": "Prezzo ridotto: {price} {currency}", "desc2": "Offerta solo online", "url_suffix": "?page=offerta#sconto-{discount}"},
    ],
    "es": [
        {"link_text": "Envío Gratuito", "desc1": "Envío gratis a toda España", "desc2": "Pago contra reembolso", "url_suffix": "#envio-gratuito"},
        {"link_text": "Producto Original", "desc1": "Producto verificado y original", "desc2": "Pide en la tienda oficial", "url_suffix": "?page=original#verificado"},
        {"link_text": "Calidad Premium", "desc1": "Producto de calidad superior", "desc2": "Distribuidor oficial", "url_suffix": "?page=calidad#premium"},
        {"link_text": "Oferta -{discount}% Hoy", "desc1": "Precio reducido: {price} {currency}", "desc2": "Oferta solo online", "url_suffix": "?page=oferta#descuento-{discount}"},
    ],
    "en": [
        {"link_text": "Free Delivery", "desc1": "Free shipping on all orders", "desc2": "Cash on delivery available", "url_suffix": "#free-delivery"},
        {"link_text": "Original Product", "desc1": "Verified authentic product", "desc2": "Order from the official store", "url_suffix": "?page=original#verified"},
        {"link_text": "Premium Quality", "desc1": "Top quality verified product", "desc2": "Official distributor", "url_suffix": "?page=quality#premium"},
        {"link_text": "Save {discount}% Today", "desc1": "Reduced price: {price} {currency}", "desc2": "Online exclusive offer", "url_suffix": "?page=offer#discount-{discount}"},
    ],
    "cs": [
        {"link_text": "Doprava Zdarma", "desc1": "Doprava zdarma po celém Česku", "desc2": "Platba na dobírku", "url_suffix": "#doprava-zdarma"},
        {"link_text": "Originální Produkt", "desc1": "Ověřený originální produkt", "desc2": "Objednejte na oficiální stránce", "url_suffix": "?page=original#overeny"},
        {"link_text": "Prémiová Kvalita", "desc1": "Produkt nejvyšší kvality", "desc2": "Oficiální distribuce", "url_suffix": "?page=kvalita#premium"},
        {"link_text": "Sleva -{discount}% Dnes", "desc1": "Snížená cena: {price} {currency}", "desc2": "Nabídka platí online", "url_suffix": "?page=nabidka#sleva-{discount}"},
    ],
    "sk": [
        {"link_text": "Doprava Zadarmo", "desc1": "Doprava zadarmo po Slovensku", "desc2": "Platba na dobierku", "url_suffix": "#doprava-zadarmo"},
        {"link_text": "Originálny Produkt", "desc1": "Overený originálny produkt", "desc2": "Objednajte na oficiálnej str.", "url_suffix": "?page=original#overeny"},
        {"link_text": "Prémiová Kvalita", "desc1": "Produkt najvyššej kvality", "desc2": "Oficiálna distribúcia", "url_suffix": "?page=kvalita#premium"},
        {"link_text": "Zľava -{discount}% Dnes", "desc1": "Znížená cena: {price} {currency}", "desc2": "Ponuka platí online", "url_suffix": "?page=ponuka#zlava-{discount}"},
    ],
    "bg": [
        {"link_text": "Безплатна Доставка", "desc1": "Безплатна доставка в България", "desc2": "Плащане при доставка", "url_suffix": "#bezplatna-dostavka"},
        {"link_text": "Оригинален Продукт", "desc1": "Проверен оригинален продукт", "desc2": "Поръчайте от официалния сайт", "url_suffix": "?page=original#proveren"},
        {"link_text": "Натурални Съставки", "desc1": "100% натурална формула", "desc2": "Без химически добавки", "url_suffix": "?page=sastavki#naturalna-formula"},
        {"link_text": "Отстъпка -{discount}%", "desc1": "Намалена цена: {price} {currency}", "desc2": "Оферта само онлайн", "url_suffix": "?page=oferta#otstapka-{discount}"},
    ],
    "el": [
        {"link_text": "Δωρεάν Αποστολή", "desc1": "Δωρεάν αποστολή στην Ελλάδα", "desc2": "Αντικαταβολή διαθέσιμη", "url_suffix": "#dorean-apostoli"},
        {"link_text": "Αυθεντικό Προϊόν", "desc1": "Επαληθευμένο αυθεντικό προϊόν", "desc2": "Παραγγελία από επίσημο site", "url_suffix": "?page=original#epalitheumeno"},
        {"link_text": "Φυσικά Συστατικά", "desc1": "100% φυσική σύνθεση", "desc2": "Χωρίς χημικά πρόσθετα", "url_suffix": "?page=systatika#fysiki-synthesi"},
        {"link_text": "Έκπτωση -{discount}%", "desc1": "Μειωμένη τιμή: {price} {currency}", "desc2": "Προσφορά μόνο online", "url_suffix": "?page=prosfora#ekptosi-{discount}"},
    ],
    "hr": [
        {"link_text": "Besplatna Dostava", "desc1": "Besplatna dostava u Hrvatskoj", "desc2": "Plaćanje pouzećem", "url_suffix": "#besplatna-dostava"},
        {"link_text": "Originalni Proizvod", "desc1": "Provjereni originalni proizvod", "desc2": "Naručite sa službene stranice", "url_suffix": "?page=original#provjereni"},
        {"link_text": "Prirodni Sastojci", "desc1": "100% prirodna formula", "desc2": "Bez kemijskih dodataka", "url_suffix": "?page=sastojci#prirodna-formula"},
        {"link_text": "Popust -{discount}% Danas", "desc1": "Snižena cijena: {price} {currency}", "desc2": "Ponuda samo online", "url_suffix": "?page=ponuda#popust-{discount}"},
    ],
    "uk": [
        {"link_text": "Безкоштовна Доставка", "desc1": "Безкоштовна доставка в Україні", "desc2": "Оплата при отриманні", "url_suffix": "#bezkoshtovna-dostavka"},
        {"link_text": "Оригінальний Продукт", "desc1": "Перевірений оригінал", "desc2": "Замовте на офіційному сайті", "url_suffix": "?page=original#perevirenyj"},
        {"link_text": "Натуральний Склад", "desc1": "100% натуральна формула", "desc2": "Без хімічних домішок", "url_suffix": "?page=sklad#naturalna-formula"},
        {"link_text": "Знижка -{discount}%", "desc1": "Ціна знижена: {price} {currency}", "desc2": "Акція тільки онлайн", "url_suffix": "?page=akcia#znyzhka-{discount}"},
    ],
}


def _build_anchor_sitelinks(
    product_url: str,
    product_name: str,
    price: float,
    currency: str,
    language_code: str,
    discount_percent: int = 50,
) -> List[Dict[str, Any]]:
    """Build anchor sitelinks from templates for given language.

    Returns list of sitelink dicts ready for _create_sitelinks().
    Placeholder substitution: {product}, {price}, {currency}, {discount}.
    """
    templates = ANCHOR_SITELINK_TEMPLATES.get(language_code, ANCHOR_SITELINK_TEMPLATES.get("en", []))
    sitelinks = []
    price_str = str(int(price))
    discount_str = str(discount_percent)

    for tmpl in templates:
        def _sub(text: str) -> str:
            return (
                text.replace("{product}", product_name)
                .replace("{price}", price_str)
                .replace("{currency}", currency)
                .replace("{discount}", discount_str)
            )

        link_text = _sub(tmpl["link_text"])[:25]
        desc1 = _sub(tmpl["desc1"])[:35]
        desc2 = _sub(tmpl["desc2"])[:35]
        url_suffix = _sub(tmpl["url_suffix"])
        final_url = product_url.rstrip("/") + url_suffix

        sitelinks.append({
            "link_text": link_text,
            "description1": desc1,
            "description2": desc2,
            "final_urls": [final_url],
        })

    return sitelinks


def _auto_changelog(category: str, title: str, description: str = "",
                     details: str = "", country_code: str = None,
                     campaign_id: str = None, severity: str = "info"):
    """Auto-log a changelog entry from batch operations. Silent on failure."""
    try:
        db = _get_db()
        db.add_changelog_entry(
            category=category, title=title, description=description,
            details=details, country_code=country_code,
            campaign_id=campaign_id, severity=severity, auto_logged=True,
        )
    except Exception as e:
        _debug("changelog", f"_auto_changelog failed: {e}", "warning")


def _get_recent_changelog_for_instructions() -> list:
    """Get last 10 changelog entries for embedding in batch_get_instructions response."""
    try:
        db = _get_db()
        entries = db.get_changelog(limit=10)
        return [
            {
                "date": e.get("created_at", ""),
                "category": e.get("category", ""),
                "title": e.get("title", ""),
                "severity": e.get("severity", "info"),
            }
            for e in entries
        ]
    except Exception as e:
        _debug("changelog", f"_get_recent_changelog failed: {e}", "warning")
        return []


def _get_config_for_country(country_code: str) -> Dict[str, Any]:
    """
    Get the full config for a country. Merges COUNTRY_CONFIG templates
    with XML_FEEDS data and domain/merchant info.
    If country has no explicit COUNTRY_CONFIG, builds a fallback from language.
    """
    cc = country_code.upper()
    feed_info = XML_FEEDS.get(cc, {})
    lang_code = feed_info.get("lang_code", "EN")
    template_country_by_lang = {
        "RO": "RO",
        "TR": "TR",
        "PL": "PL",
        "HU": "HU",
        "FR": "FR",
        "DE": "DE",
        "IT": "IT",
        "ES": "ES",
        "CS": "CZ",
        "SK": "SK",
        "BG": "BG",
        "EL": "GR",
        "HR": "HR",
        "SL": "SI",
        "LT": "LT",
        "ET": "EE",
        "LV": "LV",
        "UK": "UA",
        "MK": "MK",
        "SR": "RS",
        "SQ": "AL",
    }
    template_country = cc if cc in COUNTRY_CONFIG else template_country_by_lang.get(lang_code, "EN")

    # Start with explicit config or fallback to EN
    config = COUNTRY_CONFIG.get(template_country, COUNTRY_CONFIG.get("EN", {})).copy()

    # Override with actual feed/domain/merchant data
    config["xml_url"] = feed_info.get("xml_url", "")
    config["domain"] = DOMAINS.get(cc, f"{cc.lower()}.example.com")
    config["merchant_id"] = MERCHANT_IDS.get(cc, "")
    config["language_code"] = LANG_ISO.get(lang_code, lang_code.lower())
    config["language"] = LANG_NAMES.get(lang_code, lang_code)
    config["country_code"] = cc
    config["country_name"] = feed_info.get("name", cc)
    config["feed_products_count"] = feed_info.get("products", 0)
    config["_template_country"] = template_country

    return config

CUSTOMER_ID = _config.get('google_ads', {}).get('customer_id', '0000000000')

# SSL context for fetching XML feeds
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE


# ---------------------------------------------------------------------------
# Pydantic models for MCP tool inputs
# ---------------------------------------------------------------------------

class BatchSetupRequest(BaseModel):
    """Input for batch_setup_products tool."""
    country_code: str = Field(..., description="Country code: RO, TR, PL, HU, FR, DE, IT, ES, CZ, SK, BG, GR, HR, etc. Full list: " + ", ".join(sorted(XML_FEEDS.keys())))
    campaign_id: str = Field(..., description="Google Ads campaign ID")
    xml_feed_url: Optional[str] = Field(None, description="URL to Shopify XML product feed. If not provided, auto-resolved from country_code.")
    product_handles: List[str] = Field(..., description="List of product handles to process")
    dry_run: bool = Field(False, description="If true, only report what would be done without making changes")
    skip_existing: bool = Field(True, description="Skip products that already have a complete ad group setup")
    asset_types: Optional[List[str]] = Field(None, description="Restrict to: rsa, callouts, sitelinks, anchor_sitelinks, promotion, snippets, keywords, images. None=all (anchor_sitelinks NOT included in None/all — must be explicit)")
    force_refresh: bool = Field(False, description="Force refresh of cached GAQL data")
    force_replace_rsa: bool = Field(False, description="If true, pause existing RSAs and create new ones from AI endpoint. Use to upgrade template-generated RSAs to AI-powered copy.")
    feed_type: Optional[str] = Field(None, description="Feed source: 'main' (default) or 'new_products'. When 'new_products', auto-resolves feed URL from NP_FEEDS BW feeds.")
    shop_domain: Optional[str] = Field(None, description="Filter to specific shop domain (e.g., 'www.yourstore.com'). Only with feed_type='new_products'.")


class WarmupRequest(BaseModel):
    """Input for batch_warmup_cache tool."""
    country_code: str = Field(..., description="Country code (e.g., TR, RO, PL)")
    campaign_id: str = Field(..., description="Campaign ID (for asset performance lookup)")
    product_handles: List[str] = Field(
        default_factory=list,
        description="Specific handles to warm (empty = all from XML feed)",
    )
    force_refresh: bool = Field(False, description="Overwrite existing cache")
    max_products: int = Field(0, description="Limit products (0 = all)")
    feed_type: Optional[str] = Field(None, description="Feed source: 'main' (default) or 'new_products'. When 'new_products', uses NP_FEEDS BW feeds.")
    shop_domain: Optional[str] = Field(None, description="Filter to specific shop domain. Only with feed_type='new_products'.")


class WarmupStatusRequest(BaseModel):
    """Input for batch_warmup_status tool."""
    job_id: str = Field(..., description="Job ID returned by batch_warmup_cache")


class MarketIntelWarmupRequest(BaseModel):
    """Input for batch_warmup_market_intel tool."""
    country_code: str = Field(..., description="Country code (e.g., HR, TR, RO, PL)")
    product_handles: List[str] = Field(
        default_factory=list,
        description="Specific handles to warm (empty = discover ALL from XML feed)"
    )
    batch_size: int = Field(3, description="Parallel workers (1-10)", ge=1, le=10)
    skip_cached: bool = Field(True, description="Skip handles with fresh market intel cache")
    force_refresh: bool = Field(False, description="Force re-fetch even if cached")
    timeout: int = Field(180, description="Per-worker timeout in seconds (60-600)", ge=60, le=600)
    bots: Optional[List[str]] = Field(
        None,
        description="Subset of bots: trends, competitor_ads, suggestions, cpc_volume (None = all 4)"
    )
    feed_type: Optional[str] = Field(
        None,
        description="Feed source: 'main' (default) or 'new_products'. When 'new_products', discovery uses NP_FEEDS BW feeds."
    )
    shop_domain: Optional[str] = Field(
        None,
        description="Filter discovery to specific shop domain (e.g., 'www.yourstore.com'). Only with feed_type='new_products'. None = merge all shops."
    )


class MarketIntelWarmupStatusRequest(BaseModel):
    """Input for batch_warmup_market_intel_status tool."""
    job_id: str = Field(..., description="Job ID returned by batch_warmup_market_intel")


class BatchAuditRequest(BaseModel):
    """Input for batch_audit_campaign tool."""
    country_code: str = Field(..., description="Country code: RO, TR, PL etc.")
    campaign_id: str = Field(..., description="Google Ads campaign ID")


class AdCopyGenerateRequest(BaseModel):
    """Input for batch_generate_ad_copy tool."""
    country_code: str = Field(..., description="Country code for language selection")
    product_name: str = Field(..., description="Product display name")
    product_price: float = Field(..., description="Product price in local currency")
    product_url: str = Field(..., description="Product landing page URL")
    product_category: Optional[str] = Field(None, description="Optional product category hint")


# ---------------------------------------------------------------------------
# Session-level GAQL cache (avoids repeated startup queries within same session)
# ---------------------------------------------------------------------------
_session_cache: Dict[str, tuple] = {}  # {key: (data, timestamp)}
SESSION_CACHE_TTL = 1800  # 30 minutes


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _fetch_xml_feed(url: str) -> List[Dict[str, Any]]:
    """Fetch and parse Shopify XML product feed. Returns list of product dicts."""
    req = urllib.request.Request(url, headers={"User-Agent": "GoogleAds-BatchOptimizer/1.0"})
    with urllib.request.urlopen(req, context=_ssl_ctx, timeout=30) as resp:
        xml_data = resp.read()

    root = ET.fromstring(xml_data)
    ns = {"g": "http://base.google.com/ns/1.0"}
    products = []

    for entry in root.findall(".//entry", {"": "http://www.w3.org/2005/Atom"}):
        # Try Atom namespace first
        pass

    # Try RSS format (Shopify default)
    for item in root.findall(".//channel/item") or root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        product = {}
        # Title
        title_el = item.find("title") or item.find("g:title", ns)
        product["title"] = title_el.text.strip() if title_el is not None and title_el.text else ""

        # Link/handle
        link_el = item.find("link") or item.find("g:link", ns)
        product["link"] = link_el.text.strip() if link_el is not None and link_el.text else ""

        # Price
        price_el = item.find("g:price", ns)
        if price_el is not None and price_el.text:
            price_text = price_el.text.strip().split(" ")[0].replace(",", ".")
            try:
                product["price"] = float(price_text)
            except ValueError:
                product["price"] = 0.0
        else:
            product["price"] = 0.0

        # ID
        id_el = item.find("g:id", ns)
        product["id"] = id_el.text.strip() if id_el is not None and id_el.text else ""

        # Description (for data-driven ad copy)
        desc_el = item.find("description") or item.find("g:description", ns)
        product["description"] = desc_el.text.strip()[:500] if desc_el is not None and desc_el.text else ""

        # Product type (category from feed, e.g., "Electronics > Headphones", "Fashion > Shoes")
        pt_el = item.find("g:product_type", ns)
        product["product_type"] = pt_el.text.strip() if pt_el is not None and pt_el.text else ""

        # Sale price (actual selling price — used for promotions)
        sp_el = item.find("g:sale_price", ns)
        if sp_el is not None and sp_el.text:
            sp_text = sp_el.text.strip().split(" ")[0].replace(",", ".")
            try:
                product["sale_price"] = float(sp_text)
            except ValueError:
                product["sale_price"] = 0.0
        else:
            product["sale_price"] = 0.0

        # Custom label 0 (handle)
        cl0 = item.find("g:custom_label_0", ns)
        product["handle"] = cl0.text.strip() if cl0 is not None and cl0.text else ""

        # If no handle from custom_label, extract from link
        if not product["handle"] and product["link"]:
            product["handle"] = product["link"].rstrip("/").split("/")[-1]

        if product["handle"]:
            products.append(product)

    return products


def _fetch_xml_feed_cached(country_code: str, url: str) -> List[Dict[str, Any]]:
    """Fetch XML feed with caching (1h TTL). Falls back to direct fetch on cache miss."""
    db = _get_db()
    cached = db.get_cached_feed(country_code, url)
    if cached is not None:
        return cached
    products = _fetch_xml_feed(url)
    db.set_cached_feed(country_code, url, products)
    return products


def _handle_to_product_name(handle: str) -> str:
    """Convert product handle to display name: 'arthrovia-ro' -> 'Arthrovia'."""
    # Remove country suffix
    name = re.sub(r"-[a-z]{2}$", "", handle)
    # Remove price suffixes like '-120-ron'
    name = re.sub(r"-\d+-[a-z]{3}$", "", name)
    # Convert hyphens to spaces and title-case
    name = name.replace("-", " ").title()
    return name


# ---------------------------------------------------------------------------
# Batch Initialize (Pre-warm) endpoint + helpers
# ---------------------------------------------------------------------------
BATCH_INITIALIZE_URL = _get_endpoint('batch_initialize', "")
BATCH_INITIALIZE_POST_TIMEOUT = 30   # POST just creates a job (<1s), generous timeout for network
BATCH_INITIALIZE_POLL_TIMEOUT = 15   # Each GET poll (<1s response)
BATCH_INITIALIZE_POLL_INTERVAL = 10  # Seconds between polls

# ---------------------------------------------------------------------------
# Market Intelligence (DataForSEO/Botster) warmup endpoint
# ---------------------------------------------------------------------------
BATCH_WARM_DFO_URL = _get_endpoint('market_intel_warmup', "")
BATCH_WARM_DFO_POST_TIMEOUT = 30     # POST just creates job (<1s)
BATCH_WARM_DFO_POLL_TIMEOUT = 15     # Each GET poll (<1s response)
BATCH_WARM_DFO_POLL_INTERVAL = 15    # Seconds between polls


def _gather_product_asset_performance(
    handle: str,
    country_code: str,
    campaign_id: str,
    db: "BatchDB",
) -> dict:
    """
    Gather BEST/GOOD/LOW asset labels for a product across ALL campaigns
    where it appears (not just the target campaign).
    """
    try:
        from batch_intelligence import discover_product_everywhere
        product_discovery = discover_product_everywhere(handle)
        all_campaign_ids = list(set([campaign_id] + product_discovery.get("campaign_ids", [])))
    except Exception as e:
        _debug("perf_data", f"discover_product_everywhere failed for {handle}: {e}", "warning")
        all_campaign_ids = [campaign_id]

    conn = db._get_conn()
    perf: Dict[str, Any] = {}
    # asset_type in DB may be stored as raw enum int ("2"=HEADLINE, "3"=DESCRIPTION)
    # or as string name ("HEADLINE", "DESCRIPTION") depending on when data was gathered
    for asset_type_variants, hl_keys in [
        (("HEADLINE", "2"), ("best_headlines", "good_headlines", "low_headlines")),
        (("DESCRIPTION", "3"), ("best_descriptions", "good_descriptions", "low_descriptions")),
    ]:
        placeholders = ",".join("?" for _ in all_campaign_ids)
        at_placeholders = ",".join("?" for _ in asset_type_variants)
        rows = conn.execute(
            f"""SELECT headline_pattern, performance_label, SUM(occurrences) as total_occ
                FROM asset_performance
                WHERE country_code=? AND campaign_id IN ({placeholders}) AND asset_type IN ({at_placeholders})
                GROUP BY headline_pattern, performance_label
                ORDER BY total_occ DESC
                LIMIT 40""",
            (country_code, *all_campaign_ids, *asset_type_variants),
        ).fetchall()
        if rows:
            # performance_label may be raw enum int: "4"=LOW, "5"=GOOD, "6"=BEST
            PERF_RESOLVE = {"4": "LOW", "5": "GOOD", "6": "BEST", "7": "NOT_APPLICABLE"}
            by_label: Dict[str, list] = {"BEST": [], "GOOD": [], "LOW": []}
            for r in rows:
                label = r["performance_label"]
                label = PERF_RESOLVE.get(label, label)  # resolve raw int if needed
                if label in by_label:
                    by_label[label].append(r["headline_pattern"])
            for label, key in zip(["BEST", "GOOD", "LOW"], hl_keys):
                if by_label[label]:
                    perf[key] = by_label[label][:10]

    if perf:
        perf["_campaigns_scanned"] = len(all_campaign_ids)
    return perf


def _gather_product_keyword_intelligence(
    handle: str,
    country_code: str,
    feed_product_type: str = "",
) -> dict:
    """
    Gather proven and category keywords for a product from batch intelligence.
    """
    try:
        from batch_intelligence import get_keywords_for_setup
        kw_recs = get_keywords_for_setup(
            handle, country_code, "0",
            min_roi=0.0, max_keywords=15,
            feed_product_type=feed_product_type,
        )
        if not kw_recs:
            return {}

        proven = []
        category = []
        for kw in kw_recs:
            entry = {
                "keyword": kw.get("text", ""),
                "match_type": kw.get("match_type", "PHRASE"),
                "conversions": kw.get("conversions", 0),
                "roi_percent": kw.get("roi_percent", 0),
                "cpc": kw.get("avg_cpc", 0),
            }
            if kw.get("source") == "historical":
                proven.append(entry)
            else:
                category.append(entry)

        result: Dict[str, Any] = {}
        if proven:
            result["proven_keywords"] = proven
        if category:
            result["category_keywords"] = category
        return result
    except Exception as e:
        _debug("keyword_intel", f"get_keyword_intelligence failed for {handle}: {e}", "warning")
        return {}


def _gather_product_overrides_from_feed(
    handle: str,
    feed_products: Dict[str, dict],
) -> dict:
    """Extract product overrides from XML feed data."""
    fd = feed_products.get(handle)
    if not fd:
        return {}

    overrides: Dict[str, Any] = {}
    if fd.get("title"):
        overrides["product_name"] = fd["title"]
    if fd.get("link"):
        overrides["product_url"] = fd["link"]

    sale_price = fd.get("sale_price") or fd.get("price")
    original_price = fd.get("price")
    if sale_price:
        overrides["product_price"] = sale_price
        overrides["sale_price"] = sale_price
    if original_price and original_price != sale_price:
        overrides["original_price"] = original_price

    return overrides


def _gather_winning_patterns(
    country_code: str,
    db: "BatchDB",
) -> dict:
    """Gather global BEST/GOOD headline and description patterns for a country."""
    patterns: Dict[str, Any] = {}
    try:
        winning_hl = db.get_winning_patterns(country_code, asset_type="HEADLINE", min_occurrences=2)
        winning_desc = db.get_winning_patterns(country_code, asset_type="DESCRIPTION", min_occurrences=2)
        if winning_hl:
            patterns["best_headline_patterns"] = [
                {"pattern": h.get("headline_pattern", ""), "label": "BEST" if h.get("best_count", 0) > 0 else "GOOD", "count": h.get("total_count", 1)}
                for h in winning_hl[:15]
            ]
        if winning_desc:
            patterns["best_description_patterns"] = [
                {"pattern": d.get("headline_pattern", ""), "label": "BEST" if d.get("best_count", 0) > 0 else "GOOD", "count": d.get("total_count", 1)}
                for d in winning_desc[:10]
            ]
    except Exception as e:
        _debug("winning_patterns", f"get_winning_asset_patterns failed for {country_code}: {e}", "warning")
    return patterns


def _gather_full_analytics_from_gaql(
    country_code: str,
    campaign_id: str,
    feed_products: Dict[str, dict],
) -> Dict[str, Any]:
    """
    FULL analytics data gathering from Google Ads API via GAQL.

    Phase 1: Asset performance labels (BEST/GOOD/LOW) for all campaigns
             matching this country — via analyze_all_campaigns_for_country().
             Stores results in asset_performance DB table.

    Phase 2: Keyword intelligence — BULK GAQL query for ALL converting keywords
             across ALL campaigns in the account. Maps keywords to product handles
             by building an account-wide ad_group→handle map from RSA final_urls.
             Stores results in keyword_intelligence DB table.

    This replaces the per-product discover_product_everywhere() approach
    (which would be 73 GAQL calls for 73 products) with just 3-5 GAQL calls total.

    Returns summary dict with counts of what was gathered.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    db = _get_db()
    summary: Dict[str, Any] = {
        "asset_campaigns_scanned": 0,
        "total_assets_analyzed": 0,
        "keywords_gathered": 0,
        "products_with_keywords": 0,
        "gaql_calls": 0,
    }

    # ────────────────────────────────────────────────────────────
    # PHASE 1: Asset Performance Labels (BEST/GOOD/LOW/LEARNING)
    # ────────────────────────────────────────────────────────────
    # Find ALL campaigns (active + paused) matching this country code,
    # then scan each for asset performance labels. Paused campaigns
    # often have rich historical data (labels assigned before pausing).
    #
    # Search: campaign name starts with "CC " (e.g., "TR - Search...")
    # This avoids false positives like "STRUCTURED" matching "TR".
    try:
        from batch_analytics import analyze_campaign_assets

        # 1 GAQL call: find all campaigns for this country (active + paused)
        campaign_query = f"""
            SELECT campaign.id, campaign.name, campaign.status
            FROM campaign
            WHERE campaign.status != 'REMOVED'
              AND campaign.name LIKE '{country_code} %'
        """
        campaign_results = _execute_gaql(CUSTOMER_ID, campaign_query, page_size=200)
        summary["gaql_calls"] += 1

        campaigns_found = []
        for row in campaign_results:
            cmp_id = str(_safe_get_value(row, "campaign.id", ""))
            cmp_name = str(_safe_get_value(row, "campaign.name", ""))
            if cmp_id:
                campaigns_found.append((cmp_id, cmp_name))

        # Scan each campaign for asset labels (2 GAQL per campaign)
        # Time guard: stop if Phase 1 exceeds 25s to leave time for Phase 2 + POST
        phase1_start = time.time()
        PHASE1_MAX_SECONDS = 25
        total_assets = 0
        scanned = 0
        for cmp_id, cmp_name in campaigns_found:
            if (time.time() - phase1_start) > PHASE1_MAX_SECONDS:
                summary["phase1_truncated"] = f"Stopped after {scanned}/{len(campaigns_found)} campaigns (time limit)"
                break
            try:
                result = analyze_campaign_assets(country_code, cmp_id)
                total_assets += result.get("total_assets_analyzed", 0)
                summary["gaql_calls"] += 2
                scanned += 1
            except Exception as e:
                _debug("asset_scan", f"analyze_campaign_assets failed for {cmp_id}: {e}", "warning")
                scanned += 1

        summary["asset_campaigns_scanned"] = scanned
        summary["total_assets_analyzed"] = total_assets
        summary["asset_campaigns"] = [n for _, n in campaigns_found]
    except Exception as e:
        summary["asset_error"] = str(e)[:200]

    # ────────────────────────────────────────────────────────────
    # PHASE 2: Bulk Keyword Intelligence
    # ────────────────────────────────────────────────────────────
    # Step 2a: Build account-wide ad_group→handle map from ALL campaigns
    #          by scanning ad final_urls. One GAQL query for all.
    try:
        all_ag_handle_map: Dict[str, str] = {}  # ad_group_id -> product_handle
        all_ag_product_name: Dict[str, str] = {}  # ad_group_id -> product_name

        # Query ads from ALL campaigns that contain this country's domain in URLs.
        # This catches the product across all campaigns (active + paused) for keyword mining.
        # Much faster than scanning the entire account.
        domain_hint = feed_products.get(list(feed_products.keys())[0] if feed_products else "", {}).get("link", "")
        domain_filter = ""
        if domain_hint:
            # Extract domain like "yourstore.com"
            import urllib.parse as _up
            parsed = _up.urlparse(domain_hint)
            if parsed.netloc:
                domain_filter = f"AND ad_group_ad.ad.final_urls CONTAINS '{parsed.netloc}'"

        url_query = f"""
            SELECT ad_group.id, ad_group.name, ad_group_ad.ad.final_urls, campaign.id
            FROM ad_group_ad
            WHERE ad_group_ad.status != 'REMOVED'
              AND ad_group.status != 'REMOVED'
              {domain_filter}
        """
        url_results = _execute_gaql(CUSTOMER_ID, url_query, page_size=5000)
        summary["gaql_calls"] += 1

        for row in url_results:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            ag_name = str(_safe_get_value(row, "ad_group.name", ""))
            final_urls = str(_safe_get_value(row, "ad_group_ad.ad.final_urls", ""))

            handle_match = re.search(r'/products/([a-z0-9][a-z0-9\-]+?)(?:\?|$)', final_urls)
            if handle_match and ag_id:
                handle = handle_match.group(1)
                all_ag_handle_map[ag_id] = handle
                all_ag_product_name[ag_id] = ag_name

        # Step 2b: BULK query for ALL keywords with conversions across the account
        #          Single GAQL call — gets everything at once
        from datetime import datetime as dt, timedelta
        end_date = dt.now().strftime("%Y-%m-%d")
        start_date = (dt.now() - timedelta(days=90)).strftime("%Y-%m-%d")

        kw_query = f"""
            SELECT ad_group_criterion.keyword.text,
                   ad_group_criterion.keyword.match_type,
                   ad_group_criterion.quality_info.quality_score,
                   metrics.impressions, metrics.clicks,
                   metrics.conversions, metrics.conversions_value,
                   metrics.cost_micros,
                   ad_group.name, ad_group.id,
                   campaign.name, campaign.id
            FROM keyword_view
            WHERE metrics.conversions > 0
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY metrics.conversions DESC
        """
        kw_results = _execute_gaql(CUSTOMER_ID, kw_query, page_size=5000)
        summary["gaql_calls"] += 1

        # Step 2c: Parse results and map to product handles
        from batch_intelligence import _parse_keyword_rows, _normalize_product_name

        product_kw_sets: Dict[str, List[Dict[str, Any]]] = {}  # handle -> [kw_dicts]

        for row in kw_results:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            handle = all_ag_handle_map.get(ag_id)
            if not handle:
                continue

            product_name = _normalize_product_name(handle)
            kw_list = _parse_keyword_rows([row], product_name, country_code, _safe_get_value)
            if kw_list:
                if handle not in product_kw_sets:
                    product_kw_sets[handle] = []
                product_kw_sets[handle].extend(kw_list)

        # Step 2d: Store ALL keywords in DB
        all_kw_records: List[Dict[str, Any]] = []
        for handle, kw_list in product_kw_sets.items():
            all_kw_records.extend(kw_list)
        if all_kw_records:
            db.bulk_upsert_keyword_intel(all_kw_records)

        summary["keywords_gathered"] = len(all_kw_records)
        summary["products_with_keywords"] = len(product_kw_sets)

    except Exception as e:
        summary["keyword_error"] = str(e)[:200]

    return summary


# ---------------------------------------------------------------------------
# AI Ad Generator endpoint integration (configurable via config.json)
# ---------------------------------------------------------------------------
AD_GENERATOR_BASE_URL = _get_endpoint('ad_generator', "")
AD_GENERATOR_TIMEOUT = 25  # seconds — copy_only mode typically responds in 0.3-5s; extra headroom for cold starts

# Frozen performance context — used by batch_setup_all to stabilize perf_hash
# across all products in a single batch run. When set, _fetch_ai_generated_copy
# uses these cached values instead of querying DB (which may have changed).
# This prevents GPT cache invalidation during a batch run.
_frozen_perf_context: Optional[Dict[str, Any]] = None


def _fetch_ai_generated_copy(
    handle: str,
    country_code: str,
    config: dict,
    product_name: str,
    product_price: Optional[float],
    product_url: str,
    campaign_id: str,
    product_description: str = "",
    feed_data: Optional[dict] = None,
) -> Optional[Dict[str, Any]]:
    """
    Call gADS_ad_generator endpoint for AI-powered ad copy.
    Returns unified ad_copy dict (same format as _generate_ad_copy_local) or None on failure.

    The endpoint generates:
    - 3 RSA variants (A/B/C) with 15 headlines + 4 descriptions each
    - Keywords with match types, volumes, CPC
    - Callouts, sitelinks, promotion, structured snippets
    - Compliance validation

    Falls back to None (triggering template-based generation) on any error.
    """
    db = _get_db()

    # --- Build payload ---
    payload: Dict[str, Any] = {
        "product_handle": handle,
        "country": country_code,
        "mode": "copy_only",  # Default to copy_only (fast, reliable); full mode often 504s when Botster has no data
    }

    # Product overrides (from feed data + config)
    overrides: Dict[str, Any] = {}
    if product_name:
        overrides["product_name"] = product_name
    if product_price and product_price > 0:
        overrides["product_price"] = product_price
    if product_url:
        overrides["product_url"] = product_url

    # Try to get sale_price / original price from feed for promotion calculation
    # CRITICAL: product_price must ALWAYS be sale_price (the actual selling price).
    # original_price is sent separately for discount calculation only.
    if feed_data:
        sale_price = feed_data.get("sale_price") or feed_data.get("price")
        original_price = feed_data.get("price")  # Full price before discount
        if sale_price:
            overrides["sale_price"] = sale_price
            # Ensure product_price is ALWAYS the sale_price (what customer pays)
            overrides["product_price"] = sale_price
        if original_price and original_price != sale_price:
            overrides["original_price"] = original_price  # Separate key for full price

    if overrides:
        payload["product_overrides"] = overrides

    # --- Inject Google Ads performance data from DB ---
    # USE FROZEN CONTEXT if available (set by batch_setup_all to stabilize perf_hash).
    # This prevents GPT cache invalidation when DB is updated between products.
    global _frozen_perf_context

    if _frozen_perf_context and country_code in (_frozen_perf_context.get("_country", ""),):
        # Use frozen data — same for all products in this batch run
        frozen_perf = _frozen_perf_context.get("existing_performance", {})
        if frozen_perf:
            payload["existing_performance"] = frozen_perf
        frozen_patterns = _frozen_perf_context.get("winning_patterns", {})
        if frozen_patterns:
            payload["winning_patterns"] = frozen_patterns
    else:
        # Normal mode — query DB per product (used by batch_setup_products / single calls)

        # 1. Existing performance — cross-campaign per-product asset labels.
        #    We first discover ALL campaigns where this product appears in ad URLs,
        #    then pull asset_performance from ALL those campaigns (not just the target).
        #    This gives the endpoint the fullest picture of what headlines/descriptions
        #    historically performed well for this specific product across the entire account.
        try:
            from batch_intelligence import discover_product_everywhere
            product_discovery = discover_product_everywhere(handle)
            all_campaign_ids = list(set([campaign_id] + product_discovery.get("campaign_ids", [])))

            conn = db._get_conn()
            perf: Dict[str, Any] = {}
            for asset_type, hl_keys in [("HEADLINE", ("best_headlines", "good_headlines", "low_headlines")),
                                         ("DESCRIPTION", ("best_descriptions", "good_descriptions", "low_descriptions"))]:
                # Query across ALL discovered campaigns for this product
                placeholders = ",".join("?" for _ in all_campaign_ids)
                rows = conn.execute(
                    f"""SELECT headline_pattern, performance_label, SUM(occurrences) as total_occ
                        FROM asset_performance
                        WHERE country_code=? AND campaign_id IN ({placeholders}) AND asset_type=?
                        GROUP BY headline_pattern, performance_label
                        ORDER BY total_occ DESC
                        LIMIT 40""",
                    (country_code, *all_campaign_ids, asset_type)
                ).fetchall()
                if rows:
                    by_label: Dict[str, list] = {"BEST": [], "GOOD": [], "LOW": []}
                    for r in rows:
                        label = r["performance_label"]
                        if label in by_label:
                            by_label[label].append(r["headline_pattern"])
                    for label, key in zip(["BEST", "GOOD", "LOW"], hl_keys):
                        if by_label[label]:
                            perf[key] = by_label[label][:10]
            if perf:
                perf["_campaigns_scanned"] = len(all_campaign_ids)
                payload["existing_performance"] = perf
        except Exception as e:
            _debug("ai_copy", f"Perf data assembly failed for {handle}: {e}", "warning")

        # 2. Winning patterns (generalized across all products for this country)
        try:
            winning_hl = db.get_winning_patterns(country_code, asset_type="HEADLINE", min_occurrences=2)
            winning_desc = db.get_winning_patterns(country_code, asset_type="DESCRIPTION", min_occurrences=2)
            if winning_hl or winning_desc:
                patterns: Dict[str, Any] = {}
                if winning_hl:
                    patterns["best_headline_patterns"] = [
                        {"pattern": h.get("pattern", ""), "label": h.get("label", "GOOD"), "count": h.get("count", 1)}
                        for h in winning_hl[:15]
                    ]
                if winning_desc:
                    patterns["best_description_patterns"] = [
                        {"pattern": d.get("pattern", ""), "label": d.get("label", "GOOD"), "count": d.get("count", 1)}
                        for d in winning_desc[:10]
                    ]
                if patterns:
                    payload["winning_patterns"] = patterns
        except Exception as e:
            _debug("ai_copy", f"Winning patterns retrieval failed for {handle}: {e}", "warning")

    # 3. Keyword intelligence (proven converting keywords for this product)
    try:
        from batch_intelligence import get_keywords_for_setup
        _fpt = feed_data.get("product_type", "") if feed_data else ""
        kw_recs = get_keywords_for_setup(handle, country_code, "0", min_roi=0.0, max_keywords=15, feed_product_type=_fpt)
        if kw_recs:
            proven = []
            category = []
            for kw in kw_recs:
                entry = {
                    "keyword": kw.get("text", ""),
                    "match_type": kw.get("match_type", "PHRASE"),
                    "conversions": kw.get("conversions", 0),
                    "roi_percent": kw.get("roi_percent", 0),
                    "cpc": kw.get("avg_cpc", 0),
                }
                if kw.get("source") in ("historical", "cross_country"):
                    proven.append(entry)
                elif kw.get("source") == "category":
                    category.append(entry)
            kw_intel: Dict[str, Any] = {}
            if proven:
                kw_intel["proven_keywords"] = proven
            if category:
                kw_intel["category_keywords"] = category
            if kw_intel:
                payload["keyword_intelligence"] = kw_intel
    except Exception as e:
        _debug("ai_copy", f"Keyword intelligence failed for {handle}: {e}", "warning")

    import hashlib as _hl

    generation_attempts = 2
    for generation_attempt in range(generation_attempts):
        attempt_payload = dict(payload)
        if generation_attempt > 0:
            retry_options = dict(attempt_payload.get("options") or {})
            retry_options["force_refresh"] = True
            retry_options["strict_language_retry"] = True
            attempt_payload["options"] = retry_options

        _cache_payload = {k: v for k, v in attempt_payload.items() if k not in ("_ts",)}
        _payload_hash = _hl.md5(json.dumps(_cache_payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

        cached_response = db.get_ai_copy_cache(handle, country_code, _payload_hash)
        if cached_response is not None:
            _debug("ai_copy", f"MCP cache HIT for {handle}/{country_code} (hash={_payload_hash[:8]})", "info")
            response_data = cached_response
        else:
            _debug("ai_copy", f"MCP cache MISS for {handle}/{country_code} (hash={_payload_hash[:8]}), calling endpoint...", "info")
            try:
                max_retries = 1
                retry_delay = 3
                response_data = None

                for attempt in range(max_retries + 1):
                    try:
                        req_data = json.dumps(attempt_payload).encode("utf-8")
                        req = urllib.request.Request(
                            AD_GENERATOR_BASE_URL,
                            data=req_data,
                            headers={"Content-Type": "application/json", "Accept": "application/json"},
                            method="POST",
                        )
                        ssl_ctx = ssl.create_default_context()
                        ssl_ctx.check_hostname = False
                        ssl_ctx.verify_mode = ssl.CERT_NONE
                        with urllib.request.urlopen(req, timeout=AD_GENERATOR_TIMEOUT, context=ssl_ctx) as resp:
                            response_data = json.loads(resp.read().decode("utf-8"))
                        break
                    except (urllib.error.URLError, socket.timeout, json.JSONDecodeError) as e:
                        error_str = str(e)
                        is_retryable = any(code in error_str for code in ["502", "503", "504", "timed out", "Connection reset"])
                        if is_retryable and attempt < max_retries:
                            db.log_operation(
                                country_code=country_code, campaign_id=campaign_id,
                                handle=handle, operation="AI_COPY_RETRY",
                                status="WARNING", details=f"Attempt {attempt+1} failed ({error_str[:100]}), retrying in {retry_delay}s..."
                            )
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        raise

                if response_data is None:
                    raise RuntimeError("No response received after retries")
            except urllib.error.URLError as e:
                db.log_operation(
                    country_code=country_code, campaign_id=campaign_id,
                    handle=handle, operation="AI_COPY_ENDPOINT_ERROR",
                    status="ERROR", details=f"Endpoint unreachable after {max_retries+1} attempts: {str(e)[:200]}"
                )
                return None
            except json.JSONDecodeError as e:
                db.log_operation(
                    country_code=country_code, campaign_id=campaign_id,
                    handle=handle, operation="AI_COPY_JSON_ERROR",
                    status="ERROR", details=f"Invalid JSON response: {str(e)[:200]}"
                )
                return None
            except Exception as e:
                db.log_operation(
                    country_code=country_code, campaign_id=campaign_id,
                    handle=handle, operation="AI_COPY_ERROR",
                    status="ERROR", details=f"Unexpected error: {str(e)[:200]}"
                )
                return None

        if response_data.get("status") != "ok" and response_data.get("error"):
            error_code = response_data.get("code", "UNKNOWN")
            error_msg = response_data.get("error", "Unknown endpoint error")
            db.log_operation(
                country_code=country_code, campaign_id=campaign_id,
                handle=handle, operation="AI_COPY_ENDPOINT_REJECTED",
                status="ERROR", details=f"[{error_code}] {error_msg[:200]}"
            )
            return None

        compliance = response_data.get("compliance", {})
        if compliance.get("passed") is False:
            errors = compliance.get("errors", [])
            db.log_operation(
                country_code=country_code, campaign_id=campaign_id,
                handle=handle, operation="AI_COPY_COMPLIANCE_WARN",
                status="WARNING", details=f"Compliance errors: {errors[:3]}"
            )

        hallucination_fixes = _validate_ai_response(
            response_data, product_name, product_price, product_url,
            country_code, config, db, campaign_id, handle
        )
        reject_reasons = response_data.get("_validation_reject_reasons", [])
        if reject_reasons:
            if generation_attempt + 1 < generation_attempts:
                db.log_operation(
                    country_code=country_code, campaign_id=campaign_id,
                    handle=handle, operation="AI_COPY_RETRY",
                    status="WARNING",
                    details=f"Validation rejected AI copy ({', '.join(reject_reasons[:3])}); retrying with strict language prompt."
                )
                continue

            db.log_operation(
                country_code=country_code, campaign_id=campaign_id,
                handle=handle, operation="AI_COPY_VALIDATION_REJECTED",
                status="ERROR",
                details=f"Validation rejected AI copy after retry: {', '.join(reject_reasons[:3])}"[:500]
            )
            return None

        try:
            result = _convert_endpoint_response(response_data, config, product_name, product_price, product_url)
        except Exception as e:
            db.log_operation(
                country_code=country_code, campaign_id=campaign_id,
                handle=handle, operation="AI_COPY_CONVERT_FAIL",
                status="ERROR", details=f"Conversion error: {str(e)[:200]}"
            )
            return None

        if response_data.get("status") == "ok":
            try:
                db.set_ai_copy_cache(handle, country_code, _payload_hash, response_data, campaign_id)
                _debug("ai_copy", f"MCP cache STORED for {handle}/{country_code} (hash={_payload_hash[:8]})", "info")
            except Exception as cache_err:
                _debug("ai_copy", f"MCP cache store failed for {handle}: {cache_err}", "warning")

        if result:
            log_details = None
            if hallucination_fixes > 0:
                log_details = f"AI copy generated with {hallucination_fixes} hallucination fix(es) applied"
            db.log_operation(
                country_code=country_code, campaign_id=campaign_id,
                handle=handle, operation="AI_COPY_GENERATED",
                status="SUCCESS", details=log_details
            )
            return result

        db.log_operation(
            country_code=country_code, campaign_id=campaign_id,
            handle=handle, operation="AI_COPY_CONVERT_FAIL",
            status="ERROR", details="Failed to convert endpoint response"
        )
        return None

    return None


def _validate_ai_response(
    response_data: Dict[str, Any],
    product_name: str,
    product_price: Optional[float],
    product_url: str,
    country_code: str,
    config: dict,
    db,
    campaign_id: str,
    handle: str,
) -> int:
    """
    Validate AI-generated ad copy for hallucinations and quality issues.
    Mutates response_data in-place to fix issues. Returns count of fixes applied.

    Checks performed:
    1. Wrong product name in headlines/descriptions (GPT mixed up products)
    2. Wrong price (GPT hallucinated a different price)
    3. Fabricated medical/health claims (Google Ads policy violation risk)
    4. Wrong language (GPT responded in wrong language)
    5. Competitor brand names injected as positive content
    6. Headlines >30 chars / descriptions >90 chars (should be trimmed)
    7. Duplicate headlines within same RSA variant
    8. Empty or placeholder text
    """
    fixes = 0
    issues = []

    ads = response_data.get("ads", {})
    rsa_variants = ads.get("rsa_variants", [])
    if not rsa_variants:
        return 0

    # --- Prepare reference data ---
    product_name_lower = product_name.lower().strip() if product_name else ""
    # Extract brand core (first word, e.g. "Arthrovia" from "Arthrovia Gel")
    brand_core = product_name_lower.split()[0] if product_name_lower else ""
    price_str = str(int(product_price)) if product_price and product_price > 0 else ""

    # Known competitor/unrelated brands that GPT might hallucinate
    COMPETITOR_BRANDS = _get_known_competitor_brand_terms()

    # Prohibited medical claim patterns (could trigger Google Ads disapproval)
    PROHIBITED_CLAIM_PATTERNS = [
        r"\b(vindec[aă]|cur[eă]|trate[az]ă|elimină complet|garantat)\b",
        r"\b(cure[sd]?|heals?|eliminates? disease|guaranteed results)\b",
        r"\b(tedavi eder|kesin sonuç|hastalığı yok eder)\b",
        r"\b(leczy|wyleczy|gwarantowane rezultaty|eliminuje chorobę)\b",
        r"\b(100% effective|clinically proven to cure|miracle)\b",
    ]

    # Language detection — reject obvious English fallback for non-English markets
    ENGLISH_FALLBACK_WORDS = {
        "the", "and", "for", "with", "our", "your", "now", "free", "buy", "get",
        "shop", "order", "fast", "real", "verified", "official", "premium",
        "quality", "guaranteed", "consultation", "results", "delivery", "store",
    }
    LANG_MARKERS = {
        "ro": ["și", "pentru", "sau", "mai", "doar", "acum", "lei"],
        "tr": ["ve", "için", "ile", "bir", "şimdi", "fiyat", "tl", "₺"],
        "pl": ["dla", "jest", "nie", "lub", "teraz", "cena", "zł"],
        "hu": ["és", "egy", "nem", "vagy", "itt", "most", "ft"],
        "de": ["und", "für", "oder", "jetzt", "nur", "preis", "€"],
        "fr": ["et", "pour", "ou", "maintenant", "prix", "€"],
        "es": ["y", "para", "ahora", "precio", "comprar", "€"],
        "it": ["e", "per", "ora", "prezzo", "acquista", "€"],
        "pt": ["e", "para", "agora", "preço", "entrega", "€"],
        "sr": ["i", "za", "sada", "cena", "poruči", "dostava", "€"],
    }
    expected_lang = config.get("language_code", "en")
    reject_reasons = []

    def _asset_text(asset: Any) -> str:
        if isinstance(asset, dict):
            return str(asset.get("text", "")).strip()
        return str(asset).strip()

    def _looks_like_english_fallback(text: str) -> bool:
        text_lower = text.lower()
        return any(re.search(rf"\b{re.escape(word)}\b", text_lower) for word in ENGLISH_FALLBACK_WORDS)

    def _variant_signature(variant: Dict[str, Any]) -> tuple:
        headline_sig = tuple(_asset_text(h).lower() for h in variant.get("headlines", []))
        desc_sig = tuple(_asset_text(d).lower() for d in variant.get("descriptions", []))
        return headline_sig, desc_sig

    # --- Check each RSA variant ---
    for vi, variant in enumerate(rsa_variants):
        variant_label = ["A", "B", "C"][vi] if vi < 3 else str(vi)
        headlines = variant.get("headlines", [])
        descriptions = variant.get("descriptions", [])

        # --- 1. Check for wrong product name (GPT hallucinated different product) ---
        # At least 1 headline should contain the product brand
        if brand_core and len(brand_core) >= 3:
            brand_in_headlines = any(
                brand_core in (h.get("text", h) if isinstance(h, dict) else h).lower()
                for h in headlines[:5]  # Check first 5 headlines
            )
            if not brand_in_headlines and headlines:
                issues.append(f"RSA_{variant_label}: No brand '{brand_core}' in first 5 headlines")
                # Don't auto-fix — just warn. Brand might be in dynamic {KeyWord} macro.

        # --- 2. Check for wrong price ---
        if price_str and len(price_str) >= 2:
            for hi, h in enumerate(headlines):
                h_text = h.get("text", h) if isinstance(h, dict) else h
                # Find price-like patterns (digits followed by currency)
                price_matches = re.findall(r'(\d{2,6})\s*(?:lei|lej|ron|tl|₺|zł|pln|ft|huf|€|eur)', h_text.lower())
                for found_price in price_matches:
                    if found_price != price_str and abs(int(found_price) - int(price_str)) > 5:
                        issues.append(f"RSA_{variant_label} H{hi}: Price '{found_price}' != expected '{price_str}'")
                        # Auto-fix: replace wrong price with correct one
                        fixed_text = h_text.replace(found_price, price_str)
                        if isinstance(h, dict):
                            variant["headlines"][hi]["text"] = fixed_text[:30]
                        else:
                            variant["headlines"][hi] = fixed_text[:30]
                        fixes += 1

            # Same check for descriptions
            for di, d in enumerate(descriptions):
                d_text = d.get("text", d) if isinstance(d, dict) else d
                price_matches = re.findall(r'(\d{2,6})\s*(?:lei|lej|ron|tl|₺|zł|pln|ft|huf|€|eur)', d_text.lower())
                for found_price in price_matches:
                    if found_price != price_str and abs(int(found_price) - int(price_str)) > 5:
                        issues.append(f"RSA_{variant_label} D{di}: Price '{found_price}' != expected '{price_str}'")
                        fixed_text = d_text.replace(found_price, price_str)
                        if isinstance(d, dict):
                            variant["descriptions"][di]["text"] = fixed_text[:90]
                        else:
                            variant["descriptions"][di] = fixed_text[:90]
                        fixes += 1

        # --- 3. Prohibited medical claims ---
        all_texts = []
        for h in headlines:
            all_texts.append(h.get("text", h) if isinstance(h, dict) else h)
        for d in descriptions:
            all_texts.append(d.get("text", d) if isinstance(d, dict) else d)

        for pattern in PROHIBITED_CLAIM_PATTERNS:
            for text in all_texts:
                if re.search(pattern, text, re.IGNORECASE):
                    issues.append(f"RSA_{variant_label}: Prohibited medical claim found: '{text[:50]}...'")
                    # Don't auto-fix medical claims — remove the offending text entirely
                    # The _convert function pads with fallbacks if too few headlines

        # --- 4. Duplicate headlines within same variant ---
        seen_headlines = set()
        deduped_headlines = []
        for h in headlines:
            h_text = (h.get("text", h) if isinstance(h, dict) else h).lower().strip()
            if h_text in seen_headlines:
                issues.append(f"RSA_{variant_label}: Duplicate headline removed: '{h_text[:30]}'")
                fixes += 1
                continue
            seen_headlines.add(h_text)
            deduped_headlines.append(h)
        if len(deduped_headlines) < len(headlines):
            variant["headlines"] = deduped_headlines

        # --- 5. Empty or placeholder text ---
        for hi, h in enumerate(list(variant.get("headlines", []))):
            h_text = (h.get("text", h) if isinstance(h, dict) else h).strip()
            if not h_text or h_text.lower() in ("headline", "title", "text", "lorem ipsum", "placeholder", "..."):
                issues.append(f"RSA_{variant_label} H{hi}: Empty/placeholder headline removed")
                variant["headlines"].remove(h)
                fixes += 1

        for di, d in enumerate(list(variant.get("descriptions", []))):
            d_text = (d.get("text", d) if isinstance(d, dict) else d).strip()
            if not d_text or d_text.lower() in ("description", "text", "lorem ipsum", "placeholder", "..."):
                issues.append(f"RSA_{variant_label} D{di}: Empty/placeholder desc removed")
                variant["descriptions"].remove(d)
                fixes += 1

    # --- 6. Check keywords for competitor brand hallucinations ---
    keywords_raw = response_data.get("keywords") or ads.get("keywords") or []
    if keywords_raw:
        cleaned_kw = []
        for kw in keywords_raw:
            kw_text = kw.get("text", "") if isinstance(kw, dict) else str(kw)
            kw_lower = kw_text.lower()
            is_competitor = any(comp in kw_lower for comp in COMPETITOR_BRANDS)
            # Allow competitor keywords only if they include the brand name too
            # e.g. "mybrand amazon" is OK (brand + channel), "amazon products" is NOT
            if is_competitor and brand_core not in kw_lower:
                issues.append(f"Keyword removed (competitor without brand): '{kw_text}'")
                fixes += 1
                continue
            cleaned_kw.append(kw)
        # Update in-place (check both locations per API spec)
        if response_data.get("keywords") is not None:
            response_data["keywords"] = cleaned_kw
        elif ads.get("keywords") is not None:
            ads["keywords"] = cleaned_kw

    # --- 7. Check callouts for length and relevance ---
    callouts_raw = ads.get("callouts", [])
    if callouts_raw:
        cleaned_callouts = []
        for c in callouts_raw:
            c_text = (c.get("text", c) if isinstance(c, dict) else c).strip()
            if len(c_text) > 25:
                issues.append(f"Callout trimmed: '{c_text}' → '{c_text[:25]}'")
                fixes += 1
                c_text = c_text[:25]
            if c_text:
                cleaned_callouts.append({"text": c_text} if isinstance(c, dict) else c_text)
        ads["callouts"] = cleaned_callouts

    # --- 8. Language and variant fallback validation ---
    all_headlines = [
        _asset_text(h)
        for variant in rsa_variants
        for h in variant.get("headlines", [])
        if _asset_text(h)
    ]
    if expected_lang != "en" and all_headlines:
        english_like = sum(1 for text in all_headlines if _looks_like_english_fallback(text))
        english_ratio = english_like / max(len(all_headlines), 1)
        marker_hits = 0
        if expected_lang in LANG_MARKERS:
            sample = " ".join(text.lower() for text in all_headlines[:12])
            marker_hits = sum(1 for m in LANG_MARKERS[expected_lang] if m in sample)

        if english_ratio > 0.5:
            issues.append(
                f"LANGUAGE REJECT: {english_like}/{len(all_headlines)} headlines look English for expected '{expected_lang}'"
            )
            reject_reasons.append("ENGLISH_FALLBACK")
        elif expected_lang in LANG_MARKERS and marker_hits == 0 and len(" ".join(all_headlines)) > 40:
            issues.append(f"LANGUAGE REJECT: No '{expected_lang}' markers found in AI headlines")
            reject_reasons.append("LANGUAGE_MARKERS_MISSING")

    if len(rsa_variants) >= 3:
        unique_variant_signatures = {_variant_signature(variant) for variant in rsa_variants[:3]}
        if len(unique_variant_signatures) == 1:
            issues.append("VARIANT REJECT: RSA A/B/C are materially identical")
            reject_reasons.append("IDENTICAL_VARIANTS")

    if reject_reasons:
        response_data["_validation_reject_reasons"] = reject_reasons

    # --- Log issues ---
    if issues:
        db.log_operation(
            country_code=country_code, campaign_id=campaign_id,
            handle=handle, operation="AI_COPY_VALIDATION",
            status="WARNING" if fixes > 0 else "INFO",
            details=f"{len(issues)} issue(s) found, {fixes} auto-fixed: {'; '.join(issues[:5])}"[:500]
        )

    return fixes


def _get_known_competitor_brand_terms() -> set:
    """Whitelist of real competitor / retailer / marketplace brand terms.

    Used to catch brand hallucinations without stripping generic localized
    benefit phrases that merely resemble another language at the word level.
    """
    return {
        "amazon", "ebay", "aliexpress", "temu", "walmart",
        "dr max", "catena", "farmacia tei", "help net",
        "rossmann", "dm drogerie", "hepsiburada", "trendyol",
    }


def _contains_foreign_brand_hallucination(text: str, brand_core: str) -> bool:
    text_lower = (text or "").lower().strip()
    if not text_lower:
        return False

    return any(term in text_lower for term in _get_known_competitor_brand_terms())


def _get_localized_fillers(country_code: str, product_name: str) -> tuple:
    """
    Return (HEADLINE_FILLERS, DESC_FILLERS) localized per country.
    Used to pad RSA headlines to 15 and descriptions to 4 when AI generates fewer.
    Reads from COUNTRY_CONFIG['filler_headlines'] / ['filler_descriptions'] as single source of truth.
    """
    cc = country_code.upper()
    config = COUNTRY_CONFIG.get(cc, {})

    # Read from COUNTRY_CONFIG; fall back to English defaults
    raw_h = config.get("filler_headlines")
    raw_d = config.get("filler_descriptions")

    if raw_h and raw_d:
        h = [item.replace("{product}", product_name) for item in raw_h]
        d = [item.replace("{product}", product_name) for item in raw_d]
    else:
        if cc != "EN":
            # Avoid injecting English filler copy into unsupported non-English locales.
            return [], []

        # English fallback for English locales not in COUNTRY_CONFIG
        h = [
            f"{product_name} Original",
            f"Buy {product_name}",
            "Official Store",
            "Fast Delivery",
            "Verified Product",
            "Quality Guaranteed",
            "Cash on Delivery",
            "Original Product",
            "Great Value",
            "Special Offer",
            "Order Now Online",
            "Premium Product",
            "Positive Reviews",
            "Best Price",
        ]
        d = [
            f"{product_name} — order from the official store now.",
            "Fast delivery nationwide. Cash on delivery available.",
            "Original product, quality guaranteed. Verified product.",
            f"Order {product_name} today. Satisfaction guaranteed.",
        ]

    return h, d


def _convert_endpoint_response(
    response: Dict[str, Any],
    config: dict,
    product_name: str,
    product_price: Optional[float],
    product_url: str,
) -> Optional[Dict[str, Any]]:
    """
    Convert gADS_ad_generator endpoint response to internal ad_copy format
    compatible with process_single_product().

    Internal format expected:
    {
        "rsa_a": {"headlines": [...], "descriptions": [...]},
        "rsa_b": {"headlines": [...], "descriptions": [...]},
        "rsa_c": {"headlines": [...], "descriptions": [...]},
        "snippet_values": [...],
        # Extended fields (new):
        "_ai_callouts": [...],
        "_ai_sitelinks": [...],
        "_ai_promotion": {...},
        "_ai_keywords": [...],
        # "_ai_negative_keywords": DISABLED — not data-driven
        "_ai_generated": True,
    }
    """
    try:
        ads = response.get("ads", {})
        rsa_variants = ads.get("rsa_variants", [])

        if len(rsa_variants) < 3:
            return None  # Need all 3 variants

        result: Dict[str, Any] = {"_ai_generated": True}

        # --- Convert RSA variants ---
        variant_keys = ["rsa_a", "rsa_b", "rsa_c"]
        for i, key in enumerate(variant_keys):
            variant = rsa_variants[i]
            headlines_raw = variant.get("headlines", [])
            descriptions_raw = variant.get("descriptions", [])

            # Convert headline objects to internal format
            headlines = []
            for h in headlines_raw:
                if isinstance(h, dict):
                    text = h.get("text", "")
                    pinned = h.get("pinned_field")
                    if pinned:
                        headlines.append({"text": text[:30], "pinned_field": pinned})
                    else:
                        headlines.append(text[:30])
                elif isinstance(h, str):
                    headlines.append(h[:30])

            # Convert description objects to internal format
            descriptions = []
            for d in descriptions_raw:
                if isinstance(d, dict):
                    text = d.get("text", "")
                    descriptions.append(text[:90])
                elif isinstance(d, str):
                    descriptions.append(d[:90])

            # Ensure minimum counts — pad to 15 headlines / 4 descriptions
            # Google Ads allows up to 15 headlines; we target 15 for max Ad Strength
            HEADLINE_FILLERS, DESC_FILLERS = _get_localized_fillers(
                config.get("country_code", "EN"), product_name
            )
            filler_idx = 0
            existing_lower = {(h.get("text", h) if isinstance(h, dict) else h).lower() for h in headlines}
            while len(headlines) < 15 and filler_idx < len(HEADLINE_FILLERS):
                filler = HEADLINE_FILLERS[filler_idx][:30]
                filler_idx += 1
                if filler.lower() not in existing_lower:
                    headlines.append(filler)
                    existing_lower.add(filler.lower())
            desc_filler_idx = 0
            existing_desc_lower = {(d.get("text", d) if isinstance(d, dict) else d).lower() for d in descriptions}
            while len(descriptions) < 4 and desc_filler_idx < len(DESC_FILLERS):
                filler = DESC_FILLERS[desc_filler_idx][:90]
                desc_filler_idx += 1
                if filler.lower() not in existing_desc_lower:
                    descriptions.append(filler)
                    existing_desc_lower.add(filler.lower())

            result[key] = {
                "headlines": headlines[:15],
                "descriptions": descriptions[:4],
            }

        # --- Convert structured snippets ---
        snippets = ads.get("structured_snippets", {})
        snippet_values = []
        if snippets:
            for v in snippets.get("values", []):
                if isinstance(v, dict):
                    snippet_values.append(v.get("text", "")[:25])
                elif isinstance(v, str):
                    snippet_values.append(v[:25])
            result["_ai_snippet_header"] = snippets.get("header", config.get("snippet_header", "Types"))
        result["snippet_values"] = snippet_values if snippet_values else _get_fallback_snippets(config)

        # --- Convert callouts ---
        callouts_raw = ads.get("callouts", [])
        if callouts_raw:
            result["_ai_callouts"] = [
                (c.get("text", c) if isinstance(c, dict) else c)[:25]
                for c in callouts_raw
            ]

        # --- Convert sitelinks ---
        # CRITICAL: All sitelink URLs MUST point to the actual product page.
        # AI may return relative paths like "/buy" or "/contact" - these pages
        # DON'T EXIST and cause "Destination not working" disapprovals.
        # Instead, append AI path as ?sitelink= param to product_url.
        sitelinks_raw = ads.get("sitelinks", [])
        if sitelinks_raw:
            ai_sitelinks = []
            for sl in sitelinks_raw:
                if isinstance(sl, dict):
                    url_path = sl.get("url", "")
                    # ALWAYS use product_url as base - never build fake pages
                    if url_path and url_path != product_url and not url_path.startswith("http"):
                        slug = url_path.strip("/").split("/")[-1]
                        if slug:
                            separator = "&" if "?" in product_url else "?"
                            final_url = f"{product_url}{separator}sitelink={slug}"
                        else:
                            final_url = product_url
                    elif url_path.startswith("http") and "/products/" in url_path:
                        final_url = url_path  # Full URL to real product page - OK
                    else:
                        final_url = product_url
                    ai_sitelinks.append({
                        "link_text": (sl.get("text", sl.get("link_text", "")) or "")[:25],
                        "description1": (sl.get("desc1", sl.get("description1", "")) or "")[:35],
                        "description2": (sl.get("desc2", sl.get("description2", "")) or "")[:35],
                        "final_urls": [final_url],
                    })
            if ai_sitelinks:
                result["_ai_sitelinks"] = ai_sitelinks

        # --- Convert promotion ---
        # promotion is at TOP LEVEL per API spec v3.0, fallback to ads for compatibility
        promotion_raw = response.get("promotion") or ads.get("promotion") or {}
        if promotion_raw and promotion_raw.get("percent_off"):
            result["_ai_promotion"] = {
                "percent_off": promotion_raw["percent_off"],
                "promotion_target": (promotion_raw.get("promotion_target", product_name) or product_name)[:20],
                "language_code": promotion_raw.get("language_code", config.get("language_code", "en")),
            }

        # --- Convert keywords ---
        # keywords is at TOP LEVEL per API spec v3.0, fallback to ads for compatibility
        keywords_raw = response.get("keywords") or ads.get("keywords") or []
        if keywords_raw:
            ai_keywords = []
            for kw in keywords_raw:
                if isinstance(kw, dict) and kw.get("recommended", True):
                    ai_keywords.append({
                        "text": kw.get("text", ""),
                        "match_type": kw.get("match_type", "PHRASE"),
                        "source": kw.get("source", "ai_generator"),
                    })
            if ai_keywords:
                result["_ai_keywords"] = ai_keywords

        # --- Negative keywords: DISABLED ---
        # AI-generated negatives are not data-driven (no ROI validation).
        # Use /ads-search-terms for data-driven negative keyword management.

        # --- Metadata ---
        result["_ai_compliance"] = response.get("compliance", {})
        result["_ai_product_category"] = response.get("product", {}).get("category")
        # Timing: API spec uses timing.total_ms, fallback to sum of phase timings
        timing = response.get("timing", {})
        result["_ai_processing_time_ms"] = (
            timing.get("total_ms")
            or sum(v for k, v in timing.items() if k.endswith("_ms") and isinstance(v, (int, float)))
            or response.get("processing_time_ms", 0)
        )

        return result

    except Exception as e:
        return None


def _get_fallback_snippets(config: dict) -> list:
    """Return generic snippet values when endpoint provides none."""
    lang = config.get("language_code", "en")
    if lang == "ro":
        return ["Produs Natural", "Produs Original", "Calitate Testată"]
    elif lang == "tr":
        return ["Doğal Ürün", "Orijinal Ürün", "Kalite Sertifikalı"]
    elif lang == "pl":
        return ["Produkt Naturalny", "Produkt Oryginalny", "Certyfikowana Jakość"]
    else:
        return ["Natural Product", "Original Product", "Certified Quality"]


def _generate_data_driven_copy(
    country_code: str,
    product_name: str,
    product_price: Optional[float],
    product_url: str,
    config: dict,
    product_handle: str,
    campaign_id: str,
    product_description: str = "",
    feed_product_type: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Generate data-driven ad copy by enriching templates with:
    1. Product description from XML feed
    2. Converting keywords from keyword intelligence DB
    3. Winning headline/description patterns from asset performance DB

    Returns same structure as _generate_ad_copy_local() or None if no enrichment data available.
    Falls back gracefully — if no intelligence data, returns None and caller uses template copy.
    """
    db = _get_db()

    # 1. Get base template copy
    base_copy = _generate_ad_copy_local(country_code, product_name, product_price, product_url, config)

    enriched = False

    # 2. Try to get converting keywords from intelligence DB
    try:
        from batch_intelligence import get_keywords_for_setup
        kw_recs = get_keywords_for_setup(product_handle, country_code, "0", min_roi=0.0, max_keywords=10, feed_product_type=feed_product_type)
        historical_kws = [kw for kw in kw_recs if kw.get("source") in ("historical", "cross_country")]
        category_kws = [kw for kw in kw_recs if kw.get("source") == "category"]
    except Exception as e:
        _debug("ad_copy_gen", f"get_keywords_for_setup failed for {product_handle}: {e}", "warning")
        historical_kws = []
        category_kws = []

    # 3. Try to get winning patterns from asset performance DB
    try:
        winning_headlines = db.get_winning_patterns(country_code, asset_type="HEADLINE", min_occurrences=2)
        winning_descs = db.get_winning_patterns(country_code, asset_type="DESCRIPTION", min_occurrences=2)
    except Exception as e:
        _debug("ad_copy_gen", f"get_winning_patterns failed for {country_code}: {e}", "warning")
        winning_headlines = []
        winning_descs = []

    # 4. Extract useful keywords for headlines
    # IMPORTANT: Only use HISTORICAL keywords for THIS product as headlines.
    # Category keywords are OTHER brands (e.g., competitor brand for your product) and must
    # NEVER appear in RSA headlines — they are only for keyword targeting.
    kw_headlines = []
    brand_core = product_name.lower().split()[0] if product_name else ""
    for kw in historical_kws[:8]:  # Only historical, NEVER category
        kw_text = kw.get("text", "")
        if kw_text and len(kw_text) <= 28:
            # Only include if it contains the product brand name
            if brand_core and brand_core in kw_text.lower():
                clean = kw_text.strip().title()
                if clean not in kw_headlines and clean.lower() != product_name.lower():
                    kw_headlines.append(clean)

    # 5. Extract product description keywords (short phrases for headlines)
    desc_headlines = []
    if product_description:
        # Extract key phrases from description (simple approach: split by common delimiters)
        import html
        clean_desc = html.unescape(product_description)
        clean_desc = re.sub(r'<[^>]+>', ' ', clean_desc)  # Strip HTML tags
        clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()

        # Try to find benefit phrases (words before common benefit indicators)
        words = clean_desc.split()
        if len(words) > 5:
            # Use first meaningful phrase as a headline candidate
            first_phrase = " ".join(words[:4]).title()
            if len(first_phrase) <= 30:
                desc_headlines.append(first_phrase)

    # 6. Enrich RSA headlines if we have data
    if kw_headlines or desc_headlines:
        enriched = True
        extra_headlines = (kw_headlines + desc_headlines)[:3]

        for rsa_key in ["rsa_a", "rsa_b", "rsa_c"]:
            current = base_copy[rsa_key]["headlines"]
            # Replace last N generic headlines with data-driven ones
            for i, extra_h in enumerate(extra_headlines):
                # Replace from position 12 onwards (the newly added generic ones)
                replace_idx = 12 + i
                if replace_idx < len(current):
                    if isinstance(current[replace_idx], dict):
                        current[replace_idx] = {"text": extra_h[:30], "pinned_field": current[replace_idx].get("pinned_field")}
                    else:
                        current[replace_idx] = extra_h[:30]

    # 7. Mark that data-driven enrichment was used
    base_copy["_data_driven"] = enriched
    base_copy["_kw_headlines_count"] = len(kw_headlines)
    base_copy["_desc_headlines_count"] = len(desc_headlines)
    base_copy["_winning_patterns_count"] = len(winning_headlines)

    return base_copy if enriched else None


def _generate_ad_copy_local(
    country_code: str,
    product_name: str,
    price: float,
    product_url: str,
    config: dict,
) -> Dict[str, Any]:
    """
    Generate ad copy using templates (no API call needed).
    Returns structured dict with RSA A/B/C headlines+descriptions, callouts, snippets.
    """
    lang = config["language"]
    currency = config["currency"]
    default_kw = config["default_keyword"]
    template_country = str(config.get("_template_country", country_code)).upper()

    # ---- Price-aware flag ----
    has_price = price is not None and price > 0
    price_int = int(price) if has_price else 0

    # ---- Language-specific copy templates ----
    if template_country == "RO":
        # Price-dependent headlines/descriptions
        if has_price:
            price_headline = f"Doar {price_int} Lei"
            price_headline_alt = f"De la {price_int} Lei"
            price_headline_box = f"Doar {price_int} Lei/Pachet"
            price_desc_a = f"Pre\u021b special {price_int} Lei. Livrare rapid\u0103 \u00een toat\u0103 Rom\u00e2nia."
            price_desc_b1 = f"{{KeyWord:{default_kw}}} la pre\u021b special {price_int} Lei. Comand\u0103 acum!"
            price_desc_b2 = f"Profit\u0103 de ofert\u0103! {product_name} original la doar {price_int} Lei."
            price_desc_b4 = f"Comand\u0103 3 pachete \u0219i prime\u0219ti livrare gratuit\u0103. Doar {price_int} Lei."
            price_desc_c1 = f"{product_name} original \u2014 comand\u0103 de pe site-ul oficial cu pre\u021b {price_int} Lei."
            rsa_b_h2 = f"{product_name} \u2014 {price_int} Lei"
            rsa_b_h3 = f"Pre\u021b Special {price_int} Lei"
        else:
            price_headline = "Ofert\u0103 Special\u0103 Online"
            price_headline_alt = "Pre\u021b Redus Acum"
            price_headline_box = "Ofert\u0103 Limitat\u0103"
            price_desc_a = "Livrare rapid\u0103 \u00een toat\u0103 Rom\u00e2nia. Comand\u0103 de pe site-ul oficial."
            price_desc_b1 = f"{{KeyWord:{default_kw}}} \u2014 comand\u0103 acum de pe site-ul oficial!"
            price_desc_b2 = f"Profit\u0103 de ofert\u0103! {product_name} original pe site-ul oficial."
            price_desc_b4 = f"Comand\u0103 3 pachete \u0219i prime\u0219ti livrare gratuit\u0103. Ofert\u0103 limitat\u0103."
            price_desc_c1 = f"{product_name} original \u2014 comand\u0103 de pe site-ul oficial acum."
            rsa_b_h2 = f"Cump\u0103r\u0103 {product_name}"
            rsa_b_h3 = "Ofert\u0103 Special\u0103"

        rsa_a = {
            "headlines": [
                f"{product_name} Original",
                "Site Oficial Rom\u00e2nia",
                "Produs Verificat",
                "Livrare din Rom\u00e2nia",
                "Formul\u0103 Original\u0103",
                price_headline,
                "Plata la Livrare",
                "Produs Premium",
                "Rezultate Reale",
                "Comand\u0103 Acum Online",
                "Livrare Rapid\u0103",
                "Calitate Garantat\u0103",
                f"Comand\u0103 {product_name}",         # 13
                "100% Formula Natural\u0103",            # 14
                "Consult an\u021b\u0103 Gratuit\u0103",             # 15
            ],
            "descriptions": [
                f"{product_name} \u2014 produs original. Comand\u0103 de pe site-ul oficial acum.",
                price_desc_a,
                "Formul\u0103 original\u0103, rezultate dovedite. Plata la livrare disponibil\u0103.",
                f"Comand\u0103 {product_name} azi. Garan\u021bie de autenticitate \u0219i livrare.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                rsa_b_h2,
                rsa_b_h3,
                "Ofert\u0103 Limitat\u0103",
                price_headline_alt,
                "Cea Mai Bun\u0103 Ofert\u0103",
                "Reducere Special\u0103 Azi",
                f"Cump\u0103r\u0103 {product_name}",
                "Economise\u0219ti 50%",
                "Comand\u0103 cu Reducere",
                "Livrare Gratuit\u0103",
                price_headline_box,
                f"{{KeyWord:{default_kw}}} Online",  # 13
                "Verific\u0103 Ofert\u0103 Acum",               # 14
                "Recenzii Pozitive",                   # 15
            ],
            "descriptions": [
                price_desc_b1,
                price_desc_b2,
                "Livrare rapid\u0103 \u00een toat\u0103 Rom\u00e2nia. Plata la livrare disponibil\u0103.",
                price_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Site Oficial Rom\u00e2nia",
                "Produs Verificat",
                "Livrare din Rom\u00e2nia",
                "Formul\u0103 Original\u0103",
                price_headline,
                "Plata la Livrare",
                "Produs Premium",
                "Rezultate Reale",
                "Comand\u0103 Acum Online",
                "Livrare Rapid\u0103",
                "Calitate Garantat\u0103",
                f"Comand\u0103 {product_name} Acum",     # 13
                "Ingrediente Naturale",              # 14
                "Formul\u0103 Testat\u0103 Clinic",            # 15
            ],
            "descriptions": [
                price_desc_c1,
                "Produs verificat \u0219i testat. Livrare rapid\u0103 \u00een Rom\u00e2nia cu plata la livrare.",
                "Produs premium cu formul\u0103 original\u0103. Rezultate dovedite \u0219i recenzii.",
                "Comand\u0103 azi de pe site-ul oficial. Garan\u021bie de autenticitate \u0219i livrare.",
            ],
        }
        snippet_values = _guess_snippet_values_ro(product_name)

    elif template_country == "TR":
        # Price-dependent headlines/descriptions
        if has_price:
            tr_price_h = f"Sadece {price_int} TL"
            tr_price_box = f"Sadece {price_int} TL/Kutu"
            tr_desc_a2 = f"\u00d6zel fiyat {price_int} TL. T\u00fcrkiye geneli h\u0131zl\u0131 kargo."
            tr_desc_b1 = f"{{KeyWord:{default_kw}}} \u00f6zel fiyat {price_int} TL. Hemen sipari\u015f!"
            tr_desc_b2 = f"F\u0131rsat\u0131 ka\u00e7\u0131rmay\u0131n! {product_name} orijinal sadece {price_int} TL."
            tr_desc_b4 = f"3 kutu alana dan\u0131\u015fmanl\u0131k hediye. Sadece {price_int} TL."
            tr_desc_c1 = f"{product_name} orijinal \u2014 resmi siteden \u00f6zel fiyat {price_int} TL."
            tr_b_h2 = f"{product_name} \u2014 {price_int} TL"
            tr_b_h3 = f"\u00d6zel Fiyat {price_int} TL"
        else:
            tr_price_h = "\u00d6zel Teklif Online"
            tr_price_box = "S\u0131n\u0131rl\u0131 Stok"
            tr_desc_a2 = "T\u00fcrkiye geneli h\u0131zl\u0131 kargo. Resmi siteden sipari\u015f verin."
            tr_desc_b1 = f"{{KeyWord:{default_kw}}} \u2014 resmi siteden hemen sipari\u015f verin!"
            tr_desc_b2 = f"F\u0131rsat\u0131 ka\u00e7\u0131rmay\u0131n! {product_name} orijinal resmi sitede."
            tr_desc_b4 = f"3 kutu alana dan\u0131\u015fmanl\u0131k hediye. S\u0131n\u0131rl\u0131 teklif."
            tr_desc_c1 = f"{product_name} orijinal \u2014 resmi siteden sipari\u015f verin."
            tr_b_h2 = f"{product_name} Al"
            tr_b_h3 = "\u00d6zel Teklif"

        rsa_a = {
            "headlines": [
                f"{product_name} Orijinal",
                "Resmi Site T\u00fcrkiye",
                "Onayl\u0131 \u00dcr\u00fcn",
                "T\u00fcrkiye'den Kargo",
                "Orijinal Form\u00fcl",
                tr_price_h,
                "Kap\u0131da \u00d6deme",
                "Premium Ürün",
                "Ger\u00e7ek Sonu\u00e7lar",
                "Hemen Sipari\u015f Ver",
                "H\u0131zl\u0131 Kargo",
                "Kalite Garantisi",
                f"{product_name} Sipari\u015f Ver",      # 13
                "%100 Do\u011fal İçerik",                 # 14
                "\u00dccretsiz Dan\u0131\u015fmanl\u0131k",              # 15
            ],
            "descriptions": [
                f"{product_name} \u2014 orijinal ürün. Resmi siteden hemen sipari\u015f verin.",
                tr_desc_a2,
                "Orijinal form\u00fcl, kan\u0131tlanm\u0131\u015f sonu\u00e7lar. Kap\u0131da \u00f6deme imkan\u0131.",
                f"{product_name} sipari\u015f edin. Orijinallik garantisi ve h\u0131zl\u0131 teslimat.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                tr_b_h2,
                tr_b_h3,
                "S\u0131n\u0131rl\u0131 Teklif",
                tr_price_h,
                "En \u0130yi Fiyat",
                "Bug\u00fcne \u00d6zel \u0130ndirim",
                f"{product_name} Al",
                "%50 Tasarruf Edin",
                "\u0130ndirimli Sipari\u015f",
                "\u00dccretsiz Kargo",
                tr_price_box,
                f"{{KeyWord:{default_kw}}} Online",   # 13
                "Teklifi İncele",                       # 14
                "Müşteri Yorumları",                    # 15
            ],
            "descriptions": [
                tr_desc_b1,
                tr_desc_b2,
                "T\u00fcrkiye geneli h\u0131zl\u0131 kargo. Kap\u0131da \u00f6deme imkan\u0131 mevcut.",
                tr_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Orijinal {product_name}", "pinned_field": "HEADLINE_1"},
                "Resmi Site T\u00fcrkiye",
                "Onayl\u0131 \u00dcr\u00fcn",
                "T\u00fcrkiye'den Kargo",
                "Orijinal Form\u00fcl",
                tr_price_h,
                "Kap\u0131da \u00d6deme",
                "Premium Ürün",
                "Ger\u00e7ek Sonu\u00e7lar",
                "Hemen Sipari\u015f Ver",
                "H\u0131zl\u0131 Kargo",
                "Kalite Garantisi",
                f"{product_name} Hemen Al",          # 13
                "Do\u011fal Bile\u015fenler",                   # 14
                "Klinik Testli Formül",               # 15
            ],
            "descriptions": [
                tr_desc_c1,
                "Onayl\u0131 ve test edilmi\u015f \u00fcr\u00fcn. T\u00fcrkiye geneli h\u0131zl\u0131 kargo.",
                "Orijinal form\u00fcll\u00fc premium ürün. Kan\u0131tlanm\u0131\u015f sonu\u00e7lar.",
                "Bug\u00fcn resmi siteden sipari\u015f edin. Orijinallik garantisi.",
            ],
        }
        snippet_values = _guess_snippet_values_tr(product_name)

    elif template_country == "PL":
        if has_price:
            pl_price_h = f"Tylko {price_int} zł"
            pl_price_alt = f"Od {price_int} zł"
            pl_price_box = f"Tylko {price_int} zł/Opak."
            pl_desc_a2 = f"Cena specjalna {price_int} zł. Szybka wysyłka w całej Polsce."
            pl_desc_b1 = f"{{KeyWord:{default_kw}}} cena specjalna {price_int} zł. Zamów teraz!"
            pl_desc_b2 = f"Skorzystaj z oferty! {product_name} oryginał tylko {price_int} zł."
            pl_desc_b4 = f"Zamów 3 opakowania i otrzymaj konsultację gratis. Tylko {price_int} zł."
            pl_desc_c1 = f"{product_name} oryginał — zamów ze sklepu oficjalnego za {price_int} zł."
            pl_b_h2 = f"{product_name} — {price_int} zł"
            pl_b_h3 = f"Cena Specjalna {price_int} zł"
        else:
            pl_price_h = "Oferta Specjalna Online"
            pl_price_alt = "Obniżona Cena Teraz"
            pl_price_box = "Limitowana Oferta"
            pl_desc_a2 = "Szybka wysyłka w całej Polsce. Zamów ze sklepu oficjalnego."
            pl_desc_b1 = f"{{KeyWord:{default_kw}}} — zamów teraz ze sklepu oficjalnego!"
            pl_desc_b2 = f"Skorzystaj z oferty! {product_name} oryginał w sklepie oficjalnym."
            pl_desc_b4 = f"Zamów 3 opakowania i otrzymaj konsultację gratis. Oferta limitowana."
            pl_desc_c1 = f"{product_name} oryginał — zamów ze sklepu oficjalnego teraz."
            pl_b_h2 = f"Kup {product_name}"
            pl_b_h3 = "Oferta Specjalna"

        rsa_a = {
            "headlines": [
                f"{product_name} Oryginał",
                "Oficjalny Sklep Polska",
                "Produkt Zweryfikowany",
                "Wysyłka z Polski",
                "Oryginalna Formuła",
                pl_price_h,
                "Płatność Przy Odbiorze",
                "Produkt Premium",
                "Prawdziwe Rezultaty",
                "Zamów Teraz Online",
                "Szybka Wysyłka",
                "Gwarancja Jakości",
                f"Zamów {product_name}",
                "Najwyższa Jakość",
                "Darmowa Wysyłka",
            ],
            "descriptions": [
                f"{product_name} — produkt oryginalny. Zamów ze sklepu oficjalnego teraz.",
                pl_desc_a2,
                "Oryginalna formuła, potwierdzone rezultaty. Płatność przy odbiorze.",
                f"Zamów {product_name} dziś. Gwarancja autentyczności i wysyłki.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                pl_b_h2,
                pl_b_h3,
                "Limitowana Oferta",
                pl_price_alt,
                "Najlepsza Oferta",
                "Specjalna Zniżka Dziś",
                f"Kup {product_name}",
                "Oszczędź 50%",
                "Zamów ze Zniżką",
                "Darmowa Wysyłka",
                pl_price_box,
                f"{{KeyWord:{default_kw}}} Online",
                "Sprawdź Ofertę Teraz",
                "Pozytywne Opinie",
            ],
            "descriptions": [
                pl_desc_b1,
                pl_desc_b2,
                "Szybka wysyłka w całej Polsce. Płatność przy odbiorze dostępna.",
                pl_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Oryginalny {product_name}", "pinned_field": "HEADLINE_1"},
                "Oficjalny Sklep Polska",
                "Produkt Zweryfikowany",
                "Wysyłka z Polski",
                "Oryginalna Formuła",
                pl_price_h,
                "Płatność Przy Odbiorze",
                "Produkt Premium",
                "Prawdziwe Rezultaty",
                "Zamów Teraz Online",
                "Szybka Wysyłka",
                "Gwarancja Jakości",
                f"Zamów {product_name} Teraz",
                "Naturalne Składniki",
                "Formuła Klinicznie Test.",
            ],
            "descriptions": [
                pl_desc_c1,
                "Produkt zweryfikowany i przetestowany. Szybka wysyłka w Polsce.",
                "Produkt premium z oryginalną formułą. Potwierdzone rezultaty.",
                "Zamów dziś ze sklepu oficjalnego. Gwarancja autentyczności.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "PL")

    elif template_country == "PT":
        if has_price:
            pt_price_h = f"Apenas {price_int} €"
            pt_price_alt = f"Desde {price_int} €"
            pt_price_box = f"Apenas {price_int} €/Caixa"
            pt_desc_a2 = f"Preço especial {price_int} €. Entrega rápida em todo o país."
            pt_desc_b1 = f"{product_name} com preço especial de {price_int} €. Encomende agora!"
            pt_desc_b2 = f"Aproveite a oferta! {product_name} original por apenas {price_int} €."
            pt_desc_b4 = f"Encomende 3 caixas e receba consulta grátis. Apenas {price_int} €."
            pt_desc_c1 = f"{product_name} original — encomende na loja oficial por {price_int} €."
            pt_b_h2 = f"{product_name} — {price_int} €"
            pt_b_h3 = f"Preço Especial {price_int} €"
        else:
            pt_price_h = "Oferta Especial Online"
            pt_price_alt = "Preço Reduzido Agora"
            pt_price_box = "Oferta Limitada"
            pt_desc_a2 = "Entrega rápida em todo o país. Encomende na loja oficial."
            pt_desc_b1 = f"{product_name} — encomende agora na loja oficial!"
            pt_desc_b2 = f"Aproveite a oferta! {product_name} original na loja oficial."
            pt_desc_b4 = "Encomende 3 caixas e receba consulta grátis. Oferta limitada."
            pt_desc_c1 = f"{product_name} original — encomende na loja oficial agora."
            pt_b_h2 = f"Comprar {product_name}"
            pt_b_h3 = "Oferta Especial"

        rsa_a = {
            "headlines": [
                f"{product_name} Original",
                "Loja Oficial Portugal",
                "Produto Verificado",
                "Entrega em Portugal",
                "Fórmula Original",
                pt_price_h,
                "Pagamento na Entrega",
                "Producto Premium",
                "Resultados Reais",
                "Encomende Online",
                "Entrega Rápida",
                "Qualidade Garantida",
                f"Encomendar {product_name}",
                "Qualidade Premium",
                "Envio Grátis",
            ],
            "descriptions": [
                f"{product_name} — produto original. Encomende na loja oficial agora.",
                pt_desc_a2,
                "Fórmula original, resultados comprovados. Pagamento na entrega.",
                f"Encomende {product_name} hoje. Autenticidade garantida.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                pt_b_h2,
                pt_b_h3,
                "Oferta Limitada",
                pt_price_alt,
                "Melhor Oferta",
                "Desconto Especial Hoje",
                f"Comprar {product_name}",
                "Poupe Hoje",
                "Encomenda com Desconto",
                "Entrega Gratuita",
                pt_price_box,
                f"{{KeyWord:{default_kw}}} Online",
                "Ver Oferta Agora",
                "Avaliações Positivas",
            ],
            "descriptions": [
                pt_desc_b1,
                pt_desc_b2,
                "Entrega rápida em todo o país. Pagamento na entrega disponível.",
                pt_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Loja Oficial Portugal",
                "Produto Verificado",
                "Entrega em Portugal",
                "Fórmula Original",
                pt_price_h,
                "Pagamento na Entrega",
                "Producto Premium",
                "Resultados Reais",
                "Encomende Online",
                "Entrega Rápida",
                "Qualidade Garantida",
                f"Comprar {product_name} Já",
                "Ingredientes Naturais",
                "Fórmula Testada Clin.",
            ],
            "descriptions": [
                pt_desc_c1,
                "Produto verificado e testado. Entrega rápida em Portugal.",
                "Producto premium com fórmula original. Resultados comprovados.",
                "Encomende hoje na loja oficial. Autenticidade garantida.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "PT")

    elif template_country == "HU":
        if has_price:
            hu_price_h = f"Csak {price_int} Ft"
            hu_price_alt = f"{price_int} Ft-tól"
            hu_price_box = f"Csak {price_int} Ft/Doboz"
            hu_desc_a2 = f"Különleges ár {price_int} Ft. Gyors szállítás egész Magyarországon."
            hu_desc_b1 = f"{{KeyWord:{default_kw}}} különleges ár {price_int} Ft. Rendelje most!"
            hu_desc_b2 = f"Használja ki az ajánlatot! {product_name} eredeti csak {price_int} Ft."
            hu_desc_b4 = f"Rendeljen 3 dobozt és kapjon ingyenes konzultációt. Csak {price_int} Ft."
            hu_desc_c1 = f"{product_name} eredeti — rendelje a hivatalos boltból {price_int} Ft."
            hu_b_h2 = f"{product_name} — {price_int} Ft"
            hu_b_h3 = f"Különleges Ár {price_int} Ft"
        else:
            hu_price_h = "Különleges Ajánlat"
            hu_price_alt = "Csökkentett Ár Most"
            hu_price_box = "Korlátozott Ajánlat"
            hu_desc_a2 = "Gyors szállítás egész Magyarországon. Rendelje a hivatalos boltból."
            hu_desc_b1 = f"{{KeyWord:{default_kw}}} — rendelje most a hivatalos boltból!"
            hu_desc_b2 = f"Használja ki az ajánlatot! {product_name} eredeti a hivatalos boltban."
            hu_desc_b4 = f"Rendeljen 3 dobozt és kapjon ingyenes konzultációt. Korlátozott ajánlat."
            hu_desc_c1 = f"{product_name} eredeti — rendelje a hivatalos boltból most."
            hu_b_h2 = f"Rendelje {product_name}"
            hu_b_h3 = "Különleges Ajánlat"

        rsa_a = {
            "headlines": [
                f"{product_name} Eredeti",
                "Hivatalos Bolt Magyaro.",
                "Ellenőrzött Termék",
                "Szállítás Magyarország",
                "Eredeti Formula",
                hu_price_h,
                "Utánvéttel Fizethető",
                "Prémium Étrend-kieg.",
                "Valódi Eredmények",
                "Rendelje Most Online",
                "Gyors Szállítás",
                "Minőséggarancia",
                f"Rendelje {product_name}",
                "100% Minőségi",
                "Ingyenes Konzultáció",
            ],
            "descriptions": [
                f"{product_name} — eredeti termék. Rendelje a hivatalos boltból.",
                hu_desc_a2,
                "Eredeti formula, bizonyított eredmények. Utánvéttel fizethető.",
                f"Rendelje meg {product_name} ma. Eredetiség és szállítás garantált.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                hu_b_h2,
                hu_b_h3,
                "Korlátozott Ajánlat",
                hu_price_alt,
                "Legjobb Ajánlat",
                "Mai Kedvezmény",
                f"Rendelje {product_name}",
                "50% Megtakarítás",
                "Kedvezményes Rendelés",
                "Ingyenes Szállítás",
                hu_price_box,
                f"{{KeyWord:{default_kw}}} Online",
                "Nézze Meg Az Ajánlatot",
                "Pozitív Vélemények",
            ],
            "descriptions": [
                hu_desc_b1,
                hu_desc_b2,
                "Gyors szállítás egész Magyarországon. Utánvéttel is fizethető.",
                hu_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Eredeti {product_name}", "pinned_field": "HEADLINE_1"},
                "Hivatalos Bolt Magyaro.",
                "Ellenőrzött Termék",
                "Szállítás Magyarország",
                "Eredeti Formula",
                hu_price_h,
                "Utánvéttel Fizethető",
                "Prémium Étrend-kieg.",
                "Valódi Eredmények",
                "Rendelje Most Online",
                "Gyors Szállítás",
                "Minőséggarancia",
                f"{product_name} Rendelés",
                "Prémium Minőség",
                "Klinikailag Tesztelt",
            ],
            "descriptions": [
                hu_desc_c1,
                "Ellenőrzött és tesztelt termék. Gyors szállítás Magyarországon.",
                "Prémium termék eredeti formulával. Bizonyított eredmények.",
                "Rendelje ma a hivatalos boltból. Eredetiség garantált.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "HU")

    elif template_country == "FR":
        if has_price:
            fr_price_h = f"Seulement {price_int} €"
            fr_price_alt = f"À partir de {price_int} €"
            fr_price_box = f"Seulement {price_int} €/Boîte"
            fr_desc_a2 = f"Prix spécial {price_int} €. Livraison rapide dans toute la France."
            fr_desc_b1 = f"{{KeyWord:{default_kw}}} prix spécial {price_int} €. Commandez maintenant!"
            fr_desc_b2 = f"Profitez de l'offre! {product_name} original seulement {price_int} €."
            fr_desc_b4 = f"Commandez 3 boîtes et recevez consultation gratuite. {price_int} €."
            fr_desc_c1 = f"{product_name} original — commandez sur le site officiel à {price_int} €."
            fr_b_h2 = f"{product_name} — {price_int} €"
            fr_b_h3 = f"Prix Spécial {price_int} €"
        else:
            fr_price_h = "Offre Spéciale en Ligne"
            fr_price_alt = "Prix Réduit Maintenant"
            fr_price_box = "Offre Limitée"
            fr_desc_a2 = "Livraison rapide dans toute la France. Commandez sur le site officiel."
            fr_desc_b1 = f"{{KeyWord:{default_kw}}} — commandez maintenant sur le site officiel!"
            fr_desc_b2 = f"Profitez de l'offre! {product_name} original sur le site officiel."
            fr_desc_b4 = f"Commandez 3 boîtes et recevez consultation gratuite. Offre limitée."
            fr_desc_c1 = f"{product_name} original — commandez sur le site officiel maintenant."
            fr_b_h2 = f"Acheter {product_name}"
            fr_b_h3 = "Offre Spéciale"

        rsa_a = {
            "headlines": [
                f"{product_name} Original",
                "Site Officiel France",
                "Produit Vérifié",
                "Livraison depuis France",
                "Formule Originale",
                fr_price_h,
                "Paiement à la Livraison",
                "Produit Premium",
                "Résultats Réels",
                "Commandez Maintenant",
                "Livraison Rapide",
                "Qualité Garantie",
                f"Commander {product_name}",
                "Qualité Premium",
                "Livraison Gratuite",
            ],
            "descriptions": [
                f"{product_name} — produit original. Commandez sur le site officiel.",
                fr_desc_a2,
                "Formule originale, résultats prouvés. Paiement à la livraison.",
                f"Commandez {product_name} aujourd'hui. Authenticité garantie.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                fr_b_h2,
                fr_b_h3,
                "Offre Limitée",
                fr_price_alt,
                "Meilleure Offre",
                "Réduction Spéciale",
                f"Acheter {product_name}",
                "Économisez 50%",
                "Commandez en Promo",
                "Livraison Gratuite",
                fr_price_box,
                f"{{KeyWord:{default_kw}}} En Ligne",
                "Vérifier l'Offre",
                "Avis Positifs",
            ],
            "descriptions": [
                fr_desc_b1,
                fr_desc_b2,
                "Livraison rapide dans toute la France. Paiement à la livraison.",
                fr_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Site Officiel France",
                "Produit Vérifié",
                "Livraison depuis France",
                "Formule Originale",
                fr_price_h,
                "Paiement à la Livraison",
                "Produit Premium",
                "Résultats Réels",
                "Commandez Maintenant",
                "Livraison Rapide",
                "Qualité Garantie",
                f"Commander {product_name} Vite",
                "Ingrédients Naturels",
                "Formule Testée Cliniq.",
            ],
            "descriptions": [
                fr_desc_c1,
                "Produit vérifié et testé. Livraison rapide en France.",
                "Produit premium avec qualité originale. Résultats prouvés.",
                "Commandez aujourd'hui sur le site officiel. Authenticité garantie.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "FR")

    elif template_country == "DE":
        if has_price:
            de_price_h = f"Nur {price_int} €"
            de_price_alt = f"Ab {price_int} €"
            de_price_box = f"Nur {price_int} €/Packung"
            de_desc_a2 = f"Sonderpreis {price_int} €. Schnelle Lieferung in ganz Deutschland."
            de_desc_b1 = f"{{KeyWord:{default_kw}}} Sonderpreis {price_int} €. Jetzt bestellen!"
            de_desc_b2 = f"Angebot nutzen! {product_name} Original nur {price_int} €."
            de_desc_b4 = f"3 Packungen bestellen, Beratung gratis. Nur {price_int} €."
            de_desc_c1 = f"{product_name} Original — im offiziellen Shop für {price_int} €."
            de_b_h2 = f"{product_name} — {price_int} €"
            de_b_h3 = f"Sonderpreis {price_int} €"
        else:
            de_price_h = "Sonderangebot Online"
            de_price_alt = "Reduzierter Preis"
            de_price_box = "Limitiertes Angebot"
            de_desc_a2 = "Schnelle Lieferung in ganz Deutschland. Im offiziellen Shop bestellen."
            de_desc_b1 = f"{{KeyWord:{default_kw}}} — jetzt im offiziellen Shop bestellen!"
            de_desc_b2 = f"Angebot nutzen! {product_name} Original im offiziellen Shop."
            de_desc_b4 = f"3 Packungen bestellen, Beratung gratis. Limitiertes Angebot."
            de_desc_c1 = f"{product_name} Original — jetzt im offiziellen Shop bestellen."
            de_b_h2 = f"{product_name} Kaufen"
            de_b_h3 = "Sonderangebot"

        rsa_a = {
            "headlines": [
                f"{product_name} Original",
                "Offizieller Shop DE",
                "Geprüftes Produkt",
                "Versand aus Deutschland",
                "Originalformel",
                de_price_h,
                "Nachnahme Möglich",
                "Premium Ergänzung",
                "Echte Ergebnisse",
                "Jetzt Online Bestellen",
                "Schnelle Lieferung",
                "Qualitätsgarantie",
                f"Bestellen {product_name}",
                "100% Premium Qualität",
                "Kostenlose Beratung",
            ],
            "descriptions": [
                f"{product_name} — Original-Ergänzung. Jetzt im offiziellen Shop bestellen.",
                de_desc_a2,
                "Originalformel, nachgewiesene Ergebnisse. Nachnahme möglich.",
                f"Bestellen Sie {product_name} heute. Echtheit garantiert.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                de_b_h2,
                de_b_h3,
                "Limitiertes Angebot",
                de_price_alt,
                "Bestes Angebot",
                "Sonderrabatt Heute",
                f"{product_name} Kaufen",
                "50% Sparen",
                "Mit Rabatt Bestellen",
                "Kostenloser Versand",
                de_price_box,
                f"{{KeyWord:{default_kw}}} Online",
                "Angebot Prüfen",
                "Positive Bewertungen",
            ],
            "descriptions": [
                de_desc_b1,
                de_desc_b2,
                "Schnelle Lieferung in ganz Deutschland. Nachnahme möglich.",
                de_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Offizieller Shop DE",
                "Geprüftes Produkt",
                "Versand aus Deutschland",
                "Originalformel",
                de_price_h,
                "Nachnahme Möglich",
                "Premium Ergänzung",
                "Echte Ergebnisse",
                "Jetzt Online Bestellen",
                "Schnelle Lieferung",
                "Qualitätsgarantie",
                f"{product_name} Jetzt Kaufen",
                "Premium Zutaten",
                "Klinisch Getestet",
            ],
            "descriptions": [
                de_desc_c1,
                "Geprüftes und getestetes Produkt. Schnelle Lieferung in DE.",
                "Premium-Ergänzung mit Originalformel. Nachgewiesene Ergebnisse.",
                "Heute im offiziellen Shop bestellen. Echtheit garantiert.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "DE")

    elif template_country == "IT":
        if has_price:
            it_price_h = f"Solo {price_int} €"
            it_price_alt = f"Da {price_int} €"
            it_price_box = f"Solo {price_int} €/Conf."
            it_desc_a2 = f"Prezzo speciale {price_int} €. Consegna rapida in tutta Italia."
            it_desc_b1 = f"{{KeyWord:{default_kw}}} prezzo speciale {price_int} €. Ordina adesso!"
            it_desc_b2 = f"Approfitta dell'offerta! {product_name} originale solo {price_int} €."
            it_desc_b4 = f"Ordina 3 confezioni e ricevi consulenza gratis. Solo {price_int} €."
            it_desc_c1 = f"{product_name} originale — ordina dal sito ufficiale a {price_int} €."
            it_b_h2 = f"{product_name} — {price_int} €"
            it_b_h3 = f"Prezzo Speciale {price_int} €"
        else:
            it_price_h = "Offerta Speciale Online"
            it_price_alt = "Prezzo Ridotto Ora"
            it_price_box = "Offerta Limitata"
            it_desc_a2 = "Consegna rapida in tutta Italia. Ordina dal sito ufficiale."
            it_desc_b1 = f"{{KeyWord:{default_kw}}} — ordina adesso dal sito ufficiale!"
            it_desc_b2 = f"Approfitta dell'offerta! {product_name} originale sul sito ufficiale."
            it_desc_b4 = f"Ordina 3 confezioni e ricevi consulenza gratis. Offerta limitata."
            it_desc_c1 = f"{product_name} originale — ordina dal sito ufficiale adesso."
            it_b_h2 = f"Ordina {product_name}"
            it_b_h3 = "Offerta Speciale"

        rsa_a = {
            "headlines": [
                f"{product_name} Originale",
                "Sito Ufficiale Italia",
                "Prodotto Verificato",
                "Spedizione dall'Italia",
                "Formula Originale",
                it_price_h,
                "Pagamento alla Consegna",
                "Prodotto Premium",
                "Risultati Reali",
                "Ordina Ora Online",
                "Consegna Rapida",
                "Qualità Garantita",
                f"Ordina {product_name}",
                "100% Qualità Premium",
                "Consulenza Gratuita",
            ],
            "descriptions": [
                f"{product_name} — prodotto originale. Ordina dal sito ufficiale adesso.",
                it_desc_a2,
                "Formula originale, risultati provati. Pagamento alla consegna.",
                f"Ordina {product_name} oggi. Autenticità garantita.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                it_b_h2,
                it_b_h3,
                "Offerta Limitata",
                it_price_alt,
                "Migliore Offerta",
                "Sconto Speciale Oggi",
                f"Acquista {product_name}",
                "Risparmia il 50%",
                "Ordina con Sconto",
                "Spedizione Gratuita",
                it_price_box,
                f"{{KeyWord:{default_kw}}} Online",
                "Verifica l'Offerta",
                "Recensioni Positive",
            ],
            "descriptions": [
                it_desc_b1,
                it_desc_b2,
                "Consegna rapida in tutta Italia. Pagamento alla consegna disponibile.",
                it_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Originale {product_name}", "pinned_field": "HEADLINE_1"},
                "Sito Ufficiale Italia",
                "Prodotto Verificato",
                "Spedizione dall'Italia",
                "Formula Originale",
                it_price_h,
                "Pagamento alla Consegna",
                "Prodotto Premium",
                "Risultati Reali",
                "Ordina Ora Online",
                "Consegna Rapida",
                "Qualità Garantita",
                f"Ordina {product_name} Subito",
                "Ingredienti Naturali",
                "Formula Testata Clinic.",
            ],
            "descriptions": [
                it_desc_c1,
                "Prodotto verificato e testato. Consegna rapida in Italia.",
                "Prodotto premium con formula originale. Risultati provati.",
                "Ordina oggi dal sito ufficiale. Autenticità garantita.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "IT")

    elif template_country == "ES":
        if has_price:
            es_price_h = f"Solo {price_int} €"
            es_price_alt = f"Desde {price_int} €"
            es_price_box = f"Solo {price_int} €/Caja"
            es_desc_a2 = f"Precio especial {price_int} €. Envío rápido en toda España."
            es_desc_b1 = f"{{KeyWord:{default_kw}}} precio especial {price_int} €. ¡Compra ahora!"
            es_desc_b2 = f"¡Aprovecha la oferta! {product_name} original solo {price_int} €."
            es_desc_b4 = f"Compra 3 cajas y recibe consulta gratis. Solo {price_int} €."
            es_desc_c1 = f"{product_name} original — compra en la tienda oficial por {price_int} €."
            es_b_h2 = f"{product_name} — {price_int} €"
            es_b_h3 = f"Precio Especial {price_int} €"
        else:
            es_price_h = "Oferta Especial Online"
            es_price_alt = "Precio Reducido Ahora"
            es_price_box = "Oferta Limitada"
            es_desc_a2 = "Envío rápido en toda España. Compra en la tienda oficial."
            es_desc_b1 = f"{{KeyWord:{default_kw}}} — ¡compra ahora en la tienda oficial!"
            es_desc_b2 = f"¡Aprovecha la oferta! {product_name} original en la tienda oficial."
            es_desc_b4 = f"Compra 3 cajas y recibe consulta gratis. Oferta limitada."
            es_desc_c1 = f"{product_name} original — compra en la tienda oficial ahora."
            es_b_h2 = f"Comprar {product_name}"
            es_b_h3 = "Oferta Especial"

        rsa_a = {
            "headlines": [
                f"{product_name} Original",
                "Tienda Oficial España",
                "Producto Verificado",
                "Envío desde España",
                "Fórmula Original",
                es_price_h,
                "Pago Contra Reembolso",
                "Producto Premium",
                "Resultados Reales",
                "Compra Ahora Online",
                "Envío Rápido",
                "Calidad Garantizada",
                f"Comprar {product_name}",
                "Calidad Premium",
                "Envío Gratis",
            ],
            "descriptions": [
                f"{product_name} — producto original. Compra en la tienda oficial ahora.",
                es_desc_a2,
                "Fórmula original, resultados probados. Pago contra reembolso.",
                f"Compra {product_name} hoy. Autenticidad garantizada.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                es_b_h2,
                es_b_h3,
                "Oferta Limitada",
                es_price_alt,
                "Mejor Oferta",
                "Descuento Especial Hoy",
                f"Comprar {product_name}",
                "Ahorra un 50%",
                "Compra con Descuento",
                "Envío Gratuito",
                es_price_box,
                f"{{KeyWord:{default_kw}}} Online",
                "Ver Oferta Ahora",
                "Opiniones Positivas",
            ],
            "descriptions": [
                es_desc_b1,
                es_desc_b2,
                "Envío rápido en toda España. Pago contra reembolso disponible.",
                es_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Tienda Oficial España",
                "Producto Verificado",
                "Envío desde España",
                "Fórmula Original",
                es_price_h,
                "Pago Contra Reembolso",
                "Producto Premium",
                "Resultados Reales",
                "Compra Ahora Online",
                "Envío Rápido",
                "Calidad Garantizada",
                f"Compra {product_name} Ya",
                "Ingredientes Naturales",
                "Fórmula Clínicamente Te.",
            ],
            "descriptions": [
                es_desc_c1,
                "Producto verificado y probado. Envío rápido en España.",
                "Producto premium con fórmula original. Resultados probados.",
                "Compra hoy en la tienda oficial. Autenticidad garantizada.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "ES")

    elif template_country == "ME":
        if has_price:
            me_price_h = f"Samo {price_int} €"
            me_price_alt = f"Od {price_int} €"
            me_price_box = f"Samo {price_int} €/Pak."
            me_desc_a2 = f"Posebna cena {price_int} €. Brza dostava širom zemlje."
            me_desc_b1 = f"{product_name} po posebnoj ceni od {price_int} €. Poruči odmah!"
            me_desc_b2 = f"Iskoristi ponudu! {product_name} original za samo {price_int} €."
            me_desc_b4 = f"Poruči 3 pakovanja i dobijaš savet gratis. Samo {price_int} €."
            me_desc_c1 = f"{product_name} original — poruči sa zvaničnog sajta za {price_int} €."
            me_b_h2 = f"{product_name} — {price_int} €"
            me_b_h3 = f"Posebna Cena {price_int} €"
        else:
            me_price_h = "Posebna Online Ponuda"
            me_price_alt = "Snižena Cena Sada"
            me_price_box = "Ograničena Ponuda"
            me_desc_a2 = "Brza dostava širom zemlje. Poruči sa zvaničnog sajta."
            me_desc_b1 = f"{product_name} — poruči odmah sa zvaničnog sajta!"
            me_desc_b2 = f"Iskoristi ponudu! {product_name} original na zvaničnom sajtu."
            me_desc_b4 = "Poruči 3 pakovanja i dobijaš savet gratis. Ograničena ponuda."
            me_desc_c1 = f"{product_name} original — poruči sa zvaničnog sajta odmah."
            me_b_h2 = f"Kupi {product_name}"
            me_b_h3 = "Posebna Ponuda"

        rsa_a = {
            "headlines": [
                f"{product_name} Original",
                "Zvanična Prodavnica",
                "Proveren Proizvod",
                "Dostava u Crnoj Gori",
                "Originalna Formula",
                me_price_h,
                "Plaćanje Pouzećem",
                "Premium Dodatak",
                "Pravi Rezultati",
                "Poruči Online",
                "Brza Dostava",
                "Kvalitet Zagarantovan",
                f"Poruči {product_name}",
                "100% Prirodna Formula",
                "Besplatan Savet",
            ],
            "descriptions": [
                f"{product_name} — originalni dodatak. Poruči sa zvaničnog sajta.",
                me_desc_a2,
                "Originalna formula, provereni rezultati. Plaćanje pouzećem.",
                f"Poruči {product_name} danas. Autentičnost zagarantovana.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                me_b_h2,
                me_b_h3,
                "Ograničena Ponuda",
                me_price_alt,
                "Najbolja Ponuda",
                "Specijalni Popust Danas",
                f"Kupi {product_name}",
                "Uštedi Danas",
                "Poruči sa Popustom",
                "Besplatna Dostava",
                me_price_box,
                f"{{KeyWord:{default_kw}}} Online",
                "Pogledaj Ponudu",
                "Pozitivne Recenzije",
            ],
            "descriptions": [
                me_desc_b1,
                me_desc_b2,
                "Brza dostava širom zemlje. Plaćanje pouzećem dostupno.",
                me_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Zvanična Prodavnica",
                "Proveren Proizvod",
                "Dostava u Crnoj Gori",
                "Originalna Formula",
                me_price_h,
                "Plaćanje Pouzećem",
                "Premium Dodatak",
                "Pravi Rezultati",
                "Poruči Online",
                "Brza Dostava",
                "Kvalitet Zagarantovan",
                f"Kupi {product_name} Sada",
                "Prirodni Sastojci",
                "Klinički Testirana Form.",
            ],
            "descriptions": [
                me_desc_c1,
                "Proveren i testiran proizvod. Brza dostava u Crnoj Gori.",
                "Premium dodatak sa originalnom formulom. Provereni rezultati.",
                "Poruči danas sa zvaničnog sajta. Autentičnost zagarantovana.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "ME")

    elif template_country == "CZ":
        if has_price:
            cz_price_h = f"Jen {price_int} Kč"
            cz_price_alt = f"Od {price_int} Kč"
            cz_price_box = f"Jen {price_int} Kč/Bal."
            cz_desc_a2 = f"Speciální cena {price_int} Kč. Rychlé doručení po celém Česku."
            cz_desc_b1 = f"{{KeyWord:{default_kw}}} za speciální cenu {price_int} Kč. Objednejte!"
            cz_desc_b2 = f"Využijte nabídku! {product_name} originál jen za {price_int} Kč."
            cz_desc_b4 = f"Objednejte 3 balení a konzultace zdarma. Jen {price_int} Kč."
            cz_desc_c1 = f"{product_name} originál — objednejte z oficiálního obchodu za {price_int} Kč."
            cz_b_h2 = f"{product_name} — {price_int} Kč"
            cz_b_h3 = f"Speciální Cena {price_int} Kč"
        else:
            cz_price_h = "Speciální Nabídka Online"
            cz_price_alt = "Snížená Cena"
            cz_price_box = "Omezená Nabídka"
            cz_desc_a2 = "Rychlé doručení po celém Česku. Objednejte z oficiálního obchodu."
            cz_desc_b1 = f"{{KeyWord:{default_kw}}} — objednejte nyní z oficiálního obchodu!"
            cz_desc_b2 = f"Využijte nabídku! {product_name} originál na oficiálním obchodu."
            cz_desc_b4 = f"Objednejte 3 balení a konzultace zdarma. Omezená nabídka."
            cz_desc_c1 = f"{product_name} originál — objednejte z oficiálního obchodu nyní."
            cz_b_h2 = f"Kupte {product_name}"
            cz_b_h3 = "Speciální Nabídka"
        rsa_a = {
            "headlines": [
                f"{product_name} Originál", "Oficiální Obchod CZ", "Ověřený Produkt",
                "Doručení z Česka", "Originální Složení", cz_price_h,
                "Platba na Dobírku", "Prémiový Doplněk", "Skutečné Výsledky",
                "Objednejte Nyní Online", "Rychlé Doručení", "Záruka Kvality",
                f"Objednejte {product_name}", "100% Přírodní Složení", "Konzultace Zdarma",
            ],
            "descriptions": [
                f"{product_name} — originální doplněk. Objednejte z oficiálního obchodu.",
                cz_desc_a2,
                "Originální složení, prokázané výsledky. Platba na dobírku.",
                f"Objednejte {product_name} dnes. Záruka autenticity a doručení.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", cz_b_h2, cz_b_h3,
                "Omezená Nabídka", cz_price_alt, "Nejlepší Nabídka",
                "Speciální Sleva Dnes", f"Kupte {product_name}", "Ušetřete 50%",
                "Objednejte se Slevou", "Doprava Zdarma", cz_price_box,
                f"{{KeyWord:{default_kw}}} Online", "Zkontrolujte Nabídku", "Pozitivní Recenze",
            ],
            "descriptions": [
                cz_desc_b1, cz_desc_b2,
                "Rychlé doručení po celém Česku. Platba na dobírku dostupná.",
                cz_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Oficiální Obchod CZ", "Ověřený Produkt", "Doručení z Česka",
                "Originální Složení", cz_price_h, "Platba na Dobírku",
                "Prémiový Doplněk", "Skutečné Výsledky", "Objednejte Nyní Online",
                "Rychlé Doručení", "Záruka Kvality",
                f"Kupte {product_name} Nyní", "Přírodní Složky", "Klinicky Testováno",
            ],
            "descriptions": [
                cz_desc_c1,
                "Ověřený a testovaný produkt. Rychlé doručení po celém Česku.",
                "Prémiový doplněk s originálním složením. Prokázané výsledky.",
                "Objednejte dnes z oficiálního obchodu. Autenticita zaručena.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "CZ")

    elif template_country == "SK":
        if has_price:
            sk_price_h = f"Len {price_int} €"
            sk_price_alt = f"Od {price_int} €"
            sk_price_box = f"Len {price_int} €/Bal."
            sk_desc_a2 = f"Špeciálna cena {price_int} €. Rýchle doručenie po Slovensku."
            sk_desc_b1 = f"{{KeyWord:{default_kw}}} za špeciálnu cenu {price_int} €. Objednajte!"
            sk_desc_b2 = f"Využite ponuku! {product_name} originál len za {price_int} €."
            sk_desc_b4 = f"Objednajte 3 balenia a konzultácia zadarmo. Len {price_int} €."
            sk_desc_c1 = f"{product_name} originál — objednajte z oficiálneho obchodu za {price_int} €."
            sk_b_h2 = f"{product_name} — {price_int} €"
            sk_b_h3 = f"Špeciálna Cena {price_int} €"
        else:
            sk_price_h = "Špeciálna Ponuka Online"
            sk_price_alt = "Znížená Cena"
            sk_price_box = "Obmedzená Ponuka"
            sk_desc_a2 = "Rýchle doručenie po Slovensku. Objednajte z oficiálneho obchodu."
            sk_desc_b1 = f"{{KeyWord:{default_kw}}} — objednajte teraz z oficiálneho obchodu!"
            sk_desc_b2 = f"Využite ponuku! {product_name} originál na oficiálnom obchode."
            sk_desc_b4 = f"Objednajte 3 balenia a konzultácia zadarmo. Obmedzená ponuka."
            sk_desc_c1 = f"{product_name} originál — objednajte z oficiálneho obchodu teraz."
            sk_b_h2 = f"Kúpte {product_name}"
            sk_b_h3 = "Špeciálna Ponuka"
        rsa_a = {
            "headlines": [
                f"{product_name} Originál", "Oficiálny Obchod SK", "Overený Produkt",
                "Doručenie zo Slovenska", "Originálne Zloženie", sk_price_h,
                "Platba na Dobierku", "Prémiový Doplnok", "Skutočné Výsledky",
                "Objednajte Teraz Online", "Rýchle Doručenie", "Záruka Kvality",
                f"Objednajte {product_name}", "100% Prírodné Zloženie", "Konzultácia Zadarmo",
            ],
            "descriptions": [
                f"{product_name} — originálny doplnok. Objednajte z oficiálneho obchodu.",
                sk_desc_a2,
                "Originálne zloženie, dokázané výsledky. Platba na dobierku.",
                f"Objednajte {product_name} dnes. Záruka autenticity.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", sk_b_h2, sk_b_h3,
                "Obmedzená Ponuka", sk_price_alt, "Najlepšia Ponuka",
                "Špeciálna Zľava Dnes", f"Kúpte {product_name}", "Ušetrite 50%",
                "Objednajte so Zľavou", "Doprava Zadarmo", sk_price_box,
                f"{{KeyWord:{default_kw}}} Online", "Skontrolujte Ponuku", "Pozitívne Recenzie",
            ],
            "descriptions": [
                sk_desc_b1, sk_desc_b2,
                "Rýchle doručenie po celom Slovensku. Platba na dobierku dostupná.",
                sk_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Oficiálny Obchod SK", "Overený Produkt", "Doručenie zo Slovenska",
                "Originálne Zloženie", sk_price_h, "Platba na Dobierku",
                "Prémiový Doplnok", "Skutočné Výsledky", "Objednajte Teraz Online",
                "Rýchle Doručenie", "Záruka Kvality",
                f"Kúpte {product_name} Teraz", "Prírodné Zložky", "Klinicky Testované",
            ],
            "descriptions": [
                sk_desc_c1,
                "Overený a testovaný produkt. Rýchle doručenie po Slovensku.",
                "Prémiový doplnok s originálnym zložením. Dokázané výsledky.",
                "Objednajte dnes z oficiálneho obchodu. Autenticita zaručená.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "SK")

    elif template_country == "BG":
        if has_price:
            bg_price_h = f"Само {price_int} лв"
            bg_price_alt = f"От {price_int} лв"
            bg_price_box = f"Само {price_int} лв/Кут."
            bg_desc_a2 = f"Специална цена {price_int} лв. Бърза доставка в цяла България."
            bg_desc_b1 = f"{{KeyWord:{default_kw}}} на специална цена {price_int} лв. Поръчай!"
            bg_desc_b2 = f"Възползвай се! {product_name} оригинал само за {price_int} лв."
            bg_desc_b4 = f"Поръчай 3 кутии и получи консултация безплатно. Само {price_int} лв."
            bg_desc_c1 = f"{product_name} оригинал — поръчай от официалния магазин за {price_int} лв."
            bg_b_h2 = f"{product_name} — {price_int} лв"
            bg_b_h3 = f"Специална Цена {price_int} лв"
        else:
            bg_price_h = "Специална Онлайн Оферта"
            bg_price_alt = "Намалена Цена Сега"
            bg_price_box = "Ограничена Оферта"
            bg_desc_a2 = "Бърза доставка в цяла България. Поръчай от официалния магазин."
            bg_desc_b1 = f"{{KeyWord:{default_kw}}} — поръчай сега от официалния магазин!"
            bg_desc_b2 = f"Възползвай се! {product_name} оригинал в официалния магазин."
            bg_desc_b4 = "Поръчай 3 кутии и получи консултация безплатно. Ограничена оферта."
            bg_desc_c1 = f"{product_name} оригинал — поръчай от официалния магазин сега."
            bg_b_h2 = f"Купи {product_name}"
            bg_b_h3 = "Специална Оферта"
        rsa_a = {
            "headlines": [
                f"{product_name} Оригинал", "Официален Магазин BG", "Проверен Продукт",
                "Доставка в България", "Оригинална Формула", bg_price_h,
                "Плащане при Доставка", "Премиум Добавка", "Реални Резултати",
                "Поръчай Сега Онлайн", "Бърза Доставка", "Гарантирано Качество",
                f"Поръчай {product_name}", "100% Натурална Формула", "Безплатна Консултация",
            ],
            "descriptions": [
                f"{product_name} — оригинална добавка. Поръчай от официалния магазин.",
                bg_desc_a2,
                "Оригинална формула, доказани резултати. Плащане при доставка.",
                f"Поръчай {product_name} днес. Автентичност гарантирана.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", bg_b_h2, bg_b_h3,
                "Ограничена Оферта", bg_price_alt, "Най-добра Оферта",
                "Специална Отстъпка Днес", f"Купи {product_name}", "Спести 50%",
                "Поръчай с Отстъпка", "Безплатна Доставка", bg_price_box,
                f"{{KeyWord:{default_kw}}} Онлайн", "Виж Офертата", "Положителни Отзиви",
            ],
            "descriptions": [
                bg_desc_b1, bg_desc_b2,
                "Бърза доставка в цяла България. Плащане при доставка.",
                bg_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Оригинал {product_name}", "pinned_field": "HEADLINE_1"},
                "Официален Магазин BG", "Проверен Продукт", "Доставка в България",
                "Оригинална Формула", bg_price_h, "Плащане при Доставка",
                "Премиум Добавка", "Реални Резултати", "Поръчай Сега Онлайн",
                "Бърза Доставка", "Гарантирано Качество",
                f"Купи {product_name} Сега", "Натурални Съставки", "Клинично Тестван",
            ],
            "descriptions": [
                bg_desc_c1,
                "Проверен и тестван продукт. Бърза доставка в цяла България.",
                "Премиум добавка с оригинална формула. Доказани резултати.",
                "Поръчай днес от официалния магазин. Автентичност гарантирана.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "BG")

    elif template_country == "GR":
        if has_price:
            gr_price_h = f"Μόνο {price_int} €"
            gr_price_alt = f"Από {price_int} €"
            gr_price_box = f"Μόνο {price_int} €/Κουτ."
            gr_desc_a2 = f"Ειδική τιμή {price_int} €. Γρήγορη αποστολή σε όλη την Ελλάδα."
            gr_desc_b1 = f"{{KeyWord:{default_kw}}} σε ειδική τιμή {price_int} €. Παραγγείλτε!"
            gr_desc_b2 = f"Επωφεληθείτε! {product_name} αυθεντικό μόνο {price_int} €."
            gr_desc_b4 = f"Παραγγείλτε 3 κουτιά και λάβετε δωρεάν συμβουλή. Μόνο {price_int} €."
            gr_desc_c1 = f"{product_name} αυθεντικό — παραγγείλτε από το επίσημο κατάστημα {price_int} €."
            gr_b_h2 = f"{product_name} — {price_int} €"
            gr_b_h3 = f"Ειδική Τιμή {price_int} €"
        else:
            gr_price_h = "Ειδική Προσφορά Online"
            gr_price_alt = "Μειωμένη Τιμή Τώρα"
            gr_price_box = "Περιορισμένη Προσφορά"
            gr_desc_a2 = "Γρήγορη αποστολή σε όλη την Ελλάδα. Παραγγείλτε από το επίσημο κατάστημα."
            gr_desc_b1 = f"{{KeyWord:{default_kw}}} — παραγγείλτε τώρα από το επίσημο κατάστημα!"
            gr_desc_b2 = f"Επωφεληθείτε! {product_name} αυθεντικό στο επίσημο κατάστημα."
            gr_desc_b4 = "Παραγγείλτε 3 κουτιά και λάβετε δωρεάν συμβουλή. Περιορισμένη προσφορά."
            gr_desc_c1 = f"{product_name} αυθεντικό — παραγγείλτε από το επίσημο κατάστημα τώρα."
            gr_b_h2 = f"Αγοράστε {product_name}"
            gr_b_h3 = "Ειδική Προσφορά"
        rsa_a = {
            "headlines": [
                f"{product_name} Αυθεντικό", "Επίσημο Κατάστημα GR", "Επαληθευμένο Προϊόν",
                "Αποστολή στην Ελλάδα", "Αυθεντική Σύνθεση", gr_price_h,
                "Αντικαταβολή", "Premium Συμπλήρωμα", "Πραγματικά Αποτελέσματα",
                "Παραγγείλτε Online", "Γρήγορη Αποστολή", "Εγγυημένη Ποιότητα",
                f"Παραγγείλτε {product_name}", "100% Φυσική Σύνθεση", "Δωρεάν Συμβουλή",
            ],
            "descriptions": [
                f"{product_name} — αυθεντικό συμπλήρωμα. Παραγγείλτε από το επίσημο κατάστημα.",
                gr_desc_a2,
                "Αυθεντική σύνθεση, αποδεδειγμένα αποτελέσματα. Αντικαταβολή.",
                f"Παραγγείλτε {product_name} σήμερα. Εγγυημένη αυθεντικότητα.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", gr_b_h2, gr_b_h3,
                "Περιορισμένη Προσφορά", gr_price_alt, "Καλύτερη Προσφορά",
                "Ειδική Έκπτωση Σήμερα", f"Αγοράστε {product_name}", "Εξοικονομήστε 50%",
                "Παραγγείλτε με Έκπτωση", "Δωρεάν Αποστολή", gr_price_box,
                f"{{KeyWord:{default_kw}}} Online", "Δείτε την Προσφορά", "Θετικές Κριτικές",
            ],
            "descriptions": [
                gr_desc_b1, gr_desc_b2,
                "Γρήγορη αποστολή σε όλη την Ελλάδα. Αντικαταβολή διαθέσιμη.",
                gr_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Αυθεντικό {product_name}", "pinned_field": "HEADLINE_1"},
                "Επίσημο Κατάστημα GR", "Επαληθευμένο Προϊόν", "Αποστολή στην Ελλάδα",
                "Αυθεντική Σύνθεση", gr_price_h, "Αντικαταβολή",
                "Premium Συμπλήρωμα", "Πραγματικά Αποτελέσματα", "Παραγγείλτε Online",
                "Γρήγορη Αποστολή", "Εγγυημένη Ποιότητα",
                f"Αγοράστε {product_name} Τώρα", "Φυσικά Συστατικά", "Κλινικά Δοκιμασμένο",
            ],
            "descriptions": [
                gr_desc_c1,
                "Επαληθευμένο και δοκιμασμένο προϊόν. Γρήγορη αποστολή στην Ελλάδα.",
                "Premium συμπλήρωμα με αυθεντική σύνθεση. Αποδεδειγμένα αποτελέσματα.",
                "Παραγγείλτε σήμερα από το επίσημο κατάστημα. Αυθεντικότητα εγγυημένη.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "GR")

    elif template_country == "HR":
        if has_price:
            hr_price_h = f"Samo {price_int} €"
            hr_price_alt = f"Od {price_int} €"
            hr_price_box = f"Samo {price_int} €/Kut."
            hr_desc_a2 = f"Posebna cijena {price_int} €. Brza dostava diljem Hrvatske."
            hr_desc_b1 = f"{{KeyWord:{default_kw}}} po posebnoj cijeni {price_int} €. Naručite!"
            hr_desc_b2 = f"Iskoristite ponudu! {product_name} original samo {price_int} €."
            hr_desc_b4 = f"Naručite 3 kutije i konzultacija besplatno. Samo {price_int} €."
            hr_desc_c1 = f"{product_name} original — naručite iz službene trgovine za {price_int} €."
            hr_b_h2 = f"{product_name} — {price_int} €"
            hr_b_h3 = f"Posebna Cijena {price_int} €"
        else:
            hr_price_h = "Posebna Online Ponuda"
            hr_price_alt = "Snižena Cijena Sada"
            hr_price_box = "Ograničena Ponuda"
            hr_desc_a2 = "Brza dostava diljem Hrvatske. Naručite iz službene trgovine."
            hr_desc_b1 = f"{{KeyWord:{default_kw}}} — naručite sada iz službene trgovine!"
            hr_desc_b2 = f"Iskoristite ponudu! {product_name} original u službenoj trgovini."
            hr_desc_b4 = "Naručite 3 kutije i konzultacija besplatno. Ograničena ponuda."
            hr_desc_c1 = f"{product_name} original — naručite iz službene trgovine sada."
            hr_b_h2 = f"Kupite {product_name}"
            hr_b_h3 = "Posebna Ponuda"
        rsa_a = {
            "headlines": [
                f"{product_name} Original", "Službena Trgovina HR", "Provjereni Proizvod",
                "Dostava u Hrvatskoj", "Originalna Formula", hr_price_h,
                "Plaćanje Pouzećem", "Premium Dodatak", "Stvarni Rezultati",
                "Naručite Sada Online", "Brza Dostava", "Zajamčena Kvaliteta",
                f"Naručite {product_name}", "100% Prirodna Formula", "Besplatna Konzultacija",
            ],
            "descriptions": [
                f"{product_name} — originalni dodatak. Naručite iz službene trgovine.",
                hr_desc_a2,
                "Originalna formula, dokazani rezultati. Plaćanje pouzećem.",
                f"Naručite {product_name} danas. Autentičnost zajamčena.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", hr_b_h2, hr_b_h3,
                "Ograničena Ponuda", hr_price_alt, "Najbolja Ponuda",
                "Poseban Popust Danas", f"Kupite {product_name}", "Uštedite 50%",
                "Naručite s Popustom", "Besplatna Dostava", hr_price_box,
                f"{{KeyWord:{default_kw}}} Online", "Pogledajte Ponudu", "Pozitivne Recenzije",
            ],
            "descriptions": [
                hr_desc_b1, hr_desc_b2,
                "Brza dostava diljem Hrvatske. Plaćanje pouzećem dostupno.",
                hr_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Službena Trgovina HR", "Provjereni Proizvod", "Dostava u Hrvatskoj",
                "Originalna Formula", hr_price_h, "Plaćanje Pouzećem",
                "Premium Dodatak", "Stvarni Rezultati", "Naručite Sada Online",
                "Brza Dostava", "Zajamčena Kvaliteta",
                f"Kupite {product_name} Sada", "Prirodni Sastojci", "Klinički Testirano",
            ],
            "descriptions": [
                hr_desc_c1,
                "Provjereni i testirani proizvod. Brza dostava diljem Hrvatske.",
                "Premium dodatak s originalnom formulom. Dokazani rezultati.",
                "Naručite danas iz službene trgovine. Autentičnost zajamčena.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "HR")

    elif template_country == "SI":
        if has_price:
            si_price_h = f"Samo {price_int} €"
            si_price_alt = f"Od {price_int} €"
            si_price_box = f"Samo {price_int} €/Škat."
            si_desc_a2 = f"Posebna cena {price_int} €. Hitra dostava po vsej Sloveniji."
            si_desc_b1 = f"{{KeyWord:{default_kw}}} po posebni ceni {price_int} €. Naročite!"
            si_desc_b2 = f"Izkoristite ponudbo! {product_name} original samo {price_int} €."
            si_desc_b4 = f"Naročite 3 škatle in posvet brezplačno. Samo {price_int} €."
            si_desc_c1 = f"{product_name} original — naročite iz uradne trgovine za {price_int} €."
            si_b_h2 = f"{product_name} — {price_int} €"
            si_b_h3 = f"Posebna Cena {price_int} €"
        else:
            si_price_h = "Posebna Ponudba Online"
            si_price_alt = "Znižana Cena Zdaj"
            si_price_box = "Omejena Ponudba"
            si_desc_a2 = "Hitra dostava po vsej Sloveniji. Naročite iz uradne trgovine."
            si_desc_b1 = f"{{KeyWord:{default_kw}}} — naročite zdaj iz uradne trgovine!"
            si_desc_b2 = f"Izkoristite ponudbo! {product_name} original v uradni trgovini."
            si_desc_b4 = "Naročite 3 škatle in posvet brezplačno. Omejena ponudba."
            si_desc_c1 = f"{product_name} original — naročite iz uradne trgovine zdaj."
            si_b_h2 = f"Kupite {product_name}"
            si_b_h3 = "Posebna Ponudba"
        rsa_a = {
            "headlines": [
                f"{product_name} Original", "Uradna Trgovina SI", "Preverjen Izdelek",
                "Dostava v Sloveniji", "Originalna Formula", si_price_h,
                "Plačilo po Povzetju", "Premium Dodatek", "Resnični Rezultati",
                "Naročite Zdaj Online", "Hitra Dostava", "Zagotovljena Kakovost",
                f"Naročite {product_name}", "100% Naravna Formula", "Brezplačen Posvet",
            ],
            "descriptions": [
                f"{product_name} — originalni dodatek. Naročite iz uradne trgovine.",
                si_desc_a2,
                "Originalna formula, dokazani rezultati. Plačilo po povzetju.",
                f"Naročite {product_name} danes. Avtentičnost zagotovljena.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", si_b_h2, si_b_h3,
                "Omejena Ponudba", si_price_alt, "Najboljša Ponudba",
                "Poseben Popust Danes", f"Kupite {product_name}", "Prihranite 50%",
                "Naročite s Popustom", "Brezplačna Dostava", si_price_box,
                f"{{KeyWord:{default_kw}}} Online", "Preverite Ponudbo", "Pozitivna Mnenja",
            ],
            "descriptions": [
                si_desc_b1, si_desc_b2,
                "Hitra dostava po vsej Sloveniji. Plačilo po povzetju na voljo.",
                si_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Uradna Trgovina SI", "Preverjen Izdelek", "Dostava v Sloveniji",
                "Originalna Formula", si_price_h, "Plačilo po Povzetju",
                "Premium Dodatek", "Resnični Rezultati", "Naročite Zdaj Online",
                "Hitra Dostava", "Zagotovljena Kakovost",
                f"Kupite {product_name} Zdaj", "Naravne Sestavine", "Klinično Testirano",
            ],
            "descriptions": [
                si_desc_c1,
                "Preverjen in testiran izdelek. Hitra dostava po vsej Sloveniji.",
                "Premium dodatek z originalno formulo. Dokazani rezultati.",
                "Naročite danes iz uradne trgovine. Avtentičnost zagotovljena.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "SI")

    elif template_country == "LT":
        if has_price:
            lt_price_h = f"Tik {price_int} €"
            lt_price_alt = f"Nuo {price_int} €"
            lt_price_box = f"Tik {price_int} €/Pak."
            lt_desc_a2 = f"Speciali kaina {price_int} €. Greitas pristatymas visoje Lietuvoje."
            lt_desc_b1 = f"{{KeyWord:{default_kw}}} už specialią kainą {price_int} €. Užsisakykite!"
            lt_desc_b2 = f"Pasinaudokite! {product_name} originalas tik {price_int} €."
            lt_desc_b4 = f"Užsisakykite 3 pakuotes ir konsultacija nemokamai. Tik {price_int} €."
            lt_desc_c1 = f"{product_name} originalas — užsisakykite iš oficialios parduotuvės {price_int} €."
            lt_b_h2 = f"{product_name} — {price_int} €"
            lt_b_h3 = f"Speciali Kaina {price_int} €"
        else:
            lt_price_h = "Specialus Pasiūlymas Online"
            lt_price_alt = "Sumažinta Kaina Dabar"
            lt_price_box = "Ribotas Pasiūlymas"
            lt_desc_a2 = "Greitas pristatymas visoje Lietuvoje. Užsisakykite iš oficialios parduotuvės."
            lt_desc_b1 = f"{{KeyWord:{default_kw}}} — užsisakykite dabar iš oficialios parduotuvės!"
            lt_desc_b2 = f"Pasinaudokite! {product_name} originalas oficialioje parduotuvėje."
            lt_desc_b4 = "Užsisakykite 3 pakuotes ir konsultacija nemokamai. Ribotas pasiūlymas."
            lt_desc_c1 = f"{product_name} originalas — užsisakykite iš oficialios parduotuvės dabar."
            lt_b_h2 = f"Pirkite {product_name}"
            lt_b_h3 = "Specialus Pasiūlymas"
        rsa_a = {
            "headlines": [
                f"{product_name} Originalus", "Oficiali Parduotuvė LT", "Patikrintas Produktas",
                "Pristatymas Lietuvoje", "Originali Formulė", lt_price_h,
                "Mokėjimas Pristatymo Metu", "Premium Papildas", "Tikri Rezultatai",
                "Užsisakykite Dabar", "Greitas Pristatymas", "Kokybės Garantija",
                f"Užsisakykite {product_name}", "100% Natūrali Formulė", "Nemokama Konsultacija",
            ],
            "descriptions": [
                f"{product_name} — originalus papildas. Užsisakykite iš oficialios parduotuvės.",
                lt_desc_a2,
                "Originali formulė, įrodyti rezultatai. Mokėjimas pristatymo metu.",
                f"Užsisakykite {product_name} šiandien. Autentiškumas garantuotas.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", lt_b_h2, lt_b_h3,
                "Ribotas Pasiūlymas", lt_price_alt, "Geriausias Pasiūlymas",
                "Speciali Nuolaida Šiandien", f"Pirkite {product_name}", "Sutaupykite 50%",
                "Užsisakykite su Nuolaida", "Nemokamas Pristatymas", lt_price_box,
                f"{{KeyWord:{default_kw}}} Online", "Peržiūrėkite Pasiūlymą", "Teigiami Atsiliepimai",
            ],
            "descriptions": [
                lt_desc_b1, lt_desc_b2,
                "Greitas pristatymas visoje Lietuvoje. Mokėjimas pristatymo metu.",
                lt_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Originalus {product_name}", "pinned_field": "HEADLINE_1"},
                "Oficiali Parduotuvė LT", "Patikrintas Produktas", "Pristatymas Lietuvoje",
                "Originali Formulė", lt_price_h, "Mokėjimas Pristatymo Metu",
                "Premium Papildas", "Tikri Rezultatai", "Užsisakykite Dabar",
                "Greitas Pristatymas", "Kokybės Garantija",
                f"Pirkite {product_name} Dabar", "Natūralūs Ingredientai", "Kliniškai Ištirta",
            ],
            "descriptions": [
                lt_desc_c1,
                "Patikrintas ir ištirtas produktas. Greitas pristatymas Lietuvoje.",
                "Premium papildas su originalia formule. Įrodyti rezultatai.",
                "Užsisakykite šiandien iš oficialios parduotuvės. Autentiškumas garantuotas.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "LT")

    elif template_country == "EE":
        if has_price:
            ee_price_h = f"Vaid {price_int} €"
            ee_price_alt = f"Alates {price_int} €"
            ee_price_box = f"Vaid {price_int} €/Pak."
            ee_desc_a2 = f"Eripakkumine {price_int} €. Kiire tarne üle kogu Eesti."
            ee_desc_b1 = f"{{KeyWord:{default_kw}}} eripakkumine {price_int} €. Telli kohe!"
            ee_desc_b2 = f"Kasuta pakkumist! {product_name} originaal vaid {price_int} €."
            ee_desc_b4 = f"Telli 3 pakki ja konsultatsioon tasuta. Vaid {price_int} €."
            ee_desc_c1 = f"{product_name} originaal — telli ametlikust poest hinnaga {price_int} €."
            ee_b_h2 = f"{product_name} — {price_int} €"
            ee_b_h3 = f"Eripakkumine {price_int} €"
        else:
            ee_price_h = "Eripakkumine Veebis"
            ee_price_alt = "Alandatud Hind"
            ee_price_box = "Piiratud Pakkumine"
            ee_desc_a2 = "Kiire tarne üle kogu Eesti. Telli ametlikust poest."
            ee_desc_b1 = f"{{KeyWord:{default_kw}}} — telli kohe ametlikust poest!"
            ee_desc_b2 = f"Kasuta pakkumist! {product_name} originaal ametlikus poes."
            ee_desc_b4 = "Telli 3 pakki ja konsultatsioon tasuta. Piiratud pakkumine."
            ee_desc_c1 = f"{product_name} originaal — telli ametlikust poest kohe."
            ee_b_h2 = f"Osta {product_name}"
            ee_b_h3 = "Eripakkumine"
        rsa_a = {
            "headlines": [
                f"{product_name} Originaal", "Ametlik Pood EE", "Kontrollitud Toode",
                "Tarne Eestis", "Originaalkoostis", ee_price_h,
                "Maksmine Tarnimisel", "Premium Toidulisand", "Tõelised Tulemused",
                "Telli Kohe Veebist", "Kiire Tarne", "Kvaliteedi Garantii",
                f"Telli {product_name}", "100% Looduslik Koostis", "Tasuta Konsultatsioon",
            ],
            "descriptions": [
                f"{product_name} — originaalne toidulisand. Telli ametlikust poest.",
                ee_desc_a2,
                "Originaalkoostis, tõestatud tulemused. Maksmine tarnimisel.",
                f"Telli {product_name} täna. Autentsus garanteeritud.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", ee_b_h2, ee_b_h3,
                "Piiratud Pakkumine", ee_price_alt, "Parim Pakkumine",
                "Erisoodustus Täna", f"Osta {product_name}", "Säästa 50%",
                "Telli Soodushinnaga", "Tasuta Tarne", ee_price_box,
                f"{{KeyWord:{default_kw}}} Veebis", "Vaata Pakkumist", "Positiivsed Arvustused",
            ],
            "descriptions": [
                ee_desc_b1, ee_desc_b2,
                "Kiire tarne üle kogu Eesti. Maksmine tarnimisel võimalik.",
                ee_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Originaal {product_name}", "pinned_field": "HEADLINE_1"},
                "Ametlik Pood EE", "Kontrollitud Toode", "Tarne Eestis",
                "Originaalkoostis", ee_price_h, "Maksmine Tarnimisel",
                "Premium Toidulisand", "Tõelised Tulemused", "Telli Kohe Veebist",
                "Kiire Tarne", "Kvaliteedi Garantii",
                f"Osta {product_name} Kohe", "Looduslikud Koostisosad", "Kliiniliselt Testitud",
            ],
            "descriptions": [
                ee_desc_c1,
                "Kontrollitud ja testitud toode. Kiire tarne üle Eesti.",
                "Premium toidulisand originaalkoostisega. Tõestatud tulemused.",
                "Telli täna ametlikust poest. Autentsus garanteeritud.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "EE")

    elif template_country == "LV":
        if has_price:
            lv_price_h = f"Tikai {price_int} €"
            lv_price_alt = f"No {price_int} €"
            lv_price_box = f"Tikai {price_int} €/Iep."
            lv_desc_a2 = f"Īpašā cena {price_int} €. Ātra piegāde visā Latvijā."
            lv_desc_b1 = f"{{KeyWord:{default_kw}}} par īpašo cenu {price_int} €. Pasūtiet!"
            lv_desc_b2 = f"Izmantojiet piedāvājumu! {product_name} oriģināls tikai {price_int} €."
            lv_desc_b4 = f"Pasūtiet 3 iepakojumus un konsultācija bezmaksas. Tikai {price_int} €."
            lv_desc_c1 = f"{product_name} oriģināls — pasūtiet no oficiālā veikala par {price_int} €."
            lv_b_h2 = f"{product_name} — {price_int} €"
            lv_b_h3 = f"Īpašā Cena {price_int} €"
        else:
            lv_price_h = "Īpašais Piedāvājums"
            lv_price_alt = "Samazināta Cena Tagad"
            lv_price_box = "Ierobežots Piedāvājums"
            lv_desc_a2 = "Ātra piegāde visā Latvijā. Pasūtiet no oficiālā veikala."
            lv_desc_b1 = f"{{KeyWord:{default_kw}}} — pasūtiet tagad no oficiālā veikala!"
            lv_desc_b2 = f"Izmantojiet piedāvājumu! {product_name} oriģināls oficiālajā veikalā."
            lv_desc_b4 = "Pasūtiet 3 iepakojumus un konsultācija bezmaksas. Ierobežots piedāvājums."
            lv_desc_c1 = f"{product_name} oriģināls — pasūtiet no oficiālā veikala tagad."
            lv_b_h2 = f"Iegādājieties {product_name}"
            lv_b_h3 = "Īpašais Piedāvājums"
        rsa_a = {
            "headlines": [
                f"{product_name} Oriģināls", "Oficiālais Veikals LV", "Pārbaudīts Produkts",
                "Piegāde Latvijā", "Oriģinālā Formula", lv_price_h,
                "Apmaksa Piegādē", "Premium Uztura Bag.", "Reāli Rezultāti",
                "Pasūtiet Tagad Tiešsaistē", "Ātra Piegāde", "Kvalitātes Garantija",
                f"Pasūtiet {product_name}", "100% Dabīga Formula", "Bezmaksas Konsultācija",
            ],
            "descriptions": [
                f"{product_name} — oriģināls uztura bagātinātājs. Pasūtiet no oficiālā veikala.",
                lv_desc_a2,
                "Oriģinālā formula, pierādīti rezultāti. Apmaksa piegādē.",
                f"Pasūtiet {product_name} šodien. Autentiskums garantēts.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", lv_b_h2, lv_b_h3,
                "Ierobežots Piedāvājums", lv_price_alt, "Labākais Piedāvājums",
                "Īpaša Atlaide Šodien", f"Iegādājieties {product_name}", "Ietaupiet 50%",
                "Pasūtiet ar Atlaidi", "Bezmaksas Piegāde", lv_price_box,
                f"{{KeyWord:{default_kw}}} Tiešsaistē", "Skatiet Piedāvājumu", "Pozitīvas Atsauksmes",
            ],
            "descriptions": [
                lv_desc_b1, lv_desc_b2,
                "Ātra piegāde visā Latvijā. Apmaksa piegādē pieejama.",
                lv_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Oriģināls {product_name}", "pinned_field": "HEADLINE_1"},
                "Oficiālais Veikals LV", "Pārbaudīts Produkts", "Piegāde Latvijā",
                "Oriģinālā Formula", lv_price_h, "Apmaksa Piegādē",
                "Premium Uztura Bag.", "Reāli Rezultāti", "Pasūtiet Tagad Tiešsaistē",
                "Ātra Piegāde", "Kvalitātes Garantija",
                f"Iegādājieties {product_name}", "Dabīgas Sastāvdaļas", "Klīniski Testēts",
            ],
            "descriptions": [
                lv_desc_c1,
                "Pārbaudīts un testēts produkts. Ātra piegāde visā Latvijā.",
                "Premium uztura bagātinātājs ar oriģinālo formulu. Pierādīti rezultāti.",
                "Pasūtiet šodien no oficiālā veikala. Autentiskums garantēts.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "LV")

    elif template_country == "UA":
        if has_price:
            ua_price_h = f"Лише {price_int} ₴"
            ua_price_alt = f"Від {price_int} ₴"
            ua_price_box = f"Лише {price_int} ₴/Уп."
            ua_desc_a2 = f"Спеціальна ціна {price_int} ₴. Швидка доставка по всій Україні."
            ua_desc_b1 = f"{{KeyWord:{default_kw}}} за спеціальною ціною {price_int} ₴. Замовте!"
            ua_desc_b2 = f"Скористайтесь! {product_name} оригінал лише {price_int} ₴."
            ua_desc_b4 = f"Замовте 3 упаковки і консультація безкоштовно. Лише {price_int} ₴."
            ua_desc_c1 = f"{product_name} оригінал — замовте з офіційного магазину за {price_int} ₴."
            ua_b_h2 = f"{product_name} — {price_int} ₴"
            ua_b_h3 = f"Спеціальна Ціна {price_int} ₴"
        else:
            ua_price_h = "Спеціальна Пропозиція"
            ua_price_alt = "Знижена Ціна Зараз"
            ua_price_box = "Обмежена Пропозиція"
            ua_desc_a2 = "Швидка доставка по всій Україні. Замовте з офіційного магазину."
            ua_desc_b1 = f"{{KeyWord:{default_kw}}} — замовте зараз з офіційного магазину!"
            ua_desc_b2 = f"Скористайтесь! {product_name} оригінал в офіційному магазині."
            ua_desc_b4 = "Замовте 3 упаковки і консультація безкоштовно. Обмежена пропозиція."
            ua_desc_c1 = f"{product_name} оригінал — замовте з офіційного магазину зараз."
            ua_b_h2 = f"Купіть {product_name}"
            ua_b_h3 = "Спеціальна Пропозиція"
        rsa_a = {
            "headlines": [
                f"{product_name} Оригінал", "Офіційний Магазин UA", "Перевірений Продукт",
                "Доставка в Україні", "Оригінальна Формула", ua_price_h,
                "Оплата при Доставці", "Преміум Добавка", "Реальні Результати",
                "Замовте Зараз Онлайн", "Швидка Доставка", "Гарантія Якості",
                f"Замовте {product_name}", "100% Натуральна Формула", "Безкоштовна Консультація",
            ],
            "descriptions": [
                f"{product_name} — оригінальна добавка. Замовте з офіційного магазину.",
                ua_desc_a2,
                "Оригінальна формула, доведені результати. Оплата при доставці.",
                f"Замовте {product_name} сьогодні. Автентичність гарантована.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", ua_b_h2, ua_b_h3,
                "Обмежена Пропозиція", ua_price_alt, "Найкраща Пропозиція",
                "Спеціальна Знижка Сьогодні", f"Купіть {product_name}", "Заощаджте 50%",
                "Замовте зі Знижкою", "Безкоштовна Доставка", ua_price_box,
                f"{{KeyWord:{default_kw}}} Онлайн", "Перегляньте Пропозицію", "Позитивні Відгуки",
            ],
            "descriptions": [
                ua_desc_b1, ua_desc_b2,
                "Швидка доставка по всій Україні. Оплата при доставці доступна.",
                ua_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Оригінал {product_name}", "pinned_field": "HEADLINE_1"},
                "Офіційний Магазин UA", "Перевірений Продукт", "Доставка в Україні",
                "Оригінальна Формула", ua_price_h, "Оплата при Доставці",
                "Преміум Добавка", "Реальні Результати", "Замовте Зараз Онлайн",
                "Швидка Доставка", "Гарантія Якості",
                f"Купіть {product_name} Зараз", "Натуральні Інгредієнти", "Клінічно Перевірено",
            ],
            "descriptions": [
                ua_desc_c1,
                "Перевірений і протестований продукт. Швидка доставка по Україні.",
                "Преміум добавка з оригінальною формулою. Доведені результати.",
                "Замовте сьогодні з офіційного магазину. Автентичність гарантована.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "UA")

    elif template_country == "MK":
        if has_price:
            mk_price_h = f"Само {price_int} ден"
            mk_price_alt = f"Од {price_int} ден"
            mk_price_box = f"Само {price_int} ден/Кут."
            mk_desc_a2 = f"Специјална цена {price_int} ден. Брза достава низ Македонија."
            mk_desc_b1 = f"{{KeyWord:{default_kw}}} за специјална цена {price_int} ден. Порачај!"
            mk_desc_b2 = f"Искористи ја понудата! {product_name} оригинал само {price_int} ден."
            mk_desc_b4 = f"Порачај 3 кутии и добиваш совет бесплатно. Само {price_int} ден."
            mk_desc_c1 = f"{product_name} оригинал — порачај од официјалната продавница {price_int} ден."
            mk_b_h2 = f"{product_name} — {price_int} ден"
            mk_b_h3 = f"Специјална Цена {price_int} ден"
        else:
            mk_price_h = "Специјална Онлајн Понуда"
            mk_price_alt = "Намалена Цена Сега"
            mk_price_box = "Ограничена Понуда"
            mk_desc_a2 = "Брза достава низ Македонија. Порачај од официјалната продавница."
            mk_desc_b1 = f"{{KeyWord:{default_kw}}} — порачај сега од официјалната продавница!"
            mk_desc_b2 = f"Искористи ја понудата! {product_name} оригинал во официјална."
            mk_desc_b4 = "Порачај 3 кутии и добиваш совет бесплатно. Ограничена понуда."
            mk_desc_c1 = f"{product_name} оригинал — порачај од официјалната продавница сега."
            mk_b_h2 = f"Купи {product_name}"
            mk_b_h3 = "Специјална Понуда"
        rsa_a = {
            "headlines": [
                f"{product_name} Оригинал", "Официјална Продавница", "Проверен Производ",
                "Достава во Македонија", "Оригинална Формула", mk_price_h,
                "Плаќање при Достава", "Премиум Додаток", "Вистински Резултати",
                "Порачај Онлајн", "Брза Достава", "Гарантиран Квалитет",
                f"Порачај {product_name}", "100% Природна Формула", "Бесплатен Совет",
            ],
            "descriptions": [
                f"{product_name} — оригинален додаток. Порачај од официјалната продавница.",
                mk_desc_a2,
                "Оригинална формула, докажани резултати. Плаќање при достава.",
                f"Порачај {product_name} денес. Автентичност загарантирана.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", mk_b_h2, mk_b_h3,
                "Ограничена Понуда", mk_price_alt, "Најдобра Понуда",
                "Специјален Попуст Денес", f"Купи {product_name}", "Заштеди Денес",
                "Порачај со Попуст", "Бесплатна Достава", mk_price_box,
                f"{{KeyWord:{default_kw}}} Онлајн", "Погледни Понуда", "Позитивни Рецензии",
            ],
            "descriptions": [
                mk_desc_b1, mk_desc_b2,
                "Брза достава низ целата земја. Плаќање при достава достапно.",
                mk_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Оригинал {product_name}", "pinned_field": "HEADLINE_1"},
                "Официјална Продавница", "Проверен Производ", "Достава во Македонија",
                "Оригинална Формула", mk_price_h, "Плаќање при Достава",
                "Премиум Додаток", "Вистински Резултати", "Порачај Онлајн",
                "Брза Достава", "Гарантиран Квалитет",
                f"Купи {product_name} Сега", "Природни Состојки", "Клинички Тестирано",
            ],
            "descriptions": [
                mk_desc_c1,
                "Проверен и тестиран производ. Брза достава низ Македонија.",
                "Премиум додаток со оригинална формула. Докажани резултати.",
                "Порачај денес од официјалната продавница. Автентичност загарантирана.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "MK")

    elif template_country == "RS":
        if has_price:
            rs_price_h = f"Samo {price_int} din."
            rs_price_alt = f"Od {price_int} din."
            rs_price_box = f"Samo {price_int} din./Pak."
            rs_desc_a2 = f"Posebna cena {price_int} din. Brza dostava širom Srbije."
            rs_desc_b1 = f"{{KeyWord:{default_kw}}} po posebnoj ceni {price_int} din. Poruči!"
            rs_desc_b2 = f"Iskoristi ponudu! {product_name} original za samo {price_int} din."
            rs_desc_b4 = f"Poruči 3 pakovanja i dobijaš savet gratis. Samo {price_int} din."
            rs_desc_c1 = f"{product_name} original — poruči sa zvaničnog sajta za {price_int} din."
            rs_b_h2 = f"{product_name} — {price_int} din."
            rs_b_h3 = f"Posebna Cena {price_int} din."
        else:
            rs_price_h = "Posebna Online Ponuda"
            rs_price_alt = "Snižena Cena Sada"
            rs_price_box = "Ograničena Ponuda"
            rs_desc_a2 = "Brza dostava širom Srbije. Poruči sa zvaničnog sajta."
            rs_desc_b1 = f"{{KeyWord:{default_kw}}} — poruči odmah sa zvaničnog sajta!"
            rs_desc_b2 = f"Iskoristi ponudu! {product_name} original na zvaničnom sajtu."
            rs_desc_b4 = "Poruči 3 pakovanja i dobijaš savet gratis. Ograničena ponuda."
            rs_desc_c1 = f"{product_name} original — poruči sa zvaničnog sajta odmah."
            rs_b_h2 = f"Kupi {product_name}"
            rs_b_h3 = "Posebna Ponuda"
        rsa_a = {
            "headlines": [
                f"{product_name} Original", "Zvanična Prodavnica RS", "Proveren Proizvod",
                "Dostava u Srbiji", "Originalna Formula", rs_price_h,
                "Plaćanje Pouzećem", "Premium Dodatak", "Pravi Rezultati",
                "Poruči Sada Online", "Brza Dostava", "Garantovan Kvalitet",
                f"Poruči {product_name}", "100% Prirodna Formula", "Besplatan Savet",
            ],
            "descriptions": [
                f"{product_name} — originalni dodatak. Poruči sa zvaničnog sajta.",
                rs_desc_a2,
                "Originalna formula, provereni rezultati. Plaćanje pouzećem.",
                f"Poruči {product_name} danas. Autentičnost zagarantovana.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", rs_b_h2, rs_b_h3,
                "Ograničena Ponuda", rs_price_alt, "Najbolja Ponuda",
                "Specijalni Popust Danas", f"Kupi {product_name}", "Uštedi Danas",
                "Poruči sa Popustom", "Besplatna Dostava", rs_price_box,
                f"{{KeyWord:{default_kw}}} Online", "Pogledaj Ponudu", "Pozitivne Recenzije",
            ],
            "descriptions": [
                rs_desc_b1, rs_desc_b2,
                "Brza dostava širom Srbije. Plaćanje pouzećem dostupno.",
                rs_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Zvanična Prodavnica RS", "Proveren Proizvod", "Dostava u Srbiji",
                "Originalna Formula", rs_price_h, "Plaćanje Pouzećem",
                "Premium Dodatak", "Pravi Rezultati", "Poruči Sada Online",
                "Brza Dostava", "Garantovan Kvalitet",
                f"Kupi {product_name} Sada", "Prirodni Sastojci", "Klinički Testirana Form.",
            ],
            "descriptions": [
                rs_desc_c1,
                "Proveren i testiran proizvod. Brza dostava širom Srbije.",
                "Premium dodatak sa originalnom formulom. Provereni rezultati.",
                "Poruči danas sa zvaničnog sajta. Autentičnost zagarantovana.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "RS")

    elif template_country == "AL":
        if has_price:
            al_price_h = f"Vetëm {price_int} Lekë"
            al_price_alt = f"Nga {price_int} Lekë"
            al_price_box = f"Vetëm {price_int} Lekë/Kut."
            al_desc_a2 = f"Çmim special {price_int} Lekë. Dërgesë e shpejtë në Shqipëri."
            al_desc_b1 = f"{{KeyWord:{default_kw}}} me çmim special {price_int} Lekë. Porosit!"
            al_desc_b2 = f"Përfitoni! {product_name} origjinal vetëm {price_int} Lekë."
            al_desc_b4 = f"Porosit 3 kuti dhe konsultë falas. Vetëm {price_int} Lekë."
            al_desc_c1 = f"{product_name} origjinal — porosit nga dyqani zyrtar {price_int} Lekë."
            al_b_h2 = f"{product_name} — {price_int} Lekë"
            al_b_h3 = f"Çmim Special {price_int} Lekë"
        else:
            al_price_h = "Ofertë Speciale Online"
            al_price_alt = "Çmim i Ulur Tani"
            al_price_box = "Ofertë e Kufizuar"
            al_desc_a2 = "Dërgesë e shpejtë në Shqipëri. Porosit nga dyqani zyrtar."
            al_desc_b1 = f"{{KeyWord:{default_kw}}} — porosit tani nga dyqani zyrtar!"
            al_desc_b2 = f"Përfitoni! {product_name} origjinal në dyqanin zyrtar."
            al_desc_b4 = "Porosit 3 kuti dhe konsultë falas. Ofertë e kufizuar."
            al_desc_c1 = f"{product_name} origjinal — porosit nga dyqani zyrtar tani."
            al_b_h2 = f"Bli {product_name}"
            al_b_h3 = "Ofertë Speciale"
        rsa_a = {
            "headlines": [
                f"{product_name} Origjinal", "Dyqani Zyrtar AL", "Produkt i Verifikuar",
                "Dërgesë në Shqipëri", "Formulë Origjinale", al_price_h,
                "Pagesë në Dorëzim", "Shtesë Premium", "Rezultate Reale",
                "Porosit Tani Online", "Dërgesë e Shpejtë", "Cilësi e Garantuar",
                f"Porosit {product_name}", "100% Formulë Natyrale", "Konsultë Falas",
            ],
            "descriptions": [
                f"{product_name} — shtesë origjinale. Porosit nga dyqani zyrtar.",
                al_desc_a2,
                "Formulë origjinale, rezultate të provuara. Pagesë në dorëzim.",
                f"Porosit {product_name} sot. Autenticitet i garantuar.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}", al_b_h2, al_b_h3,
                "Ofertë e Kufizuar", al_price_alt, "Oferta Më e Mirë",
                "Zbritje Speciale Sot", f"Bli {product_name}", "Kurse 50%",
                "Porosit me Zbritje", "Dërgesë Falas", al_price_box,
                f"{{KeyWord:{default_kw}}} Online", "Shiko Ofertën", "Vlerësime Pozitive",
            ],
            "descriptions": [
                al_desc_b1, al_desc_b2,
                "Dërgesë e shpejtë në të gjithë Shqipërinë. Pagesë në dorëzim.",
                al_desc_b4,
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Origjinal {product_name}", "pinned_field": "HEADLINE_1"},
                "Dyqani Zyrtar AL", "Produkt i Verifikuar", "Dërgesë në Shqipëri",
                "Formulë Origjinale", al_price_h, "Pagesë në Dorëzim",
                "Shtesë Premium", "Rezultate Reale", "Porosit Tani Online",
                "Dërgesë e Shpejtë", "Cilësi e Garantuar",
                f"Bli {product_name} Tani", "Përbërës Natyralë", "E Testuar Klinikisht",
            ],
            "descriptions": [
                al_desc_c1,
                "Produkt i verifikuar dhe i testuar. Dërgesë e shpejtë në Shqipëri.",
                "Shtesë premium me formulë origjinale. Rezultate të provuara.",
                "Porosit sot nga dyqani zyrtar. Autenticitet i garantuar.",
            ],
        }
        snippet_values = _guess_snippet_values_generic(product_name, "AL")

    else:
        # Fallback: English-style templates for unknown countries, but keep A/B/C distinct.
        if has_price:
            en_price_h = f"Only {price_int} {currency}"
            en_price_alt = f"From {price_int} {currency}"
            en_price_box = f"Only {price_int}/{currency}"
            en_desc_a2 = f"Special price {price_int} {currency}. Fast delivery nationwide."
            en_desc_b1 = f"{product_name} special price {price_int} {currency}. Order now online."
            en_desc_b2 = f"Limited-time offer on {product_name}. Order today for {price_int} {currency}."
            en_desc_c1 = f"{product_name} original formula with premium ingredients. Order from the official store."
        else:
            en_price_h = "Special Offer Online"
            en_price_alt = "Reduced Price Now"
            en_price_box = "Limited Offer"
            en_desc_a2 = "Fast delivery nationwide. Order from the official store."
            en_desc_b1 = f"{product_name} limited-time offer. Order now from the official store."
            en_desc_b2 = f"Buy {product_name} online with a special offer while stock lasts."
            en_desc_c1 = f"{product_name} original formula with premium ingredients. Order online today."
        rsa_a = {
            "headlines": [
                f"{product_name} Original",
                "Official Store",
                "Verified Product",
                "Fast Delivery",
                "Original Formula",
                en_price_h,
                "Cash on Delivery",
                "Premium Product",
                "Real Results",
                "Order Now Online",
                "Quick Shipping",
                "Quality Guaranteed",
                f"Buy {product_name} Now",           # 13
                "100% Authentic",                      # 14
                "Free Support",                        # 15
            ],
            "descriptions": [
                f"{product_name} \u2014 original product. Order from the official store now.",
                en_desc_a2,
                "Original formula, proven results. Cash on delivery available.",
                f"Order {product_name} today. Authenticity guarantee and fast delivery.",
            ],
        }
        rsa_b = {
            "headlines": [
                f"{{KeyWord:{default_kw}}}",
                f"{product_name} Special Offer",
                en_price_alt,
                "Limited Time Offer",
                "Order Before It Ends",
                "Best Offer Today",
                "Buy Online Now",
                f"{product_name} Sale",
                "Save 50% Today",
                "Fast Checkout Online",
                "Free Delivery",
                en_price_box,
                f"{{KeyWord:{default_kw}}} Online",
                "Check Offer Now",
                "Popular Choice",
            ],
            "descriptions": [
                en_desc_b1,
                en_desc_b2,
                "Fast delivery nationwide. Cash on delivery available.",
                "Order 3 boxes and receive a free consultation while the offer lasts.",
            ],
        }
        rsa_c = {
            "headlines": [
                {"text": f"Original {product_name}", "pinned_field": "HEADLINE_1"},
                "Official Store",
                "Verified Product",
                "Original Formula",
                "Verified Quality",
                en_price_h,
                "Premium Product",
                "Best Seller",
                "Quality Guaranteed",
                "Order Online",
                "Trusted Product",
                "Fast Delivery",
                f"{product_name} Formula",
                "Clinically Tested Blend",
                "Real Customer Reviews",
            ],
            "descriptions": [
                en_desc_c1,
                "Verified product with a premium formula. Fast delivery nationwide.",
                "Natural ingredient support with trusted quality and easy ordering.",
                "Order today from the official store with authenticity guaranteed.",
            ],
        }
        snippet_values = ["Products", "Quality", "Premium Delivery"]

    for rsa_data in (rsa_a, rsa_b, rsa_c):
        pinned_h1_exists = any(
            isinstance(h, dict) and h.get("pinned_field") == "HEADLINE_1"
            for h in rsa_data["headlines"]
        )
        if pinned_h1_exists:
            continue
        for idx, headline in enumerate(rsa_data["headlines"]):
            text = headline.get("text", "") if isinstance(headline, dict) else str(headline)
            if text.startswith("{KeyWord:"):
                rsa_data["headlines"][idx] = {"text": text, "pinned_field": "HEADLINE_1"}
                break

    # Validate headline length (max 30 chars)
    # For {KeyWord:X} templates, Google counts the FALLBACK text length, not the full template string.
    for rsa_name, rsa_data in [("A", rsa_a), ("B", rsa_b), ("C", rsa_c)]:
        validated_headlines = []
        for h in rsa_data["headlines"]:
            text = h["text"] if isinstance(h, dict) else h
            # Check effective length: for {KeyWord:X} use fallback text length
            kw_match = re.match(r"^\{KeyWord:([^}]+)\}(.*)$", text)
            if kw_match:
                fallback = kw_match.group(1)
                suffix = kw_match.group(2)
                effective_len = len(fallback) + len(suffix)
                if effective_len > 30:
                    # Trim suffix to fit, keeping fallback intact
                    max_suffix = 30 - len(fallback)
                    suffix = suffix[:max_suffix].rstrip()
                    text = f"{{KeyWord:{fallback}}}{suffix}" if suffix else f"{{KeyWord:{fallback}}}"
            else:
                if len(text) > 30:
                    text = text[:30]
            if isinstance(h, dict):
                validated_headlines.append({"text": text, "pinned_field": h.get("pinned_field")})
            else:
                validated_headlines.append(text)
        rsa_data["headlines"] = validated_headlines

        # Validate description length (max 90 chars)
        validated_descs = []
        for d in rsa_data["descriptions"]:
            text = d["text"] if isinstance(d, dict) else d
            text = re.sub(r"\{KeyWord:[^}]+\}", product_name or default_kw, text).strip()
            if len(text) > 90:
                text = text[:87] + "..."
            validated_descs.append(text)
        rsa_data["descriptions"] = validated_descs

    return {
        "rsa_a": rsa_a,
        "rsa_b": rsa_b,
        "rsa_c": rsa_c,
        "snippet_values": snippet_values,
    }


def _guess_snippet_values_ro(product_name: str) -> List[str]:
    """Guess structured snippet values based on product name (Romanian)."""
    name_lower = product_name.lower()
    if any(w in name_lower for w in ["gel", "loțiune", "cremă"]):
        return ["Gel", "Produs Topical", "Aplicare Locală"]
    elif any(w in name_lower for w in ["capsulă", "pastilă", "tabletă"]):
        return ["Produs", "Calitate Premium", "Livrare Rapidă"]
    elif any(w in name_lower for w in ["plic", "pudră", "amestec"]):
        return ["Plicuri", "Produs Instant", "Ușor de Folosit"]
    elif any(w in name_lower for w in ["barmane", "băutură", "suc"]):
        return ["Băutură", "Produs Lichid", "Gust Plăcut"]
    elif any(w in name_lower for w in ["ulei", "ser", "loțiune"]):
        return ["Ulei", "Produs Premium", "Prelucrare Naturală"]
    elif any(w in name_lower for w in ["cremă", "balsam", "unguent"]):
        return ["Cremă", "Produs Premium", "Prelucrare Naturală"]
    elif any(w in name_lower for w in ["pudră", "pulbere", "extract"]):
        return ["Pudră", "Produs Pur", "Prelucrare Naturală"]
    elif any(w in name_lower for w in ["eye", "ochi", "vision", "opti"]):
        return ["Produs", "Calitate Premium", "Livrare Rapidă"]
    elif any(w in name_lower for w in ["heart", "cardio", "inim"]):
        return ["Produs", "Calitate Premium", "Livrare Rapidă"]
    else:
        return ["Produs", "Calitate Premium", "Livrare Rapidă"]


def _guess_snippet_values_tr(product_name: str) -> List[str]:
    """Guess structured snippet values based on product name (Turkish)."""
    name_lower = product_name.lower()
    if any(w in name_lower for w in ["jel", "krem", "losyon"]):
        return ["Jel", "Topikal Ürün", "Lokal Uygulama"]
    elif any(w in name_lower for w in ["kapsül", "hap", "tablet"]):
        return ["Kapsül", "Oral Ürün", "Doğal Formül"]
    elif any(w in name_lower for w in ["poşet", "toz", "karışım"]):
        return ["Poşet", "Instant Ürün", "Kolay Kullanım"]
    else:
        return ["Kapsül", "Premium Ürün", "Doğal Formül"]


def _guess_snippet_values_generic(product_name: str, country_code: str) -> List[str]:
    """Guess structured snippet values based on product name, localized per country."""
    _SNIPPET_MAP = {
        "PL": {"product": "Produkty", "quality": "Jakość Premium", "delivery": "Szybka Dostawa",
               "gel": "Żel", "joint": "Produkt Premium", "topical": "Aplikacja Miejscowa"},
        "HU": {"product": "Termékek", "quality": "Minőségi Premium", "delivery": "Gyors Szállítás",
               "gel": "Gél", "joint": "Premium Termék", "topical": "Helyi Alkalmazás"},
        "FR": {"product": "Produits", "quality": "Qualité Premium", "delivery": "Livraison Rapide",
               "gel": "Gel", "joint": "Produit Premium", "topical": "Application Topique"},
        "DE": {"product": "Produkte", "quality": "Premium Qualität", "delivery": "Schnelle Lieferung",
               "gel": "Gel", "joint": "Premium-Produkt", "topical": "Topische Anwendung"},
        "IT": {"product": "Prodotti", "quality": "Qualità Premium", "delivery": "Consegna Rapida",
               "gel": "Gel", "joint": "Prodotto Premium", "topical": "Applicazione Topica"},
        "ES": {"product": "Productos", "quality": "Calidad Premium", "delivery": "Envío Rápido",
               "gel": "Gel", "joint": "Producto Premium", "topical": "Aplicación Tópica"},
        "PT": {"product": "Produtos", "quality": "Qualidade Premium", "delivery": "Envio Rápido",
               "gel": "Gel", "joint": "Produto Premium", "topical": "Aplicação Tópica"},
        "ME": {"product": "Proizvodi", "quality": "Premium Kvalitet", "delivery": "Brza Dostava",
               "gel": "Gel", "joint": "Premium Proizvod", "topical": "Lokalna Primena"},
        "CZ": {"product": "Produkty", "quality": "Premium Kvalita", "delivery": "Rychlá Doprava",
               "gel": "Gel", "joint": "Premium Produkt", "topical": "Lokální Aplikace"},
        "SK": {"product": "Produkty", "quality": "Premium Kvalita", "delivery": "Rýchla Doprava",
               "gel": "Gél", "joint": "Premium Produkt", "topical": "Lokálna Aplikácia"},
        "BG": {"product": "Продукти", "quality": "Премиум Качество", "delivery": "Бърза Доставка",
               "gel": "Гел", "joint": "Премиум Продукт", "topical": "Локално Приложение"},
        "GR": {"product": "Προϊόντα", "quality": "Premium Ποιότητα", "delivery": "Γρήγορη Αποστολή",
               "gel": "Τζελ", "joint": "Premium Προϊόν", "topical": "Τοπική Εφαρμογή"},
        "HR": {"product": "Proizvodi", "quality": "Premium Kvaliteta", "delivery": "Brza Dostava",
               "gel": "Gel", "joint": "Premium Proizvod", "topical": "Lokalna Primjena"},
        "SI": {"product": "Proizvodi", "quality": "Premium Kvaliteta", "delivery": "Hitra Dostava",
               "gel": "Gel", "joint": "Premium Proizvod", "topical": "Lokalna Uporaba"},
        "LT": {"product": "Produktai", "quality": "Premium Kokybė", "delivery": "Greita Pristatymas",
               "gel": "Gelis", "joint": "Premium Produktas", "topical": "Vietinis Naudojimas"},
        "EE": {"product": "Tooted", "quality": "Premium Kvaliteet", "delivery": "Kiire Tarne",
               "gel": "Geel", "joint": "Premium Toode", "topical": "Paikne Kasutamine"},
        "LV": {"product": "Produkti", "quality": "Premium Kvalitāte", "delivery": "Ātra Piegāde",
               "gel": "Gels", "joint": "Premium Produkts", "topical": "Lokāla Lietošana"},
        "UA": {"product": "Продукти", "quality": "Premium Якість", "delivery": "Швидка Доставка",
               "gel": "Гель", "joint": "Premium Продукт", "topical": "Зовнішнє Застосув."},
        "MK": {"product": "Производи", "quality": "Premium Квалитет", "delivery": "Брза Достава",
               "gel": "Гел", "joint": "Premium Производ", "topical": "Локална Примена"},
        "RS": {"product": "Proizvodi", "quality": "Premium Kvalitet", "delivery": "Brza Dostava",
               "gel": "Gel", "joint": "Dodatak za Zglobove", "topical": "Lokalna Primena"},
        "AL": {"product": "Produktet", "quality": "Premium Cilësi", "delivery": "Dorëzim i Shpejtë",
               "gel": "Xhel", "joint": "Shtesë për Nyje", "topical": "Përdorim Lokal"},
    }
    cc = country_code.upper()
    terms = _SNIPPET_MAP.get(cc, {"product": "Products", "quality": "Quality", "delivery": "Delivery",
                                   "gel": "Gel", "joint": "Premium Product", "topical": "Topical"})
    name_lower = product_name.lower()
    if any(w in name_lower for w in ["gel", "lotion", "cream", "oil"]):
        return [terms["gel"], terms["joint"], terms["topical"]]
    else:
        return [terms["product"], terms["quality"], terms["delivery"]]


# ---------------------------------------------------------------------------
# Core batch processing logic
# ---------------------------------------------------------------------------

def _get_ad_group_url_map(campaign_id: str, force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Build a mapping from product handle -> ad group by parsing RSA final_urls.
    Uses session-level cache (30-min TTL) to avoid repeated GAQL queries.

    Queries all ads in a campaign, extracts product handle from URL path
    (e.g. https://yourstore.com/products/product-name?variant=... -> 'product-name'),
    and returns dict keyed by handle: {handle: {id, name, status, url}}.

    This is the most reliable matching method because it uses the actual product URL
    instead of guessing from ad group names.
    """
    cache_key = f"url_map_{campaign_id}"
    if not force_refresh and cache_key in _session_cache:
        data, ts = _session_cache[cache_key]
        if time.time() - ts < SESSION_CACHE_TTL:
            return data

    from google_ads_mcp import _execute_gaql, _safe_get_value

    query = f"""
        SELECT campaign.id, ad_group.id, ad_group.name, ad_group.status, ad_group_ad.ad.final_urls
        FROM ad_group_ad
        WHERE campaign.id = {campaign_id}
          AND ad_group_ad.status != 'REMOVED'
          AND ad_group.status != 'REMOVED'
    """
    results = _execute_gaql(CUSTOMER_ID, query, page_size=1000)

    url_map: Dict[str, Dict[str, Any]] = {}
    for row in results:
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        ag_name = str(_safe_get_value(row, "ad_group.name", ""))
        status = str(_safe_get_value(row, "ad_group.status", ""))
        final_urls = str(_safe_get_value(row, "ad_group_ad.ad.final_urls", ""))

        # Extract handle from URL: .../products/{handle}?...
        handle_match = re.search(r'/products/([a-z0-9][a-z0-9\-]+?)(?:\?|$)', final_urls)
        if handle_match:
            handle = handle_match.group(1)
            # Prefer ENABLED > PAUSED > other; never pick REMOVED (filtered above)
            existing = url_map.get(handle)
            if existing is None:
                url_map[handle] = {
                    "id": ag_id,
                    "name": ag_name,
                    "status": status,
                    "url": final_urls,
                }
            elif "ENABLED" in status and "ENABLED" not in existing.get("status", ""):
                # Upgrade: replace PAUSED match with ENABLED one
                url_map[handle] = {
                    "id": ag_id,
                    "name": ag_name,
                    "status": status,
                    "url": final_urls,
                }

    _session_cache[cache_key] = (url_map, time.time())
    return url_map


def _get_existing_ad_groups(campaign_id: str, force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Get all ad groups in campaign, returns dict keyed by lowercase name.
    Uses session-level cache (30-min TTL) to avoid repeated GAQL queries.
    """
    cache_key = f"ad_groups_{campaign_id}"
    if not force_refresh and cache_key in _session_cache:
        data, ts = _session_cache[cache_key]
        if time.time() - ts < SESSION_CACHE_TTL:
            return data

    from google_ads_mcp import _execute_gaql, _format_customer_id, _safe_get_value

    query = f"""
        SELECT campaign.id, ad_group.id, ad_group.name, ad_group.status
        FROM ad_group
        WHERE campaign.id = {campaign_id}
          AND ad_group.status != 'REMOVED'
    """
    results = _execute_gaql(CUSTOMER_ID, query, page_size=1000)

    ad_groups = {}
    for row in results:
        name = str(_safe_get_value(row, "ad_group.name", ""))
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        raw_status = _safe_get_value(row, "ad_group.status", "")
        status = raw_status.name if hasattr(raw_status, 'name') else str(raw_status)
        key = name.lower()
        existing = ad_groups.get(key)
        if existing is None:
            ad_groups[key] = {"id": ag_id, "name": name, "status": status}
        elif "ENABLED" in status and "ENABLED" not in existing.get("status", ""):
            # Prefer ENABLED over PAUSED when duplicate names exist
            ad_groups[key] = {"id": ag_id, "name": name, "status": status}

    # Debug: status breakdown
    status_counts = {}
    for ag in ad_groups.values():
        s = ag.get("status", "UNKNOWN")
        status_counts[s] = status_counts.get(s, 0) + 1
    _debug("ad_groups", f"campaign {campaign_id}: {len(ad_groups)} ad groups, statuses={status_counts}")

    _session_cache[cache_key] = (ad_groups, time.time())
    return ad_groups


def _get_ad_group_assets(campaign_id: str, force_refresh: bool = False) -> Dict[str, Dict[str, int]]:
    """
    Get asset counts per ad group. Uses session-level cache (30-min TTL).
    Returns dict: { ad_group_id: { "RSA": count, "CALLOUT": count, ... } }
    """
    cache_key = f"assets_{campaign_id}"
    if not force_refresh and cache_key in _session_cache:
        data, ts = _session_cache[cache_key]
        if time.time() - ts < SESSION_CACHE_TTL:
            return data

    from google_ads_mcp import _execute_gaql, _safe_get_value

    # Count RSAs per ad group (only ENABLED — PAUSED/REMOVED don't count toward limit)
    rsa_query = f"""
        SELECT campaign.id, ad_group.id, ad_group_ad.ad.id
        FROM ad_group_ad
        WHERE campaign.id = {campaign_id}
          AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
          AND ad_group_ad.status = 'ENABLED'
    """
    rsa_results = _execute_gaql(CUSTOMER_ID, rsa_query, page_size=1000)

    assets: Dict[str, Dict[str, int]] = {}
    for row in rsa_results:
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        if ag_id not in assets:
            assets[ag_id] = {"RSA": 0, "CALLOUT": 0, "SITELINK": 0, "PROMOTION": 0, "STRUCTURED_SNIPPET": 0, "AD_IMAGE": 0}
        assets[ag_id]["RSA"] += 1

    # Count ad group-level asset links
    # Map AssetFieldType enum int values → names (google-ads-python returns ints)
    _FIELD_TYPE_MAP = {
        2: "SITELINK", 3: "CALL", 4: "CALLOUT", 7: "CALLOUT",
        8: "STRUCTURED_SNIPPET", 12: "PROMOTION", 15: "AD_IMAGE",
        # Also handle string-based values (newer google-ads-python versions)
    }
    asset_query = f"""
        SELECT campaign.id, ad_group.id, ad_group_asset.field_type
        FROM ad_group_asset
        WHERE campaign.id = {campaign_id}
          AND ad_group_asset.status = 'ENABLED'
    """
    try:
        asset_results = _execute_gaql(CUSTOMER_ID, asset_query, page_size=1000)
        _debug_ft_samples = []
        for row in asset_results:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            raw_ft = _safe_get_value(row, "ad_group_asset.field_type", "")

            # Resolve enum: try .name (proto-plus object), int map, or str fallback
            if hasattr(raw_ft, 'name'):
                field_type = raw_ft.name
            elif isinstance(raw_ft, int) and raw_ft in _FIELD_TYPE_MAP:
                field_type = _FIELD_TYPE_MAP[raw_ft]
            else:
                field_type = str(raw_ft)

            if len(_debug_ft_samples) < 5:
                _debug_ft_samples.append(f"{type(raw_ft).__name__}:{raw_ft!r}→{field_type}")

            if ag_id not in assets:
                assets[ag_id] = {"RSA": 0, "CALLOUT": 0, "SITELINK": 0, "PROMOTION": 0, "STRUCTURED_SNIPPET": 0, "AD_IMAGE": 0}
            if "CALLOUT" in field_type:
                assets[ag_id]["CALLOUT"] += 1
            elif "SITELINK" in field_type:
                assets[ag_id]["SITELINK"] += 1
            elif "PROMOTION" in field_type:
                assets[ag_id]["PROMOTION"] += 1
            elif "STRUCTURED_SNIPPET" in field_type:
                assets[ag_id]["STRUCTURED_SNIPPET"] += 1
            elif "AD_IMAGE" in field_type:
                assets[ag_id]["AD_IMAGE"] += 1

        # Store debug info for diagnosing enum format issues
        if _debug_ft_samples:
            _session_cache[f"_debug_ft_{campaign_id}"] = (_debug_ft_samples, time.time())

        # Debug summary
        total_assets = sum(sum(v.values()) for v in assets.values())
        type_totals = {}
        for ag_assets in assets.values():
            for k, v in ag_assets.items():
                type_totals[k] = type_totals.get(k, 0) + v
        _debug("assets", f"campaign {campaign_id}: {len(asset_results)} rows → {len(assets)} ad groups, totals={type_totals}")
    except Exception as e:
        _debug("assets", f"Asset query FAILED for campaign {campaign_id}: {e}", "error")
        # Store error for diagnosis
        _session_cache[f"_debug_ft_{campaign_id}"] = ([f"ERROR: {e}"], time.time())

    # Debug RSA summary
    rsa_totals = sum(1 for a in assets.values() if a.get("RSA", 0) > 0)
    _debug("assets", f"campaign {campaign_id}: RSA query found {rsa_totals} ad groups with RSAs")

    _session_cache[cache_key] = (assets, time.time())
    return assets


def _create_ad_group(campaign_id: str, name: str, cpc_bid_micros: int = 1000000) -> str:
    """Create a Search ad group and return its ID."""
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    ad_group_service = google_ads_client.get_service("AdGroupService")

    operation = google_ads_client.get_type("AdGroupOperation")
    ad_group = operation.create
    ad_group.name = name
    ad_group.campaign = ad_group_service.campaign_path(customer_id, campaign_id)
    ad_group.type_ = google_ads_client.enums.AdGroupTypeEnum.SEARCH_STANDARD
    ad_group.cpc_bid_micros = cpc_bid_micros

    result = ad_group_service.mutate_ad_groups(customer_id=customer_id, operations=[operation])
    return result.results[0].resource_name.split("/")[-1]


def _enable_ad_group(ad_group_id: str) -> None:
    """Enable a paused ad group."""
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    ad_group_service = google_ads_client.get_service("AdGroupService")

    operation = google_ads_client.get_type("AdGroupOperation")
    ad_group = operation.update
    ad_group.resource_name = ad_group_service.ad_group_path(customer_id, ad_group_id)
    ad_group.status = google_ads_client.enums.AdGroupStatusEnum.ENABLED

    from google.protobuf import field_mask_pb2
    operation.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=["status"]))

    ad_group_service.mutate_ad_groups(customer_id=customer_id, operations=[operation])


def _remove_ad_group(ad_group_id: str) -> None:
    """Permanently REMOVE an ad group (status=REMOVED). Cannot be undone."""
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    ad_group_service = google_ads_client.get_service("AdGroupService")

    operation = google_ads_client.get_type("AdGroupOperation")
    resource_name = ad_group_service.ad_group_path(customer_id, ad_group_id)
    operation.remove = resource_name

    ad_group_service.mutate_ad_groups(customer_id=customer_id, operations=[operation])


def _create_rsa(ad_group_id: str, headlines: list, descriptions: list, final_urls: list) -> str:
    """Create a Responsive Search Ad with automatic policy exemption retry.

    Flow:
    1. Try creating the RSA normally
    2. If POLICY_FINDING error (e.g. DESTINATION_NOT_WORKING), extract policy topics
    3. Retry with policy_validation_parameter.ignorable_policy_topics to request exemption
    4. If exemption also fails, raise the original error

    Returns ad resource name.
    """
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client
    from google.ads.googleads.errors import GoogleAdsException

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    service = google_ads_client.get_service("AdGroupAdService")

    operation = google_ads_client.get_type("AdGroupAdOperation")
    ad_group_ad = operation.create
    ad_group_ad.ad_group = service.ad_group_path(customer_id, ad_group_id)
    ad = ad_group_ad.ad

    for h in headlines:
        asset = google_ads_client.get_type("AdTextAsset")
        if isinstance(h, dict):
            asset.text = h["text"]
            if h.get("pinned_field"):
                asset.pinned_field = getattr(
                    google_ads_client.enums.ServedAssetFieldTypeEnum, h["pinned_field"]
                )
        else:
            asset.text = h
        ad.responsive_search_ad.headlines.append(asset)

    for d in descriptions:
        asset = google_ads_client.get_type("AdTextAsset")
        if isinstance(d, dict):
            asset.text = d["text"]
        else:
            asset.text = d
        ad.responsive_search_ad.descriptions.append(asset)

    ad.final_urls.extend(final_urls)

    try:
        result = service.mutate_ad_group_ads(customer_id=customer_id, operations=[operation])
        return result.results[0].resource_name
    except GoogleAdsException as ex:
        ignorable_topics = []
        has_non_policy_error = False
        for error in ex.failure.errors:
            if error.details and error.details.policy_finding_details:
                for entry in error.details.policy_finding_details.policy_topic_entries:
                    ignorable_topics.append(entry.topic)
            else:
                has_non_policy_error = True
        if has_non_policy_error or not ignorable_topics:
            raise

        import logging
        logging.getLogger("batch_optimizer").info(
            f"[PolicyExempt] RSA for ag={ad_group_id}: retrying with ignorable_policy_topics={ignorable_topics}"
        )

        operation.policy_validation_parameter.ignorable_policy_topics.extend(ignorable_topics)
        result = service.mutate_ad_group_ads(customer_id=customer_id, operations=[operation])
        return result.results[0].resource_name


def _create_callouts(ad_group_id: str, texts: List[str]) -> None:
    """Create callout assets and link to ad group. Uses single batch API call."""
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client, _link_assets_to_entity

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    asset_service = google_ads_client.get_service("AssetService")

    # Batch all callout creations into a single API call
    operations = []
    for text in texts:
        op = google_ads_client.get_type("AssetOperation")
        op.create.callout_asset.callout_text = text
        operations.append(op)

    result = _retry_with_backoff(asset_service.mutate_assets, customer_id=customer_id, operations=operations)
    resource_names = [r.resource_name for r in result.results]

    _link_assets_to_entity(
        customer_id, resource_names,
        google_ads_client.enums.AssetFieldTypeEnum.CALLOUT,
        ad_group_id=ad_group_id,
    )


def _create_sitelinks(ad_group_id: str, sitelinks: List[dict]) -> None:
    """Create sitelink assets and link to ad group. Uses single batch API call."""
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client, _link_assets_to_entity

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    asset_service = google_ads_client.get_service("AssetService")

    # Batch all sitelink creations into a single API call
    operations = []
    for sl in sitelinks:
        op = google_ads_client.get_type("AssetOperation")
        asset = op.create
        asset.sitelink_asset.link_text = sl["link_text"]
        if sl.get("description1"):
            asset.sitelink_asset.description1 = sl["description1"]
        if sl.get("description2"):
            asset.sitelink_asset.description2 = sl["description2"]
        for url in sl.get("final_urls", []):
            asset.final_urls.append(url)
        operations.append(op)

    result = _retry_with_backoff(asset_service.mutate_assets, customer_id=customer_id, operations=operations)
    resource_names = [r.resource_name for r in result.results]

    _link_assets_to_entity(
        customer_id, resource_names,
        google_ads_client.enums.AssetFieldTypeEnum.SITELINK,
        ad_group_id=ad_group_id,
    )


def _create_promotion(ad_group_id: str, product_name: str, percent_off: int, language_code: str, final_url: str) -> None:
    """Create a promotion asset and link to ad group."""
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client, _link_assets_to_entity

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    asset_service = google_ads_client.get_service("AssetService")

    op = google_ads_client.get_type("AssetOperation")
    asset = op.create
    asset.promotion_asset.promotion_target = product_name[:20]  # max 20 chars
    asset.promotion_asset.language_code = language_code
    asset.promotion_asset.percent_off = percent_off
    asset.final_urls.append(final_url)

    result = asset_service.mutate_assets(customer_id=customer_id, operations=[op])
    resource_name = result.results[0].resource_name

    _link_assets_to_entity(
        customer_id, [resource_name],
        google_ads_client.enums.AssetFieldTypeEnum.PROMOTION,
        ad_group_id=ad_group_id,
    )


def _create_structured_snippets(ad_group_id: str, header: str, values: List[str]) -> None:
    """Create structured snippet asset and link to ad group."""
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client, _link_assets_to_entity

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    asset_service = google_ads_client.get_service("AssetService")

    op = google_ads_client.get_type("AssetOperation")
    asset = op.create
    asset.structured_snippet_asset.header = header
    for v in values:
        asset.structured_snippet_asset.values.append(v)

    result = asset_service.mutate_assets(customer_id=customer_id, operations=[op])
    resource_name = result.results[0].resource_name

    _link_assets_to_entity(
        customer_id, [resource_name],
        google_ads_client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET,
        ad_group_id=ad_group_id,
    )


def _add_keyword(ad_group_id: str, keyword_text: str, match_type: str = "PHRASE") -> Optional[str]:
    """Try to add a keyword. Returns error message or None on success.
    CRITICAL: BROAD match is BLOCKED. Only PHRASE or EXACT allowed.
    Broad match wastes budget on irrelevant clicks.
    """
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client

    # SAFETY: Force BROAD → PHRASE to prevent budget waste
    if match_type == "BROAD":
        match_type = "PHRASE"

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    service = google_ads_client.get_service("AdGroupCriterionService")

    op = google_ads_client.get_type("AdGroupCriterionOperation")
    criterion = op.create
    criterion.ad_group = service.ad_group_path(customer_id, ad_group_id)
    criterion.keyword.text = keyword_text

    match_map = {
        "PHRASE": google_ads_client.enums.KeywordMatchTypeEnum.PHRASE,
        "EXACT": google_ads_client.enums.KeywordMatchTypeEnum.EXACT,
    }
    criterion.keyword.match_type = match_map.get(match_type, match_map["PHRASE"])

    try:
        service.mutate_ad_group_criteria(customer_id=customer_id, operations=[op])
        return None
    except Exception as e:
        return str(e)


def _add_negative_kw_internal(campaign_id: str, keyword_text: str, match_type: str = "PHRASE") -> Optional[str]:
    """Add a negative keyword to a campaign. Returns error message or None on success."""
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client, _track_api_call

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)
    service = google_ads_client.get_service("CampaignCriterionService")

    op = google_ads_client.get_type("CampaignCriterionOperation")
    criterion = op.create
    criterion.campaign = service.campaign_path(customer_id, campaign_id)
    criterion.keyword.text = keyword_text

    match_map = {
        "BROAD": google_ads_client.enums.KeywordMatchTypeEnum.BROAD,
        "PHRASE": google_ads_client.enums.KeywordMatchTypeEnum.PHRASE,
        "EXACT": google_ads_client.enums.KeywordMatchTypeEnum.EXACT,
    }
    criterion.keyword.match_type = match_map.get(match_type, match_map["PHRASE"])
    criterion.negative = True

    try:
        service.mutate_campaign_criteria(customer_id=customer_id, operations=[op])
        _track_api_call("MUTATE_CRITERION")
        return None
    except Exception as e:
        return str(e)


# ---------------------------------------------------------------------------
# Image handling functions
# ---------------------------------------------------------------------------

def _fetch_product_images(handle: str, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    Fetch product image candidates from local find_products_images.php endpoint.
    Returns list of image dicts sorted by quality_score (best first), deduplicated by MD5.
    PHP endpoint handles TRIM+PAD to correct aspect ratio (1:1 or 1.91:1).
    Falls back to xml_feed_images if no local images found.
    Includes auto-retry with exponential backoff for transient HTTP errors.
    """
    # --- MCP-side SQLite cache: check before HTTP call ---
    # TTL: 24h — images change rarely, avoids repeated HTTP roundtrips.
    db = _get_db()
    cached_candidates = db.get_image_candidates_cache(handle)
    if cached_candidates is not None:
        _debug(DebugCat.IMAGES, f"MCP image cache HIT for {handle} ({len(cached_candidates)} candidates)", "info")
        return cached_candidates

    _debug(DebugCat.IMAGES, f"MCP image cache MISS for {handle}, calling endpoint...", "info")

    params = urllib.parse.urlencode({
        "product_handle": handle,
        "min_width": 300,
        "min_height": 300,
        "aspect_filter": "landscape,square",
        "sort": "quality_score",
        "deduplicate": "true",
        "crop": "auto",
        "limit": 10,
        "format": "json",
    })
    url = f"{IMAGE_FINDER_BASE_URL}?{params}"

    data = None
    last_err = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "BatchOptimizer/1.0"})
            with urllib.request.urlopen(req, timeout=IMAGE_FINDER_TIMEOUT, context=_img_ssl_ctx) as resp:
                data = json.loads(resp.read())
            break  # success
        except Exception as e:
            last_err = e
            err_str = str(e)
            # Retry on transient errors (timeouts, 5xx, connection errors)
            is_transient = any(x in err_str for x in ["timed out", "503", "502", "500", "Connection", "URLError"])
            if is_transient and attempt < max_retries - 1:
                delay = 2 ** attempt  # 1s, 2s, 4s
                _debug(DebugCat.IMAGES, f"Image fetch retry {attempt+1}/{max_retries} for {handle} after {delay}s: {err_str[:100]}", "warning")
                time.sleep(delay)
            else:
                _debug(DebugCat.IMAGES, f"_fetch_product_images failed for {handle}: {err_str[:200]}", "warning")
                return []

    if data is None:
        _debug(DebugCat.IMAGES, f"_fetch_product_images exhausted retries for {handle}: {last_err}", "warning")
        return []

    # Log debug fields from PHP endpoint (handle normalization, lookup candidates)
    search_handle = data.get("search_handle")
    lookup_candidates = data.get("handle_lookup_candidates")
    if search_handle and search_handle != handle:
        _debug(DebugCat.IMAGES, f"PHP normalized handle: '{handle}' -> '{search_handle}'", "info")
    if lookup_candidates:
        _debug(DebugCat.IMAGES, f"PHP lookup candidates for '{handle}': {lookup_candidates}", "info")

    # Primary: images with recommended_for_google_ads=true
    # PHP endpoint now promotes XML feed fallbacks into images[] after processing,
    # so this list includes BOTH local packshots AND processed XML feed images.
    candidates = [
        img for img in data.get("images", [])
        if img.get("recommended_for_google_ads") is True
    ]

    # xml_feed_images is now a LAST-RESORT diagnostic backup only.
    # PHP promotes processed XML images to images[] with recommended=true.
    # Only use raw xml_feed_images if images[] gave 0 candidates.
    if not candidates:
        xml_imgs = data.get("xml_feed_images", [])
        seen_urls: set = set()
        for xi in xml_imgs:
            xi_url = xi.get("url", "")
            if xi_url and xi_url not in seen_urls:
                candidates.append({
                    "url": xi_url,
                    "local_path": None,
                    "md5_hash": None,
                    "quality_score": 0,
                    "source": "xml_feed_raw",
                    "filename": xi_url.split("/")[-1].split("?")[0],
                    "aspect_ratio": xi.get("aspect_ratio"),
                    "processed": False,
                })
                seen_urls.add(xi_url)
        if xml_imgs:
            _debug(DebugCat.IMAGES, f"Using raw xml_feed_images fallback for '{handle}' ({len(candidates)} candidates)", "warning")

    # --- Store in MCP-side SQLite cache (TTL 24h) ---
    if candidates:
        try:
            db.set_image_candidates_cache(handle, candidates)
            _debug(DebugCat.IMAGES, f"MCP image cache STORED for {handle} ({len(candidates)} candidates)", "info")
        except Exception as cache_err:
            _debug(DebugCat.IMAGES, f"MCP image cache store failed for {handle}: {cache_err}", "warning")

    return candidates


def _get_existing_image_md5s(ad_group_id: str) -> set:
    """
    Get MD5-like identifiers of images already linked to an ad group.
    Returns set of asset resource names (used for dedup since we can't get MD5 from API).
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    query = f"""
        SELECT ad_group.id, ad_group_asset.asset, ad_group_asset.status
        FROM ad_group_asset
        WHERE ad_group.id = {ad_group_id}
          AND ad_group_asset.field_type = 'AD_IMAGE'
          AND ad_group_asset.status = 'ENABLED'
    """
    try:
        results = _execute_gaql(CUSTOMER_ID, query, page_size=100)
        return {
            str(_safe_get_value(row, "ad_group_asset.asset", ""))
            for row in results
        }
    except Exception as e:
        _debug("images", f"_get_existing_image_md5s failed for ag {ad_group_id}: {e}", "warning")
        return set()


def _validate_and_fix_image(image_bytes: bytes) -> bytes:
    """
    Pre-validate image for Google Ads requirements and auto-fix common issues.
    Returns fixed image bytes or raises ValueError if unfixable.

    Google Ads requirements:
    - Format: JPEG or PNG (NOT WebP)
    - Min dimensions: 300x300px for square
    - Max file size: 5MB
    - Color mode: RGB (not RGBA, CMYK, etc.)
    - Aspect ratio: 1:1 (square) or 1.91:1 (landscape)
    """
    from io import BytesIO

    try:
        from PIL import Image
    except ImportError:
        # If Pillow not available, do basic checks only
        if len(image_bytes) < 5000:
            raise ValueError(f"Image too small: {len(image_bytes)} bytes")
        if len(image_bytes) > 5242880:
            raise ValueError(f"Image too large: {len(image_bytes)} bytes")
        return image_bytes

    buf = BytesIO(image_bytes)
    try:
        img = Image.open(buf)
    except Exception as e:
        raise ValueError(f"Cannot open image: {e}")

    modified = False

    # Fix 1: Convert WebP to JPEG
    if img.format == "WEBP":
        modified = True

    # Fix 2: Convert RGBA/P/CMYK to RGB
    if img.mode in ("RGBA", "P", "LA", "PA"):
        background = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "P":
            img = img.convert("RGBA")
        if "A" in img.mode:
            background.paste(img, mask=img.split()[-1])
        else:
            background.paste(img)
        img = background
        modified = True
    elif img.mode != "RGB":
        img = img.convert("RGB")
        modified = True

    # Fix 3: Upscale if too small (min 300x300)
    w, h = img.size
    if w < 300 or h < 300:
        scale = max(300 / w, 300 / h)
        new_w, new_h = int(w * scale), int(h * scale)
        img = img.resize((new_w, new_h), Image.LANCZOS)
        modified = True
        w, h = img.size

    # Fix 4: Pad to square if aspect ratio is off
    if w != h:
        ratio = max(w, h) / min(w, h)
        if ratio < 1.5:
            # Close to square — pad to perfect square
            size = max(w, h)
            padded = Image.new("RGB", (size, size), (255, 255, 255))
            padded.paste(img, ((size - w) // 2, (size - h) // 2))
            img = padded
            modified = True

    # Save as JPEG if modified or if original was not JPEG
    if modified or img.format != "JPEG":
        out = BytesIO()
        img.save(out, "JPEG", quality=90, optimize=True)
        image_bytes = out.getvalue()

    # Final size check
    if len(image_bytes) < 5000:
        raise ValueError(f"Image too small after processing: {len(image_bytes)} bytes")
    if len(image_bytes) > 5242880:
        raise ValueError(f"Image too large after processing: {len(image_bytes)} bytes")

    return image_bytes


def _add_image_to_ad_group(ad_group_id: str, image_candidate: Dict[str, Any]) -> str:
    """
    Download image and create an AD_IMAGE asset linked to the ad group.
    Prefers local_path (fast disk read) over URL (HTTP download).
    Returns asset resource name on success, raises on failure.
    """
    from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client, _link_assets_to_entity

    _ensure_client()
    customer_id = _format_customer_id(CUSTOMER_ID)

    # Step 1: Get image bytes — prefer local path
    local_path = image_candidate.get("local_path")
    image_url = image_candidate.get("url", "")

    if local_path and os.path.isfile(local_path):
        with open(local_path, "rb") as f:
            image_bytes = f.read()
    elif image_url.startswith(("http://", "https://")):
        req = urllib.request.Request(image_url, headers={"User-Agent": "BatchOptimizer/1.0"})
        image_bytes = urllib.request.urlopen(req, context=_img_ssl_ctx, timeout=15).read()
    else:
        raise ValueError(f"No valid image source: local_path={local_path}, url={image_url}")

    # Pre-validate and auto-fix image (format, dimensions, color mode)
    image_bytes = _validate_and_fix_image(image_bytes)

    # Step 1b: Compute full MD5 for dedup (#9 Duplicate Detection)
    import hashlib as _hashlib
    full_md5 = _hashlib.md5(image_bytes).hexdigest()

    # Check DB-based dedup (skip if already uploaded to this ad group)
    db = _get_db()
    if db.check_image_uploaded("", "", ad_group_id, full_md5):
        raise ValueError(f"Image already uploaded (MD5 dedup: {full_md5[:8]})")

    # Step 2: Create image asset
    asset_service = google_ads_client.get_service("AssetService")
    asset_operation = google_ads_client.get_type("AssetOperation")
    asset = asset_operation.create

    filename = image_candidate.get("filename", "product-image")
    # Use content hash prefix to avoid naming conflicts on retry
    img_hash = full_md5[:8]
    safe_name = filename[:40].replace(".jpg", "").replace(".png", "").replace(".webp", "")
    asset.name = f"batch_{img_hash}_{safe_name}"
    asset.type_ = google_ads_client.enums.AssetTypeEnum.IMAGE
    asset.image_asset.data = image_bytes

    asset_result = _retry_with_backoff(
        asset_service.mutate_assets,
        customer_id=customer_id,
        operations=[asset_operation]
    )
    asset_resource_name = asset_result.results[0].resource_name

    # Step 3: Link to ad group as AD_IMAGE
    _link_assets_to_entity(
        customer_id,
        [asset_resource_name],
        google_ads_client.enums.AssetFieldTypeEnum.AD_IMAGE,
        ad_group_id=ad_group_id,
    )

    # Step 4: Record in DB for future dedup (#9)
    try:
        db.record_image_upload(
            country_code="",  # Will be set by caller context
            campaign_id="",
            ad_group_id=ad_group_id,
            product_handle=image_candidate.get("filename", ""),
            image_md5=full_md5,
            asset_resource_name=asset_resource_name,
            image_url=image_url,
            filename=filename,
            width=image_candidate.get("width"),
            height=image_candidate.get("height"),
            file_size=len(image_bytes),
        )
    except Exception as e:
        _debug("images", f"Image DB tracking failed for ag {ad_group_id}: {e}", "warning")

    return asset_resource_name


# ---------------------------------------------------------------------------
# Main batch processing function
# ---------------------------------------------------------------------------

def process_single_product(
    handle: str,
    feed_products: Dict[str, Dict],
    existing_ad_groups: Dict[str, Dict],
    existing_assets: Dict[str, Dict],
    campaign_id: str,
    config: dict,
    country_code: str,
    dry_run: bool = False,
    url_map: Optional[Dict[str, Dict]] = None,
    asset_types: Optional[List[str]] = None,
    force_replace_rsa: bool = False,
    feed_domain_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Process a single product handle: create/enable ad group, RSAs, extensions.
    Returns a status report dict.

    Uses URL-based matching (url_map) as primary method to find existing ad groups,
    with name-based matching as fallback for backward compatibility.
    """
    report = {
        "handle": handle,
        "actions": [],
        "errors": [],
        "skipped": False,
    }

    # 1. Find product in feed
    feed_data = feed_products.get(handle)
    if not feed_data:
        report["errors"].append(f"Handle '{handle}' not found in XML feed")
        return report

    product_name = _handle_to_product_name(handle)
    # Use sale_price (discounted) when available, fallback to full price
    price = feed_data.get("sale_price") or feed_data["price"]
    product_url = feed_data.get("link", f"https://{config['domain']}/products/{handle}")

    # --- Domain guardrail: ensure URL matches expected domain for this country ---
    # When feed_domain_override is set (e.g. HLE new products feed), trust the feed URL
    # and skip the guardrail — the feed domain IS the correct domain for this campaign.
    if feed_domain_override:
        from urllib.parse import urlparse as _urlparse
        _parsed = _urlparse(product_url)
        _feed_netloc = _parsed.netloc
        if _feed_netloc and feed_domain_override not in _feed_netloc:
            # Feed URL doesn't match expected feed domain — unusual, log warning
            _debug("domain_guard", f"feed_domain_override={feed_domain_override} but URL has {_feed_netloc} for {handle}. Keeping feed URL as-is.", "warning")
        # else: feed URL matches override domain — no correction needed
    else:
        expected_domain = config.get("domain", "")
        if expected_domain and expected_domain not in product_url:
            from urllib.parse import urlparse as _urlparse
            _parsed = _urlparse(product_url)
            _old_domain = _parsed.netloc
            if _old_domain and _old_domain != expected_domain:
                product_url = product_url.replace(_old_domain, expected_domain, 1)
                _debug("domain_guard", f"Domain mismatch for {handle}: {_old_domain} → {expected_domain}. URL corrected.", "warning")

    final_urls = [product_url]

    # 2. Find or create ad group
    # PRIMARY: URL-based matching (exact handle from RSA final_urls)
    ag_match = None
    match_source = None

    if url_map and handle in url_map:
        ag_match = url_map[handle]
        match_source = "url"
    else:
        # FALLBACK: Name-based matching (legacy method)
        ag_match = existing_ad_groups.get(product_name.lower())
        if ag_match:
            match_source = "name"

    ad_group_id = None

    if ag_match:
        ad_group_id = ag_match["id"]
        # Get status: prefer from existing_ad_groups (live), fallback to ag_match
        ag_name_lower = ag_match.get("name", "").lower()
        live_ag = existing_ad_groups.get(ag_name_lower, {})
        status = str(live_ag.get("status", ag_match.get("status", "ENABLED"))).upper()
        if "ENABLED" not in status and not dry_run:
            # Safety net: ALWAYS ensure ad group is ENABLED (handles PAUSED, unknown states)
            try:
                _enable_ad_group(ad_group_id)
                report["actions"].append(f"Enabled ad group '{ag_match.get('name', product_name)}' ({ad_group_id}) [was {status}, matched by {match_source}]")
            except Exception as e:
                report["errors"].append(f"Failed to enable ad group: {e}")
        else:
            report["actions"].append(f"Ad group '{ag_match.get('name', product_name)}' already enabled ({ad_group_id}) [matched by {match_source}]")
    else:
        if dry_run:
            report["actions"].append(f"[DRY RUN] Would create ad group '{product_name}'")
            ad_group_id = "DRY_RUN"
        else:
            try:
                ad_group_id = _create_ad_group(campaign_id, product_name)
                report["actions"].append(f"Created ad group '{product_name}' ({ad_group_id})")
            except Exception as e:
                if "DUPLICATE" in str(e).upper():
                    report["errors"].append(f"Ad group '{product_name}' already exists (duplicate name)")
                else:
                    report["errors"].append(f"Failed to create ad group: {e}")
                return report

    if not ad_group_id:
        return report

    # 3. Check existing assets
    asset_counts = existing_assets.get(ad_group_id, {})
    rsa_count = asset_counts.get("RSA", 0)

    # 4. Generate ad copy — 3-tier fallback chain:
    #    1) AI endpoint (configurable) — best quality, GPT + market intelligence
    #    2) Data-driven templates — enriched with keyword/performance DB
    #    3) Static templates — hardcoded per-country copy
    product_desc = feed_products.get(handle, {}).get("description", "")
    ad_copy = None

    # Tier 1: AI endpoint
    ad_copy = _fetch_ai_generated_copy(
        handle=handle, country_code=country_code, config=config,
        product_name=product_name, product_price=price,
        product_url=product_url, campaign_id=campaign_id,
        product_description=product_desc, feed_data=feed_data,
    )
    if ad_copy and ad_copy.get("_ai_generated"):
        report["actions"].append(
            f"Generated AI-powered ad copy via endpoint "
            f"({ad_copy.get('_ai_processing_time_ms', 0)}ms, "
            f"category={ad_copy.get('_ai_product_category', 'unknown')})"
        )
    else:
        # Tier 2: Data-driven templates
        ad_copy = _generate_data_driven_copy(
            country_code, product_name, price, product_url, config,
            handle, campaign_id, product_description=product_desc,
            feed_product_type=feed_data.get("product_type", ""),
        )
        if ad_copy:
            report["actions"].append("Generated data-driven ad copy (enriched with keyword intelligence)")
        else:
            # Tier 3: Static templates
            ad_copy = _generate_ad_copy_local(country_code, product_name, price, product_url, config)
            report["actions"].append("Generated template-based ad copy (fallback)")

    # 5. Create RSAs (target: 3 RSAs — A, B, C)
    if asset_types is None or "rsa" in asset_types:
        # v3.2.0: If ad group already has 3+ RSAs, mark all as present
        if rsa_count >= 3:
            report["actions"].append(f"RSAs already exist ({rsa_count} found)")
            # Ensure DB flags reflect reality — all 3 variants considered present
            report["_rsa_all_present"] = True

        # --- force_replace_rsa: SAFE pattern — create new RSAs FIRST, remove old ones ONLY after success ---
        # SAFETY: We collect removable RSA IDs here but do NOT remove them yet.
        # Old RSAs are only removed AFTER at least one new RSA is successfully created.
        # This prevents leaving an ad group with 0 RSAs if creation fails (e.g. DESTINATION_NOT_WORKING cache).
        _force_replace_removable_ad_ids: List[str] = []
        _force_replace_preserved_count = 0
        if force_replace_rsa and rsa_count > 0:
            if dry_run:
                report["actions"].append(f"[DRY RUN] Would replace RSAs WITHOUT stats (preserving RSAs with impressions/clicks)")
                rsa_count = 0  # so new RSAs get created below
            else:
                try:
                    from google_ads_mcp import _execute_gaql, _ensure_client, _format_customer_id, google_ads_client, _safe_get_value
                    from datetime import datetime, timedelta
                    _10d_ago = (datetime.utcnow() - timedelta(days=10)).strftime("%Y-%m-%d")
                    _today = datetime.utcnow().strftime("%Y-%m-%d")
                    gaql = (
                        f"SELECT ad_group_ad.ad.id, ad_group_ad.status, "
                        f"metrics.impressions, metrics.clicks, metrics.ctr "
                        f"FROM ad_group_ad "
                        f"WHERE ad_group.id = {ad_group_id} "
                        f"AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD' "
                        f"AND ad_group_ad.status != 'REMOVED' "
                        f"AND segments.date BETWEEN '{_10d_ago}' AND '{_today}'"
                    )
                    gaql_result = _execute_gaql(CUSTOMER_ID, gaql, page_size=50)

                    # Aggregate metrics per ad_id (multiple rows = multiple days)
                    ad_metrics: Dict[str, Dict] = {}
                    for row in gaql_result:
                        ad_id = str(_safe_get_value(row, "ad_group_ad.ad.id") or "")
                        if not ad_id:
                            continue
                        if ad_id not in ad_metrics:
                            ad_metrics[ad_id] = {"impressions": 0, "clicks": 0}
                        ad_metrics[ad_id]["impressions"] += int(_safe_get_value(row, "metrics.impressions", 0) or 0)
                        ad_metrics[ad_id]["clicks"] += int(_safe_get_value(row, "metrics.clicks", 0) or 0)

                    # Also query ALL-TIME for RSAs not in last 10 days
                    gaql_alltime = (
                        f"SELECT ad_group_ad.ad.id, ad_group_ad.status, "
                        f"metrics.impressions, metrics.clicks "
                        f"FROM ad_group_ad "
                        f"WHERE ad_group.id = {ad_group_id} "
                        f"AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD' "
                        f"AND ad_group_ad.status != 'REMOVED'"
                    )
                    gaql_alltime_result = _execute_gaql(CUSTOMER_ID, gaql_alltime, page_size=50)
                    for row in gaql_alltime_result:
                        ad_id = str(_safe_get_value(row, "ad_group_ad.ad.id") or "")
                        if not ad_id:
                            continue
                        if ad_id not in ad_metrics:
                            ad_metrics[ad_id] = {"impressions": 0, "clicks": 0}

                    for ad_id, metrics in ad_metrics.items():
                        impressions = metrics["impressions"]
                        clicks = metrics["clicks"]
                        ctr_10d = (clicks / impressions * 100) if impressions > 0 else 0

                        # RULE 1: CTR > 10% in last 10 days → ALWAYS preserve (user requirement)
                        if ctr_10d > 10:
                            _force_replace_preserved_count += 1
                            report["actions"].append(
                                f"Preserved RSA {ad_id} (HIGH CTR {ctr_10d:.1f}% in last 10d: {impressions} impr, {clicks} clicks)"
                            )
                            continue
                        # RULE 2: Has any stats → preserve (safe default)
                        if impressions > 0 or clicks > 0:
                            _force_replace_preserved_count += 1
                            report["actions"].append(
                                f"Preserved RSA {ad_id} (has stats: {impressions} impr, {clicks} clicks, CTR {ctr_10d:.1f}%)"
                            )
                            continue
                        # No stats — MARK for removal (but do NOT remove yet!)
                        _force_replace_removable_ad_ids.append(ad_id)

                    if _force_replace_removable_ad_ids:
                        report["actions"].append(
                            f"Marked {len(_force_replace_removable_ad_ids)} RSAs without stats for deferred removal (will remove AFTER new RSAs succeed)"
                        )
                    if _force_replace_preserved_count > 0:
                        report["actions"].append(f"Preserved {_force_replace_preserved_count} RSAs with stats (safe)")
                    if not _force_replace_removable_ad_ids and _force_replace_preserved_count == 0:
                        report["actions"].append("force_replace_rsa: no active RSAs found, creating new ones")
                    # Allow new RSA creation: count only preserved toward the 3-limit
                    rsa_count = _force_replace_preserved_count
                except Exception as e:
                    report["errors"].append(f"force_replace_rsa query failed: {e}")
                    # Continue with normal flow — don't create duplicates
        # --- end force_replace_rsa (deferred removal — actual removal happens after successful RSA creation below) ---

        rsas_to_create = max(0, 3 - rsa_count)
        if rsas_to_create > 0:
            rsa_variants = [
                ("A", ad_copy["rsa_a"]),
                ("B", ad_copy["rsa_b"]),
                ("C", ad_copy["rsa_c"]),
            ]
            # --- PRE-CREATE GUARDRAIL: validate headlines before sending to API ---
            brand_lower = product_name.lower().split()[0] if product_name else ""
            for variant_name, variant_data in rsa_variants[:rsas_to_create]:
                hl = variant_data.get("headlines", [])
                dl = variant_data.get("descriptions", [])

                # Guardrail 1: Remove headlines containing other brand names
                # (e.g., competitor brand in your product ad = hallucination from category keywords)
                if brand_lower and len(brand_lower) >= 3:
                    cleaned_hl = []
                    for h in hl:
                        h_text = h.get("text", h) if isinstance(h, dict) else h
                        if _contains_foreign_brand_hallucination(h_text, brand_lower):
                            report.setdefault("flags", {}).setdefault("headlines_removed_foreign_brand", []).append(h_text)
                            continue
                        cleaned_hl.append(h)
                    variant_data["headlines"] = cleaned_hl
                    hl = cleaned_hl

                # Guardrail 2: Ensure 15 headlines (pad with localized fillers)
                _h_fillers, _d_fillers = _get_localized_fillers(country_code, product_name)
                existing_lower = {(h.get("text", h) if isinstance(h, dict) else h).lower() for h in hl}
                for filler in _h_fillers:
                    if len(hl) >= 15:
                        break
                    if filler[:30].lower() not in existing_lower:
                        hl.append(filler[:30])
                        existing_lower.add(filler[:30].lower())
                variant_data["headlines"] = hl[:15]

                # Guardrail 3: Ensure 4 descriptions
                existing_desc_lower = {(d.get("text", d) if isinstance(d, dict) else d).lower() for d in dl}
                for filler in _d_fillers:
                    if len(dl) >= 4:
                        break
                    if filler[:90].lower() not in existing_desc_lower:
                        dl.append(filler[:90])
                        existing_desc_lower.add(filler[:90].lower())
                variant_data["descriptions"] = dl[:4]

                # --- Create RSA ---
                if dry_run:
                    report["actions"].append(f"[DRY RUN] Would create RSA {variant_name} for '{product_name}' ({len(variant_data['headlines'])}H/{len(variant_data['descriptions'])}D)")
                else:
                    try:
                        _create_rsa(ad_group_id, variant_data["headlines"], variant_data["descriptions"], final_urls)
                        report["actions"].append(f"Created RSA {variant_name} for '{product_name}' ({len(variant_data['headlines'])}H/{len(variant_data['descriptions'])}D)")
                        report.setdefault("_new_rsas_created", 0)
                        report["_new_rsas_created"] = report.get("_new_rsas_created", 0) + 1
                    except Exception as e:
                        report["errors"].append(f"Failed RSA {variant_name}: {e}")

            # --- DEFERRED REMOVAL: Only remove old RSAs if at least 1 new RSA was created successfully ---
            # SAFETY: If all new RSA creations failed (e.g. DESTINATION_NOT_WORKING), we keep old RSAs intact.
            if _force_replace_removable_ad_ids and report.get("_new_rsas_created", 0) > 0:
                from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client
                _ensure_client()
                customer_id = _format_customer_id(CUSTOMER_ID)
                ad_group_ad_service = google_ads_client.get_service("AdGroupAdService")
                _removed_count = 0
                for old_ad_id in _force_replace_removable_ad_ids:
                    try:
                        rm_op = google_ads_client.get_type("AdGroupAdOperation")
                        rm_resource = f"customers/{customer_id}/adGroupAds/{ad_group_id}~{old_ad_id}"
                        rm_op.remove = rm_resource
                        ad_group_ad_service.mutate_ad_group_ads(customer_id=customer_id, operations=[rm_op])
                        _removed_count += 1
                    except Exception as rm_err:
                        try:
                            pause_op = google_ads_client.get_type("AdGroupAdOperation")
                            ad_upd = pause_op.update
                            ad_upd.resource_name = rm_resource
                            ad_upd.status = google_ads_client.enums.AdGroupAdStatusEnum.PAUSED
                            from google.protobuf import field_mask_pb2
                            pause_op.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=["status"]))
                            ad_group_ad_service.mutate_ad_group_ads(customer_id=customer_id, operations=[pause_op])
                            _removed_count += 1
                            report["actions"].append(f"Paused old RSA {old_ad_id} (fallback — remove failed)")
                        except Exception as pause_err:
                            report["errors"].append(f"Failed to remove/pause old RSA {old_ad_id}: {rm_err} | {pause_err}")
                if _removed_count > 0:
                    report["actions"].append(f"Removed {_removed_count} old RSAs without stats (deferred, after new RSAs confirmed)")
            elif _force_replace_removable_ad_ids and report.get("_new_rsas_created", 0) == 0:
                report["actions"].append(
                    f"SAFETY: Kept {len(_force_replace_removable_ad_ids)} old RSAs intact — new RSA creation failed, not removing old ones"
                )
            # --- GUARDRAIL: Post-creation language validation ---
            # Check ALL RSA variants for foreign-language contamination
            for variant_name, variant_data in rsa_variants[:rsas_to_create]:
                if not variant_data:
                    continue
                hl_texts = []
                for h in variant_data.get("headlines", []):
                    hl_texts.append(h["text"] if isinstance(h, dict) else h)
                contamination = _validate_rsa_language(hl_texts, country_code)
                if contamination:
                    report["errors"].append(f"LANGUAGE_CONTAMINATION in RSA {variant_name}: {contamination}")
                    _debug("guardrail", f"LANGUAGE CONTAMINATION DETECTED for {handle} RSA {variant_name}: {contamination}", "error")
        else:
            report["actions"].append(f"RSAs already exist ({rsa_count} found)")

    # 6. Create callouts if missing
    if asset_types is None or "callouts" in asset_types:
        if asset_counts.get("CALLOUT", 0) == 0:
            # Prefer AI-generated callouts, fallback to config templates
            if ad_copy.get("_ai_callouts"):
                callout_texts = ad_copy["_ai_callouts"]
            else:
                has_price = price is not None and price > 0
                callout_texts = []
                for t in config["callout_templates"]:
                    if "{price}" in t and not has_price:
                        continue
                    text = t.replace("{price}", str(int(price)) if has_price else "").replace("{product}", product_name)
                    callout_texts.append(text)
                # Ensure minimum 2 callouts even without price
                if len(callout_texts) < 2:
                    callout_texts.append("Official Store")
                    callout_texts.append("Fast Shipping")
            if dry_run:
                report["actions"].append(f"[DRY RUN] Would create callouts: {callout_texts}")
            else:
                try:
                    _create_callouts(ad_group_id, callout_texts)
                    report["actions"].append(f"Created {len(callout_texts)} callouts")
                except Exception as e:
                    err_info = classify_error(e)
                    if err_info["category"] == ErrorCategory.RESOURCE_LIMIT:
                        report["actions"].append(f"Callouts at limit ({err_info['detail']}) — skipped")
                    elif err_info["category"] == ErrorCategory.TRANSIENT:
                        report["errors"].append(f"Failed callouts (transient, retry later): {e}")
                    else:
                        report["errors"].append(f"Failed callouts: {e}")
        else:
            report["actions"].append("Callouts already exist")

    # 7. Create sitelinks if missing
    if asset_types is None or "sitelinks" in asset_types:
        if asset_counts.get("SITELINK", 0) == 0:
            # Prefer AI-generated sitelinks, fallback to config templates
            if ad_copy.get("_ai_sitelinks"):
                sitelinks = ad_copy["_ai_sitelinks"]
            else:
                sitelinks = []
                for tmpl in config["sitelink_templates"]:
                    desc1 = tmpl["desc1"].replace("{product}", product_name).replace("{price}", str(int(price)))
                    desc2 = tmpl["desc2"].replace("{product}", product_name).replace("{price}", str(int(price)))
                    if len(desc1) > 35:
                        short_name = product_name.split()[0] if " " in product_name else product_name[:12]
                        desc1 = tmpl["desc1"].replace("{product}", short_name).replace("{price}", str(int(price)))
                        if len(desc1) > 35:
                            desc1 = desc1[:35]
                    if len(desc2) > 35:
                        desc2 = desc2[:35]
                    sl = {
                        "link_text": tmpl["link_text"][:25],
                        "description1": desc1,
                        "description2": desc2,
                        "final_urls": final_urls,
                    }
                    sitelinks.append(sl)
            if dry_run:
                report["actions"].append(f"[DRY RUN] Would create 4 sitelinks")
            else:
                try:
                    _create_sitelinks(ad_group_id, sitelinks)
                    report["actions"].append(f"Created {len(sitelinks)} sitelinks")
                except Exception as e:
                    err_info = classify_error(e)
                    if err_info["category"] == ErrorCategory.RESOURCE_LIMIT:
                        report["actions"].append(f"Sitelinks at limit ({err_info['detail']}) — skipped")
                    elif err_info["category"] == ErrorCategory.TRANSIENT:
                        report["errors"].append(f"Failed sitelinks (transient, retry later): {e}")
                    else:
                        report["errors"].append(f"Failed sitelinks: {e}")
        else:
            report["actions"].append("Sitelinks already exist")

    # 7b. Create anchor sitelinks — catchy marketing anchors on product URL
    # NOT included in default (asset_types=None) — must be explicitly requested
    # via asset_types=["anchor_sitelinks"] to avoid hitting the 20 sitelink limit.
    if asset_types is not None and "anchor_sitelinks" in asset_types:
        # Check how many sitelinks already exist (limit is 20 per ad group)
        current_sitelinks = asset_counts.get("SITELINK", 0)
        if current_sitelinks < 20:
            discount_percent = config.get("promotion_percent_off", 50000) // 1000  # micros → %
            anchor_sitelinks = _build_anchor_sitelinks(
                product_url=product_url,
                product_name=product_name,
                price=price,
                currency=config.get("currency", ""),
                language_code=config.get("language_code", "en"),
                discount_percent=discount_percent,
            )
            # Trim to available slots (max 20 - current)
            available_slots = 20 - current_sitelinks
            anchor_sitelinks = anchor_sitelinks[:available_slots]

            if anchor_sitelinks:
                if dry_run:
                    report["actions"].append(f"[DRY RUN] Would create {len(anchor_sitelinks)} anchor sitelinks")
                else:
                    try:
                        _create_sitelinks(ad_group_id, anchor_sitelinks)
                        report["actions"].append(f"Created {len(anchor_sitelinks)} anchor sitelinks")
                    except Exception as e:
                        err_info = classify_error(e)
                        if err_info["category"] == ErrorCategory.RESOURCE_LIMIT:
                            report["actions"].append(f"Anchor sitelinks at limit ({err_info['detail']}) — skipped")
                        elif err_info["category"] == ErrorCategory.TRANSIENT:
                            report["errors"].append(f"Failed anchor sitelinks (transient, retry later): {e}")
                        else:
                            report["errors"].append(f"Failed anchor sitelinks: {e}")
        else:
            report["actions"].append("Anchor sitelinks skipped — sitelink limit (20) reached")

    # 8. Create promotion if missing
    if asset_types is None or "promotion" in asset_types:
        if asset_counts.get("PROMOTION", 0) == 0:
            # Use AI-generated promotion data if available
            ai_promo = ad_copy.get("_ai_promotion", {})
            promo_percent = ai_promo.get("percent_off", config.get("promotion_percent_off", 50000))
            promo_target = ai_promo.get("promotion_target", product_name)[:20]
            promo_lang = ai_promo.get("language_code", config.get("language_code", "en"))
            # Convert percent_off from endpoint (simple int like 50) to micros (50000) if needed
            if isinstance(promo_percent, (int, float)) and promo_percent < 100:
                promo_percent_micros = int(promo_percent * 1000)
            else:
                promo_percent_micros = int(promo_percent)
            if dry_run:
                report["actions"].append(f"[DRY RUN] Would create promotion ({promo_percent_micros // 1000}% off)")
            else:
                try:
                    _create_promotion(
                        ad_group_id, promo_target,
                        promo_percent_micros,
                        promo_lang,
                        product_url,
                    )
                    report["actions"].append(f"Created promotion ({promo_percent_micros // 1000}% off)")
                except Exception as e:
                    err_info = classify_error(e)
                    if err_info["category"] == ErrorCategory.RESOURCE_LIMIT:
                        report["actions"].append(f"Promotion at limit ({err_info['detail']}) — skipped")
                    elif err_info["category"] == ErrorCategory.TRANSIENT:
                        report["errors"].append(f"Failed promotion (transient, retry later): {e}")
                    else:
                        report["errors"].append(f"Failed promotion: {e}")
        else:
            report["actions"].append("Promotion already exists")

    # 9. Create structured snippets if missing
    if asset_types is None or "snippets" in asset_types:
        if asset_counts.get("STRUCTURED_SNIPPET", 0) == 0:
            if dry_run:
                report["actions"].append(f"[DRY RUN] Would create snippets: {ad_copy['snippet_values']}")
            else:
                try:
                    snippet_header = ad_copy.get("_ai_snippet_header", config.get("snippet_header", "Types"))
                    _create_structured_snippets(ad_group_id, snippet_header, ad_copy["snippet_values"])
                    report["actions"].append(f"Created structured snippets")
                except Exception as e:
                    err_info = classify_error(e)
                    if err_info["category"] == ErrorCategory.RESOURCE_LIMIT:
                        report["actions"].append(f"Snippets at limit ({err_info['detail']}) — skipped")
                    elif err_info["category"] == ErrorCategory.TRANSIENT:
                        report["errors"].append(f"Failed snippets (transient, retry later): {e}")
                    else:
                        report["errors"].append(f"Failed snippets: {e}")
        else:
            report["actions"].append("Structured snippets already exist")

    # 10. Add keywords (with AI endpoint → intelligence DB → brand fallback)
    if asset_types is None or "keywords" in asset_types:
        kw_added = 0
        kw_intel_count = 0
        kw_cat_count = 0
        kw_brand_count = 0
        kw_ai_count = 0
        used_intelligence = False

        # Build keyword list: prefer AI-generated, then batch_intelligence, then brand fallback
        kw_recs = []

        # Source 1: AI endpoint keywords (already enriched with volume/CPC by Botster)
        if ad_copy.get("_ai_keywords"):
            for kw in ad_copy["_ai_keywords"]:
                kw_recs.append({
                    "text": kw.get("text", ""),
                    "match_type": kw.get("match_type", "PHRASE"),
                    "source": "ai_generator",
                })
            used_intelligence = True

        # Source 2: batch_intelligence DB (if AI didn't provide enough)
        if len(kw_recs) < 10:
            try:
                from batch_intelligence import get_keywords_for_setup
                _fpt = feed_data.get("product_type", "") if feed_data else ""
                intel_kws = get_keywords_for_setup(product_handle=handle, country_code=country_code,
                                                   ad_group_id=ad_group_id, min_roi=0.0, max_keywords=15,
                                                   feed_product_type=_fpt)
                # Deduplicate against AI keywords
                existing_texts = {kw["text"].lower() for kw in kw_recs}
                for kw in intel_kws:
                    if kw.get("text", "").lower() not in existing_texts:
                        kw_recs.append(kw)
                        existing_texts.add(kw["text"].lower())
                if intel_kws:
                    used_intelligence = True
            except ImportError:
                pass
            except Exception as e:
                report["errors"].append(f"Keyword intelligence failed: {str(e)[:100]}")

        # Source 3: Enhanced brand fallback (if nothing else available)
        if not kw_recs:
            keyword_base = product_name.lower()
            default_kw = config.get("default_keyword", "Product").lower()

            # Core brand keywords (EXACT + PHRASE)
            kw_recs.append({"text": keyword_base, "match_type": "EXACT", "source": "brand_fallback"})
            kw_recs.append({"text": keyword_base, "match_type": "PHRASE", "source": "brand_fallback"})

            # Brand + intent variants (PHRASE)
            lang_code = config.get("language_code", "en")
            if lang_code == "ro":
                intent_variants = [
                    f"{keyword_base} pret",
                    f"{keyword_base} pareri",
                    f"{keyword_base} farmacia",
                    f"{keyword_base} original",
                    f"cumpara {keyword_base}",
                    f"{keyword_base} {default_kw.lower()}",
                ]
            elif lang_code == "tr":
                intent_variants = [
                    f"{keyword_base} fiyat",
                    f"{keyword_base} yorumlar",
                    f"{keyword_base} eczane",
                    f"{keyword_base} orijinal",
                    f"{keyword_base} sipariş",
                    f"{keyword_base} {default_kw.lower()}",
                ]
            elif lang_code == "pl":
                intent_variants = [
                    f"{keyword_base} cena",
                    f"{keyword_base} opinie",
                    f"{keyword_base} apteka",
                    f"{keyword_base} oryginalny",
                    f"kup {keyword_base}",
                    f"{keyword_base} {default_kw.lower()}",
                ]
            else:
                intent_variants = [
                    f"{keyword_base} price",
                    f"{keyword_base} reviews",
                    f"buy {keyword_base}",
                    f"{keyword_base} original",
                    f"{keyword_base} {default_kw.lower()}",
                ]
            for variant in intent_variants:
                if len(variant) <= 80:  # Google Ads keyword max length
                    kw_recs.append({"text": variant, "match_type": "PHRASE", "source": "brand_fallback"})

        # Add all keywords to ad group
        for kw in kw_recs[:20]:  # Cap at 20 keywords
            kw_text = kw.get("text", "")
            kw_match = kw.get("match_type", "PHRASE")
            kw_source = kw.get("source", "unknown")

            # SAFETY: Force BROAD → PHRASE before logging and API call
            if kw_match == "BROAD":
                kw_match = "PHRASE"

            if not kw_text:
                continue

            if dry_run:
                report["actions"].append(f"[DRY RUN] Would add keyword '{kw_text}' [{kw_match}] (source: {kw_source})")
                kw_added += 1
            else:
                err = _add_keyword(ad_group_id, kw_text, kw_match)
                if err:
                    if "HEALTH_IN_PERSONALIZED_ADS" in err:
                        report["errors"].append(f"Keyword '{kw_text}' [{kw_match}] blocked by health policy")
                    elif "DUPLICATE" in err.upper() or "ALREADY_EXISTS" in err.upper():
                        pass  # Silently skip duplicates
                    else:
                        report["errors"].append(f"Keyword '{kw_text}' [{kw_match}] failed: {err[:100]}")
                else:
                    report["actions"].append(f"Added keyword '{kw_text}' [{kw_match}] (source: {kw_source})")
                    kw_added += 1
                    if kw_source == "ai_generator":
                        kw_ai_count += 1
                    elif kw_source == "historical":
                        kw_intel_count += 1
                    elif kw_source == "category":
                        kw_cat_count += 1
                    elif kw_source == "brand_fallback":
                        kw_brand_count += 1

        # 10b. Negative keywords — DISABLED
        # AI-generated negatives are not data-driven (no ROI validation).
        # Negative keywords should be managed manually or via /ads-search-terms
        # which uses actual search term data to identify wasted spend.

        report["flags"] = report.get("flags", {})
        report["flags"]["used_keyword_intelligence"] = used_intelligence
        report["flags"]["used_ai_generator"] = ad_copy.get("_ai_generated", False)
        report["flags"]["keywords_added"] = kw_added
        report["flags"]["kw_ai_count"] = kw_ai_count
        report["flags"]["kw_intel_count"] = kw_intel_count
        report["flags"]["kw_category_count"] = kw_cat_count
        report["flags"]["kw_brand_count"] = kw_brand_count

    # 11. Add images if missing or insufficient
    if asset_types is None or "images" in asset_types:
        existing_image_count = asset_counts.get("AD_IMAGE", 0)
        # Live-check if cache says 0 but ad group existed before this run
        if existing_image_count == 0 and ad_group_id and ad_group_id != "DRY_RUN":
            live_img_assets = _get_existing_image_md5s(ad_group_id)
            if live_img_assets:
                existing_image_count = len(live_img_assets)
                report["actions"].append(f"Images already sufficient ({existing_image_count} found via live query)")
        if existing_image_count < IMAGE_MIN_EXISTING:
            images_needed = IMAGE_MAX_PER_AD_GROUP - existing_image_count
            if images_needed > 0:
                try:
                    candidates = _fetch_product_images(handle)
                    if candidates:
                        if dry_run:
                            descs = []
                            for c in candidates[:images_needed]:
                                pm = c.get("processing_method", "none")
                                dims = f"{c.get('width', '?')}x{c.get('height', '?')}"
                                descs.append(f"{c.get('filename', '?')[:30]} [{pm}, {dims}, q={c.get('quality_score', '?')}]")
                            report["actions"].append(
                                f"[DRY RUN] Would add up to {images_needed} images "
                                f"(found {len(candidates)} candidates, {existing_image_count} existing): "
                                + "; ".join(descs)
                            )
                        else:
                            # Get existing asset resource names for dedup
                            existing_assets_set = _get_existing_image_md5s(ad_group_id) if ad_group_id != "DRY_RUN" else set()
                            seen_md5 = set()
                            added = 0
                            skipped_duplicate = 0
                            rejected_by_google = 0
                            rejected_by_policy = 0

                            for img in candidates:
                                if added >= images_needed:
                                    break

                                # Deduplicate by MD5 hash (skip if same image already processed)
                                md5 = img.get("md5_hash")
                                if md5 and md5 in seen_md5:
                                    skipped_duplicate += 1
                                    continue
                                if md5:
                                    seen_md5.add(md5)

                                try:
                                    asset_rn = _add_image_to_ad_group(ad_group_id, img)
                                    added += 1
                                    source = img.get("source", "local")
                                    fname = img.get("filename", "?")[:40]
                                    pm = img.get("processing_method", "none")
                                    dims = f"{img.get('width', '?')}x{img.get('height', '?')}"
                                    report["actions"].append(
                                        f"Added image {added}/{images_needed}: {fname} [{pm}, {dims}, q={img.get('quality_score', '?')}, {source}]"
                                    )
                                except Exception as e:
                                    full_err = str(e)
                                    err_msg = full_err[:600]
                                    # Extract specific Google Ads error details if available
                                    err_upper = err_msg.upper()
                                    fname_short = img.get("filename", "?")[:30]
                                    # Duplicate/already-exists = skip (success), NOT error
                                    if "DUPLICATE" in err_upper or "ALREADY_EXISTS" in err_upper or "MD5 DEDUP" in err_upper:
                                        skipped_duplicate += 1
                                        report["actions"].append(f"Image '{fname_short}' already exists (skip)")
                                    elif "POLICY" in err_upper or "DISAPPROVED" in err_upper or "HEALTH_IN_PERSONALIZED" in err_upper:
                                        rejected_by_policy += 1
                                        report["errors"].append(f"Image policy rejected: {fname_short}")
                                    elif "INVALID_ARGUMENT" in err_upper:
                                        rejected_by_google += 1
                                        # Try to extract the specific field/reason from gRPC error
                                        import re
                                        field_match = re.search(r'field_name:\s*"([^"]+)"', full_err)
                                        trigger_match = re.search(r'trigger \{[^}]*string_value:\s*"([^"]+)"', full_err)
                                        msg_match = re.search(r'message:\s*"([^"]+)"', full_err)
                                        details_match = re.search(r'details\s*=\s*"([^"]+)"', full_err)
                                        detail = (
                                            f"field={field_match.group(1)}" if field_match else
                                            f"trigger={trigger_match.group(1)[:50]}" if trigger_match else
                                            f"msg={msg_match.group(1)[:80]}" if msg_match else
                                            f"details={details_match.group(1)[:80]}" if details_match else
                                            err_msg[:200]
                                        )
                                        report["errors"].append(f"Image INVALID_ARGUMENT ({fname_short}): {detail}")
                                    else:
                                        rejected_by_google += 1
                                        report["errors"].append(f"Image failed: {err_msg[:300]}")

                            # Summary with granular status reporting
                            if added > 0:
                                report["actions"].append(f"Total images added: {added}")
                            elif skipped_duplicate > 0:
                                # All images already exist — this is SUCCESS, not error
                                report["actions"].append(f"All images already exist ({skipped_duplicate} skipped as duplicate)")
                            elif rejected_by_google > 0 or rejected_by_policy > 0:
                                report["errors"].append(
                                    f"All {len(candidates)} image candidates rejected by Google "
                                    f"({rejected_by_google} invalid, {rejected_by_policy} policy)"
                                )
                            elif len(candidates) > 0:
                                report["errors"].append(f"All {len(candidates)} image candidates failed")
                    else:
                        report["actions"].append(f"No image candidates found for '{handle}'")
                except Exception as e:
                    report["errors"].append(f"Image fetch error: {str(e)[:100]}")
        else:
            report["actions"].append(f"Images already sufficient ({existing_image_count} found)")

    return report


# ---------------------------------------------------------------------------
# DB tracking helper
# ---------------------------------------------------------------------------

def _track_product_in_db(
    db: BatchDB,
    country_code: str,
    campaign_id: str,
    handle: str,
    report: Dict[str, Any],
    feed_data: Dict[str, Any],
    duration_ms: int,
    asset_types: Optional[List[str]] = None,
):
    """Extract flags from process_single_product report and store in DB."""
    actions_text = " | ".join(report.get("actions", []))

    # Parse flags from actions text
    flags: Dict[str, Any] = {}

    if feed_data:
        flags["product_name"] = _handle_to_product_name(handle)
        flags["product_price"] = feed_data.get("price", 0)
        flags["product_url"] = feed_data.get("link", "")

    # Ad group
    for a in report.get("actions", []):
        if "Created ad group" in a or "already enabled" in a:
            # Extract ad group ID from parentheses
            import re as _re
            m = _re.search(r"\((\d+)\)", a)
            if m:
                flags["ad_group_id"] = m.group(1)

    # RSAs
    if "Created RSA A" in actions_text:
        flags["has_rsa_a"] = 1
    if "Created RSA B" in actions_text:
        flags["has_rsa_b"] = 1
    if "Created RSA C" in actions_text:
        flags["has_rsa_c"] = 1
    if "RSAs already exist" in actions_text:
        flags["has_rsa_a"] = 1
        flags["has_rsa_b"] = 1
        flags["has_rsa_c"] = 1
    # v3.2.0: If 3+ RSAs exist in ad group, mark all present regardless of labels
    if report.get("_rsa_all_present"):
        flags["has_rsa_a"] = 1
        flags["has_rsa_b"] = 1
        flags["has_rsa_c"] = 1

    # Extensions
    if "Created 4 callouts" in actions_text or "Callouts already exist" in actions_text:
        flags["has_callouts"] = 1
    if "Created 4 sitelinks" in actions_text or "Sitelinks already exist" in actions_text:
        flags["has_sitelinks"] = 1
    if "Created promotion" in actions_text or "Promotion already exists" in actions_text:
        flags["has_promotion"] = 1
    if "Created structured snippets" in actions_text or "Structured snippets already exist" in actions_text:
        flags["has_snippets"] = 1

    # Keywords
    if "Added keyword" in actions_text:
        flags["has_keywords"] = 1

    # Images
    image_added_count = actions_text.count("Added image")
    if image_added_count > 0 or "Images already sufficient" in actions_text:
        flags["has_images"] = 1
        if image_added_count > 0:
            flags["image_count"] = image_added_count

    # Errors
    errors = report.get("errors", [])
    if errors:
        flags["last_error"] = " | ".join(errors)[:500]

    # Determine status — SOFT COMPLETENESS (v3.2.0)
    # Core flags: RSAs + sitelinks + promotion + snippets (NOT callouts — they hit campaign limits)
    # Callouts are "nice to have" but don't block "complete" status
    core_flags = [
        flags.get("has_rsa_a", 0), flags.get("has_rsa_b", 0), flags.get("has_rsa_c", 0),
        flags.get("has_sitelinks", 0),
        flags.get("has_promotion", 0), flags.get("has_snippets", 0),
    ]
    # Callouts tracked separately (soft requirement)
    all_flag_names = ["has_rsa_a", "has_rsa_b", "has_rsa_c",
                      "has_sitelinks", "has_promotion", "has_snippets"]

    # Merge with existing DB flags
    existing = db.get_product(country_code, campaign_id, handle)
    if existing:
        for k in ["has_rsa_a", "has_rsa_b", "has_rsa_c", "has_callouts",
                   "has_sitelinks", "has_promotion", "has_snippets",
                   "has_keywords", "has_images"]:
            if k not in flags and existing.get(k):
                if k in all_flag_names:
                    idx = all_flag_names.index(k)
                    core_flags[idx] = existing[k]
                flags[k] = existing[k]

    text_complete = all(f == 1 for f in core_flags)
    has_images_flag = flags.get("has_images", 0) or (existing and existing.get("has_images", 0))

    if text_complete and has_images_flag:
        flags["status"] = "complete"
    elif text_complete and not has_images_flag:
        flags["status"] = "complete_no_images"
    elif any(f == 1 for f in core_flags):
        flags["status"] = "partial"
    elif errors:
        flags["status"] = "error"
    else:
        flags["status"] = "pending"

    db.upsert_product(country_code, campaign_id, handle, **flags)

    # Log the operation
    op_status = "error" if errors else "success"
    db.log_operation(
        country_code, campaign_id, handle,
        "batch_setup",
        op_status,
        details={"actions": report.get("actions", []), "errors": errors},
        duration_ms=duration_ms,
    )

    # === GUARDRAIL VALIDATION (zero extra API calls) ===
    try:
        guardrail_checks = _extract_guardrails_from_report(report, asset_types=asset_types)
        ag_id = flags.get("ad_group_id") or (existing.get("ad_group_id") if existing else None)
        db.upsert_guardrail(country_code, campaign_id, handle, guardrail_checks,
                           ad_group_id=ag_id, source='setup')
    except Exception as e:
        _debug("guardrails", f"Guardrail tracking failed for {handle}: {e}", "warning")


def _check_ad_eligibility(campaign_id: str) -> Dict[str, Any]:
    """Check approval status of all ads in a campaign. Cost: 1 GAQL call."""
    from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id

    customer_id = _format_customer_id(CUSTOMER_ID)
    query = f"""
        SELECT campaign.id, ad_group_ad.ad.id, ad_group.id, ad_group.name,
               ad_group_ad.status,
               ad_group_ad.policy_summary.approval_status,
               ad_group_ad.policy_summary.review_status
        FROM ad_group_ad
        WHERE campaign.id = {campaign_id}
          AND ad_group.status = 'ENABLED'
          AND ad_group_ad.status != 'REMOVED'
    """
    results = _execute_gaql(customer_id, query, page_size=1000)

    counts = {"total": len(results), "APPROVED": 0, "APPROVED_LIMITED": 0,
              "DISAPPROVED": 0, "UNDER_REVIEW": 0, "UNKNOWN": 0}
    disapproved = []

    for row in results:
        ad_id = str(_safe_get_value(row, "ad_group_ad.ad.id", ""))
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        ag_name = str(_safe_get_value(row, "ad_group.name", ""))
        approval = str(_safe_get_value(row, "ad_group_ad.policy_summary.approval_status", "UNKNOWN"))
        review = str(_safe_get_value(row, "ad_group_ad.policy_summary.review_status", ""))

        if "APPROVED_LIMITED" in approval:
            counts["APPROVED_LIMITED"] += 1
        elif "APPROVED" in approval:
            counts["APPROVED"] += 1
        elif "DISAPPROVED" in approval:
            counts["DISAPPROVED"] += 1
            disapproved.append({
                "ad_id": ad_id, "ad_group_id": ag_id, "ad_group_name": ag_name,
                "approval_status": approval, "review_status": review,
            })
        elif "UNDER_REVIEW" in review:
            counts["UNDER_REVIEW"] += 1
        else:
            counts["UNKNOWN"] += 1

    return {**counts, "disapproved_details": disapproved}


def _check_asset_eligibility(campaign_id: str) -> Dict[str, Any]:
    """Check primary status of all assets in a campaign. Cost: 1 GAQL call."""
    from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id

    customer_id = _format_customer_id(CUSTOMER_ID)
    query = f"""
        SELECT campaign.id, ad_group_asset.asset, ad_group_asset.field_type,
               ad_group.id, ad_group.name,
               ad_group_asset.primary_status,
               ad_group_asset.primary_status_reasons
        FROM ad_group_asset
        WHERE campaign.id = {campaign_id}
          AND ad_group.status = 'ENABLED'
    """
    results = _execute_gaql(customer_id, query, page_size=1000)

    counts = {"total": len(results), "ELIGIBLE": 0, "NOT_ELIGIBLE": 0,
              "PENDING": 0, "PAUSED": 0, "OTHER": 0}
    not_eligible = []

    for row in results:
        asset_ref = str(_safe_get_value(row, "ad_group_asset.asset", ""))
        raw_ft = _safe_get_value(row, "ad_group_asset.field_type", "")
        field_type = raw_ft.name if hasattr(raw_ft, 'name') else str(raw_ft)
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        ag_name = str(_safe_get_value(row, "ad_group.name", ""))
        raw_ps = _safe_get_value(row, "ad_group_asset.primary_status", "")
        primary_status = raw_ps.name if hasattr(raw_ps, 'name') else str(raw_ps)
        reasons_raw = _safe_get_value(row, "ad_group_asset.primary_status_reasons", [])

        if "ELIGIBLE" in primary_status and "NOT" not in primary_status:
            counts["ELIGIBLE"] += 1
        elif "NOT_ELIGIBLE" in primary_status:
            counts["NOT_ELIGIBLE"] += 1
            reasons = [str(r) for r in (reasons_raw if isinstance(reasons_raw, list) else [reasons_raw])] if reasons_raw else []
            not_eligible.append({
                "asset_ref": asset_ref, "field_type": field_type,
                "ad_group_id": ag_id, "ad_group_name": ag_name,
                "primary_status": primary_status, "reasons": reasons,
            })
        elif "PENDING" in primary_status:
            counts["PENDING"] += 1
        elif "PAUSED" in primary_status:
            counts["PAUSED"] += 1
        else:
            counts["OTHER"] += 1

    return {**counts, "not_eligible_details": not_eligible}


def _extract_guardrails_from_report(
    report: Dict[str, Any],
    asset_types: Optional[List[str]] = None,
) -> Dict[str, Dict[str, str]]:
    """
    Extract guardrail check results from process_single_product report.
    Zero API calls — purely parses the actions/errors strings.

    Statuses: PASS (optimal), WARN (suboptimal), FAIL (missing), SKIP (intentional/external)

    Returns: {"rsa_a": {"status": "PASS", "headlines": 15, "descriptions": 4}, ...}
    """
    actions = report.get("actions", [])
    errors = report.get("errors", [])
    flags = report.get("flags", {})
    actions_text = " | ".join(actions)
    errors_text = " | ".join(errors)
    errors_upper = errors_text.upper()
    checks: Dict[str, Dict[str, str]] = {}

    # Helper: check if asset_type was intentionally skipped
    def _is_skipped(asset_key: str) -> bool:
        return asset_types is not None and asset_key not in asset_types

    # Helper: extract number from action string pattern
    def _extract_count(pattern: str, text: str) -> Optional[int]:
        m = re.search(pattern, text)
        return int(m.group(1)) if m else None

    # 1. RSA A/B/C — with headline/description count validation
    for variant in ["A", "B", "C"]:
        key = f"rsa_{variant.lower()}"
        if _is_skipped("rsa"):
            checks[key] = {"status": "SKIP", "reason": "INTENTIONAL_SKIP"}
        elif f"Created RSA {variant}" in actions_text or "RSAs already exist" in actions_text:
            # Try to extract headline/description counts
            h_count = _extract_count(
                rf"Created RSA {variant}[^|]*?(\d+)\s*headlines", actions_text
            )
            d_count = _extract_count(
                rf"Created RSA {variant}[^|]*?(\d+)\s*descriptions", actions_text
            )
            check_data = {"status": "PASS"}
            if h_count is not None:
                check_data["headlines"] = str(h_count)
                if h_count < 12:
                    check_data["status"] = "FAIL"
                    check_data["reason"] = f"ONLY_{h_count}_HEADLINES"
                elif h_count < 15:
                    check_data["status"] = "WARN"
                    check_data["reason"] = f"SUBOPTIMAL_{h_count}_HEADLINES"
            if d_count is not None:
                check_data["descriptions"] = str(d_count)
                if d_count < 2:
                    check_data["status"] = "FAIL"
                    check_data["reason"] = f"ONLY_{d_count}_DESCRIPTIONS"
                elif d_count < 4 and check_data["status"] != "FAIL":
                    check_data["status"] = "WARN"
                    check_data["reason"] = check_data.get("reason", "") + f"_SUBOPTIMAL_{d_count}_DESCRIPTIONS"
            checks[key] = check_data
        elif f"Failed RSA {variant}" in errors_text:
            reason = "POLICY_REJECTED" if "POLICY" in errors_upper or "HEALTH" in errors_upper else "API_ERROR"
            checks[key] = {"status": "FAIL", "reason": reason}
        else:
            checks[key] = {"status": "FAIL", "reason": "NOT_CREATED"}

    # 2. Callouts — with count validation
    if _is_skipped("callouts"):
        checks["callouts"] = {"status": "SKIP", "reason": "INTENTIONAL_SKIP"}
    elif "Created" in actions_text and "callout" in actions_text.lower():
        c_count = _extract_count(r"Created\s+(\d+)\s+callouts?", actions_text)
        if c_count is not None:
            checks["callouts"] = {"status": "PASS" if c_count >= 4 else "WARN", "count": str(c_count)}
            if c_count < 4:
                checks["callouts"]["reason"] = f"ONLY_{c_count}_CALLOUTS"
        else:
            checks["callouts"] = {"status": "PASS"}
    elif "Callouts already exist" in actions_text:
        checks["callouts"] = {"status": "PASS"}
    elif "Callouts at limit" in actions_text:
        checks["callouts"] = {"status": "SKIP", "reason": "RESOURCE_LIMIT"}
    elif "Failed callouts" in errors_text:
        checks["callouts"] = {"status": "FAIL", "reason": "API_ERROR"}
    else:
        checks["callouts"] = {"status": "SKIP", "reason": "NOT_ATTEMPTED"}

    # 3. Sitelinks — with count validation
    if _is_skipped("sitelinks"):
        checks["sitelinks"] = {"status": "SKIP", "reason": "INTENTIONAL_SKIP"}
    elif "Created" in actions_text and "sitelink" in actions_text.lower():
        s_count = _extract_count(r"Created\s+(\d+)\s+sitelinks?", actions_text)
        if s_count is not None:
            checks["sitelinks"] = {"status": "PASS" if s_count >= 4 else "WARN", "count": str(s_count)}
            if s_count < 4:
                checks["sitelinks"]["reason"] = f"ONLY_{s_count}_SITELINKS"
        else:
            checks["sitelinks"] = {"status": "PASS"}
    elif "Sitelinks already exist" in actions_text:
        checks["sitelinks"] = {"status": "PASS"}
    elif "Sitelinks at limit" in actions_text or "too long" in actions_text.lower():
        checks["sitelinks"] = {"status": "SKIP", "reason": "RESOURCE_LIMIT"}
    elif "Failed sitelinks" in errors_text:
        checks["sitelinks"] = {"status": "FAIL", "reason": "API_ERROR"}
    else:
        checks["sitelinks"] = {"status": "SKIP", "reason": "NOT_ATTEMPTED"}

    # 4. Promotion
    if _is_skipped("promotion"):
        checks["promotion"] = {"status": "SKIP", "reason": "INTENTIONAL_SKIP"}
    elif "Created promotion" in actions_text or "Promotion already exists" in actions_text:
        checks["promotion"] = {"status": "PASS"}
    elif "Promotion at limit" in actions_text:
        checks["promotion"] = {"status": "SKIP", "reason": "RESOURCE_LIMIT"}
    elif "Failed promotion" in errors_text:
        checks["promotion"] = {"status": "FAIL", "reason": "API_ERROR"}
    else:
        checks["promotion"] = {"status": "SKIP", "reason": "NOT_ATTEMPTED"}

    # 5. Snippets
    if _is_skipped("snippets"):
        checks["snippets"] = {"status": "SKIP", "reason": "INTENTIONAL_SKIP"}
    elif "Created structured snippets" in actions_text or "Structured snippets already exist" in actions_text:
        checks["snippets"] = {"status": "PASS"}
    elif "Snippets at limit" in actions_text:
        checks["snippets"] = {"status": "SKIP", "reason": "RESOURCE_LIMIT"}
    elif "Failed snippets" in errors_text:
        checks["snippets"] = {"status": "FAIL", "reason": "API_ERROR"}
    else:
        checks["snippets"] = {"status": "SKIP", "reason": "NOT_ATTEMPTED"}

    # 6. Keywords — with count and intelligence quality check
    if _is_skipped("keywords"):
        checks["keywords"] = {"status": "SKIP", "reason": "INTENTIONAL_SKIP"}
    elif "Added keyword" in actions_text:
        kw_count = sum(1 for a in actions if "Added keyword" in a)
        intel_count = flags.get("kw_intel_count", 0)
        cat_count = flags.get("kw_category_count", 0)
        brand_count = flags.get("kw_brand_count", 0)

        checks["keywords"] = {
            "status": "PASS" if kw_count >= 5 else ("WARN" if kw_count >= 3 else "WARN"),
            "count": str(kw_count),
            "intel": str(intel_count),
            "category": str(cat_count),
            "brand": str(brand_count),
        }
        if kw_count < 5 and intel_count == 0:
            checks["keywords"]["status"] = "WARN"
            checks["keywords"]["reason"] = "NO_INTELLIGENCE_DATA"
    elif "blocked by health policy" in errors_text.lower():
        checks["keywords"] = {"status": "SKIP", "reason": "HEALTH_POLICY"}
    elif "Keyword" in errors_text and "failed" in errors_text.lower():
        checks["keywords"] = {"status": "FAIL", "reason": "API_ERROR"}
    else:
        checks["keywords"] = {"status": "SKIP", "reason": "EXISTING_AD_GROUP"}

    # 7. Images
    if _is_skipped("images"):
        checks["images"] = {"status": "SKIP", "reason": "INTENTIONAL_SKIP"}
    elif "Added image" in actions_text or "Images already sufficient" in actions_text:
        checks["images"] = {"status": "PASS"}
    elif "All images already exist" in actions_text:
        # Duplicate/already-exists images are SUCCESS, not failure
        checks["images"] = {"status": "PASS", "reason": "ALREADY_EXISTS"}
    elif "No image candidates" in actions_text:
        checks["images"] = {"status": "SKIP", "reason": "IMAGE_NOT_AVAILABLE"}
    elif "Image policy rejected" in errors_text:
        checks["images"] = {"status": "SKIP", "reason": "POLICY_REJECTED"}
    elif "Image INVALID_ARGUMENT" in errors_text:
        checks["images"] = {"status": "FAIL", "reason": "INVALID_ARGUMENT"}
    elif "image candidates rejected by Google" in errors_text:
        checks["images"] = {"status": "FAIL", "reason": "ALL_REJECTED_BY_GOOGLE"}
    elif "image candidates failed" in errors_text:
        checks["images"] = {"status": "FAIL", "reason": "ALL_CANDIDATES_FAILED"}
    elif "Image fetch error" in errors_text:
        checks["images"] = {"status": "FAIL", "reason": "FETCH_ERROR"}
    else:
        checks["images"] = {"status": "SKIP", "reason": "NOT_ATTEMPTED"}

    # 8. Ad group
    if "Created ad group" in actions_text or "already enabled" in actions_text:
        checks["ad_group"] = {"status": "PASS"}
    elif "Enabled paused ad group" in actions_text:
        checks["ad_group"] = {"status": "PASS"}
    elif "Failed to enable ad group" in errors_text:
        checks["ad_group"] = {"status": "FAIL", "reason": "ENABLE_FAILED"}
    elif "not found in XML feed" in errors_text:
        checks["ad_group"] = {"status": "FAIL", "reason": "NOT_IN_FEED"}
    elif report.get("skipped"):
        checks["ad_group"] = {"status": "SKIP", "reason": "SKIPPED_COMPLETE"}
    else:
        checks["ad_group"] = {"status": "SKIP", "reason": "NOT_ATTEMPTED"}

    # 9. Data-driven copy quality (new check)
    if "data-driven ad copy" in actions_text.lower():
        checks["data_driven"] = {"status": "PASS"}
    elif "template-based ad copy" in actions_text.lower():
        checks["data_driven"] = {"status": "WARN", "reason": "TEMPLATE_ONLY"}
    else:
        checks["data_driven"] = {"status": "SKIP", "reason": "NOT_TRACKED"}

    return checks


# ---------------------------------------------------------------------------
# Time Analysis Core (standalone, callable from batch_analytics orchestrator)
# ---------------------------------------------------------------------------

def _batch_time_analysis_core(
    country_code: str,
    campaign_ids=None,
    start_date: str = "",
    end_date: str = "",
    min_impressions_hour: int = 50,
    roas_threshold_low: float = 1.0,
    roas_threshold_high: float = 1.5,
    generate_schedules: bool = True,
) -> dict:
    """Core time-of-day + day-of-week analysis. Callable directly or via MCP tool."""
    from google_ads_mcp import _execute_gaql, _safe_get_value
    from collections import defaultdict

    cc = country_code.upper()

    # 1. Discover campaigns if not specified
    if not campaign_ids:
        disc_query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   campaign.advertising_channel_type
            FROM campaign
            WHERE campaign.status = 'ENABLED'
              AND campaign.advertising_channel_type = 'SEARCH'
              AND campaign.name LIKE '{cc} %'
        """
        disc_results = _execute_gaql(CUSTOMER_ID, disc_query, page_size=100)
        campaign_ids = []
        campaign_names = {}
        for row in disc_results:
            cid = str(_safe_get_value(row, "campaign.id"))
            cname = _safe_get_value(row, "campaign.name")
            campaign_ids.append(cid)
            campaign_names[cid] = cname
    else:
        campaign_names = {cid: cid for cid in campaign_ids}

    if not campaign_ids:
        return {"status": "error", "message": f"No ENABLED Search campaigns found for {cc}"}

    # 2. Pull hourly + DOW data per campaign
    hourly_data = defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "conversions": 0.0,
        "conversions_value": 0.0, "cost_micros": 0
    })
    dow_data = defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "conversions": 0.0,
        "conversions_value": 0.0, "cost_micros": 0
    })
    campaign_hourly = defaultdict(lambda: defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "conversions": 0.0,
        "conversions_value": 0.0, "cost_micros": 0
    }))

    total_rows = 0
    for cid in campaign_ids:
        h_query = f"""
            SELECT campaign.id, segments.hour,
                   metrics.impressions, metrics.clicks,
                   metrics.conversions, metrics.conversions_value,
                   metrics.cost_micros
            FROM campaign
            WHERE campaign.id = {cid}
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
              AND metrics.impressions > 0
        """
        try:
            h_results = _execute_gaql(CUSTOMER_ID, h_query, page_size=1000)
            for row in h_results:
                hour = int(_safe_get_value(row, "segments.hour", 0))
                impr = int(_safe_get_value(row, "metrics.impressions", 0))
                clicks = int(_safe_get_value(row, "metrics.clicks", 0))
                convs = float(_safe_get_value(row, "metrics.conversions", 0))
                conv_val = float(_safe_get_value(row, "metrics.conversions_value", 0))
                cost = int(_safe_get_value(row, "metrics.cost_micros", 0))
                hourly_data[hour]["impressions"] += impr
                hourly_data[hour]["clicks"] += clicks
                hourly_data[hour]["conversions"] += convs
                hourly_data[hour]["conversions_value"] += conv_val
                hourly_data[hour]["cost_micros"] += cost
                campaign_hourly[cid][hour]["impressions"] += impr
                campaign_hourly[cid][hour]["clicks"] += clicks
                campaign_hourly[cid][hour]["conversions"] += convs
                campaign_hourly[cid][hour]["conversions_value"] += conv_val
                campaign_hourly[cid][hour]["cost_micros"] += cost
                total_rows += 1
        except Exception:
            pass

        dow_query = f"""
            SELECT campaign.id, segments.day_of_week,
                   metrics.impressions, metrics.clicks,
                   metrics.conversions, metrics.conversions_value,
                   metrics.cost_micros
            FROM campaign
            WHERE campaign.id = {cid}
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
              AND metrics.impressions > 0
        """
        try:
            d_results = _execute_gaql(CUSTOMER_ID, dow_query, page_size=1000)
            for row in d_results:
                dow = str(_safe_get_value(row, "segments.day_of_week", "UNKNOWN"))
                impr = int(_safe_get_value(row, "metrics.impressions", 0))
                clicks = int(_safe_get_value(row, "metrics.clicks", 0))
                convs = float(_safe_get_value(row, "metrics.conversions", 0))
                conv_val = float(_safe_get_value(row, "metrics.conversions_value", 0))
                cost = int(_safe_get_value(row, "metrics.cost_micros", 0))
                dow_data[dow]["impressions"] += impr
                dow_data[dow]["clicks"] += clicks
                dow_data[dow]["conversions"] += convs
                dow_data[dow]["conversions_value"] += conv_val
                dow_data[dow]["cost_micros"] += cost
        except Exception:
            pass

    if total_rows == 0:
        return {"status": "error", "message": f"No hourly data found for {cc} campaigns in {start_date} - {end_date}"}

    # 3. Calculate derived metrics
    def _calc_metrics(data):
        cost_gbp = data["cost_micros"] / 1_000_000
        ctr = (data["clicks"] / data["impressions"] * 100) if data["impressions"] > 0 else 0
        cpc = (cost_gbp / data["clicks"]) if data["clicks"] > 0 else 0
        conv_rate = (data["conversions"] / data["clicks"] * 100) if data["clicks"] > 0 else 0
        cpa = (cost_gbp / data["conversions"]) if data["conversions"] > 0 else 0
        roas = (data["conversions_value"] / cost_gbp) if cost_gbp > 0 else 0
        return {
            "impressions": data["impressions"], "clicks": data["clicks"],
            "conversions": round(data["conversions"], 2),
            "conversion_value": round(data["conversions_value"], 2),
            "cost_gbp": round(cost_gbp, 2),
            "ctr": round(ctr, 2), "cpc": round(cpc, 2),
            "conv_rate": round(conv_rate, 2), "cpa": round(cpa, 2), "roas": round(roas, 2),
        }

    hourly_metrics = {}
    for hour in range(24):
        if hour in hourly_data and hourly_data[hour]["impressions"] >= min_impressions_hour:
            hourly_metrics[hour] = _calc_metrics(hourly_data[hour])
        elif hour in hourly_data:
            hourly_metrics[hour] = _calc_metrics(hourly_data[hour])
            hourly_metrics[hour]["low_data"] = True

    dow_metrics = {}
    for dow in ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]:
        if dow in dow_data:
            dow_metrics[dow] = _calc_metrics(dow_data[dow])

    # 4. Time blocks
    blocks = {
        "Night (0-5)": list(range(0, 6)), "Morning (6-9)": list(range(6, 10)),
        "Day (10-14)": list(range(10, 15)), "Afternoon (15-18)": list(range(15, 19)),
        "Evening (19-23)": list(range(19, 24)),
    }
    block_metrics = {}
    for block_name, hours in blocks.items():
        agg = {"impressions": 0, "clicks": 0, "conversions": 0.0,
               "conversions_value": 0.0, "cost_micros": 0}
        for h in hours:
            if h in hourly_data:
                for k in agg:
                    agg[k] += hourly_data[h][k]
        block_metrics[block_name] = _calc_metrics(agg)

    # 5. Waste & peak analysis
    total_cost = sum(hourly_data[h]["cost_micros"] for h in hourly_data) / 1_000_000
    waste_hours = []
    peak_hours = []
    for hour, m in hourly_metrics.items():
        if m.get("low_data"):
            continue
        if m["roas"] < roas_threshold_low and m["cost_gbp"] > 0:
            waste_hours.append({"hour": hour, "roas": m["roas"], "cost_gbp": m["cost_gbp"],
                                "conversions": m["conversions"],
                                "potential_savings": round(m["cost_gbp"] - m["conversion_value"], 2)})
        elif m["roas"] >= roas_threshold_high:
            peak_hours.append({"hour": hour, "roas": m["roas"], "cost_gbp": m["cost_gbp"],
                               "conversions": m["conversions"], "conv_rate": m["conv_rate"]})
    waste_total = sum(w["cost_gbp"] for w in waste_hours)
    waste_pct = round(waste_total / total_cost * 100, 1) if total_cost > 0 else 0

    # 6. Schedule recommendations
    schedules_recommendation = []
    if generate_schedules:
        avg_roas = sum(m["roas"] for m in hourly_metrics.values()) / len(hourly_metrics) if hourly_metrics else 1.0
        for hour in range(24):
            m = hourly_metrics.get(hour, {})
            roas = m.get("roas", 0)
            convs = m.get("conversions", 0)
            cost = m.get("cost_gbp", 0)
            if cost > 0 and convs == 0 and cost > 1.0:
                bid_mod, category = 0.7, "dead"
            elif roas < roas_threshold_low * 0.5 and cost > 0:
                bid_mod, category = 0.7, "waste_severe"
            elif roas < roas_threshold_low and cost > 0:
                bid_mod, category = 0.85, "waste"
            elif roas >= roas_threshold_high * 1.5:
                bid_mod, category = 1.25, "peak_strong"
            elif roas >= roas_threshold_high:
                bid_mod, category = 1.15, "peak"
            else:
                bid_mod, category = 1.0, "normal"
            schedules_recommendation.append({"hour": hour, "bid_modifier": bid_mod, "category": category,
                                              "roas": round(roas, 2), "cost_gbp": round(cost, 2),
                                              "conversions": round(convs, 2)})

    # 7. Ad schedule payload
    ad_schedule_payload = []
    if generate_schedules and schedules_recommendation:
        for dow in ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]:
            i = 0
            while i < 24:
                bid_mod = schedules_recommendation[i]["bid_modifier"]
                start_hour = i
                while i < 24 and schedules_recommendation[i]["bid_modifier"] == bid_mod:
                    i += 1
                ad_schedule_payload.append({"day_of_week": dow, "start_hour": start_hour,
                    "start_minute": "ZERO", "end_hour": i, "end_minute": "ZERO", "bid_modifier": bid_mod})

    # 8. Per-campaign breakdown
    per_campaign = {}
    for cid in campaign_ids:
        total_impr = sum(campaign_hourly[cid][h]["impressions"] for h in campaign_hourly[cid])
        total_clicks = sum(campaign_hourly[cid][h]["clicks"] for h in campaign_hourly[cid])
        total_convs = sum(campaign_hourly[cid][h]["conversions"] for h in campaign_hourly[cid])
        total_conv_val = sum(campaign_hourly[cid][h]["conversions_value"] for h in campaign_hourly[cid])
        total_cost_c = sum(campaign_hourly[cid][h]["cost_micros"] for h in campaign_hourly[cid]) / 1_000_000
        per_campaign[cid] = {
            "name": campaign_names.get(cid, cid), "impressions": total_impr,
            "clicks": total_clicks, "conversions": round(total_convs, 2),
            "conversion_value": round(total_conv_val, 2), "cost_gbp": round(total_cost_c, 2),
            "roas": round(total_conv_val / total_cost_c, 2) if total_cost_c > 0 else 0,
        }

    return {
        "status": "success", "country_code": cc,
        "period": f"{start_date} to {end_date}",
        "campaigns_analyzed": len(campaign_ids), "campaign_details": per_campaign,
        "total_data_rows": total_rows,
        "hourly_metrics": hourly_metrics, "dow_metrics": dow_metrics,
        "time_blocks": block_metrics,
        "waste_analysis": {"waste_hours": sorted(waste_hours, key=lambda x: x["roas"]),
            "waste_total_gbp": round(waste_total, 2), "waste_pct_of_total": waste_pct,
            "total_cost_gbp": round(total_cost, 2)},
        "peak_analysis": {"peak_hours": sorted(peak_hours, key=lambda x: -x["roas"]),
            "peak_count": len(peak_hours)},
        "schedule_recommendations": schedules_recommendation,
        "ad_schedule_payload": ad_schedule_payload,
        "ad_schedule_count": len(ad_schedule_payload),
        "implementation_notes": {
            "step_1": "Review hourly_metrics and schedule_recommendations",
            "step_2": "If approved, call google_ads_remove_ad_schedule to clear existing schedules",
            "step_3": "Call google_ads_create_ad_schedule with ad_schedule_payload",
            "step_4": "Monitor for 2 weeks, then re-run analysis to measure impact",
            "smart_bidding_note": "With Smart Bidding, bid modifiers are SIGNALS — the algorithm may override.",
        }
    }


# ---------------------------------------------------------------------------
# MCP Tool Registration
# ---------------------------------------------------------------------------

def register_batch_tools(mcp_app):
    """Register batch optimizer tools on the MCP server instance."""

    @mcp_app.tool()
    async def batch_setup_products(request: BatchSetupRequest) -> dict:
        """
        Batch-create ad groups, RSAs (A/B/C with 12 headlines + 4 descriptions),
        callouts, sitelinks, promotions, structured snippets, and image extensions
        for multiple products.

        Fetches product data from XML feed, checks existing ad groups and assets,
        and only creates what's missing. Images are fetched from the local
        find_products_images.php endpoint (partners-API packshots) with XML feed
        CDN fallback. Use dry_run=true to preview changes.

        Supported countries: RO, TR, PL, HU, FR, DE, IT, ES, CZ, SK, BG, GR, HR,
        SI, LT, EE, LV, UA (more can be added in COUNTRY_CONFIG).
        """
        country_code = request.country_code.upper()
        config = _get_config_for_country(country_code)

        # Feed URL resolution: explicit > NP_FEEDS > XML_FEEDS
        if request.xml_feed_url:
            xml_url = request.xml_feed_url
        elif request.feed_type == "new_products":
            xml_url = _resolve_np_feed_url(country_code, request.shop_domain)
            if not xml_url:
                return {
                    "status": "error",
                    "message": f"No NP_FEEDS BW feed for {country_code}" + (f" / {request.shop_domain}" if request.shop_domain else "") + ". Provide xml_feed_url manually.",
                }
        elif config.get("xml_url"):
            xml_url = config["xml_url"]
        else:
            return {
                "status": "error",
                "message": f"No XML feed URL for country {country_code}. Provide xml_feed_url or feed_type='new_products'.",
            }

        # 1. Fetch XML feed (cached)
        try:
            raw_products = _fetch_xml_feed_cached(country_code, xml_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch XML feed: {e}"}

        feed_products = {p["handle"]: p for p in raw_products}
        db = _get_db()

        # Force refresh from API when force_refresh=true or skip_existing=false (retry mode)
        # Prevents stale REMOVED ad group IDs from DB/session cache
        force_url_refresh = request.force_refresh or not request.skip_existing

        # 2. Get existing ad groups (name-based), with session cache
        try:
            existing_ad_groups = _get_existing_ad_groups(request.campaign_id, force_refresh=force_url_refresh)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch ad groups: {e}"}

        # 2b. Get URL-based handle->ad_group map (primary matching method)
        url_map = {} if force_url_refresh else db.get_cached_url_map(country_code, request.campaign_id)
        if not url_map:
            try:
                url_map = _get_ad_group_url_map(request.campaign_id, force_refresh=True)
                if url_map:
                    db.save_url_map(country_code, request.campaign_id, url_map)
            except Exception as e:
                _debug("exception", f"url_map fallback at line 4492: {e}", "warning")
                url_map = {}

        # 3. Get existing assets (with session cache)
        try:
            existing_assets = _get_ad_group_assets(request.campaign_id, force_refresh=request.force_refresh)
        except Exception as e:
            existing_assets = {}

        # 4. Process each product with DB tracking
        results = []
        total = len(request.product_handles)
        success_count = 0
        error_count = 0

        for i, handle in enumerate(request.product_handles):
            t0 = time.time()

            # Check DB for skip_existing
            # Only skip products that are FULLY complete (including images).
            # complete_no_images → retry to add images that may now be available.
            if request.skip_existing and not request.dry_run:
                db_product = db.get_product(country_code, request.campaign_id, handle)
                if db_product and db_product.get("status") == "complete":
                    results.append({
                        "handle": handle,
                        "actions": [f"Skipped (complete in DB)"],
                        "errors": [],
                        "skipped": True,
                    })
                    success_count += 1
                    continue

            report = process_single_product(
                handle=handle,
                feed_products=feed_products,
                existing_ad_groups=existing_ad_groups,
                existing_assets=existing_assets,
                campaign_id=request.campaign_id,
                config=config,
                country_code=country_code,
                dry_run=request.dry_run,
                url_map=url_map,
                asset_types=request.asset_types,
                force_replace_rsa=request.force_replace_rsa,
                feed_domain_override=request.shop_domain,
            )
            results.append(report)
            duration_ms = int((time.time() - t0) * 1000)

            if report["errors"]:
                error_count += 1
            else:
                success_count += 1

            # Track in DB (skip for dry_run)
            if not request.dry_run:
                _track_product_in_db(
                    db, country_code, request.campaign_id, handle,
                    report, feed_products.get(handle, {}), duration_ms,
                    asset_types=request.asset_types,
                )

            # Small delay between products to avoid API rate limits
            if not request.dry_run and i < total - 1:
                time.sleep(1.5)

        # Auto-log to changelog
        if not request.dry_run and total > 0:
            handles_str = ", ".join(request.product_handles[:5])
            if len(request.product_handles) > 5:
                handles_str += f" +{len(request.product_handles) - 5} more"
            _auto_changelog(
                category="rollout",
                title=f"batch_setup_products: {success_count}/{total} OK",
                description=f"Products: {handles_str}. Errors: {error_count}. force_replace_rsa={request.force_replace_rsa}",
                country_code=country_code,
                campaign_id=request.campaign_id,
            )

        return {
            "status": "success",
            "summary": {
                "total_products": total,
                "success": success_count,
                "with_errors": error_count,
                "dry_run": request.dry_run,
            },
            "results": results,
        }

    class BatchUpgradeToAiRequest(BaseModel):
        """Input for batch_upgrade_to_ai tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        product_handles: Optional[List[str]] = Field(None, description="Specific handles to upgrade. If None, upgrades all products with template-generated RSAs.")
        dry_run: bool = Field(False, description="Preview only, no changes")
        max_products: int = Field(0, description="Max products to process (0 = all)")

    @mcp_app.tool()
    async def batch_upgrade_to_ai(request: BatchUpgradeToAiRequest) -> dict:
        """
        Upgrade template-generated RSAs to AI-powered copy.

        Finds products that have RSAs created from template/data-driven fallback
        (not AI endpoint), REMOVES only RSAs without stats (0 impressions, 0 clicks),
        and creates new AI-generated ones. RSAs with ANY stats are PRESERVED.
        Requires the AI endpoint (configurable via config.json endpoints.ad_generator)
        to be accessible.

        Use this after fixing endpoint connectivity issues to mass-upgrade
        all products that were initially set up with fallback templates.
        """
        country_code = request.country_code.upper()

        # 1. Test AI endpoint connectivity — just TCP connect + HTTP response
        #    Don't wait for full processing (pipeline is slow for invalid handles)
        try:
            import urllib.request
            import ssl
            import socket
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            # Quick TCP connectivity test — just check if server accepts connections
            from urllib.parse import urlparse
            parsed = urlparse(AD_GENERATOR_BASE_URL)
            host = parsed.hostname
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            sock = socket.create_connection((host, port), timeout=5)
            sock.close()
            endpoint_ok = True
        except Exception as e:
            return {
                "status": "error",
                "message": f"AI endpoint not reachable (TCP connect failed): {e}. Fix connectivity before upgrading.",
                "endpoint_url": AD_GENERATOR_BASE_URL,
            }

        # 2. Get products to upgrade
        handles = request.product_handles
        if not handles:
            # Find all products with ad groups (from DB)
            db = BatchDB()
            all_products = db.get_all_products(country_code, request.campaign_id)
            handles = [
                p["product_handle"] for p in all_products
                if p.get("status") in ("complete", "complete_no_images", "partial")
                and p.get("ad_group_id")
            ]

        if request.max_products > 0:
            handles = handles[:request.max_products]

        if not handles:
            return {"status": "success", "message": "No products found to upgrade", "processed": 0}

        # 3. Use batch_setup_products with force_replace_rsa=true, asset_types=["rsa"]
        setup_request = BatchSetupRequest(
            country_code=country_code,
            campaign_id=request.campaign_id,
            product_handles=handles,
            dry_run=request.dry_run,
            skip_existing=False,  # Don't skip — we want to re-process
            asset_types=["rsa"],  # Only RSAs, don't touch other assets
            force_replace_rsa=True,  # Pause old + create new
        )
        # Reuse batch_setup_products logic
        result = await batch_setup_products(setup_request)
        result["tool"] = "batch_upgrade_to_ai"
        result["endpoint_health"] = "OK" if endpoint_ok else "DEGRADED"
        return result

    @mcp_app.tool()
    async def batch_audit_campaign(request: BatchAuditRequest) -> dict:
        """
        Audit a Search campaign to find which ad groups are missing assets.

        Returns a completeness report showing RSA count, callouts, sitelinks,
        promotions, and structured snippets for each ENABLED ad group.
        """
        country_code = request.country_code.upper()
        config = _get_config_for_country(country_code)

        try:
            ad_groups = _get_existing_ad_groups(request.campaign_id)
            assets = _get_ad_group_assets(request.campaign_id)
        except Exception as e:
            return {"status": "error", "message": str(e)}

        complete = []
        incomplete = []

        for name_lower, ag in ad_groups.items():
            if "ENABLED" not in ag["status"]:
                continue
            ag_id = ag["id"]
            ag_assets = assets.get(ag_id, {"RSA": 0, "CALLOUT": 0, "SITELINK": 0, "PROMOTION": 0, "STRUCTURED_SNIPPET": 0, "AD_IMAGE": 0})

            entry = {
                "name": ag["name"],
                "id": ag_id,
                "assets": ag_assets,
                "missing": [],
            }

            if ag_assets.get("RSA", 0) < 3:
                entry["missing"].append(f"RSA ({ag_assets.get('RSA', 0)}/3)")
            if ag_assets.get("CALLOUT", 0) == 0:
                entry["missing"].append("CALLOUT")
            if ag_assets.get("SITELINK", 0) == 0:
                entry["missing"].append("SITELINK")
            if ag_assets.get("PROMOTION", 0) == 0:
                entry["missing"].append("PROMOTION")
            if ag_assets.get("STRUCTURED_SNIPPET", 0) == 0:
                entry["missing"].append("STRUCTURED_SNIPPET")
            if ag_assets.get("AD_IMAGE", 0) < IMAGE_MIN_EXISTING:
                entry["missing"].append(f"AD_IMAGE ({ag_assets.get('AD_IMAGE', 0)}/{IMAGE_MIN_EXISTING})")

            if entry["missing"]:
                incomplete.append(entry)
            else:
                complete.append(entry)

        return {
            "status": "success",
            "campaign_id": request.campaign_id,
            "country": country_code,
            "total_enabled": len(complete) + len(incomplete),
            "complete": len(complete),
            "incomplete": len(incomplete),
            "incomplete_details": incomplete,
            "complete_names": [e["name"] for e in complete],
        }

    @mcp_app.tool()
    async def batch_generate_ad_copy(request: AdCopyGenerateRequest) -> dict:
        """
        Generate localized ad copy for a product (RSA A/B/C headlines + descriptions,
        callouts, structured snippets). Uses built-in templates optimized per country.

        Returns structured JSON ready for use with batch_setup_products or manual RSA creation.
        """
        country_code = request.country_code.upper()
        config = _get_config_for_country(country_code)

        ad_copy = _generate_ad_copy_local(
            country_code=country_code,
            product_name=request.product_name,
            price=request.product_price,
            product_url=request.product_url,
            config=config,
        )

        return {
            "status": "success",
            "product_name": request.product_name,
            "country": country_code,
            "language": config["language"],
            "ad_copy": ad_copy,
        }

    # ------------------------------------------------------------------
    # New DB-backed tools
    # ------------------------------------------------------------------

    class BatchStatusRequest(BaseModel):
        """Input for batch_status tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")

    class BatchMissingRequest(BaseModel):
        """Input for batch_missing tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")

    class BatchSyncRequest(BaseModel):
        """Input for batch_sync_from_api tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        include_feed: bool = Field(True, description="Also load XML feed handles into DB")
        feed_type: Optional[str] = Field(None, description="Feed source: 'main' (default) or 'new_products'. When 'new_products', discovery uses NP_FEEDS BW feeds.")
        shop_domain: Optional[str] = Field(None, description="Filter discovery to specific shop domain (e.g., 'www.yourstore.com'). Only with feed_type='new_products'. None = merge all shops.")

    class BatchLogsRequest(BaseModel):
        """Input for batch_logs tool."""
        country_code: Optional[str] = Field(None, description="Filter by country code")
        campaign_id: Optional[str] = Field(None, description="Filter by campaign ID")
        product_handle: Optional[str] = Field(None, description="Filter by product handle")
        operation: Optional[str] = Field(None, description="Filter by operation type")
        limit: int = Field(50, description="Max results to return")

    class BatchClearCacheRequest(BaseModel):
        """Input for batch_clear_cache tool."""
        cache_type: str = Field("all", description="Cache to clear: 'gaql', 'feed', or 'all'")
        country_code: Optional[str] = Field(None, description="Clear feed cache for specific country only")

    class BatchResetErrorsRequest(BaseModel):
        """Input for batch_reset_errors tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        error_filter: Optional[str] = Field(None, description="Only reset errors containing this substring (e.g., 'DNS', '503'). None=all errors.")

    @mcp_app.tool()
    async def batch_reset_errors(request: BatchResetErrorsRequest) -> dict:
        """
        Reset products with 'error' status back to 'pending' for retry.

        Use after transient failures (DNS, 503, rate limits) are resolved.
        Optionally filter by error substring to reset only specific error types.
        After reset, run batch_setup_all with skip_existing=True to process them.
        """
        db = _get_db()
        cc = request.country_code.upper()
        conn = db._get_conn()

        # Count errors before reset
        if request.error_filter:
            rows = conn.execute(
                "SELECT COUNT(*) as cnt FROM product_setup WHERE country_code=? AND campaign_id=? AND status='error' AND last_error LIKE ?",
                (cc, request.campaign_id, f"%{request.error_filter}%")
            ).fetchone()
        else:
            rows = conn.execute(
                "SELECT COUNT(*) as cnt FROM product_setup WHERE country_code=? AND campaign_id=? AND status='error'",
                (cc, request.campaign_id)
            ).fetchone()

        count = rows["cnt"] if rows else 0

        if count == 0:
            return {"status": "success", "message": "No error products to reset.", "reset_count": 0}

        # Reset status to pending and clear last_error
        if request.error_filter:
            conn.execute(
                "UPDATE product_setup SET status='pending', last_error=NULL, last_updated_at=datetime('now') "
                "WHERE country_code=? AND campaign_id=? AND status='error' AND last_error LIKE ?",
                (cc, request.campaign_id, f"%{request.error_filter}%")
            )
        else:
            conn.execute(
                "UPDATE product_setup SET status='pending', last_error=NULL, last_updated_at=datetime('now') "
                "WHERE country_code=? AND campaign_id=? AND status='error'",
                (cc, request.campaign_id)
            )
        conn.commit()

        return {
            "status": "success",
            "message": f"Reset {count} error products to pending.",
            "reset_count": count,
            "error_filter": request.error_filter,
            "next_step": "Run batch_setup_all with skip_existing=True to process them.",
        }

    @mcp_app.tool()
    async def batch_status(request: BatchStatusRequest) -> dict:
        """
        Get setup status for all products in a campaign.

        Returns aggregated stats: total products, by-status breakdown,
        asset totals, and recent operation counts.
        """
        db = _get_db()
        return {
            "status": "success",
            **db.get_setup_summary(request.country_code.upper(), request.campaign_id),
        }

    @mcp_app.tool()
    async def batch_missing(request: BatchMissingRequest) -> dict:
        """
        Get list of products with incomplete setup.

        Shows which specific assets are missing for each product
        (RSA A/B/C, callouts, sitelinks, promotion, snippets, images).
        """
        db = _get_db()
        report = db.get_missing_assets_report(request.country_code.upper(), request.campaign_id)
        return {
            "status": "success",
            "total_incomplete": len(report),
            "products": report,
        }

    @mcp_app.tool()
    async def batch_sync_from_api(request: BatchSyncRequest) -> dict:
        """
        Sync DB state from actual Google Ads API (full audit).

        Queries Google Ads for all ad groups and their assets in the campaign,
        then updates the DB to reflect reality. Also optionally loads XML feed
        handles to identify products without ad groups.
        """
        db = _get_db()
        country_code = request.country_code.upper()

        try:
            # v3.2.0: Always force_refresh for sync — stale cache defeats the purpose
            ad_groups = _get_existing_ad_groups(request.campaign_id, force_refresh=True)
            assets = _get_ad_group_assets(request.campaign_id, force_refresh=True)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch from API: {e}"}

        # Build URL-based handle map for precise matching
        try:
            url_map = _get_ad_group_url_map(request.campaign_id)
            # Persist URL map to DB as foundation for future lookups
            if url_map:
                db.save_url_map(country_code, request.campaign_id, url_map)
        except Exception as e:
            _debug("exception", f"url_map fallback at line 4901: {e}", "warning")
            url_map = {}

        feed_handles = None
        if request.include_feed:
            # NEW_PRODUCTS support: use NP_FEEDS BW feeds when feed_type='new_products'
            if request.feed_type == "new_products":
                try:
                    feed_handles = _load_np_feed_handles(country_code, request.shop_domain)
                    if not feed_handles:
                        return {"status": "error", "message": f"No NP_FEEDS BW feed found for {country_code}" + (f" / {request.shop_domain}" if request.shop_domain else "")}
                    _debug("sync", f"Loaded {len(feed_handles)} handles from NP_FEEDS for {country_code}")
                except Exception as e:
                    return {"status": "error", "message": f"Failed to fetch NP feed: {e}"}
            else:
                config = _get_config_for_country(country_code)
                xml_url = config.get("xml_url", "")
                if xml_url:
                    try:
                        raw_products = _fetch_xml_feed_cached(country_code, xml_url)
                        feed_handles = [p["handle"] for p in raw_products]
                    except Exception as e:
                        return {"status": "error", "message": f"Failed to fetch XML feed: {e}"}

        sync_result = db.sync_from_audit(
            country_code, request.campaign_id,
            ad_groups, assets, feed_handles,
            url_map=url_map,
        )

        # Log the sync operation
        db.log_operation(
            country_code, request.campaign_id, "*",
            "sync_from_api", "success",
            details=sync_result,
        )

        summary = db.get_setup_summary(country_code, request.campaign_id)

        return {
            "status": "success",
            "sync": sync_result,
            "summary": summary,
        }

    @mcp_app.tool()
    async def batch_enable_feed_ad_groups(request: BatchSyncRequest) -> dict:
        """
        Find and enable PAUSED ad groups that match products in the XML feed.

        Uses URL-based matching (parsing product handles from RSA final_urls)
        as primary method, with name-based matching as fallback.
        This ensures all feed products have ENABLED ad groups.

        Returns list of enabled ad groups and any unmatched feed products.
        """
        country_code = request.country_code.upper()
        config = _get_config_for_country(country_code)

        # 1. Get all ad groups (including PAUSED)
        try:
            all_ad_groups = _get_existing_ad_groups(request.campaign_id)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch ad groups: {e}"}

        # 2. Build URL map for precise matching (DB cache first, then API)
        db = _get_db()
        url_map = db.get_cached_url_map(country_code, request.campaign_id)
        if not url_map:
            try:
                url_map = _get_ad_group_url_map(request.campaign_id)
                if url_map:
                    db.save_url_map(country_code, request.campaign_id, url_map)
            except Exception as e:
                _debug("exception", f"url_map fallback at line 4965: {e}", "warning")
                url_map = {}

        # 3. Get feed products (NP_FEEDS support)
        if request.feed_type == "new_products":
            np_feed_url = _resolve_np_feed_url(country_code, request.shop_domain)
            if not np_feed_url:
                return {"status": "error", "message": f"No NP_FEEDS BW feed for {country_code}" + (f" / {request.shop_domain}" if request.shop_domain else "")}
            xml_url = np_feed_url
        else:
            xml_url = config.get("xml_url", "")
            if not xml_url:
                return {"status": "error", "message": f"No XML feed URL for {country_code}"}

        try:
            raw_products = _fetch_xml_feed_cached(country_code, xml_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch feed: {e}"}

        feed_handles = {p["handle"] for p in raw_products}

        # 4. For each feed product, find matching PAUSED ad group and enable it
        enabled = []
        already_enabled = []
        unmatched = []

        # Build reverse lookup: ag_id -> ad group info (with status from API)
        ag_by_id = {}
        for _, ag_info in all_ad_groups.items():
            ag_by_id[ag_info["id"]] = ag_info

        for handle in sorted(feed_handles):
            # PRIMARY: URL-based matching
            ag_match = url_map.get(handle)
            match_source = "url" if ag_match else None

            if not ag_match:
                # FALLBACK: Name-based matching
                product_name = _handle_to_product_name(handle)
                ag_match = all_ad_groups.get(product_name.lower())
                match_source = "name" if ag_match else None

            if not ag_match:
                unmatched.append(handle)
                continue

            ag_id = ag_match["id"]
            ag_name = ag_match.get("name", "")
            # Get live status from all_ad_groups (url_map cache may not have status)
            live_info = ag_by_id.get(ag_id, {})
            status = live_info.get("status", ag_match.get("status", ""))

            if "PAUSED" in status:
                try:
                    _enable_ad_group(ag_id)
                    enabled.append({
                        "handle": handle,
                        "ad_group_id": ag_id,
                        "ad_group_name": ag_name,
                        "match_source": match_source,
                    })
                except Exception as e:
                    unmatched.append(f"{handle} (enable failed: {e})")
            elif "ENABLED" in status:
                already_enabled.append(handle)

        db = _get_db()
        db.log_operation(
            country_code, request.campaign_id, "*",
            "batch_enable_feed", "success",
            details={
                "enabled_count": len(enabled),
                "already_enabled": len(already_enabled),
                "unmatched": len(unmatched),
            },
        )

        return {
            "status": "success",
            "enabled": enabled,
            "enabled_count": len(enabled),
            "already_enabled_count": len(already_enabled),
            "unmatched": unmatched,
            "unmatched_count": len(unmatched),
            "total_feed_products": len(feed_handles),
        }

    @mcp_app.tool()
    async def batch_logs(request: BatchLogsRequest) -> dict:
        """
        Get operation logs for a product or campaign.

        Shows timestamped log of all batch operations with details,
        useful for debugging and audit trail.
        """
        db = _get_db()
        logs = db.get_operations(
            country_code=request.country_code.upper() if request.country_code else None,
            campaign_id=request.campaign_id,
            handle=request.product_handle,
            operation=request.operation,
            limit=request.limit,
        )
        return {
            "status": "success",
            "count": len(logs),
            "logs": logs,
        }

    @mcp_app.tool()
    async def batch_clear_cache(request: BatchClearCacheRequest) -> dict:
        """
        Clear GAQL and/or XML feed caches.

        Use when you need fresh data from Google Ads API or XML feed.
        Use cache_type='purge_products' with country_code to DELETE all product
        entries from DB for a country (requires re-sync after).
        """
        db = _get_db()
        cleared = []
        if request.cache_type == "purge_products":
            # Delete all products for this country from DB — nuclear option
            cc = request.country_code.upper() if request.country_code else None
            if not cc:
                return {"status": "error", "message": "country_code required for purge_products"}
            # Find all campaign_ids for this country
            conn = db._get_conn()
            rows = conn.execute(
                "SELECT DISTINCT campaign_id FROM product_setup WHERE country_code = ?", (cc,)
            ).fetchall()
            total_deleted = 0
            for row in rows:
                deleted = db.delete_campaign_products(cc, row["campaign_id"])
                total_deleted += deleted
            cleared.append(f"purge_products({cc}): {total_deleted} deleted")
            _session_cache.clear()
            return {"status": "success", "cleared": cleared, "deleted_count": total_deleted}

        if request.cache_type in ("gaql", "all"):
            db.clear_gaql_cache()
            cleared.append("gaql")
            # Also clear URL map cache (may contain stale REMOVED ad group IDs)
            cc = request.country_code.upper() if request.country_code else None
            db.clear_url_map(country_code=cc)
            cleared.append("url_map")
            # Clear in-memory session cache too
            _session_cache.clear()
        if request.cache_type in ("feed", "all"):
            db.clear_feed_cache(request.country_code.upper() if request.country_code else None)
            cleared.append("feed")
        # MCP-side response caches (AI copy + image candidates)
        if request.cache_type in ("ai_copy", "images", "all"):
            cc = request.country_code.upper() if request.country_code else None
            db.clear_response_caches(cache_type=request.cache_type, country_code=cc)
            if request.cache_type == "all":
                cleared.append("ai_copy_cache")
                cleared.append("image_candidates_cache")
            else:
                cleared.append(f"{request.cache_type}_cache")
        return {
            "status": "success",
            "cleared": cleared,
        }

    # ------------------------------------------------------------------
    # Queue — smart product prioritization for agent processing
    # ------------------------------------------------------------------

    class QueueNextRequest(BaseModel):
        """Input for batch_queue_next tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        count: int = Field(5, description="Number of products to return (1-20)", ge=1, le=20)
        status_filter: str = Field(None, description="Filter by specific status: partial, pending, error")
        include_errors: bool = Field(False, description="Include error products in queue")

    @mcp_app.tool()
    async def batch_queue_next(request: QueueNextRequest) -> dict:
        """
        Get next N products to process from the queue.

        Returns products prioritized for agent processing:
        1. 'partial' first (have ad group, missing assets — FAST, fits in 60s MCP timeout)
        2. 'error' if include_errors=True (retry failed — may or may not fit in 60s)
        3. 'pending' last (new products, need full setup — SLOW, may timeout)

        Each product includes: handle, status, missing assets, priority (fast/slow).
        Agent should process 'fast' products with batch_setup_products (1 at a time),
        and 'slow' products with batch_setup_all (server-side, auto-resume).
        """
        db = _get_db()
        products = db.queue_next(
            country_code=request.country_code.upper(),
            campaign_id=request.campaign_id,
            count=request.count,
            status_filter=request.status_filter,
            include_errors=request.include_errors,
        )
        stats = db.queue_stats(request.country_code.upper(), request.campaign_id)
        return {
            "status": "success",
            "queue": products,
            "stats": stats,
            "instructions": {
                "fast": "Use batch_setup_products with 1 handle — fits in 60s",
                "slow": "Use batch_setup_all with max_products=5 — auto-resumes on timeout",
            },
        }

    # ------------------------------------------------------------------
    # Product Dashboard — cached summary per product
    # ------------------------------------------------------------------

    class ProductDashboardRequest(BaseModel):
        """Input for batch_product_dashboard tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        status_filter: str = Field(None, description="Filter by status: complete, partial, pending, error, complete_no_images")
        limit: int = Field(50, description="Max products to return", ge=1, le=200)
        offset: int = Field(0, description="Offset for pagination", ge=0)

    @mcp_app.tool()
    async def batch_product_dashboard(request: ProductDashboardRequest) -> dict:
        """
        Cached product dashboard with asset completeness per product.

        Returns a compact table showing each product's status and which assets
        are present (RSA count, callouts, sitelinks, promotion, snippets,
        keywords, images). Sorted: errors first, then partial, pending, complete.

        Fast — reads from DB cache only, no API calls.
        Use status_filter to focus on specific groups (e.g., 'error' to see failures).
        Use limit/offset for pagination on large campaigns.
        """
        db = _get_db()
        dashboard = db.product_dashboard(
            country_code=request.country_code.upper(),
            campaign_id=request.campaign_id,
            status_filter=request.status_filter,
            limit=request.limit,
            offset=request.offset,
        )
        stats = db.queue_stats(request.country_code.upper(), request.campaign_id)
        return {
            "status": "success",
            "dashboard": dashboard,
            "stats": stats,
        }

    # ------------------------------------------------------------------
    # Changelog — persistent change tracking across sessions
    # ------------------------------------------------------------------

    class ChangelogAddRequest(BaseModel):
        """Input for adding a changelog entry."""
        category: str = Field(..., description="Category: code_fix, feature, config, rollout, bugfix, optimization, manual")
        title: str = Field(..., description="Short title of the change")
        description: str = Field("", description="Detailed description")
        details: str = Field("", description="Technical details (code snippets, line numbers, etc.)")
        country_code: str = Field(None, description="Country code if country-specific")
        campaign_id: str = Field(None, description="Campaign ID if campaign-specific")
        severity: str = Field("info", description="Severity: info, warning, critical")

    class ChangelogReadRequest(BaseModel):
        """Input for reading changelog entries."""
        limit: int = Field(20, description="Max entries to return")
        category: str = Field(None, description="Filter by category")
        country_code: str = Field(None, description="Filter by country")
        since: str = Field(None, description="Only entries after this date (YYYY-MM-DD)")

    @mcp_app.tool()
    async def batch_changelog_add(request: ChangelogAddRequest) -> dict:
        """
        Add a changelog entry to track changes, fixes, and decisions.

        Use this to document code fixes, feature additions, config changes,
        rollout progress, and any important decisions. Entries persist in
        the SQLite DB and are shown in batch_get_instructions for session continuity.

        Categories: code_fix, feature, config, rollout, bugfix, optimization, manual
        Severity: info, warning, critical
        """
        db = _get_db()
        entry_id = db.add_changelog_entry(
            category=request.category,
            title=request.title,
            description=request.description,
            details=request.details,
            country_code=request.country_code,
            campaign_id=request.campaign_id,
            severity=request.severity,
            auto_logged=False,
        )
        return {
            "status": "success",
            "entry_id": entry_id,
            "message": f"Changelog entry #{entry_id} added: [{request.category}] {request.title}",
        }

    @mcp_app.tool()
    async def batch_changelog_read(request: ChangelogReadRequest) -> dict:
        """
        Read changelog entries. Shows recent changes, fixes, and decisions.

        Call this at the start of every new conversation to understand
        what was done previously and what's pending. Entries are sorted
        newest-first.
        """
        db = _get_db()
        entries = db.get_changelog(
            limit=request.limit,
            category=request.category,
            country_code=request.country_code,
            since=request.since,
        )
        summary = db.get_changelog_summary()
        return {
            "status": "success",
            "summary": summary,
            "entries": entries,
        }

    @mcp_app.tool()
    async def batch_get_instructions() -> dict:
        """
        Get operational instructions for batch campaign management.

        Returns a comprehensive guide on how to use the batch optimizer system,
        including available tools, workflows, and best practices.
        Call this at the start of a new conversation to bootstrap context.
        """
        return {
            "status": "success",
            "system": "Universal Batch Campaign Optimizer v2",
            "customer_id": CUSTOMER_ID,
            "mcc_id": "7624431817",
            "overview": (
                "This system manages Google Ads Search campaigns "
                "across 20+ countries. It creates ad groups with RSAs (A/B/C variants), callouts, "
                "sitelinks, promotions, structured snippets, keywords, and images for each product."
            ),
            "modules": {
                "batch_optimizer": {
                    "description": "Core module — creates and manages ad groups + all assets",
                    "key_tools": [
                        "batch_setup_products(country_code, campaign_id, product_handles) — main entry point, processes products end-to-end",
                        "batch_audit_campaign(country_code, campaign_id) — checks what's missing per ad group",
                        "batch_generate_ad_copy(country_code, product_name, price, url) — preview ad copy templates",
                    ],
                },
                "batch_intelligence": {
                    "description": "Cross-campaign keyword miner — finds converting keywords from historical campaigns",
                    "key_tools": [
                        "batch_keyword_research(handle, country_code, min_roi) — mine keywords for a product across all campaigns",
                        "batch_category_keywords(category, country_code) — get shared keywords from product category",
                    ],
                    "notes": "Only uses keywords with confirmed ROI > 50%. Groups products into categories (electronics, fashion, home_garden, etc.)",
                },
                "batch_analytics": {
                    "description": "RSA asset performance tracker — analyzes headline/description performance labels",
                    "key_tools": [
                        "batch_analyze_assets(country_code, campaign_id) — scan campaign for BEST/GOOD/LOW patterns",
                        "batch_winning_patterns(country_code, asset_type) — get top-performing patterns for a country",
                    ],
                    "notes": "Generalizes patterns (replaces product names with {product}, prices with {price}) to find universal winners",
                },
                "batch_db": {
                    "description": "SQLite persistence layer — tracks product setup status, caches GAQL/XML",
                    "key_tools": [
                        "batch_status(country_code, campaign_id) — overview of setup completeness",
                        "batch_missing(country_code, campaign_id) — list incomplete products",
                        "batch_sync_from_api(country_code, campaign_id) — sync DB with actual Google Ads state",
                        "batch_logs(country_code, campaign_id) — operation history",
                    ],
                },
            },
            "workflows": {
                "new_country_setup": [
                    "PIPELINE WORKFLOW — work in small batches, background refresh between steps:",
                    "1. batch_sync_from_api(CC, campaign_id) — load feed + sync DB with Google Ads state",
                    "2. batch_analyze_assets(CC, campaign_id) — metrics-based BEST/GOOD/LOW scoring (~6s)",
                    "3. WARMUP in batches of 10: batch_warmup_cache(CC, campaign_id, max_products=10) — repeat until all warmed",
                    "   While warmup runs for batch N+1, you can already setup batch N:",
                    "4. batch_setup_products(CC, campaign_id, [handles_batch]) — 1-3 products at a time",
                    "   KEY INSIGHT: Even if MCP returns timeout, the work completes server-side!",
                    "   On retry, products show as 'Skipped (complete in DB)'",
                    "5. After each batch: batch_status(CC, campaign_id) to track progress",
                    "6. Final: batch_audit_campaign(CC, campaign_id) for completeness verification",
                    "",
                    "PIPELINE PARALLELISM: While batch_setup processes products 1-10,",
                    "you can warmup cache for products 11-20 simultaneously.",
                    "This overlapping pattern keeps the work flowing without idle waits.",
                ],
                "optimize_existing": [
                    "1. batch_dashboard(CC, campaign_id) — full overview with error analysis",
                    "2. batch_missing(CC, campaign_id) — find gaps (use status_filter='partial' or 'error')",
                    "3. For partial products: batch_setup_products with specific asset_types=['callouts','sitelinks']",
                    "4. For errors: check classify_error pattern — transient errors can be retried",
                    "5. batch_keyword_research for high-ROI keywords",
                    "6. batch_winning_patterns to identify best headline patterns",
                    "7. Re-run batch_setup_products for incomplete products",
                ],
                "keyword_enrichment": [
                    "1. batch_keyword_research(handle, country_code) for specific product",
                    "2. batch_category_keywords(category, country_code) for category-wide keywords",
                    "3. Only add keywords with ROI > 50%",
                    "4. Use google_ads_add_keyword to add manually, or let batch_setup_products do it",
                ],
            },
            "active_campaigns": {
                "RO": {"campaign_id": "22734106761", "name": "RO - Search | Product | BW (Romania) | HLP | ALL_PRODUCTS_NEW | ECOM_PROFIT"},
                "TR": {"campaign_id": "20452025214", "name": "TR - Search | Product | BW (Turkey) | HLP | ALL_PRODUCTS_NEW | ECOM_PROFIT"},
            },
            "session_continuity": {
                "changelog_db": "Use batch_changelog_read() to see all changes, fixes, and pending work — stored in SQLite, auto-grows.",
                "changelog_file": "google_ads_mcp/CHANGELOG.md — legacy reference for code-level fix details.",
                "db_status": "Use batch_dashboard(country_code, campaign_id) to see current progress and completion stats.",
                "db_logs": "Use batch_logs(country_code, campaign_id) for recent operation history.",
                "recent_changelog": _get_recent_changelog_for_instructions(),
            },
            "rules": [
                "FIRST: Run batch_changelog_read() to understand recent changes and pending work",
                "MANDATORY WARMUP PROCEDURE: Before ANY batch_setup_products or batch_setup_all, ALWAYS run: "
                "1) batch_sync_from_api(CC, campaign_id), "
                "2) batch_analyze_assets(CC, campaign_id) — metrics-based scoring (~6s), "
                "3) batch_warmup_cache(CC, campaign_id, max_products=10) in batches of 10 — "
                "this pre-caches AI ad copy on PHP endpoint so setup doesn't timeout waiting for GPT",
                "PIPELINE PATTERN: Work in small batches (1-3 products). While one batch processes, "
                "warmup cache for the next batch. Overlap warmup + setup for continuous flow.",
                "TIMEOUT RESILIENCE: MCP 60s timeout does NOT kill server-side operations. "
                "Products that timeout usually complete in background. Retry to confirm via DB status.",
                "ERROR CLASSIFICATION: Errors are auto-classified — transient (retry), permanent (skip), "
                "resource_limit (change strategy), quota (stop all). Check batch_dashboard for error analysis.",
                "PRE-FLIGHT CHECK: Use _preflight_resource_check before bulk operations to detect "
                "campaigns nearing asset limits (callouts, sitelinks, promotions).",
                "Process max 1-3 products at a time (60s MCP timeout) — use batch_setup_all for bulk",
                "Only add keywords with confirmed ROI > 50%",
                "Products without prices/images still get text optimization (status: complete_no_images)",
                "After setup, verify with batch_audit_campaign",
                f"Customer ID: {CUSTOMER_ID} (no dashes)",
            ],
            "product_categories": [
                "electronics", "fashion", "home_garden", "beauty", "sports",
                "food", "toys", "pet", "automotive", "other",
            ],
        }

    # ------------------------------------------------------------------
    # #5 Batch queue with auto-resume (processes ALL feed products)
    # ------------------------------------------------------------------

    class BatchSetupAllRequest(BaseModel):
        """Input for batch_setup_all tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        dry_run: bool = Field(False, description="Preview only, no changes")
        batch_size: int = Field(5, description="Products per batch (1-10)", ge=1, le=10)
        delay_between_batches: float = Field(3.0, description="Seconds to wait between batches")
        max_products: int = Field(0, description="Max products to process (0 = all)")
        skip_existing: bool = Field(True, description="Skip products already complete in DB")
        force_replace_rsa: bool = Field(False, description="If true, pause existing RSAs and create new AI-generated ones")
        feed_type: Optional[str] = Field(None, description="Feed source: 'main' (default) or 'new_products'. When 'new_products', uses NP_FEEDS BW feeds.")
        shop_domain: Optional[str] = Field(None, description="Filter to specific shop domain. Only with feed_type='new_products'.")

    # ------------------------------------------------------------------
    # Background worker function for async batch_setup_all
    # ------------------------------------------------------------------
    def _batch_setup_all_worker(
        job_id: str,
        country_code: str,
        campaign_id: str,
        pending_handles: list,
        feed_products: dict,
        existing_ad_groups: dict,
        existing_assets: dict,
        config: dict,
        url_map: dict,
        batch_size: int,
        delay_between_batches: float,
        dry_run: bool,
        force_replace_rsa: bool,
        frozen_perf_snapshot: Optional[Dict[str, Any]],
        all_handles_count: int,
        shop_domain: Optional[str] = None,
    ):
        """
        Background worker thread for batch_setup_all.
        Processes all products and updates progress in DB + _background_jobs registry.
        """
        global _frozen_perf_context
        _frozen_perf_context = frozen_perf_snapshot

        _debug("worker", f"Worker started: job={job_id}, {len(pending_handles)} products, country={country_code}")

        db = _get_db()
        results = []
        success_count = 0
        error_count = 0
        consecutive_errors = 0
        language_contamination_count = 0
        rate_limited = False
        progress_key = f"setup_all_{country_code}_{campaign_id}"

        try:
            for i, handle in enumerate(pending_handles):
                # Check stop flag
                if _stop_flags.get(job_id):
                    _debug("worker", f"Stop flag set for job {job_id}, stopping after {i} products")
                    break

                # Auto-stop: too many consecutive errors (non-keyword errors)
                if consecutive_errors >= 10:
                    _debug("worker", f"AUTO-STOP: {consecutive_errors} consecutive errors for job {job_id}", "error")
                    _stop_flags[job_id] = True
                    db.add_changelog(
                        category="bugfix", title=f"AUTO-STOP: {consecutive_errors} consecutive errors",
                        description=f"Job {job_id} auto-stopped after {consecutive_errors} consecutive errors. "
                                    f"Processed {i}/{len(pending_handles)}.",
                        country_code=country_code, campaign_id=campaign_id, severity="critical",
                    )
                    break

                # Auto-stop: language contamination detected
                if language_contamination_count >= 3:
                    _debug("worker", f"AUTO-STOP: {language_contamination_count} language contaminations for job {job_id}", "error")
                    _stop_flags[job_id] = True
                    db.add_changelog(
                        category="bugfix", title=f"AUTO-STOP: Language contamination ({language_contamination_count}x)",
                        description=f"Job {job_id} auto-stopped after {language_contamination_count} products with "
                                    f"foreign-language RSA headlines. Check _get_localized_fillers() and AI endpoint.",
                        country_code=country_code, campaign_id=campaign_id, severity="critical",
                    )
                    break

                t0 = time.time()
                _debug("worker", f"Processing {i+1}/{len(pending_handles)}: {handle}")

                # Update progress in DB and in-memory registry
                progress_data = {
                    "job_id": job_id,
                    "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "total": len(pending_handles),
                    "processed": i,
                    "success": success_count,
                    "errors": error_count,
                    "current_handle": handle,
                    "status": "running",
                    "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "percent": round(i / len(pending_handles) * 100, 1) if pending_handles else 0,
                }
                db.set_cached_gaql("progress", progress_key, [json.dumps(progress_data)], ttl=3600)

                with _background_jobs_lock:
                    if job_id in _background_jobs:
                        _background_jobs[job_id].update(progress_data)

                # --- v3.4.4: Smart rate limit check BEFORE processing ---
                pause_needed, wait_seconds, pause_reason = _rate_limiter.should_pause()
                if pause_needed:
                    if wait_seconds > 300:  # >5 min — pause-resume instead of inline wait
                        _debug("worker", f"Rate limit pause: {pause_reason}. Saving checkpoint for resume.", "warning")
                        rate_limited = True
                        db.log_rate_limit(0, "batch_setup_all", f"Paused at product {i+1}/{len(pending_handles)}: {pause_reason}")
                        # Save unprocessed handles for auto-resume
                        remaining_handles = pending_handles[i:]
                        db.set_cached_gaql("resume", f"resume_{country_code}_{campaign_id}",
                                           [json.dumps({"handles": remaining_handles, "job_id": job_id,
                                                        "paused_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                                        "resume_after": time.strftime("%Y-%m-%d %H:%M:%S",
                                                            time.localtime(time.time() + wait_seconds))})],
                                           ttl=int(wait_seconds + 3600))
                        results.append({"handle": handle, "actions": [], "errors": [f"Rate limited: {pause_reason}"]})
                        break
                    else:
                        _debug("worker", f"Short rate limit pause: {int(wait_seconds)}s. Waiting inline.", "info")
                        time.sleep(wait_seconds + 1)  # Wait + 1s buffer

                # --- v3.4.4: Per-product retry (up to 2 retries for transient errors) ---
                product_retries = 0
                max_product_retries = 2
                report = None

                while product_retries <= max_product_retries:
                    try:
                        report = process_single_product(
                            handle=handle,
                            feed_products=feed_products,
                            existing_ad_groups=existing_ad_groups,
                            existing_assets=existing_assets,
                            campaign_id=campaign_id,
                            config=config,
                            country_code=country_code,
                            dry_run=dry_run,
                            url_map=url_map,
                            force_replace_rsa=force_replace_rsa,
                            feed_domain_override=shop_domain,
                        )
                        # Count mutations from report actions
                        mutation_count = len([a for a in report.get("actions", [])
                                              if any(v in a.lower() for v in ["created", "paused", "updated", "added"])])
                        _rate_limiter.record_mutation(max(mutation_count, 1))
                        break  # Success — exit retry loop

                    except Exception as e:
                        err_str = str(e).upper()
                        if "429" in err_str or "RATE_EXCEEDED" in err_str:
                            _rate_limiter.record_429(str(e))
                            rate_limited = True
                            db.log_rate_limit(0, "batch_setup_all", f"429 at product {i+1}/{len(pending_handles)}: {handle}")
                            # Save checkpoint for resume
                            remaining_handles = pending_handles[i:]
                            db.set_cached_gaql("resume", f"resume_{country_code}_{campaign_id}",
                                               [json.dumps({"handles": remaining_handles, "job_id": job_id,
                                                            "paused_at": time.strftime("%Y-%m-%d %H:%M:%S")})],
                                               ttl=14400)  # 4h TTL
                            results.append({"handle": handle, "actions": [], "errors": [f"Rate limited: {e}"]})
                            break
                        # Classify error for retry decision
                        err_info = classify_error(e)
                        if err_info["retryable"] and product_retries < max_product_retries:
                            product_retries += 1
                            import random
                            retry_delay = RETRY_BASE_DELAY * (2 ** (product_retries - 1)) + random.uniform(0, 2)
                            _debug("worker", f"Product {handle} transient error (attempt {product_retries}/{max_product_retries}): "
                                   f"{str(e)[:100]}. Retrying in {retry_delay:.1f}s")
                            time.sleep(retry_delay)
                            continue
                        report = {"handle": handle, "actions": [], "errors": [str(e)[:200]]}
                        break

                if rate_limited:
                    break  # Exit main loop

                if report is None:
                    report = {"handle": handle, "actions": [], "errors": ["Unknown error (no report generated)"]}

                results.append(report)
                duration_ms = int((time.time() - t0) * 1000)

                has_errors = bool(report.get("errors"))
                # Distinguish "real" errors from minor keyword policy blocks
                real_errors = [err for err in report.get("errors", [])
                               if "blocked by health policy" not in err
                               and "LANGUAGE_CONTAMINATION" not in err]
                has_contamination = any("LANGUAGE_CONTAMINATION" in err for err in report.get("errors", []))

                if has_errors:
                    error_count += 1
                    if real_errors:
                        consecutive_errors += 1
                    else:
                        consecutive_errors = 0  # Only keyword policy errors → reset
                else:
                    success_count += 1
                    consecutive_errors = 0

                if has_contamination:
                    language_contamination_count += 1

                # Track in DB
                if not dry_run:
                    _track_product_in_db(
                        db, country_code, campaign_id, handle,
                        report, feed_products.get(handle, {}), duration_ms,
                        asset_types=None,
                    )

                # --- v3.4.4: Adaptive delay between products ---
                if not dry_run and i < len(pending_handles) - 1:
                    if (i + 1) % batch_size == 0:
                        time.sleep(max(delay_between_batches, _rate_limiter.get_delay()))
                    else:
                        time.sleep(_rate_limiter.get_delay())

        except Exception as e:
            error_count += 1
            _debug("worker", f"WORKER CRASH: {e}", "error")
            import traceback
            tb_str = traceback.format_exc()[:800]
            _debug("worker", f"Traceback: {tb_str}", "error")
            results.append({"handle": "WORKER_CRASH", "actions": [], "errors": [str(e)[:500]]})

            # v3.4.4: Save crash checkpoint for recovery
            try:
                processed_handles = [r.get("handle", "?") for r in results if r.get("handle") != "WORKER_CRASH"]
                remaining = [h for h in pending_handles if h not in processed_handles]
                if remaining:
                    db.set_cached_gaql("resume", f"resume_{country_code}_{campaign_id}",
                                       [json.dumps({"handles": remaining, "job_id": job_id,
                                                    "crash_reason": str(e)[:200],
                                                    "crashed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                                    "can_resume_immediately": True})],
                                       ttl=86400)  # 24h TTL
                    _debug("worker", f"Crash checkpoint saved: {len(remaining)} products remaining for resume")
            except Exception:
                pass  # Don't fail saving checkpoint

            # Log to changelog
            db.add_changelog(
                category="bugfix", title=f"WORKER_CRASH in job {job_id}",
                description=f"Crash after processing {len(results)-1} products. {len(pending_handles) - len(results) + 1} remaining. "
                            f"Error: {str(e)[:200]}. Resume checkpoint saved.",
                country_code=country_code, campaign_id=campaign_id, severity="critical",
            )
        finally:
            _debug("worker", f"Worker finished: job={job_id}, success={success_count}, errors={error_count}")
            # Clear frozen perf context after batch run
            _frozen_perf_context = None

            # Clean up stop flag
            stopped = _stop_flags.pop(job_id, False)

            # Mark progress as complete
            final_status = "stopped" if stopped else ("completed" if not rate_limited else "rate_limited")
            remaining_count = len(pending_handles) - len(results)
            final_data = {
                "job_id": job_id,
                "total": len(pending_handles),
                "total_in_feed": all_handles_count,
                "processed": len(results),
                "success": success_count,
                "errors": error_count,
                "rate_limited": rate_limited,
                "status": final_status,
                "completed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "remaining_for_resume": remaining_count,
                "rate_limit_stats": _rate_limiter.get_stats(),
                "last_results": [
                    {"handle": r.get("handle", "?"), "ok": not r.get("errors"),
                     "actions_count": len(r.get("actions", []))}
                    for r in results[-20:]
                ],
            }
            db.set_cached_gaql("progress", progress_key, [json.dumps(final_data)], ttl=86400)

            with _background_jobs_lock:
                if job_id in _background_jobs:
                    _background_jobs[job_id].update(final_data)

            # Auto-log to changelog
            if not dry_run and len(results) > 0:
                _auto_changelog(
                    category="rollout",
                    title=f"batch_setup_all: {success_count}/{len(results)} products processed",
                    description=f"Feed: {all_handles_count} total, {len(pending_handles)} pending. Errors: {error_count}. Rate limited: {rate_limited}",
                    country_code=country_code,
                    campaign_id=campaign_id,
                )

    @mcp_app.tool()
    async def batch_setup_all(request: BatchSetupAllRequest) -> dict:
        """
        Process ALL products from the XML feed automatically with auto-resume.

        v3.2.0: ASYNC mode — spawns a background thread and returns a job_id
        immediately (<1s). Poll progress with batch_setup_progress(job_id).
        No more 60s MCP timeout issues. One call processes ALL products.

        Progress is saved to DB after each product — safe to interrupt and resume.
        """
        country_code = request.country_code.upper()
        config = _get_config_for_country(country_code)

        # Feed URL resolution: NP_FEEDS > XML_FEEDS
        if request.feed_type == "new_products":
            xml_url = _resolve_np_feed_url(country_code, request.shop_domain)
            if not xml_url:
                return {"status": "error", "message": f"No NP_FEEDS BW feed for {country_code}" + (f" / {request.shop_domain}" if request.shop_domain else "")}
        else:
            xml_url = config.get("xml_url", "")
            if not xml_url:
                return {"status": "error", "message": f"No XML feed URL for {country_code}"}

        db = _get_db()

        # Check if there's already a running job for this country+campaign
        with _background_jobs_lock:
            for jid, jdata in _background_jobs.items():
                if (jdata.get("country_code") == country_code
                    and jdata.get("campaign_id") == request.campaign_id
                    and jdata.get("status") == "running"):
                    # Return existing job status instead of starting a new one
                    return {
                        "status": "already_running",
                        "job_id": jid,
                        "message": f"Job {jid} is already running for {country_code}/{request.campaign_id}. Poll with batch_setup_progress.",
                        "progress": {
                            "processed": jdata.get("processed", 0),
                            "total": jdata.get("total", 0),
                            "success": jdata.get("success", 0),
                            "errors": jdata.get("errors", 0),
                            "current_handle": jdata.get("current_handle", ""),
                            "percent": jdata.get("percent", 0),
                        },
                    }

        # v3.4.4: Check for resume checkpoint from previous rate-limited/crashed job
        resume_key = f"resume_{country_code}_{request.campaign_id}"
        resume_data = db.get_cached_gaql("resume", resume_key)
        has_resume_checkpoint = False
        resume_handles = []
        if resume_data:
            try:
                checkpoint = json.loads(resume_data[0]) if resume_data else {}
                resume_handles = checkpoint.get("handles", [])
                paused_at = checkpoint.get("paused_at", "unknown")
                resume_after = checkpoint.get("resume_after", "")
                can_resume = checkpoint.get("can_resume_immediately", False)
                if resume_handles:
                    has_resume_checkpoint = True
                    _debug("worker", f"Found resume checkpoint from {paused_at}: {len(resume_handles)} products remaining")
            except (json.JSONDecodeError, IndexError):
                pass

        # 1. Fetch feed
        try:
            raw_products = _fetch_xml_feed_cached(country_code, xml_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch XML feed: {e}"}

        feed_products = {p["handle"]: p for p in raw_products}
        all_handles = sorted(feed_products.keys())

        # 2. Filter and smart-order
        if has_resume_checkpoint and request.skip_existing:
            # Use resume checkpoint handles, but re-validate they're still in feed and not complete
            pending_handles = [h for h in resume_handles if h in feed_products]
            _debug("worker", f"Resuming from checkpoint: {len(pending_handles)} products (from {len(resume_handles)} saved)")
            # Clear the checkpoint since we're picking it up
            db.set_cached_gaql("resume", resume_key, [], ttl=1)
        elif request.skip_existing:
            pending_handles = []
            for h in all_handles:
                db_product = db.get_product(country_code, request.campaign_id, h)
                if not db_product or db_product.get("status") not in ("complete",):
                    pending_handles.append(h)
        else:
            pending_handles = all_handles

        # Smart re-ordering: errors first, then pending, then partial
        if pending_handles:
            error_handles = []
            pending_new = []
            partial_handles = []
            complete_handles = []
            for h in pending_handles:
                db_product = db.get_product(country_code, request.campaign_id, h)
                if not db_product:
                    pending_new.append(h)
                elif db_product.get("status") == "error":
                    error_handles.append(h)
                elif db_product.get("status") == "partial":
                    partial_handles.append(h)
                elif db_product.get("status") == "complete":
                    complete_handles.append(h)
                else:
                    pending_new.append(h)
            pending_handles = error_handles + pending_new + partial_handles + complete_handles

        if request.max_products > 0:
            pending_handles = pending_handles[:request.max_products]

        # Quota guard
        remaining = db.get_remaining_quota(15000)
        estimated_needed = len(pending_handles) * 24
        if remaining < estimated_needed and remaining < 500:
            return {
                "status": "quota_exceeded",
                "message": f"Insufficient API quota. Remaining: {remaining}, estimated needed: {estimated_needed}",
                "remaining_quota": remaining,
                "can_process": max(0, remaining // 24),
            }

        if not pending_handles:
            return {
                "status": "all_complete",
                "message": "All products are already complete!",
                "total_in_feed": len(all_handles),
                "pending": 0,
            }

        # 3. Get ad groups & assets (once for all — done in main thread before spawning)
        try:
            existing_ad_groups = _get_existing_ad_groups(request.campaign_id)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch ad groups: {e}"}

        url_map = db.get_cached_url_map(country_code, request.campaign_id)
        if not url_map:
            try:
                url_map = _get_ad_group_url_map(request.campaign_id)
                if url_map:
                    db.save_url_map(country_code, request.campaign_id, url_map)
            except Exception as e:
                _debug("exception", f"url_map fallback at line 5670: {e}", "warning")
                url_map = {}

        try:
            existing_assets = _get_ad_group_assets(request.campaign_id)
        except Exception as e:
            _debug("exception", f"existing_assets fallback at line 5676: {e}", "warning")
            existing_assets = {}

        # 3.5 Freeze performance context snapshot
        frozen_perf_snapshot = None
        try:
            frozen_perf: Dict[str, Any] = {}
            conn = db._get_conn()
            for asset_type, hl_keys in [("HEADLINE", ("best_headlines", "good_headlines", "low_headlines")),
                                         ("DESCRIPTION", ("best_descriptions", "good_descriptions", "low_descriptions"))]:
                rows = conn.execute(
                    """SELECT headline_pattern, performance_label, SUM(occurrences) as total_occ
                       FROM asset_performance
                       WHERE country_code=? AND asset_type=?
                       GROUP BY headline_pattern, performance_label
                       ORDER BY total_occ DESC LIMIT 40""",
                    (country_code, asset_type)
                ).fetchall()
                if rows:
                    by_label: Dict[str, list] = {"BEST": [], "GOOD": [], "LOW": []}
                    for r in rows:
                        label = r["performance_label"]
                        if label in by_label:
                            by_label[label].append(r["headline_pattern"])
                    for label, key in zip(["BEST", "GOOD", "LOW"], hl_keys):
                        if by_label[label]:
                            frozen_perf[key] = by_label[label][:10]

            frozen_patterns: Dict[str, Any] = {}
            winning_hl = db.get_winning_patterns(country_code, asset_type="HEADLINE", min_occurrences=2)
            winning_desc = db.get_winning_patterns(country_code, asset_type="DESCRIPTION", min_occurrences=2)
            if winning_hl:
                frozen_patterns["best_headline_patterns"] = [
                    {"pattern": h.get("pattern", ""), "label": h.get("label", "GOOD"), "count": h.get("count", 1)}
                    for h in winning_hl[:15]
                ]
            if winning_desc:
                frozen_patterns["best_description_patterns"] = [
                    {"pattern": d.get("pattern", ""), "label": d.get("label", "GOOD"), "count": d.get("count", 1)}
                    for d in winning_desc[:10]
                ]

            frozen_perf_snapshot = {
                "_country": country_code,
                "existing_performance": frozen_perf,
                "winning_patterns": frozen_patterns,
            }
        except Exception as e:
            _debug("exception", f"frozen_perf fallback at line 5724: {e}", "warning")
            frozen_perf_snapshot = None

        # 4. ASYNC: Spawn background thread and return immediately
        job_id = uuid.uuid4().hex[:16]
        progress_key = f"setup_all_{country_code}_{request.campaign_id}"

        initial_progress = {
            "job_id": job_id,
            "country_code": country_code,
            "campaign_id": request.campaign_id,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(pending_handles),
            "total_in_feed": len(all_handles),
            "processed": 0,
            "success": 0,
            "errors": 0,
            "current_handle": "",
            "status": "running",
            "percent": 0,
            "dry_run": request.dry_run,
            "ordering": f"{len(error_handles if 'error_handles' in dir() else [])} errors → "
                        f"{len(pending_new if 'pending_new' in dir() else [])} pending → "
                        f"{len(partial_handles if 'partial_handles' in dir() else [])} partial",
        }

        with _background_jobs_lock:
            _background_jobs[job_id] = initial_progress.copy()

        db.set_cached_gaql("progress", progress_key, [json.dumps(initial_progress)], ttl=3600)

        worker = threading.Thread(
            target=_batch_setup_all_worker,
            args=(
                job_id, country_code, request.campaign_id,
                pending_handles, feed_products,
                existing_ad_groups, existing_assets,
                config, url_map,
                request.batch_size, request.delay_between_batches,
                request.dry_run, request.force_replace_rsa,
                frozen_perf_snapshot, len(all_handles),
                request.shop_domain,
            ),
            daemon=True,
            name=f"batch_setup_{country_code}_{job_id[:8]}",
        )
        worker.start()

        return {
            "status": "submitted",
            "job_id": job_id,
            "message": f"Background job started for {len(pending_handles)} products. Poll with batch_setup_progress(job_id='{job_id}').",
            "total_in_feed": len(all_handles),
            "pending": len(pending_handles),
            "dry_run": request.dry_run,
        }

    # ------------------------------------------------------------------
    # #5b-stop Stop a running background job
    # ------------------------------------------------------------------

    class BatchStopJobRequest(BaseModel):
        """Input for batch_stop_job tool."""
        job_id: str = Field(..., description="Job ID to stop")

    @mcp_app.tool()
    async def batch_stop_job(request: BatchStopJobRequest) -> dict:
        """
        Stop a running background job (batch_setup_all, batch_add_images_only, etc).

        Sets a stop flag that the worker checks between products.
        The job will stop after finishing the current product.
        """
        job_id = request.job_id.strip()
        _stop_flags[job_id] = True
        _debug("worker", f"Stop flag set for job {job_id}")

        with _background_jobs_lock:
            if job_id in _background_jobs:
                status = _background_jobs[job_id].get("status", "unknown")
                processed = _background_jobs[job_id].get("processed", 0)
                total = _background_jobs[job_id].get("total", 0)
                return {
                    "status": "stop_requested",
                    "message": f"Stop flag set. Job will stop after current product ({processed}/{total} processed). Current status: {status}.",
                    "job_id": job_id,
                    "processed": processed,
                    "total": total,
                }

        return {
            "status": "stop_requested",
            "message": f"Stop flag set for job {job_id}. If the job is running, it will stop after the current product.",
            "job_id": job_id,
        }

    # ------------------------------------------------------------------
    # #5b Async batch_setup_all progress polling
    # ------------------------------------------------------------------

    class BatchSetupProgressRequest(BaseModel):
        """Input for batch_setup_progress tool."""
        job_id: str = Field(..., description="Job ID returned by batch_setup_all")

    @mcp_app.tool()
    async def batch_setup_progress(request: BatchSetupProgressRequest) -> dict:
        """
        Check status of a batch_setup_all background job.

        Returns current progress: processed/total, success/errors,
        current_handle being processed, and percent complete.

        Call this every 30-60s after batch_setup_all returns status="submitted".
        When status is "completed" or "rate_limited", the job is done.
        """
        job_id = request.job_id.strip()

        # 1. Check in-memory registry first (fastest)
        with _background_jobs_lock:
            if job_id in _background_jobs:
                job_data = dict(_background_jobs[job_id])
                # If completed, clean up from registry after returning
                if job_data.get("status") in ("completed", "rate_limited"):
                    # Keep for 5 more minutes for final reads
                    pass
                return job_data

        # 2. Fallback: check DB progress cache (survives process restarts)
        db = _get_db()
        # Try all possible progress keys
        for country_code in ["RO", "TR", "PL", "HU", "FR", "DE", "IT", "ES", "CZ", "SK", "BG", "GR"]:
            try:
                conn = db._get_conn()
                rows = conn.execute(
                    """SELECT value FROM gaql_cache
                       WHERE cache_type='progress' AND cache_key LIKE ?
                       ORDER BY cached_at DESC LIMIT 5""",
                    (f"setup_all_%",)
                ).fetchall()
                for row in rows:
                    try:
                        data = json.loads(row["value"])
                        if isinstance(data, list) and data:
                            data = json.loads(data[0])
                        if data.get("job_id") == job_id:
                            return data
                    except (json.JSONDecodeError, IndexError, TypeError):
                        continue
            except Exception as e:
                _debug("exception", f"silent continue at line 5833: {e}", "warning")
                continue

        return {
            "status": "not_found",
            "message": f"Job {job_id} not found. It may have completed and been cleaned up, or the process was restarted.",
            "rate_limit_stats": _rate_limiter.get_stats(),
        }

    # ------------------------------------------------------------------
    # #6 GPT-4o Image Enhancement Pipeline
    # ------------------------------------------------------------------

    class ImageEnhanceRequest(BaseModel):
        """Input for batch_enhance_images tool."""
        country_code: str = Field(..., description="Country code")
        campaign_id: str = Field(..., description="Campaign ID")
        product_handles: List[str] = Field(..., description="Product handles to enhance images for")
        dry_run: bool = Field(False, description="Preview only")
        ad_group_id: Optional[str] = Field(None, description="Direct ad_group_id (skip DB lookup)")

    @mcp_app.tool()
    async def batch_enhance_images(request: ImageEnhanceRequest) -> dict:
        """
        Enhance product images using GPT-4o image generation API.

        For products whose images failed Google Ads validation, this tool:
        1. Fetches the original image from find_products_images.php
        2. Applies local fixes (format, dimensions, aspect ratio)
        3. If still failing, uses GPT-4o to enhance/recreate the image
        4. Uploads the enhanced image to Google Ads

        Requires OPENAI_API_KEY environment variable or hardcoded key.
        """
        import base64
        from io import BytesIO

        OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

        if not OPENAI_API_KEY:
            return {"status": "error", "message": "No OpenAI API key set. Set OPENAI_API_KEY env variable."}

        country_code = request.country_code.upper()
        db = _get_db()
        results = []

        for handle in request.product_handles:
            result = {"handle": handle, "status": "pending", "details": ""}

            try:
                # 1. Fetch image candidates
                candidates = _fetch_product_images(handle)
                if not candidates:
                    result["status"] = "no_candidates"
                    result["details"] = "No image candidates from endpoint"
                    results.append(result)
                    continue

                # 2. Try to download and fix locally first
                img_candidate = candidates[0]
                local_path = img_candidate.get("local_path")
                image_url = img_candidate.get("url", "")

                if local_path and os.path.isfile(local_path):
                    with open(local_path, "rb") as f:
                        image_bytes = f.read()
                elif image_url:
                    req = urllib.request.Request(image_url, headers={"User-Agent": "BatchOptimizer/1.0"})
                    image_bytes = urllib.request.urlopen(req, context=_img_ssl_ctx, timeout=15).read()
                else:
                    result["status"] = "no_source"
                    results.append(result)
                    continue

                # 3. Try local validation/fix
                try:
                    fixed_bytes = _validate_and_fix_image(image_bytes)
                    result["status"] = "fixed_locally"
                    result["details"] = f"Image fixed locally ({len(fixed_bytes)} bytes)"
                    if not request.dry_run:
                        # Find ad group for this handle
                        db_product = db.get_product(country_code, request.campaign_id, handle)
                        if db_product and db_product.get("ad_group_id"):
                            # Create a synthetic candidate with the fixed bytes
                            fixed_candidate = {
                                **img_candidate,
                                "local_path": None,
                                "url": "",
                                "_raw_bytes": fixed_bytes,
                            }
                            # Override _add_image_to_ad_group to use raw bytes
                            # For now, save fixed image to temp file
                            import tempfile
                            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                                tmp.write(fixed_bytes)
                                tmp_path = tmp.name
                            fixed_candidate["local_path"] = tmp_path
                            try:
                                asset_rn = _add_image_to_ad_group(db_product["ad_group_id"], fixed_candidate)
                                result["status"] = "uploaded"
                                result["details"] = f"Fixed and uploaded: {asset_rn}"
                            finally:
                                os.unlink(tmp_path)
                except ValueError:
                    # Local fix failed — try GPT-4o
                    result["details"] = "Local fix failed, trying GPT-4o..."

                    if request.dry_run:
                        result["status"] = "would_enhance_gpt4o"
                        results.append(result)
                        continue

                    # 4. Call GPT-4o image edit API
                    try:
                        img_b64 = base64.b64encode(image_bytes).decode()

                        product_name = _handle_to_product_name(handle)
                        prompt = (
                            f"Clean up this product photo of '{product_name}'. "
                            "Make the background pure white, ensure the product is centered, "
                            "and create a professional e-commerce product image at 1:1 aspect ratio. "
                            "Output at 1024x1024."
                        )

                        api_data = json.dumps({
                            "model": "gpt-image-1",
                            "prompt": prompt,
                            "n": 1,
                            "size": "1024x1024",
                            "quality": "medium",
                        }).encode()

                        api_req = urllib.request.Request(
                            "https://api.openai.com/v1/images/generations",
                            data=api_data,
                            headers={
                                "Authorization": f"Bearer {OPENAI_API_KEY}",
                                "Content-Type": "application/json",
                            },
                        )
                        api_resp = urllib.request.urlopen(api_req, timeout=60).read()
                        api_json = json.loads(api_resp)

                        if api_json.get("data") and api_json["data"][0].get("b64_json"):
                            enhanced_bytes = base64.b64decode(api_json["data"][0]["b64_json"])
                            enhanced_bytes = _validate_and_fix_image(enhanced_bytes)

                            db_product = db.get_product(country_code, request.campaign_id, handle)
                            if db_product and db_product.get("ad_group_id"):
                                import tempfile
                                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                                    tmp.write(enhanced_bytes)
                                    tmp_path = tmp.name
                                enhanced_candidate = {
                                    "local_path": tmp_path,
                                    "url": "",
                                    "filename": f"gpt4o_{handle}.jpg",
                                    "md5_hash": None,
                                    "quality_score": 95,
                                    "source": "gpt4o_enhanced",
                                }
                                try:
                                    asset_rn = _add_image_to_ad_group(db_product["ad_group_id"], enhanced_candidate)
                                    result["status"] = "enhanced_and_uploaded"
                                    result["details"] = f"GPT-4o enhanced and uploaded: {asset_rn}"
                                finally:
                                    os.unlink(tmp_path)
                            else:
                                result["status"] = "enhanced_no_ag"
                                result["details"] = "GPT-4o enhanced but no ad group found"
                        else:
                            result["status"] = "gpt4o_error"
                            result["details"] = f"GPT-4o returned no image data"

                    except Exception as e:
                        result["status"] = "gpt4o_error"
                        result["details"] = f"GPT-4o API error: {str(e)[:200]}"

            except Exception as e:
                result["status"] = "error"
                result["details"] = str(e)[:200]

            results.append(result)

            # Delay between products
            if not request.dry_run:
                time.sleep(2)

        return {
            "status": "success",
            "processed": len(results),
            "results": results,
        }

    # ------------------------------------------------------------------
    # #7 Stale Products Cleanup Tool
    # ------------------------------------------------------------------

    class BatchCleanupStaleRequest(BaseModel):
        """Input for batch_cleanup_stale tool."""
        country_code: str = Field(..., description="Country code")
        campaign_id: str = Field(..., description="Campaign ID")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")
        feed_type: Optional[str] = Field(None, description="Feed source: 'main' (default) or 'new_products'.")
        shop_domain: Optional[str] = Field(None, description="Filter to specific shop domain. Only with feed_type='new_products'.")

    @mcp_app.tool()
    async def batch_cleanup_stale(request: BatchCleanupStaleRequest) -> dict:
        """
        Find and optionally pause ad groups for products no longer in the XML feed.

        Compares DB products against the current XML feed to identify stale entries.
        In dry_run mode, only reports what would be paused.
        When dry_run=false, pauses the ad groups of stale products.
        """
        country_code = request.country_code.upper()
        config = _get_config_for_country(country_code)

        # Feed URL resolution: NP_FEEDS > XML_FEEDS
        if request.feed_type == "new_products":
            xml_url = _resolve_np_feed_url(country_code, request.shop_domain)
            if not xml_url:
                return {"status": "error", "message": f"No NP_FEEDS BW feed for {country_code}" + (f" / {request.shop_domain}" if request.shop_domain else "")}
        else:
            xml_url = config.get("xml_url", "")
            if not xml_url:
                return {"status": "error", "message": f"No XML feed URL for {country_code}"}

        db = _get_db()

        # Get current feed handles
        try:
            raw_products = _fetch_xml_feed_cached(country_code, xml_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch XML feed: {e}"}

        active_handles = [p["handle"] for p in raw_products]
        stale_all = db.get_stale_products(country_code, request.campaign_id, active_handles)

        # Filter to only ENABLED ad groups (skip already-PAUSED)
        try:
            from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id
            customer_id = _format_customer_id(CUSTOMER_ID)
            enabled_query = f"""
                SELECT ad_group.id, ad_group.status
                FROM ad_group
                WHERE campaign.id = {request.campaign_id}
                  AND ad_group.status = 'ENABLED'
            """
            enabled_rows = _execute_gaql(customer_id, enabled_query, page_size=1000)
            enabled_ag_ids = {str(_safe_get_value(r, "ad_group.id", "")) for r in enabled_rows}
            stale = [s for s in stale_all if s.get("ad_group_id") in enabled_ag_ids]
        except Exception as e:
            _debug("stale_cleanup", f"ENABLED filter failed, using all stale: {e}", "warning")
            stale = stale_all

        if not stale:
            return {
                "status": "success",
                "message": "No stale products found. All DB products are in the XML feed.",
                "active_feed_count": len(active_handles),
            }

        paused = []
        failed = []
        kept_with_conversions = []

        if not request.dry_run:
            from google_ads_mcp import _ensure_client, _format_customer_id, _execute_gaql, _safe_get_value, google_ads_client

            _ensure_client()
            customer_id = _format_customer_id(CUSTOMER_ID)

            # Collect all stale ag_ids to batch-check conversions (1 GAQL call)
            stale_ag_ids = [s.get("ad_group_id") for s in stale if s.get("ad_group_id")]
            conversion_counts = {}
            if stale_ag_ids:
                ag_ids_str = ",".join(stale_ag_ids)
                conv_query = f"""
                    SELECT ad_group.id, metrics.conversions
                    FROM ad_group
                    WHERE ad_group.id IN ({ag_ids_str})
                      AND segments.date DURING LAST_365_DAYS
                """
                try:
                    conv_results = _execute_gaql(customer_id, conv_query, page_size=1000)
                    for row in conv_results:
                        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
                        convs = float(_safe_get_value(row, "metrics.conversions", 0) or 0)
                        conversion_counts[ag_id] = conversion_counts.get(ag_id, 0) + convs
                except Exception as e:
                    _debug("stale_cleanup", f"Conversion check failed: {e}", "warning")

            for product in stale:
                ag_id = product.get("ad_group_id")
                if not ag_id:
                    continue

                # Guard: keep ad groups with 3+ conversions in last year
                total_convs = conversion_counts.get(ag_id, 0)
                if total_convs >= 3:
                    kept_with_conversions.append({
                        "handle": product["product_handle"],
                        "ad_group_id": ag_id,
                        "conversions_365d": round(total_convs, 1),
                    })
                    continue

                try:
                    service = google_ads_client.get_service("AdGroupService")
                    operation = google_ads_client.get_type("AdGroupOperation")
                    operation.update.resource_name = service.ad_group_path(customer_id, ag_id)
                    operation.update.status = google_ads_client.enums.AdGroupStatusEnum.PAUSED
                    from google.protobuf import field_mask_pb2
                    operation.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=["status"]))
                    _retry_with_backoff(service.mutate_ad_groups, customer_id=customer_id, operations=[operation])
                    paused.append(product["product_handle"])
                except Exception as e:
                    failed.append({"handle": product["product_handle"], "error": str(e)[:100]})

        db.log_operation(
            country_code, request.campaign_id, "*",
            "cleanup_stale", "success" if not failed else "partial",
            details={
                "stale_count": len(stale),
                "paused": len(paused),
                "failed": len(failed),
                "dry_run": request.dry_run,
            },
        )

        # Auto-log cleanup
        if not request.dry_run and len(paused) > 0:
            _auto_changelog(
                category="optimization",
                title=f"batch_cleanup_stale: {len(paused)} paused, {len(kept_with_conversions)} kept (3+ conv)",
                description=f"Stale: {len(stale)} found, {len(paused)} paused, {len(kept_with_conversions)} kept (3+ conv/year), {len(failed)} failed. Feed has {len(active_handles)} active products.",
                country_code=request.country_code,
                campaign_id=request.campaign_id,
            )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "active_feed_count": len(active_handles),
            "stale_count": len(stale),
            "stale_products": [s["product_handle"] for s in stale],
            "paused": paused,
            "kept_with_conversions": kept_with_conversions,
            "failed": failed,
        }

    # ------------------------------------------------------------------
    # #8 Dashboard / Status Endpoint
    # ------------------------------------------------------------------

    @mcp_app.tool()
    async def batch_dashboard(request: BatchStatusRequest) -> dict:
        """
        Comprehensive campaign dashboard with setup progress, error analysis,
        image stats, rate limit history, and operational overview.

        More detailed than batch_status — includes top errors, image stats,
        rate limit tracking, and last sync time.
        """
        db = _get_db()
        country_code = request.country_code.upper()
        stats = db.get_dashboard_stats(country_code, request.campaign_id)

        # Add completion percentage
        total = stats.get("total_products", 0)
        by_status = stats.get("by_status", {})
        complete = by_status.get("complete", 0)
        complete_no_img = by_status.get("complete_no_images", 0)

        stats["completion"] = {
            "full_complete_pct": round(complete / total * 100, 1) if total else 0,
            "text_complete_pct": round((complete + complete_no_img) / total * 100, 1) if total else 0,
            "needs_images": complete_no_img,
        }

        # Endpoint health checks
        endpoint_health = {}
        # AI ad copy endpoint
        try:
            import urllib.request
            import ssl
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            t0 = time.time()
            test_url = AD_GENERATOR_BASE_URL
            # Health check: POST with minimal valid payload (endpoint requires product_handle, returns 405 on GET)
            health_payload = json.dumps({"product_handle": "_health_check", "country": "RO", "mode": "copy_only"}).encode("utf-8")
            req = urllib.request.Request(
                test_url,
                data=health_payload,
                headers={"Content-Type": "application/json", "User-Agent": "BatchOptimizer/1.0"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as resp:
                resp.read()
            latency_ms = int((time.time() - t0) * 1000)
            endpoint_health["ai_ad_generator"] = {"status": "OK", "latency_ms": latency_ms, "url": AD_GENERATOR_BASE_URL}
        except Exception as e:
            endpoint_health["ai_ad_generator"] = {"status": "UNREACHABLE", "error": str(e)[:100], "url": AD_GENERATOR_BASE_URL}

        # Image endpoint
        try:
            t0 = time.time()
            # Health check with a valid product_handle (endpoint returns 400 without it)
            img_url = IMAGE_FINDER_BASE_URL.rstrip("/") + "?product_handle=health_check&format=json&limit=1"
            req = urllib.request.Request(img_url, headers={"User-Agent": "BatchOptimizer/1.0"})
            with urllib.request.urlopen(req, timeout=10, context=ssl_ctx) as resp:
                resp.read()
            latency_ms = int((time.time() - t0) * 1000)
            endpoint_health["image_finder"] = {"status": "OK", "latency_ms": latency_ms, "url": IMAGE_FINDER_BASE_URL}
        except Exception as e:
            endpoint_health["image_finder"] = {"status": "UNREACHABLE", "error": str(e)[:100], "url": IMAGE_FINDER_BASE_URL}

        stats["endpoint_health"] = endpoint_health

        # Debug: field_type enum format samples (for diagnosing asset sync issues)
        debug_key = f"_debug_ft_{request.campaign_id}"
        if debug_key in _session_cache:
            debug_data, _ = _session_cache[debug_key]
            stats["_debug_field_type_samples"] = debug_data

        # Debug store — all accumulated debug messages
        if _debug_store:
            stats["_debug_log"] = {k: v[-10:] for k, v in _debug_store.items()}  # last 10 per category

        # MCP-side response cache stats
        try:
            stats["response_cache"] = db.get_response_cache_stats()
        except Exception:
            pass

        # v3.4.4: Add rate limit stats and resume checkpoint info
        stats["rate_limit"] = _rate_limiter.get_stats()

        # Check for pending resume checkpoint
        resume_key = f"resume_{country_code}_{request.campaign_id}"
        resume_data = db.get_cached_gaql("resume", resume_key)
        if resume_data:
            try:
                checkpoint = json.loads(resume_data[0]) if resume_data else {}
                if checkpoint.get("handles"):
                    stats["resume_checkpoint"] = {
                        "available": True,
                        "remaining_products": len(checkpoint["handles"]),
                        "paused_at": checkpoint.get("paused_at", "unknown"),
                        "resume_after": checkpoint.get("resume_after", ""),
                        "hint": "Call batch_setup_all with skip_existing=True to auto-resume from checkpoint",
                    }
            except (json.JSONDecodeError, IndexError):
                pass

        return {
            "status": "success",
            **stats,
        }

    # ------------------------------------------------------------------
    # #9 Duplicate Detection (integrated into image upload - no separate tool needed)
    # Already integrated through batch_db.py image_uploads table
    # The _add_image_to_ad_group uses MD5 dedup via _get_existing_image_md5s
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # #10 Parallel Batch Processing
    # ------------------------------------------------------------------

    class BatchSetupParallelRequest(BaseModel):
        """Input for batch_setup_parallel tool."""
        country_code: str = Field(..., description="Country code")
        campaign_id: str = Field(..., description="Campaign ID")
        product_handles: List[str] = Field(..., description="Product handles to process")
        dry_run: bool = Field(False, description="Preview only")
        max_workers: int = Field(3, description="Concurrent workers (1-5)", ge=1, le=5)
        skip_existing: bool = Field(True, description="Skip complete products")
        force_replace_rsa: bool = Field(False, description="If true, pause existing RSAs and create new AI-generated ones")

    @mcp_app.tool()
    async def batch_setup_parallel(request: BatchSetupParallelRequest) -> dict:
        """
        Process multiple products in parallel using ThreadPoolExecutor.

        Faster than sequential batch_setup_products but uses more API quota.
        Use max_workers=2-3 for normal operation, up to 5 for dry_run.
        WARNING: Higher parallelism increases rate limit risk.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        country_code = request.country_code.upper()
        config = _get_config_for_country(country_code)
        xml_url = config.get("xml_url", "")
        if not xml_url:
            return {"status": "error", "message": f"No XML feed URL for {country_code}"}

        db = _get_db()

        # Fetch feed
        try:
            raw_products = _fetch_xml_feed_cached(country_code, xml_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch XML feed: {e}"}

        feed_products = {p["handle"]: p for p in raw_products}

        # Get ad groups & assets
        try:
            existing_ad_groups = _get_existing_ad_groups(request.campaign_id)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch ad groups: {e}"}

        url_map = db.get_cached_url_map(country_code, request.campaign_id)
        if not url_map:
            try:
                url_map = _get_ad_group_url_map(request.campaign_id)
                if url_map:
                    db.save_url_map(country_code, request.campaign_id, url_map)
            except Exception as e:
                _debug("exception", f"url_map fallback at line 6266: {e}", "warning")
                url_map = {}

        try:
            existing_assets = _get_ad_group_assets(request.campaign_id)
        except Exception as e:
            _debug("exception", f"existing_assets fallback at line 6272: {e}", "warning")
            existing_assets = {}

        # Filter pending handles
        handles = request.product_handles
        if request.skip_existing:
            handles = [
                h for h in handles
                if not (db.get_product(country_code, request.campaign_id, h) or {}).get("status") == "complete"
            ]

        if not handles:
            return {"status": "success", "message": "All products already complete", "processed": 0}

        # Process in parallel
        results = []
        success_count = 0
        error_count = 0

        def _process_one(handle):
            t0 = time.time()
            report = process_single_product(
                handle=handle,
                feed_products=feed_products,
                existing_ad_groups=existing_ad_groups,
                existing_assets=existing_assets,
                campaign_id=request.campaign_id,
                config=config,
                country_code=country_code,
                dry_run=request.dry_run,
                url_map=url_map,
                force_replace_rsa=request.force_replace_rsa,
            )
            duration_ms = int((time.time() - t0) * 1000)
            if not request.dry_run:
                _track_product_in_db(
                    db, country_code, request.campaign_id, handle,
                    report, feed_products.get(handle, {}), duration_ms,
                    asset_types=None,
                )
            return report

        with ThreadPoolExecutor(max_workers=request.max_workers) as executor:
            future_to_handle = {executor.submit(_process_one, h): h for h in handles}
            for future in as_completed(future_to_handle):
                handle = future_to_handle[future]
                try:
                    report = future.result()
                    results.append(report)
                    if report.get("errors"):
                        error_count += 1
                    else:
                        success_count += 1
                except Exception as e:
                    results.append({"handle": handle, "actions": [], "errors": [str(e)[:200]]})
                    error_count += 1

        return {
            "status": "success",
            "summary": {
                "total": len(handles),
                "processed": len(results),
                "success": success_count,
                "with_errors": error_count,
                "workers": request.max_workers,
                "dry_run": request.dry_run,
            },
            "results": results,
        }

    # ------------------------------------------------------------------
    # XML Feed Price Analyzer
    # ------------------------------------------------------------------

    class FeedPricesRequest(BaseModel):
        """Input for batch_feed_prices tool."""
        country_code: str = Field(description="Country code (e.g., RO, TR, PL)")
        only_mismatched: bool = Field(default=True, description="Only show products where price != sale_price")

    @mcp_app.tool()
    async def batch_feed_prices(request: FeedPricesRequest) -> dict:
        """
        Parse XML feed and return price vs sale_price for all products.

        Identifies products where the regular price differs from sale_price,
        which means RSAs might show wrong prices if they used price instead of sale_price.
        Use this to find products that need RSA price correction.
        """
        country_code = request.country_code.upper()
        only_mismatched = request.only_mismatched

        config = _get_config_for_country(country_code)
        if not config:
            return {"status": "error", "message": f"Country {country_code} not configured"}

        # Load feed
        xml_url = config.get("xml_url")
        if not xml_url:
            return {"status": "error", "message": f"No XML feed URL for {country_code}"}
        try:
            raw_products = _fetch_xml_feed_cached(country_code, xml_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch XML feed: {e}"}
        feed_products = {p["handle"]: p for p in raw_products}
        if not feed_products:
            return {"status": "error", "message": "Empty XML feed"}

        products = []
        mismatched_count = 0
        for handle, fd in sorted(feed_products.items()):
            price = fd.get("price", 0)
            sale_price = fd.get("sale_price", 0)
            has_mismatch = sale_price > 0 and price > 0 and abs(price - sale_price) > 1

            if has_mismatch:
                mismatched_count += 1

            if only_mismatched and not has_mismatch:
                continue

            products.append({
                "handle": handle,
                "price": price,
                "sale_price": sale_price if sale_price > 0 else None,
                "diff": round(price - sale_price) if has_mismatch else 0,
                "discount_pct": round((price - sale_price) / price * 100) if has_mismatch and price > 0 else 0,
            })

        return {
            "status": "success",
            "country_code": country_code,
            "total_in_feed": len(feed_products),
            "mismatched_count": mismatched_count,
            "showing": len(products),
            "products": products,
        }

    # ------------------------------------------------------------------
    # RSA Price Audit — find RSAs with wrong prices vs feed
    # ------------------------------------------------------------------

    class RSAPriceAuditRequest(BaseModel):
        """Input for batch_rsa_price_audit tool."""
        country_code: str = Field(description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(description="Google Ads campaign ID")

    @mcp_app.tool()
    async def batch_rsa_price_audit(request: RSAPriceAuditRequest) -> dict:
        """
        Audit RSA headlines for wrong prices compared to XML feed sale_price.

        Queries all RSA headlines in the campaign, extracts price patterns,
        and compares against the feed's sale_price. Flags products where
        RSA shows regular price instead of sale_price.

        Returns list of products needing force_replace_rsa to fix prices.
        """
        country_code = request.country_code.upper()
        campaign_id = request.campaign_id
        config = _get_config_for_country(country_code)
        if not config:
            return {"status": "error", "message": f"Country {country_code} not configured"}

        import google_ads_mcp as _gam
        _gam._ensure_client()
        customer_id = _gam._format_customer_id(CUSTOMER_ID)

        # 1. Load feed prices
        xml_url = config.get("xml_url")
        if not xml_url:
            return {"status": "error", "message": f"No XML feed URL for {country_code}"}
        try:
            raw_products = _fetch_xml_feed_cached(country_code, xml_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch XML feed: {e}"}
        feed_products = {p["handle"]: p for p in raw_products}
        if not feed_products:
            return {"status": "error", "message": "Empty XML feed"}

        # Build price lookup: handle -> {price, sale_price}
        price_lookup = {}
        for handle, fd in feed_products.items():
            price = fd.get("price", 0)
            sale_price = fd.get("sale_price", 0)
            if sale_price > 0 and price > 0 and abs(price - sale_price) > 1:
                price_lookup[handle] = {
                    "price": price,
                    "sale_price": sale_price,
                    "correct_price_str": str(int(sale_price)),
                    "wrong_price_str": str(int(price)),
                }

        if not price_lookup:
            return {"status": "success", "message": "No products with price != sale_price in feed", "wrong_price_products": []}

        # 2. Query RSA headlines via GAQL
        ga_service = _gam.google_ads_client.get_service("GoogleAdsService")
        gaql = (
            f"SELECT ad_group.name, ad_group.id, "
            f"ad_group_ad.ad.responsive_search_ad.headlines, "
            f"ad_group_ad.ad.id "
            f"FROM ad_group_ad "
            f"WHERE campaign.id = {campaign_id} "
            f"AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD' "
            f"AND ad_group_ad.status != 'REMOVED' "
        )

        # Currency patterns per country
        currency_patterns = {
            "RO": r'(\d{2,4})\s*(?:Lei|lei|Ron|RON|ron|Lej|lej)',
            "TR": r'(\d{2,6})\s*(?:TL|tl|₺|Lira|lira)',
            "PL": r'(\d{2,4})\s*(?:zł|PLN|pln|Zł)',
            "HU": r'(\d{3,6})\s*(?:Ft|ft|HUF|huf|forint)',
        }
        price_pattern = currency_patterns.get(country_code, r'(\d{2,6})\s*(?:Lei|lei|TL|tl|zł|Ft)')

        # 3. Parse results
        wrong_price_products = []
        try:
            response = ga_service.search(customer_id=customer_id, query=gaql)
            # Group by ad_group
            ag_data = {}  # ag_name -> {ag_id, headlines_with_wrong_price}
            for row in response:
                ag_name = row.ad_group.name
                ag_id = str(row.ad_group.id)
                ad_id = str(row.ad_group_ad.ad.id)

                for h in row.ad_group_ad.ad.responsive_search_ad.headlines:
                    text = h.text
                    matches = re.findall(price_pattern, text)
                    if matches:
                        if ag_name not in ag_data:
                            ag_data[ag_name] = {"ag_id": ag_id, "prices_in_rsa": set(), "sample_headlines": []}
                        for m in matches:
                            ag_data[ag_name]["prices_in_rsa"].add(int(m))
                        if len(ag_data[ag_name]["sample_headlines"]) < 3:
                            ag_data[ag_name]["sample_headlines"].append(text)

            # 4. Cross-reference with feed
            db = _get_db()
            for ag_name, ag_info in ag_data.items():
                # Find handle from DB (url_map or name matching)
                handle = None
                conn = db._get_conn()
                row = conn.execute(
                    "SELECT product_handle FROM handle_ag_map WHERE ad_group_id=? AND country_code=?",
                    (ag_info["ag_id"], country_code)
                ).fetchone()
                if row:
                    handle = row["product_handle"]

                if not handle:
                    # Try name-based matching
                    clean_name = ag_name.lower().replace(" ", "-")
                    for h in price_lookup:
                        if clean_name in h or h.replace("-" + country_code.lower(), "") == clean_name:
                            handle = h
                            break

                if handle and handle in price_lookup:
                    feed_info = price_lookup[handle]
                    wrong_price = int(feed_info["price"])
                    correct_price = int(feed_info["sale_price"])

                    # Check if RSA contains the WRONG price (regular price instead of sale_price)
                    if wrong_price in ag_info["prices_in_rsa"] and correct_price not in ag_info["prices_in_rsa"]:
                        wrong_price_products.append({
                            "handle": handle,
                            "ag_name": ag_name,
                            "ag_id": ag_info["ag_id"],
                            "wrong_price_in_rsa": wrong_price,
                            "correct_sale_price": correct_price,
                            "sample_headlines": ag_info["sample_headlines"][:2],
                        })
                    elif wrong_price in ag_info["prices_in_rsa"] and correct_price in ag_info["prices_in_rsa"]:
                        wrong_price_products.append({
                            "handle": handle,
                            "ag_name": ag_name,
                            "ag_id": ag_info["ag_id"],
                            "wrong_price_in_rsa": wrong_price,
                            "correct_sale_price": correct_price,
                            "note": "MIXED - both prices found",
                            "sample_headlines": ag_info["sample_headlines"][:2],
                        })

        except Exception as e:
            return {"status": "error", "message": f"GAQL query failed: {e}"}

        return {
            "status": "success",
            "country_code": country_code,
            "campaign_id": campaign_id,
            "products_with_discount": len(price_lookup),
            "wrong_price_count": len(wrong_price_products),
            "wrong_price_products": wrong_price_products,
            "fix_command": f"Use batch_setup_products with force_replace_rsa=true on these handles to fix prices",
        }

    # ------------------------------------------------------------------
    # API Quota Monitoring
    # ------------------------------------------------------------------

    @mcp_app.tool()
    async def batch_api_quota() -> dict:
        """
        Get API quota usage for today.

        Returns total API calls used, remaining quota, breakdown by type and hour.
        Status: "healthy" (<70%), "warning" (70-90%), "critical" (>90%).
        """
        db = _get_db()
        usage = db.get_api_usage_today()
        daily_limit = 15000
        remaining = db.get_remaining_quota(daily_limit)
        pct_used = round((usage["total"] / daily_limit) * 100, 1) if daily_limit > 0 else 0

        if pct_used >= 90:
            status = "critical"
        elif pct_used >= 70:
            status = "warning"
        else:
            status = "healthy"

        return {
            "date": usage["date"],
            "daily_limit": daily_limit,
            "total_used": usage["total"],
            "remaining": remaining,
            "pct_used": pct_used,
            "status": status,
            "by_type": usage["by_type"],
            "by_hour": usage["by_hour"],
        }

    # ------------------------------------------------------------------
    # Health Check — validate all system components in one call
    # ------------------------------------------------------------------

    @mcp_app.tool()
    async def batch_health_check() -> dict:
        """
        Validate system health: DB connectivity, Google Ads API auth,
        image server reachability, cache freshness, and background jobs.

        Returns per-component status (ok/warn/error) and overall health.
        Use this as a first diagnostic step when something seems broken.
        """
        checks = {}
        overall = "ok"

        # 1. SQLite DB connectivity
        try:
            db = _get_db()
            conn = db._get_conn()
            conn.execute("SELECT 1").fetchone()
            table_count = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            checks["database"] = {"status": "ok", "tables": table_count}
        except Exception as e:
            checks["database"] = {"status": "error", "error": str(e)[:200]}
            overall = "error"

        # 2. Google Ads API auth
        try:
            from google_ads_mcp import _execute_gaql, _format_customer_id
            customer_id = _format_customer_id(CUSTOMER_ID)
            results = _execute_gaql(customer_id, "SELECT customer.id FROM customer LIMIT 1")
            checks["google_ads_api"] = {"status": "ok", "customer_id": CUSTOMER_ID}
        except Exception as e:
            err_str = str(e)[:200]
            checks["google_ads_api"] = {"status": "error", "error": err_str}
            overall = "error"

        # 3. Image server reachability
        try:
            test_url = f"{IMAGE_FINDER_BASE_URL}?product_handle=test&format=json&limit=1"
            req = urllib.request.Request(test_url, headers={"User-Agent": "BatchOptimizer/1.0"})
            with urllib.request.urlopen(req, timeout=10, context=_img_ssl_ctx) as resp:
                status_code = resp.getcode()
                checks["image_server"] = {"status": "ok", "http_status": status_code, "url": IMAGE_FINDER_BASE_URL}
        except Exception as e:
            err_str = str(e)[:200]
            is_timeout = "timed out" in err_str.lower()
            checks["image_server"] = {
                "status": "warn" if is_timeout else "error",
                "error": err_str,
                "url": IMAGE_FINDER_BASE_URL,
            }
            if overall == "ok":
                overall = "warn" if is_timeout else "error"

        # 4. API quota
        try:
            usage = db.get_api_usage_today()
            daily_limit = 15000
            remaining = db.get_remaining_quota(daily_limit)
            pct_used = round(usage["total"] / daily_limit * 100, 1)
            quota_status = "ok" if pct_used < 70 else ("warn" if pct_used < 90 else "error")
            checks["api_quota"] = {
                "status": quota_status,
                "used": usage["total"],
                "remaining": remaining,
                "pct_used": pct_used,
            }
            if quota_status == "error":
                overall = "error"
            elif quota_status == "warn" and overall == "ok":
                overall = "warn"
        except Exception as e:
            checks["api_quota"] = {"status": "error", "error": str(e)[:200]}

        # 5. Cache freshness
        try:
            conn = db._get_conn()
            cache_row = conn.execute(
                "SELECT MAX(cached_at) as latest FROM gaql_cache WHERE cache_type='gaql'"
            ).fetchone()
            latest = cache_row["latest"] if cache_row else None
            if latest:
                age_min = round((time.time() - latest) / 60, 1)
                cache_status = "ok" if age_min < 60 else "warn"
                checks["gaql_cache"] = {"status": cache_status, "latest_age_minutes": age_min}
            else:
                checks["gaql_cache"] = {"status": "warn", "message": "No cached GAQL data"}
            if checks["gaql_cache"]["status"] == "warn" and overall == "ok":
                overall = "warn"
        except Exception as e:
            checks["gaql_cache"] = {"status": "error", "error": str(e)[:200]}

        # 6. Background jobs
        with _background_jobs_lock:
            active_jobs = {k: v.get("status") for k, v in _background_jobs.items()}
        running = sum(1 for s in active_jobs.values() if s == "running")
        checks["background_jobs"] = {
            "status": "ok",
            "active": len(active_jobs),
            "running": running,
            "jobs": active_jobs if active_jobs else "none",
        }

        # 7. Debug log summary
        debug_summary = {k: len(v) for k, v in _debug_store.items()} if _debug_store else {}
        checks["debug_log"] = {
            "status": "ok",
            "categories": len(debug_summary),
            "total_entries": sum(debug_summary.values()) if debug_summary else 0,
        }

        # 8. v3.4.4: Rate limit manager stats
        rl_stats = _rate_limiter.get_stats()
        rl_status = "ok"
        if rl_stats["is_paused"]:
            rl_status = "error"
        elif rl_stats["consecutive_429s"] > 0:
            rl_status = "warn"
        elif rl_stats["mutations_this_hour"] > 800:
            rl_status = "warn"
        checks["rate_limiter"] = {
            "status": rl_status,
            **rl_stats,
        }
        if rl_status == "error" and overall != "error":
            overall = "error"
        elif rl_status == "warn" and overall == "ok":
            overall = "warn"

        return {
            "status": overall,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "checks": checks,
        }

    # ------------------------------------------------------------------
    # Images-Only Upload (lightweight, ~1-3 API calls per product)
    # ------------------------------------------------------------------

    class BatchAddImagesRequest(BaseModel):
        """Input for batch_add_images_only tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        product_handles: List[str] = Field(..., description="List of product handles to process")
        dry_run: bool = Field(False, description="Preview only, no changes")

    # ── Images worker (background thread) ──

    def _batch_images_worker(
        job_id: str,
        country_code: str,
        campaign_id: str,
        handles: List[str],
        dry_run: bool,
    ):
        """Background worker thread for batch_add_images_only (async mode)."""
        _debug("images_worker", f"Worker started: job={job_id}, {len(handles)} products, country={country_code}")

        db = _get_db()
        results = {}
        success_count = 0
        error_count = 0
        progress_key = f"images_{country_code}_{campaign_id}"

        try:
            for i, handle in enumerate(handles):
                result = {"handle": handle, "status": "pending", "details": []}

                # Update progress
                progress_data = {
                    "job_id": job_id,
                    "country_code": country_code,
                    "campaign_id": campaign_id,
                    "status": "running",
                    "total": len(handles),
                    "processed": i,
                    "success": success_count,
                    "errors": error_count,
                    "current_handle": handle,
                    "percent": round(i * 100 / len(handles)),
                    "dry_run": dry_run,
                }
                with _background_jobs_lock:
                    _background_jobs[job_id] = progress_data.copy()
                try:
                    db.set_cached_gaql("progress", progress_key, [json.dumps(progress_data)], ttl=3600)
                except Exception:
                    pass

                _debug("images_worker", f"Processing {i+1}/{len(handles)}: {handle}")

                # Get ad_group_id from DB
                ag_info = db.get_ag_id_for_handle(country_code, campaign_id, handle)
                if not ag_info or not ag_info.get("ad_group_id"):
                    product = db.get_product(country_code, campaign_id, handle)
                    ag_id = product.get("ad_group_id") if product else None
                    if not ag_id:
                        result["status"] = "no_ad_group"
                        result["details"].append(f"No ad_group_id found for '{handle}'")
                        results[handle] = result
                        error_count += 1
                        continue
                else:
                    ag_id = ag_info["ad_group_id"]

                # Fetch image candidates
                try:
                    candidates = _fetch_product_images(handle)
                    if not candidates:
                        result["status"] = "no_images"
                        result["details"].append("No image candidates found")
                        results[handle] = result
                        error_count += 1
                        continue
                except Exception as e:
                    result["status"] = "fetch_error"
                    result["details"].append(f"Image fetch error: {str(e)[:200]}")
                    results[handle] = result
                    error_count += 1
                    continue

                if dry_run:
                    descs = []
                    for c in candidates[:IMAGE_MAX_PER_AD_GROUP]:
                        pm = c.get("processing_method", "none")
                        dims = f"{c.get('width', '?')}x{c.get('height', '?')}"
                        descs.append(f"{c.get('filename', '?')[:30]} [{pm}, {dims}]")
                    result["status"] = "dry_run"
                    result["details"].append(f"Would upload up to {IMAGE_MAX_PER_AD_GROUP} images: {'; '.join(descs)}")
                    results[handle] = result
                    success_count += 1
                    continue

                # Upload images
                added = 0
                skipped_dup = 0
                seen_md5 = set()
                for img in candidates:
                    if added >= IMAGE_MAX_PER_AD_GROUP:
                        break
                    md5 = img.get("md5_hash")
                    if md5 and md5 in seen_md5:
                        skipped_dup += 1
                        continue
                    if md5:
                        seen_md5.add(md5)
                    try:
                        _add_image_to_ad_group(ag_id, img)
                        added += 1
                        fname = img.get("filename", "?")[:40]
                        result["details"].append(f"Uploaded: {fname}")
                    except Exception as e:
                        err_str = str(e)[:200]
                        if "DUPLICATE" in err_str.upper() or "ALREADY_EXISTS" in err_str.upper() or "MD5 dedup" in err_str:
                            skipped_dup += 1
                            result["details"].append("Image already exists (skip)")
                        else:
                            result["details"].append(f"Upload error: {err_str}")

                # Determine status: added > 0 OR all skipped as duplicate = success
                if added > 0:
                    result["status"] = "success"
                elif skipped_dup > 0:
                    result["status"] = "success"  # all images already exist = success
                    result["details"].append(f"All images already exist ({skipped_dup} skipped)")
                else:
                    result["status"] = "no_new_images"
                result["images_added"] = added

                # Update DB
                if added > 0:
                    try:
                        db.upsert_product(country_code, campaign_id, handle, has_images=True, image_count=added)
                    except Exception as e:
                        _debug("images_worker", f"DB update failed for {handle}: {e}", "warning")

                results[handle] = result
                if added > 0 or skipped_dup > 0:
                    success_count += 1
                else:
                    error_count += 1

                time.sleep(1)  # Rate limit protection

        except Exception as e:
            _debug("images_worker", f"WORKER CRASH: {e}", "error")
            import traceback
            _debug("images_worker", f"Traceback: {traceback.format_exc()[:500]}", "error")

        # Final status
        final_status = "completed"
        final_progress = {
            "job_id": job_id,
            "country_code": country_code,
            "campaign_id": campaign_id,
            "status": final_status,
            "total": len(handles),
            "processed": len(results),
            "success": success_count,
            "errors": error_count,
            "percent": 100,
            "dry_run": dry_run,
            "duration_seconds": None,
            "results": results,
        }
        with _background_jobs_lock:
            _background_jobs[job_id] = final_progress
        try:
            db.set_cached_gaql("progress", progress_key, [json.dumps(final_progress)], ttl=86400)
        except Exception:
            pass

        _debug("images_worker", f"Worker finished: job={job_id}, success={success_count}, errors={error_count}")

    # ── Main tool (async mode — spawns background worker) ──

    @mcp_app.tool()
    async def batch_add_images_only(request: BatchAddImagesRequest) -> dict:
        """
        Add images to existing ad groups WITHOUT creating RSAs/callouts/sitelinks/etc.

        v3.2.2: ASYNC mode — spawns a background thread and returns a job_id
        immediately (<1s). Poll progress with batch_images_progress(job_id).

        Much lighter than batch_setup_products (~1-3 API calls per product vs ~24).
        Fetches images from find_products_images.php, validates, and uploads.
        Requires products to already have ad groups (uses handle_ag_map DB).
        """
        country_code = request.country_code.upper()
        handles = [h.strip() for h in request.product_handles if h.strip()]

        if not handles:
            return {"status": "error", "message": "No product handles provided"}

        job_id = uuid.uuid4().hex[:16]
        _debug("images", f"Launching async images job {job_id} for {len(handles)} products")

        initial_progress = {
            "job_id": job_id,
            "country_code": country_code,
            "campaign_id": request.campaign_id,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(handles),
            "processed": 0,
            "success": 0,
            "errors": 0,
            "current_handle": "",
            "status": "running",
            "percent": 0,
            "dry_run": request.dry_run,
        }

        with _background_jobs_lock:
            _background_jobs[job_id] = initial_progress.copy()

        db = _get_db()
        progress_key = f"images_{country_code}_{request.campaign_id}"
        db.set_cached_gaql("progress", progress_key, [json.dumps(initial_progress)], ttl=3600)

        worker = threading.Thread(
            target=_batch_images_worker,
            args=(job_id, country_code, request.campaign_id, handles, request.dry_run),
            daemon=True,
            name=f"batch_images_{country_code}_{job_id[:8]}",
        )
        worker.start()

        return {
            "status": "submitted",
            "job_id": job_id,
            "message": f"Background image job started for {len(handles)} products. Poll with batch_images_progress(job_id='{job_id}').",
            "total": len(handles),
            "dry_run": request.dry_run,
        }

    # ── Images progress polling ──

    class BatchImagesProgressRequest(BaseModel):
        """Input for batch_images_progress tool."""
        job_id: str = Field(..., description="Job ID returned by batch_add_images_only")

    @mcp_app.tool()
    async def batch_images_progress(request: BatchImagesProgressRequest) -> dict:
        """
        Check status of a batch_add_images_only background job.

        Returns current progress: processed/total, success/errors,
        current_handle being processed, and percent complete.

        Call this every 10-30s after batch_add_images_only returns status="submitted".
        When status is "completed", the job is done and results are included.
        """
        job_id = request.job_id.strip()

        # 1. Check in-memory registry first (fastest)
        with _background_jobs_lock:
            if job_id in _background_jobs:
                return dict(_background_jobs[job_id])

        # 2. Fallback: check DB progress cache
        db = _get_db()
        try:
            conn = db._get_conn()
            rows = conn.execute(
                """SELECT value FROM gaql_cache
                   WHERE cache_type='progress' AND cache_key LIKE 'images_%'
                   ORDER BY cached_at DESC LIMIT 10""",
                ()
            ).fetchall()
            for row in rows:
                try:
                    data = json.loads(row["value"])
                    if isinstance(data, list) and data:
                        data = json.loads(data[0])
                    if data.get("job_id") == job_id:
                        return data
                except (json.JSONDecodeError, IndexError, TypeError):
                    continue
        except Exception as e:
            _debug("images", f"Progress DB lookup failed: {e}", "warning")

        return {
            "status": "not_found",
            "message": f"Job {job_id} not found. It may have completed and been cleaned up.",
        }

    # ------------------------------------------------------------------
    # Batch Process Images via PHP Async Endpoint
    # ------------------------------------------------------------------

    class BatchProcessImagesRequest(BaseModel):
        """Input for batch_process_images tool."""
        product_handles: List[str] = Field(..., description="List of product handles to process")
        force_refresh: bool = Field(False, description="Force re-process even if cached")

    @mcp_app.tool()
    async def batch_process_images(request: BatchProcessImagesRequest) -> dict:
        """
        Pre-process product images via async PHP endpoint (batch_process_images.php).

        Submits handles to the PHP worker which fetches, validates, crops, and caches
        images server-side. Returns job_id for polling. Use this BEFORE batch_add_images_only
        to pre-warm the image cache so uploads are faster and more reliable.

        Flow: POST → get job_id → poll until completed → return results.
        If not done within 50s, returns job_id for manual polling.
        """
        handles = [h.strip() for h in request.product_handles if h.strip()]
        if not handles:
            return {"status": "error", "message": "No product handles provided"}

        MCP_HARD_LIMIT = 55  # seconds — leave 5s buffer for MCP 60s timeout
        tool_start = time.time()

        # POST to create async job
        payload = json.dumps({
            "product_handles": handles,
            "min_width": 300,
            "min_height": 300,
            "aspect_filter": "landscape,square",
            "sort": "quality_score",
            "deduplicate": True,
            "crop": "auto",
            "limit_per_product": 10,
            "force_refresh": request.force_refresh,
        }).encode("utf-8")

        try:
            req = urllib.request.Request(
                BATCH_PROCESS_IMAGES_URL,
                data=payload,
                headers={"Content-Type": "application/json", "User-Agent": "BatchOptimizer/3.2.2"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30, context=_img_ssl_ctx) as resp:
                job_response = json.loads(resp.read().decode("utf-8"))
                job_id = job_response.get("job_id")
        except Exception as e:
            _debug(DebugCat.IMAGES, f"batch_process_images POST failed: {e}", "error")
            return {"status": "error", "message": f"Failed to create image processing job: {str(e)[:200]}"}

        if not job_id:
            return {"status": "error", "message": "No job_id returned from PHP endpoint", "response": job_response}

        _debug(DebugCat.IMAGES, f"PHP image job created: {job_id} for {len(handles)} products")

        # Poll until completion or MCP timeout
        last_status = job_response
        while (time.time() - tool_start) < MCP_HARD_LIMIT:
            time.sleep(10)  # poll every 10s

            try:
                poll_url = f"{BATCH_PROCESS_IMAGES_URL}?job_id={job_id}"
                poll_req = urllib.request.Request(poll_url, method="GET", headers={"User-Agent": "BatchOptimizer/3.2.2"})
                with urllib.request.urlopen(poll_req, timeout=15, context=_img_ssl_ctx) as resp:
                    last_status = json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                _debug(DebugCat.IMAGES, f"Image job poll failed: {e}", "warning")
                continue

            job_status = last_status.get("status", "")
            if job_status == "completed":
                elapsed = round(time.time() - tool_start, 1)
                _debug(DebugCat.IMAGES, f"PHP image job {job_id} completed in {elapsed}s")
                return {
                    "status": "completed",
                    "job_id": job_id,
                    "elapsed_seconds": elapsed,
                    "summary": last_status.get("summary", {}),
                    "results": last_status.get("results", {}),
                }
            elif job_status == "failed":
                return {
                    "status": "failed",
                    "job_id": job_id,
                    "error": last_status.get("error", "Unknown error"),
                }

        # Timeout — return job_id for manual polling
        return {
            "status": "submitted",
            "job_id": job_id,
            "message": f"Image processing still running. Poll manually: GET {BATCH_PROCESS_IMAGES_URL}?job_id={job_id}",
            "processed": last_status.get("processed", 0),
            "total": last_status.get("total", len(handles)),
        }

    # ------------------------------------------------------------------
    # Guardrails Report
    # ------------------------------------------------------------------

    class BatchGuardrailsReportRequest(BaseModel):
        """Input for batch_guardrails_report tool."""
        country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")

    @mcp_app.tool()
    async def batch_guardrails_report(request: BatchGuardrailsReportRequest) -> dict:
        """
        Get guardrail validation report for a campaign.

        Shows which products passed/failed/skipped each guardrail check
        (RSA A/B/C, callouts, sitelinks, promotion, snippets, keywords, images, ad_group).
        """
        db = _get_db()
        country_code = request.country_code.upper()
        summary = db.get_guardrail_summary(country_code, request.campaign_id)
        failures = db.get_guardrail_failures(country_code, request.campaign_id, limit=20)

        return {
            "status": "success",
            "summary": summary,
            "failed_products": failures,
        }

    # ------------------------------------------------------------------
    # Check Eligibility (disapproved ads/assets)
    # ------------------------------------------------------------------

    class BatchCheckEligibilityRequest(BaseModel):
        """Input for batch_check_eligibility tool."""
        country_code: str = Field(..., description="Country code")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        auto_pause_disapproved: bool = Field(False, description="Auto-pause DISAPPROVED ads")
        auto_remove_assets: bool = Field(False, description="Auto-remove NOT_ELIGIBLE assets")
        dry_run: bool = Field(False, description="Preview only")

    @mcp_app.tool()
    async def batch_check_eligibility(request: BatchCheckEligibilityRequest) -> dict:
        """
        Check ad and asset eligibility for a campaign.

        Queries Google Ads API for approval status of all ads and assets.
        Detects DISAPPROVED content and optionally auto-pauses/removes it.
        Tracks disapproval patterns for analysis.
        Cost: 2 GAQL calls per campaign.
        """
        from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client

        country_code = request.country_code.upper()
        db = _get_db()
        result = {
            "status": "success",
            "ads": {},
            "assets": {},
            "actions_taken": [],
            "patterns": {},
        }

        # 1. Check ad eligibility
        try:
            ad_data = _check_ad_eligibility(request.campaign_id)
            result["ads"] = {k: v for k, v in ad_data.items() if k != "disapproved_details"}
            result["ads"]["disapproved_details"] = ad_data.get("disapproved_details", [])

            # Record in DB + auto-pause
            for d in ad_data.get("disapproved_details", []):
                db.upsert_eligibility(
                    country_code, request.campaign_id, d["ad_group_id"],
                    "AD", d["ad_id"], d["approval_status"], d.get("review_status"),
                    disapproval_reasons=[d.get("approval_status", "DISAPPROVED")],
                )

                if request.auto_pause_disapproved and not request.dry_run:
                    try:
                        _ensure_client()
                        customer_id = _format_customer_id(CUSTOMER_ID)
                        service = google_ads_client.get_service("AdGroupAdService")
                        op = google_ads_client.get_type("AdGroupAdOperation")
                        ad_group_ad = op.update
                        ad_group_ad.resource_name = service.ad_group_ad_path(
                            customer_id, d["ad_group_id"], d["ad_id"]
                        )
                        ad_group_ad.status = google_ads_client.enums.AdGroupAdStatusEnum.PAUSED
                        from google.protobuf import field_mask_pb2
                        op.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=["status"]))
                        _retry_with_backoff(service.mutate_ad_group_ads,
                                          customer_id=customer_id, operations=[op])
                        result["actions_taken"].append({
                            "type": "PAUSED", "entity": "AD",
                            "id": d["ad_id"], "ad_group": d["ad_group_name"],
                        })
                        db.upsert_eligibility(
                            country_code, request.campaign_id, d["ad_group_id"],
                            "AD", d["ad_id"], "DISAPPROVED", action_taken="PAUSED",
                        )
                    except Exception as e:
                        result["actions_taken"].append({
                            "type": "PAUSE_FAILED", "entity": "AD",
                            "id": d["ad_id"], "error": str(e)[:100],
                        })
        except Exception as e:
            result["ads"] = {"error": str(e)[:200]}

        # 2. Check asset eligibility
        try:
            asset_data = _check_asset_eligibility(request.campaign_id)
            result["assets"] = {k: v for k, v in asset_data.items() if k != "not_eligible_details"}
            result["assets"]["not_eligible_details"] = asset_data.get("not_eligible_details", [])

            for a in asset_data.get("not_eligible_details", []):
                db.upsert_eligibility(
                    country_code, request.campaign_id, a["ad_group_id"],
                    "ASSET", a.get("asset_ref", ""), "DISAPPROVED",
                    disapproval_reasons=a.get("reasons", []),
                )

                if request.auto_remove_assets and not request.dry_run:
                    try:
                        # Remove asset link
                        _ensure_client()
                        customer_id = _format_customer_id(CUSTOMER_ID)
                        from google_ads_mcp import _execute_gaql, _safe_get_value
                        # Find asset resource name for removal
                        query = f"""
                            SELECT campaign.id, ad_group.id, ad_group_asset.resource_name
                            FROM ad_group_asset
                            WHERE campaign.id = {request.campaign_id}
                              AND ad_group.id = {a['ad_group_id']}
                              AND ad_group_asset.asset = '{a['asset_ref']}'
                            LIMIT 1
                        """
                        res = _execute_gaql(customer_id, query)
                        if res:
                            rn = str(_safe_get_value(res[0], "ad_group_asset.resource_name", ""))
                            if rn:
                                ag_asset_service = google_ads_client.get_service("AdGroupAssetService")
                                remove_op = google_ads_client.get_type("AdGroupAssetOperation")
                                remove_op.remove = rn
                                _retry_with_backoff(ag_asset_service.mutate_ad_group_assets,
                                                  customer_id=customer_id, operations=[remove_op])
                                result["actions_taken"].append({
                                    "type": "REMOVED", "entity": "ASSET",
                                    "asset_ref": a["asset_ref"], "ad_group": a["ad_group_name"],
                                })
                                db.upsert_eligibility(
                                    country_code, request.campaign_id, a["ad_group_id"],
                                    "ASSET", a.get("asset_ref", ""), "DISAPPROVED",
                                    action_taken="REMOVED",
                                )
                    except Exception as e:
                        result["actions_taken"].append({
                            "type": "REMOVE_FAILED", "entity": "ASSET",
                            "asset_ref": a.get("asset_ref", ""), "error": str(e)[:100],
                        })
        except Exception as e:
            result["assets"] = {"error": str(e)[:200]}

        # 3. Get patterns
        result["patterns"] = db.get_disapproval_patterns(country_code, request.campaign_id)

        return result

    # ------------------------------------------------------------------
    # Domain Fix Tool — find and fix wrong-domain RSAs
    # ------------------------------------------------------------------

    class BatchFixDomainsRequest(BaseModel):
        """Input for batch_fix_domains tool."""
        country_code: str = Field(..., description="Country code (e.g., PL, RO, TR)")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")
        target_domain: Optional[str] = Field(None, description="Override target domain (else uses DOMAINS[cc])")

    @mcp_app.tool()
    async def batch_fix_domains(request: BatchFixDomainsRequest) -> dict:
        """
        Find and fix RSAs with wrong domain in final_urls.

        Scans all ENABLED RSAs in campaign, identifies those whose final_url
        domain doesn't match the expected domain for this country.
        Pauses wrong-domain RSAs and creates replacements with correct URLs,
        copying headlines and descriptions from the original.

        Use dry_run=true first to see what would change.
        """
        from google_ads_mcp import _execute_gaql, _ensure_client, _format_customer_id, google_ads_client
        from google.protobuf import field_mask_pb2
        import re

        country_code = request.country_code.upper()
        campaign_id = request.campaign_id

        # Resolve expected domain
        expected_domain = request.target_domain or DOMAINS.get(country_code)
        if not expected_domain:
            return {"status": "error", "message": f"No domain configured for {country_code}. Pass target_domain."}

        # Query ALL RSAs with their headlines, descriptions, final_urls
        query = f"""
            SELECT ad_group.id, ad_group.name,
                   ad_group_ad.ad.id, ad_group_ad.ad.final_urls,
                   ad_group_ad.ad.responsive_search_ad.headlines,
                   ad_group_ad.ad.responsive_search_ad.descriptions,
                   ad_group_ad.ad.responsive_search_ad.path1,
                   ad_group_ad.ad.responsive_search_ad.path2,
                   ad_group_ad.status
            FROM ad_group_ad
            WHERE campaign.id = {campaign_id}
              AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
              AND ad_group_ad.status = 'ENABLED'
        """

        try:
            results = _execute_gaql(CUSTOMER_ID, query, page_size=5000)
        except Exception as e:
            return {"status": "error", "message": f"GAQL query failed: {e}"}

        total_rsas = 0
        wrong_domain = []
        correct_domain_count = 0

        for row in results:
            total_rsas += 1
            ad = row.ad_group_ad.ad
            ad_id = str(ad.id)
            ag_id = str(row.ad_group.id)
            ag_name = row.ad_group.name or ag_id
            final_urls = list(ad.final_urls)

            if not final_urls:
                continue

            url = final_urls[0]
            # Check if domain matches
            from urllib.parse import urlparse as _up
            parsed = _up(url)
            current_domain = parsed.netloc

            if current_domain == expected_domain:
                correct_domain_count += 1
                continue

            # Extract headlines and descriptions
            headlines = []
            for h in ad.responsive_search_ad.headlines:
                h_data = {"text": h.text}
                if h.pinned_field and h.pinned_field != 0:
                    try:
                        field_name = google_ads_client.enums.ServedAssetFieldTypeEnum.ServedAssetFieldType(h.pinned_field).name
                        h_data["pinned_field"] = field_name
                    except Exception:
                        pass
                headlines.append(h_data)

            descriptions = []
            for d in ad.responsive_search_ad.descriptions:
                descriptions.append(d.text)

            path1 = ad.responsive_search_ad.path1 or ""
            path2 = ad.responsive_search_ad.path2 or ""

            # Build corrected URL
            corrected_url = url.replace(current_domain, expected_domain, 1)

            wrong_domain.append({
                "ad_group_id": ag_id,
                "ad_group_name": ag_name,
                "ad_id": ad_id,
                "old_url": url,
                "new_url": corrected_url,
                "old_domain": current_domain,
                "headlines": headlines,
                "descriptions": descriptions,
                "path1": path1,
                "path2": path2,
            })

        result = {
            "status": "success",
            "country_code": country_code,
            "campaign_id": campaign_id,
            "expected_domain": expected_domain,
            "dry_run": request.dry_run,
            "total_rsas_scanned": total_rsas,
            "correct_domain": correct_domain_count,
            "wrong_domain": len(wrong_domain),
            "wrong_domains_found": list(set(r["old_domain"] for r in wrong_domain)),
        }

        if request.dry_run:
            # Report only
            result["would_fix"] = [
                {"ad_group": r["ad_group_name"], "ad_id": r["ad_id"],
                 "old_url": r["old_url"], "new_url": r["new_url"]}
                for r in wrong_domain[:50]  # limit output
            ]
            if len(wrong_domain) > 50:
                result["truncated"] = True
                result["total_would_fix"] = len(wrong_domain)
            return result

        # --- Execute fixes ASYNC (background thread) ---
        import uuid as _uuid_fix
        import threading as _threading_fix

        job_id = _uuid_fix.uuid4().hex[:16]

        # Store job state in module-level dict
        if not hasattr(batch_fix_domains, "_jobs"):
            batch_fix_domains._jobs = {}

        job_state = {
            "job_id": job_id,
            "status": "running",
            "total": len(wrong_domain),
            "processed": 0,
            "fixed": 0,
            "errors": 0,
            "error_details": [],
            "current_ad_group": "",
        }
        batch_fix_domains._jobs[job_id] = job_state

        def _fix_worker():
            from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client
            from google.protobuf import field_mask_pb2

            _ensure_client()
            customer_id = _format_customer_id(CUSTOMER_ID)
            service = google_ads_client.get_service("AdGroupAdService")

            # Process in batches: pause old RSA, create new one
            for i, rsa in enumerate(wrong_domain):
                job_state["current_ad_group"] = rsa["ad_group_name"]
                try:
                    # Step 1: Pause old RSA
                    op = google_ads_client.get_type("AdGroupAdOperation")
                    resource_name = f"customers/{customer_id}/adGroupAds/{rsa['ad_group_id']}~{rsa['ad_id']}"
                    ad_upd = op.update
                    ad_upd.resource_name = resource_name
                    ad_upd.status = google_ads_client.enums.AdGroupAdStatusEnum.PAUSED
                    op.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=["status"]))
                    service.mutate_ad_group_ads(customer_id=customer_id, operations=[op])

                    # Step 2: Create new RSA with corrected URL
                    _create_rsa(
                        ad_group_id=rsa["ad_group_id"],
                        headlines=rsa["headlines"],
                        descriptions=rsa["descriptions"],
                        final_urls=[rsa["new_url"]]
                    )
                    job_state["fixed"] += 1
                except Exception as e:
                    job_state["errors"] += 1
                    if len(job_state["error_details"]) < 20:
                        job_state["error_details"].append(
                            f"{rsa['ad_group_name']} ad {rsa['ad_id']}: {str(e)[:200]}"
                        )

                job_state["processed"] = i + 1

            job_state["status"] = "completed"

            # Log to changelog
            try:
                db = _get_db()
                db.add_changelog(
                    category="bugfix",
                    title=f"Fixed {job_state['fixed']} wrong-domain RSAs in {country_code} campaign {campaign_id}",
                    description=f"Domains corrected: {result['wrong_domains_found']} → {expected_domain}",
                    details=f"Total: {job_state['total']}, fixed: {job_state['fixed']}, errors: {job_state['errors']}",
                    country_code=country_code,
                    campaign_id=campaign_id,
                    severity="warning"
                )
            except Exception:
                pass

        thread = _threading_fix.Thread(target=_fix_worker, daemon=True)
        thread.start()

        result["status"] = "submitted"
        result["job_id"] = job_id
        result["message"] = f"Background job started to fix {len(wrong_domain)} RSAs. Poll with batch_fix_domains_progress(job_id='{job_id}')."
        return result

    class BatchFixDomainsProgressRequest(BaseModel):
        """Input for batch_fix_domains_progress tool."""
        job_id: str = Field(..., description="Job ID returned by batch_fix_domains")

    @mcp_app.tool()
    async def batch_fix_domains_progress(request: BatchFixDomainsProgressRequest) -> dict:
        """Check status of a batch_fix_domains background job."""
        if not hasattr(batch_fix_domains, "_jobs"):
            return {"status": "error", "message": "No jobs found"}
        job = batch_fix_domains._jobs.get(request.job_id)
        if not job:
            return {"status": "error", "message": f"Job {request.job_id} not found"}
        pct = round(job["processed"] / max(job["total"], 1) * 100, 1)
        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "total": job["total"],
            "processed": job["processed"],
            "fixed": job["fixed"],
            "errors": job["errors"],
            "current_ad_group": job["current_ad_group"],
            "percent": pct,
            "error_details": job["error_details"][:10] if job["error_details"] else [],
        }

    # ------------------------------------------------------------------
    # Live Guardrail Validation (API-based audit)
    # ------------------------------------------------------------------

    class BatchValidateGuardrailsRequest(BaseModel):
        """Input for batch_validate_guardrails tool."""
        country_code: str = Field(..., description="Country code")
        campaign_id: str = Field(..., description="Google Ads campaign ID")
        product_handles: Optional[List[str]] = Field(None, description="Specific handles (None=all with ad_groups)")
        dry_run: bool = Field(False, description="Preview only")

    @mcp_app.tool()
    async def batch_validate_guardrails(request: BatchValidateGuardrailsRequest) -> dict:
        """
        Live API validation of guardrails — checks actual Google Ads state.

        More expensive than report-based guardrails (2 GAQL calls for campaign-wide data),
        but detects post-creation issues like moderation rejections.
        """
        country_code = request.country_code.upper()
        db = _get_db()

        # Fetch campaign-wide data (2 GAQL calls, cached)
        try:
            existing_ad_groups = _get_existing_ad_groups(request.campaign_id)
            existing_assets = _get_ad_group_assets(request.campaign_id, force_refresh=True)
        except Exception as e:
            return {"status": "error", "message": f"GAQL fetch failed: {e}"}

        # Fetch ad strength data (1 GAQL call)
        ad_strength_map = {}
        try:
            from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id
            customer_id = _format_customer_id(CUSTOMER_ID)
            strength_query = f"""
                SELECT campaign.id, ad_group.id, ad_group_ad.ad.id,
                       ad_group_ad.ad.responsive_search_ad.ad_strength
                FROM ad_group_ad
                WHERE campaign.id = {request.campaign_id}
                  AND ad_group_ad.status != 'REMOVED'
                  AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
            """
            strength_rows = _execute_gaql(customer_id, strength_query)
            for row in strength_rows:
                ag_id = _safe_get_value(row, "ad_group.id")
                strength = _safe_get_value(row, "ad_group_ad.ad.responsive_search_ad.ad_strength")
                if ag_id:
                    ag_id_str = str(ag_id)
                    if ag_id_str not in ad_strength_map:
                        ad_strength_map[ag_id_str] = []
                    ad_strength_map[ag_id_str].append(str(strength))
        except Exception as e:
            _debug("exception", f"ad_strength fallback at line 6945: {e}", "warning")
            ad_strength_map = {}

        # Get URL map for handle->ad_group matching
        url_map = db.get_cached_url_map(country_code, request.campaign_id)
        if not url_map:
            try:
                url_map = _get_ad_group_url_map(request.campaign_id, force_refresh=True)
                if url_map:
                    db.save_url_map(country_code, request.campaign_id, url_map)
            except Exception as e:
                _debug("exception", f"url_map fallback at line 6956: {e}", "warning")
                url_map = {}

        # Determine which handles to validate
        if request.product_handles:
            handles = request.product_handles
        else:
            handles = list(url_map.keys())

        results = []
        for handle in handles:
            ag_info = url_map.get(handle)
            if not ag_info:
                results.append({"handle": handle, "overall_status": "NO_AD_GROUP"})
                continue

            ag_id = ag_info["id"]
            ag_status = ag_info.get("status", "")
            ag_assets = existing_assets.get(ag_id, {})

            checks = {}
            # RSA A/B/C
            rsa_count = ag_assets.get("RSA", 0)
            checks["rsa_a"] = {"status": "PASS"} if rsa_count >= 1 else {"status": "FAIL", "reason": "MISSING_IN_API"}
            checks["rsa_b"] = {"status": "PASS"} if rsa_count >= 2 else {"status": "FAIL", "reason": "MISSING_IN_API"}
            checks["rsa_c"] = {"status": "PASS"} if rsa_count >= 3 else {"status": "FAIL", "reason": "MISSING_IN_API"}
            # Extensions
            checks["callouts"] = {"status": "PASS"} if ag_assets.get("CALLOUT", 0) >= 2 else {"status": "FAIL", "reason": "MISSING_IN_API"}
            checks["sitelinks"] = {"status": "PASS"} if ag_assets.get("SITELINK", 0) >= 4 else {"status": "FAIL", "reason": "MISSING_IN_API"}
            checks["promotion"] = {"status": "PASS"} if ag_assets.get("PROMOTION", 0) > 0 else {"status": "FAIL", "reason": "MISSING_IN_API"}
            checks["snippets"] = {"status": "PASS"} if ag_assets.get("STRUCTURED_SNIPPET", 0) > 0 else {"status": "FAIL", "reason": "MISSING_IN_API"}
            # Images
            checks["images"] = {"status": "PASS"} if ag_assets.get("AD_IMAGE", 0) >= IMAGE_MIN_EXISTING else {"status": "FAIL", "reason": "MISSING_IN_API"}
            # Ad group status
            checks["ad_group"] = {"status": "PASS"} if "ENABLED" in str(ag_status) else {"status": "FAIL", "reason": f"STATUS_{ag_status}"}
            # Keywords — would need separate GAQL, skip for now
            checks["keywords"] = {"status": "SKIP", "reason": "NOT_CHECKED_IN_LIVE_AUDIT"}

            # Ad strength check
            strengths = ad_strength_map.get(ag_id, [])
            if strengths:
                # Use worst strength across RSAs
                strength_order = {"EXCELLENT": 4, "GOOD": 3, "AVERAGE": 2, "POOR": 1, "UNSPECIFIED": 0}
                worst = min(strengths, key=lambda s: strength_order.get(s, 0))
                checks["ad_strength"] = {
                    "status": "PASS" if worst in ("EXCELLENT", "GOOD") else "WARN",
                    "value": worst,
                    "all_strengths": ",".join(strengths),
                }
                if worst in ("POOR", "UNSPECIFIED"):
                    checks["ad_strength"]["status"] = "FAIL"
                    checks["ad_strength"]["reason"] = f"AD_STRENGTH_{worst}"
            else:
                checks["ad_strength"] = {"status": "SKIP", "reason": "NO_RSA_DATA"}

            if not request.dry_run:
                db.upsert_guardrail(country_code, request.campaign_id, handle, checks,
                                   ad_group_id=ag_id, source='sync')

            passed = sum(1 for c in checks.values() if c.get("status") == "PASS")
            failed = sum(1 for c in checks.values() if c.get("status") == "FAIL")
            skipped = sum(1 for c in checks.values() if c.get("status") == "SKIP")

            overall = "FAILED" if failed > 0 else ("PARTIAL" if skipped > 0 else "COMPLETE")
            results.append({
                "handle": handle, "ad_group_id": ag_id,
                "overall_status": overall,
                "passed": passed, "failed": failed, "skipped": skipped,
                "failed_checks": [name for name, c in checks.items() if c.get("status") == "FAIL"],
            })

        # Summary
        complete = sum(1 for r in results if r.get("overall_status") == "COMPLETE")
        partial = sum(1 for r in results if r.get("overall_status") == "PARTIAL")
        failed = sum(1 for r in results if r.get("overall_status") == "FAILED")

        return {
            "status": "success",
            "summary": {"total": len(results), "complete": complete, "partial": partial, "failed": failed},
            "results": [r for r in results if r.get("overall_status") != "COMPLETE"][:50],  # Only show non-complete
        }

    # ── 21. batch_global_dashboard ──────────────────────────────────────
    class GlobalDashboardRequest(BaseModel):
        include_xml: bool = True

    @mcp_app.tool("batch_global_dashboard")
    async def batch_global_dashboard(request: GlobalDashboardRequest) -> dict:
        """Global cross-country dashboard showing batch optimizer progress per country.

        Merges: priority ranking (from hardcoded v3 data), XML feed product counts,
        Google Ads campaign mapping, and batch_state.db setup progress.
        Returns countries sorted by priority (SCALE UP first, then by revenue).
        """
        # Priority ranking from GoogleAds_v3_PerCountry.xlsx — Countries - Ranking
        COUNTRY_PRIORITY = [
            {"rank":1,"cc":"RO","name":"Romania","conv":3341,"gbp":59772,"rec":"SCALE UP"},
            {"rank":2,"cc":"HU","name":"Hungary","conv":2091,"gbp":39272,"rec":"SCALE UP"},
            {"rank":3,"cc":"PL","name":"Poland","conv":1262,"gbp":20294,"rec":"SCALE UP"},
            {"rank":4,"cc":"TR","name":"Turkey","conv":1521,"gbp":15540,"rec":"SCALE UP"},
            {"rank":5,"cc":"GR","name":"Greece","conv":605,"gbp":9553,"rec":"SCALE UP"},
            {"rank":6,"cc":"IT","name":"Italy","conv":442,"gbp":8938,"rec":"SCALE UP"},
            {"rank":7,"cc":"FR","name":"France","conv":629,"gbp":8520,"rec":"SCALE UP"},
            {"rank":8,"cc":"ES","name":"Spain","conv":518,"gbp":8497,"rec":"SCALE UP"},
            {"rank":9,"cc":"CZ","name":"Czech Rep.","conv":366,"gbp":6042,"rec":"SCALE UP"},
            {"rank":10,"cc":"HR","name":"Croatia","conv":323,"gbp":5749,"rec":"SCALE UP"},
            {"rank":11,"cc":"CH","name":"Switzerland","conv":211,"gbp":4413,"rec":"SCALE UP"},
            {"rank":12,"cc":"PE","name":"Peru","conv":418,"gbp":4400,"rec":"Ogranicz"},
            {"rank":13,"cc":"SK","name":"Slovakia","conv":261,"gbp":4392,"rec":"Ogranicz"},
            {"rank":14,"cc":"BG","name":"Bulgaria","conv":282,"gbp":4059,"rec":"Ogranicz"},
            {"rank":15,"cc":"DE","name":"Germany","conv":225,"gbp":4011,"rec":"Ogranicz"},
            {"rank":16,"cc":"LT","name":"Lithuania","conv":177,"gbp":3397,"rec":"Ogranicz"},
            {"rank":17,"cc":"CO","name":"Colombia","conv":346,"gbp":3260,"rec":"Ogranicz"},
            {"rank":18,"cc":"UA","name":"Ukraine","conv":219,"gbp":2202,"rec":"Ogranicz"},
            {"rank":19,"cc":"PH","name":"Philippines","conv":180,"gbp":2067,"rec":"Ogranicz"},
            {"rank":20,"cc":"AR","name":"Argentina","conv":264,"gbp":1993,"rec":"Ogranicz"},
            {"rank":21,"cc":"SI","name":"Slovenia","conv":100,"gbp":1722,"rec":"Ogranicz"},
            {"rank":22,"cc":"EE","name":"Estonia","conv":71,"gbp":1557,"rec":"Ogranicz"},
            {"rank":23,"cc":"MY","name":"Malaysia","conv":94,"gbp":1224,"rec":"Ogranicz"},
            {"rank":24,"cc":"BD","name":"Bangladesh","conv":296,"gbp":1126,"rec":"Ogranicz"},
            {"rank":25,"cc":"SA","name":"Saudi Arabia","conv":81,"gbp":1081,"rec":"Ogranicz"},
            {"rank":26,"cc":"TH","name":"Thailand","conv":60,"gbp":1037,"rec":"Ogranicz"},
            {"rank":27,"cc":"IN","name":"India","conv":248,"gbp":990,"rec":"Ogranicz"},
            {"rank":28,"cc":"BA","name":"Bosnia","conv":59,"gbp":843,"rec":"Ogranicz"},
            {"rank":29,"cc":"BE","name":"Belgium","conv":60,"gbp":791,"rec":"Ogranicz"},
            {"rank":30,"cc":"MX","name":"Mexico","conv":58,"gbp":687,"rec":"Ogranicz"},
            {"rank":31,"cc":"AT","name":"Austria","conv":46,"gbp":668,"rec":"Ogranicz"},
            {"rank":32,"cc":"AE","name":"UAE","conv":62,"gbp":647,"rec":"Ogranicz"},
            {"rank":33,"cc":"UZ","name":"Uzbekistan","conv":76,"gbp":557,"rec":"Ogranicz"},
            {"rank":34,"cc":"DZ","name":"Algeria","conv":46,"gbp":401,"rec":"Ogranicz"},
            {"rank":35,"cc":"CY","name":"Cyprus","conv":26,"gbp":387,"rec":"Ogranicz"},
            {"rank":36,"cc":"LV","name":"Latvia","conv":18,"gbp":353,"rec":"Ogranicz"},
            {"rank":37,"cc":"MA","name":"Morocco","conv":35,"gbp":325,"rec":"Ogranicz"},
            {"rank":38,"cc":"PT","name":"Portugal","conv":18,"gbp":289,"rec":"Ogranicz"},
            {"rank":39,"cc":"EG","name":"Egypt","conv":37,"gbp":271,"rec":"Ogranicz"},
            {"rank":40,"cc":"NG","name":"Nigeria","conv":29,"gbp":211,"rec":"Ogranicz"},
            {"rank":41,"cc":"RS","name":"Serbia","conv":13,"gbp":197,"rec":"PAUZA"},
            {"rank":42,"cc":"CI","name":"Ivory Coast","conv":14,"gbp":146,"rec":"PAUZA"},
        ]
        cc_priority = {c["cc"]: c for c in COUNTRY_PRIORITY}

        # XML feed product counts (from Bluewinston CSV)
        XML_FEEDS = {
            "PL":417,"US":410,"RO":251,"HU":167,"CZ":102,"IT":97,"ES":97,
            "GR":81,"SK":76,"BG":74,"TR":73,"HR":62,"DE":52,"UA":48,"FR":47,
            "LT":46,"MD":43,"CO":39,"IN":33,"SI":32,"PH":26,"CH":25,"PE":25,
            "EE":25,"MX":24,"BE":21,"BA":20,"UK":19,"AR":19,"BD":18,"AT":16,
            "TH":16,"PT":15,"MY":14,"SG":14,"DZ":13,"EG":12,"RS":11,"MA":10,
            "CY":10,"NG":9,"AE":9,"LV":8,"UZ":8,"KE":7,"CA":6,"VN":6,
            "GT":5,"CI":5,"KZ":4,"ID":4,"NZ":4,"AU":4,"UG":3,"LB":3,
            "ZA":3,"PK":3,"GE":3,"LU":2,"AM":2,"CL":2,"EC":2,"QA":2,
            "NL":2,"SA":2,"AL":2,"SV":1,"RU":1,"TZ":1,"SN":1,"GA":1,
            "MK":1,"AZ":1,"XK":1,
        }

        # Gather Google Ads campaign data
        campaigns_by_cc = {}
        try:
            from google.ads.googleads.client import GoogleAdsClient
            # Use GAQL to list enabled campaigns
            gaql = """SELECT campaign.id, campaign.name, campaign.status,
                             campaign.advertising_channel_type,
                             campaign_budget.amount_micros,
                             metrics.impressions, metrics.clicks,
                             metrics.conversions, metrics.cost_micros
                      FROM campaign
                      WHERE campaign.status = 'ENABLED'
                      ORDER BY metrics.cost_micros DESC"""
            result = _cached_gaql(CUSTOMER_ID, gaql)
            for row in result:
                name = row.get("campaign_name", row.get("name", ""))
                # Extract CC from campaign name (first 2 chars or "XX |" pattern)
                cc = None
                if " | " in name:
                    cc = name.split(" | ")[0].strip().split(" ")[0].upper()
                elif " - " in name:
                    cc = name.split(" - ")[0].strip().upper()
                if cc and len(cc) == 2:
                    if cc not in campaigns_by_cc:
                        campaigns_by_cc[cc] = {"count": 0, "budget_d": 0, "cost": 0, "conv": 0, "campaign_ids": []}
                    campaigns_by_cc[cc]["count"] += 1
                    cid = str(row.get("campaign_id", row.get("id", "")))
                    campaigns_by_cc[cc]["campaign_ids"].append(cid)
                    budget_micros = row.get("campaign_budget_amount_micros", row.get("budget_micros", 0)) or 0
                    campaigns_by_cc[cc]["budget_d"] += budget_micros / 1_000_000
                    cost_micros = row.get("metrics_cost_micros", row.get("cost_micros", 0)) or 0
                    campaigns_by_cc[cc]["cost"] += cost_micros / 1_000_000
                    conv = row.get("metrics_conversions", row.get("conversions", 0)) or 0
                    campaigns_by_cc[cc]["conv"] += conv
        except Exception as e:
            campaigns_by_cc = {"_error": str(e)[:200]}

        # Gather batch_state.db progress per country
        batch_progress = {}
        try:
            conn = db._get_conn()
            cursor = conn.execute("""
                SELECT country_code, campaign_id,
                       COUNT(*) as total,
                       SUM(CASE WHEN status = 'SETUP_COMPLETE' THEN 1 ELSE 0 END) as complete,
                       SUM(CASE WHEN status LIKE '%IMAGE%' OR status = 'COMPLETE_NO_IMAGES' THEN 1 ELSE 0 END) as no_images
                FROM product_setup
                GROUP BY country_code, campaign_id
            """)
            for row in cursor.fetchall():
                cc = row[0].upper() if row[0] else "??"
                batch_progress[cc] = {
                    "campaign_id": row[1],
                    "total_in_db": row[2],
                    "complete": row[3],
                    "no_images": row[4],
                    "pct": round(row[3] / row[2] * 100, 1) if row[2] > 0 else 0,
                }
        except Exception as e:
            _debug("exception", f"silent pass at line 7170: {e}", "warning")
            pass

        # Build unified dashboard
        countries = []
        all_ccs = set(list(cc_priority.keys()) + list(XML_FEEDS.keys()))
        for cc in all_ccs:
            priority = cc_priority.get(cc, {})
            ads = campaigns_by_cc.get(cc, {})
            batch = batch_progress.get(cc, {})
            xml_products = XML_FEEDS.get(cc, 0)

            entry = {
                "rank": priority.get("rank", 99),
                "cc": cc,
                "name": priority.get("name", cc),
                "recommendation": priority.get("rec", "N/A"),
                "gbp_180d": priority.get("gbp", 0),
                "conv_180d": priority.get("conv", 0),
                "xml_products": xml_products,
                "ads_campaigns": ads.get("count", 0) if isinstance(ads, dict) else 0,
                "ads_campaign_ids": ads.get("campaign_ids", []) if isinstance(ads, dict) else [],
                "ads_budget_day": round(ads.get("budget_d", 0), 0) if isinstance(ads, dict) else 0,
                "ads_cost_total": round(ads.get("cost", 0), 0) if isinstance(ads, dict) else 0,
                "ads_conv_total": round(ads.get("conv", 0), 0) if isinstance(ads, dict) else 0,
                "batch_setup": batch.get("total_in_db", 0),
                "batch_complete": batch.get("complete", 0),
                "batch_pct": batch.get("pct", 0),
                "batch_campaign_id": batch.get("campaign_id", ""),
                "status": "NOT_STARTED",
            }

            # Determine status
            if entry["batch_pct"] >= 90:
                entry["status"] = "DONE"
            elif entry["batch_pct"] > 0:
                entry["status"] = "IN_PROGRESS"
            elif entry["ads_campaigns"] > 0:
                entry["status"] = "HAS_ADS"
            elif xml_products > 0:
                entry["status"] = "XML_READY"
            else:
                entry["status"] = "NO_FEED"

            countries.append(entry)

        # Sort: by rank (priority order)
        countries.sort(key=lambda x: x["rank"])

        # Summary stats
        scale_up = [c for c in countries if c["recommendation"] == "SCALE UP"]
        in_progress = [c for c in countries if c["status"] == "IN_PROGRESS"]
        done = [c for c in countries if c["status"] == "DONE"]

        return {
            "status": "success",
            "summary": {
                "total_countries": len(countries),
                "scale_up_countries": len(scale_up),
                "countries_with_ads": sum(1 for c in countries if c["ads_campaigns"] > 0),
                "countries_with_xml": sum(1 for c in countries if c["xml_products"] > 0),
                "batch_done": len(done),
                "batch_in_progress": len(in_progress),
                "batch_not_started": sum(1 for c in countries if c["status"] in ("NOT_STARTED", "HAS_ADS", "XML_READY")),
                "total_xml_products": sum(c["xml_products"] for c in countries),
                "total_ads_cost": sum(c["ads_cost_total"] for c in countries),
                "total_ads_conv": sum(c["ads_conv_total"] for c in countries),
            },
            "countries": countries,
        }

    # ------------------------------------------------------------------
    # batch_warmup_cache — Pre-warm GPT ad copy cache for all products
    # ------------------------------------------------------------------
    @mcp_app.tool()
    async def batch_warmup_cache(request: WarmupRequest) -> dict:
        """
        Pre-warm AI ad copy cache for all products before batch setup.

        Gathers performance data from Google Ads (winning patterns,
        per-product asset performance, keyword intelligence) and sends
        it to batch_initialize.php as an async job.

        The PHP endpoint spawns a background worker and returns a job_id
        immediately (<1s). This tool then polls for job completion every
        10 seconds until done (~35s per cold product, ~1ms per cached).

        After warmup, generate_ads.php returns cached results instantly
        (~1ms vs ~35s).

        Run this BEFORE batch_setup_all or batch_setup_products.

        Typical flow:
        1. batch_sync_from_api(TR, campaign_id)  — load feed
        2. batch_warmup_cache(TR, campaign_id)   — pre-warm cache (THIS TOOL)
        3. batch_setup_all(TR, campaign_id)      — setup products (instant AI copy)

        FULL analytics gathering (3-5 GAQL calls total):
        - Phase 1: Scans ALL campaigns for this country for BEST/GOOD/LOW
          asset performance labels → stores in asset_performance DB table
        - Phase 2: BULK query for ALL converting keywords across account,
          mapped to products by ad_group→URL → stores in keyword_intelligence DB
        - Data cached in DB for 2h (use force_refresh=True to re-gather)
        - If GAQL takes >45s, returns analytics_gathered status — call again
          and the 2nd call skips GAQL (uses cached DB) and goes straight to POST
        """
        tool_start = time.time()
        MCP_HARD_LIMIT = 55  # seconds — leave 5s safety margin below MCP 60s timeout

        country_code = request.country_code.upper()
        config = _get_config_for_country(country_code)

        # Feed URL resolution: NP_FEEDS > XML_FEEDS
        if request.feed_type == "new_products":
            xml_url = _resolve_np_feed_url(country_code, request.shop_domain)
            if not xml_url:
                return {"status": "error", "message": f"No NP_FEEDS BW feed for {country_code}" + (f" / {request.shop_domain}" if request.shop_domain else "")}
        else:
            xml_url = config.get("xml_url", "")
            if not xml_url:
                return {"status": "error", "message": f"No XML feed URL for country {country_code}."}

        # 1. Fetch + parse XML feed
        try:
            raw_products = _fetch_xml_feed_cached(country_code, xml_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch XML feed: {e}"}

        feed_products = {p["handle"]: p for p in raw_products}

        # 2. Determine handles
        if request.product_handles:
            handles = list(request.product_handles)
        else:
            handles = list(feed_products.keys())

        if request.max_products > 0:
            handles = handles[: request.max_products]

        if not handles:
            return {"status": "error", "message": "No product handles to warm."}

        db = _get_db()

        # 3. FULL analytics: Gather data from Google Ads API via GAQL
        #    Phase 1: Asset performance labels (all campaigns for this country)
        #    Phase 2: Bulk keyword intelligence (all converting keywords → mapped to products)
        #    Both phases store results in SQLite DB, so subsequent reads are instant.
        #
        #    OPTIMIZATION: If DB already has fresh data (cached_at within last 2 hours),
        #    skip GAQL and use cached DB data — saves ~15-30s of API calls.
        conn = db._get_conn()
        fresh_asset_count = 0
        try:
            row = conn.execute(
                """SELECT COUNT(*) as cnt FROM asset_performance
                   WHERE country_code=? AND cached_at > datetime('now', '-2 hours')""",
                (country_code,)
            ).fetchone()
            fresh_asset_count = row["cnt"] if row else 0
        except Exception as e:
            _debug("exception", f"silent pass at line 7325: {e}", "warning")
            pass

        if fresh_asset_count > 0 and not request.force_refresh:
            analytics_summary = {
                "skipped": True,
                "reason": f"DB has {fresh_asset_count} fresh asset records (< 2h old). Use force_refresh=True to re-gather.",
            }
        else:
            analytics_summary = _gather_full_analytics_from_gaql(
                country_code, request.campaign_id, feed_products
            )

        # 4. Read from DB and build per-product performance data
        winning_patterns = _gather_winning_patterns(country_code, db)

        per_product: Dict[str, Any] = {}
        products_with_perf = 0
        products_with_kw = 0

        for h in handles:
            product_data: Dict[str, Any] = {}

            # Asset performance — now populated by GAQL Phase 1 above
            try:
                conn = db._get_conn()
                perf: Dict[str, Any] = {}
                for asset_type, hl_keys in [
                    ("HEADLINE", ("best_headlines", "good_headlines", "low_headlines")),
                    ("DESCRIPTION", ("best_descriptions", "good_descriptions", "low_descriptions")),
                ]:
                    rows = conn.execute(
                        """SELECT headline_pattern, performance_label, SUM(occurrences) as total_occ
                           FROM asset_performance
                           WHERE country_code=? AND asset_type=?
                           GROUP BY headline_pattern, performance_label
                           ORDER BY total_occ DESC
                           LIMIT 30""",
                        (country_code, asset_type),
                    ).fetchall()
                    if rows:
                        by_label: Dict[str, list] = {"BEST": [], "GOOD": [], "LOW": []}
                        for r in rows:
                            label = r["performance_label"]
                            if label in by_label:
                                by_label[label].append(r["headline_pattern"])
                        for label, key in zip(["BEST", "GOOD", "LOW"], hl_keys):
                            if by_label[label]:
                                perf[key] = by_label[label][:8]
                if perf:
                    product_data["existing_performance"] = perf
                    products_with_perf += 1
            except Exception as e:
                _debug("exception", f"silent pass at line 7378: {e}", "warning")
                pass

            # Keyword intelligence — now populated by GAQL Phase 2 above
            fpt = feed_products.get(h, {}).get("product_type", "")
            kw_intel = _gather_product_keyword_intelligence(h, country_code, feed_product_type=fpt)
            if kw_intel:
                product_data["keyword_intelligence"] = kw_intel
                products_with_kw += 1

            overrides = _gather_product_overrides_from_feed(h, feed_products)
            if overrides:
                product_data["product_overrides"] = overrides

            if product_data:
                per_product[h] = product_data

        # Check time budget before POST — GAQL gathering may have consumed most of the 55s
        elapsed_so_far = time.time() - tool_start
        if elapsed_so_far > (MCP_HARD_LIMIT - 10):
            # Not enough time for POST + any polling. Return analytics summary only.
            return {
                "status": "analytics_gathered",
                "message": f"GAQL analytics gathered in {elapsed_so_far:.0f}s (stored in DB). "
                           f"Call batch_warmup_cache again to POST to PHP — DB reads will be instant.",
                "country_code": country_code,
                "campaign_id": request.campaign_id,
                "analytics_gaql": analytics_summary,
                "performance_data_summary": {
                    "winning_patterns_count": len(winning_patterns.get("best_headline_patterns", [])) + len(winning_patterns.get("best_description_patterns", [])),
                    "products_with_performance": products_with_perf,
                    "products_with_keywords": products_with_kw,
                },
                "total_handles": len(handles),
                "elapsed_seconds": round(elapsed_so_far, 1),
            }

        # 5. POST to create async job (returns job_id in <1s)
        #    PHP endpoint has BATCH_INITIALIZE_MAX_HANDLES=100 limit.
        #    If >100 handles, split into chunks and submit sequentially.
        WARMUP_CHUNK_SIZE = 95  # Stay below 100 limit with safety margin

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        handle_chunks = [handles[i:i + WARMUP_CHUNK_SIZE] for i in range(0, len(handles), WARMUP_CHUNK_SIZE)]
        all_job_ids = []

        for chunk_idx, chunk_handles in enumerate(handle_chunks):
            # Build per-product subset for this chunk
            chunk_per_product = {h: per_product[h] for h in chunk_handles if h in per_product}

            payload = {
                "country_code": country_code,
                "product_handles": chunk_handles,
                "mode": "copy_only",
                "force_refresh": request.force_refresh,
                "performance_data": {
                    "winning_patterns": winning_patterns,
                    "per_product": chunk_per_product,
                },
            }

            try:
                payload_json = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(
                    BATCH_INITIALIZE_URL,
                    data=payload_json,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=BATCH_INITIALIZE_POST_TIMEOUT, context=ssl_ctx) as resp:
                    job_response = json.loads(resp.read().decode("utf-8"))
                    jid = job_response.get("job_id")
                    if jid:
                        all_job_ids.append(jid)
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create warmup job (chunk {chunk_idx+1}/{len(handle_chunks)}): {str(e)[:300]}",
                    "country_code": country_code,
                    "total_handles": len(handles),
                    "chunk_size": len(chunk_handles),
                    "completed_chunks": chunk_idx,
                    "job_ids_so_far": all_job_ids,
                }

            # Check time budget between chunks
            if time.time() - tool_start > (MCP_HARD_LIMIT - 5) and chunk_idx < len(handle_chunks) - 1:
                return {
                    "status": "partial_submitted",
                    "message": f"Submitted {chunk_idx+1}/{len(handle_chunks)} chunks before MCP timeout. "
                               f"Call again to submit remaining chunks.",
                    "job_ids": all_job_ids,
                    "total_handles": len(handles),
                    "submitted_handles": (chunk_idx + 1) * WARMUP_CHUNK_SIZE,
                    "remaining_chunks": len(handle_chunks) - chunk_idx - 1,
                }

        if not all_job_ids:
            return {"status": "error", "message": f"No job_ids received from any chunk."}

        # Use the LAST job_id for polling (it will be the one processing the last chunk)
        job_id = all_job_ids[-1]

        # 6. Poll in a tight loop until MCP timeout approaches.
        #    GAQL gathering above may have consumed 10-30s, so dynamically calculate remaining time.
        #    If job finishes quickly (all cached), we return immediately.
        #    If job is still processing when time runs out, return job_id for manual polling
        #    via batch_warmup_status tool.
        poll_url = f"{BATCH_INITIALIZE_URL}?job_id={job_id}"
        last_status = None

        while (time.time() - tool_start) < MCP_HARD_LIMIT:
            time.sleep(BATCH_INITIALIZE_POLL_INTERVAL)

            try:
                poll_req = urllib.request.Request(poll_url, method="GET")
                with urllib.request.urlopen(poll_req, timeout=BATCH_INITIALIZE_POLL_TIMEOUT, context=ssl_ctx) as resp:
                    last_status = json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                _debug("exception", f"silent continue at line 7500: {e}", "warning")
                continue

            job_status = last_status.get("status", "")

            if job_status == "completed":
                total_time_s = round(last_status.get("total_time_ms", 0) / 1000, 1)
                return {
                    "status": "success",
                    "country_code": country_code,
                    "campaign_id": request.campaign_id,
                    "job_id": job_id,
                    "total": last_status.get("total", len(handles)),
                    "warmed": last_status.get("success", 0),
                    "cached": last_status.get("cached", 0),
                    "errors": last_status.get("errors", 0),
                    "total_time_seconds": total_time_s,
                    "error_handles": last_status.get("error_handles", []),
                    "analytics_gaql": analytics_summary,
                    "performance_data_summary": {
                        "winning_patterns_count": len(winning_patterns.get("best_headline_patterns", [])) + len(winning_patterns.get("best_description_patterns", [])),
                        "products_with_performance": products_with_perf,
                        "products_with_keywords": products_with_kw,
                    },
                }

            if job_status == "failed":
                return {
                    "status": "error",
                    "message": f"Job {job_id} failed: {last_status.get('error', 'unknown')}",
                    "job_id": job_id,
                    "country_code": country_code,
                }

        # Job still running after ~45s — return job_id so agent can poll via batch_warmup_status
        processed = last_status.get("processed", 0) if last_status else 0
        total = last_status.get("total", len(handles)) if last_status else len(handles)
        return {
            "status": "submitted",
            "message": f"Job {job_id} is running ({processed}/{total} processed). Use batch_warmup_status(job_id='{job_id}') to check progress. Job continues on server.",
            "job_id": job_id,
            "country_code": country_code,
            "campaign_id": request.campaign_id,
            "total": total,
            "processed": processed,
        }

    # ------------------------------------------------------------------
    # batch_warmup_status — Poll async warmup job status
    # ------------------------------------------------------------------
    @mcp_app.tool()
    async def batch_warmup_status(request: WarmupStatusRequest) -> dict:
        """
        Check status of a warmup job created by batch_warmup_cache.

        Returns current progress: processed/total, success/cached/errors,
        current_handle being processed, and per-product results.

        Call this periodically (every 30-60s) after batch_warmup_cache
        returns status="submitted" with a job_id.

        When status is "completed" or "failed", the job is done.
        """
        job_id = request.job_id.strip()
        if not job_id:
            return {"status": "error", "message": "job_id is required"}

        poll_url = f"{BATCH_INITIALIZE_URL}?job_id={job_id}"

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        try:
            poll_req = urllib.request.Request(poll_url, method="GET")
            with urllib.request.urlopen(poll_req, timeout=BATCH_INITIALIZE_POLL_TIMEOUT, context=ssl_ctx) as resp:
                status_data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            return {"status": "error", "message": f"Failed to poll job {job_id}: {str(e)[:200]}"}

        # Enrich with computed fields
        job_status = status_data.get("status", "unknown")
        processed = status_data.get("processed", 0)
        total = status_data.get("total", 0)

        if job_status == "completed":
            total_time_s = round(status_data.get("total_time_ms", 0) / 1000, 1)
            status_data["total_time_seconds"] = total_time_s

        if total > 0 and processed > 0 and job_status == "processing":
            # Estimate remaining time
            elapsed_ms = status_data.get("elapsed_ms", 0)
            if elapsed_ms > 0:
                avg_per_product = elapsed_ms / processed
                remaining = (total - processed) * avg_per_product
                status_data["eta_seconds"] = round(remaining / 1000)

        return status_data


    # ------------------------------------------------------------------
    # Guard: Remove wrong-domain ads account-wide
    # ------------------------------------------------------------------

    class BatchRemoveWrongDomainAdsRequest(BaseModel):
        """Input for batch_remove_wrong_domain_ads tool."""
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_remove_wrong_domain_ads(request: BatchRemoveWrongDomainAdsRequest) -> dict:
        """
        Find and REMOVE ads with wrong domains across ALL campaigns on the account.

        Scans every ENABLED campaign, checks RSA final_urls against expected
        DOMAINS[country_code] mapping. Wrong-domain ads are permanently REMOVED
        (not just paused) because they show errors like "Disapproved (One website
        per ad group)".

        Use dry_run=true first to see what would be removed.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        # 1. Single GAQL query for ALL non-removed RSAs across all ENABLED Search campaigns
        rsa_query = """
            SELECT campaign.id, campaign.name, campaign.status,
                   ad_group_ad.ad.id, ad_group.id, ad_group.name,
                   ad_group_ad.ad.final_urls, ad_group_ad.status
            FROM ad_group_ad
            WHERE campaign.status = 'ENABLED'
              AND campaign.advertising_channel_type = 'SEARCH'
              AND ad_group_ad.ad.type = RESPONSIVE_SEARCH_AD
              AND ad_group_ad.status != 'REMOVED'
        """
        rsas = _execute_gaql(customer_id, rsa_query, page_size=10000)

        total_scanned = len(rsas)
        wrong_domain_ads = []
        removed = []
        failed = []
        campaigns_seen = set()

        # Build campaign->country_code map from first URL per campaign
        camp_cc_map = {}
        for row in rsas:
            camp_id = str(_safe_get_value(row, "campaign.id", ""))
            campaigns_seen.add(camp_id)
            if camp_id in camp_cc_map:
                continue
            camp_name = str(_safe_get_value(row, "campaign.name", ""))
            # Try name-based detection
            detected_cc = None
            for cc in DOMAINS:
                if f"-{cc.lower()}" in camp_name.lower() or f"_{cc.lower()}" in camp_name.lower() or f" {cc} " in camp_name.upper():
                    detected_cc = cc
                    break
            if not detected_cc:
                urls = _safe_get_value(row, "ad_group_ad.ad.final_urls", [])
                url_str = str(urls[0]) if urls else ""
                for cc, domain in DOMAINS.items():
                    if domain in url_str:
                        detected_cc = cc
                        break
            camp_cc_map[camp_id] = detected_cc

        for row in rsas:
            camp_id = str(_safe_get_value(row, "campaign.id", ""))
            detected_cc = camp_cc_map.get(camp_id)
            if not detected_cc:
                continue
            expected_domain = DOMAINS.get(detected_cc, "")
            if not expected_domain:
                continue

            ad_id = str(_safe_get_value(row, "ad_group_ad.ad.id", ""))
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            ag_name = str(_safe_get_value(row, "ad_group.name", ""))
            camp_name = str(_safe_get_value(row, "campaign.name", ""))
            urls = _safe_get_value(row, "ad_group_ad.ad.final_urls", [])
            ad_status = _safe_get_value(row, "ad_group_ad.status", "")
            ad_status_str = ad_status.name if hasattr(ad_status, 'name') else str(ad_status)
            url_str = str(urls[0]) if urls else ""

            if expected_domain not in url_str and url_str:
                wrong_domain_ads.append({
                    "campaign_id": camp_id, "campaign_name": camp_name,
                    "ad_group_id": ag_id, "ad_group_name": ag_name,
                    "ad_id": ad_id, "ad_status": ad_status_str,
                    "url": url_str[:100], "expected_domain": expected_domain,
                    "country_code": detected_cc,
                })

        # Batch remove wrong-domain ads (up to 5000 ops per mutate call)
        if not request.dry_run and wrong_domain_ads:
            service = google_ads_client.get_service("AdGroupAdService")
            ops = []
            for ad_info in wrong_domain_ads:
                op = google_ads_client.get_type("AdGroupAdOperation")
                op.remove = service.ad_group_ad_path(customer_id, ad_info["ad_group_id"], ad_info["ad_id"])
                ops.append(op)

            # Send in batches of 1000
            BATCH_SIZE = 1000
            for i in range(0, len(ops), BATCH_SIZE):
                batch = ops[i:i+BATCH_SIZE]
                try:
                    _retry_with_backoff(
                        service.mutate_ad_group_ads,
                        customer_id=customer_id, operations=batch
                    )
                    removed.extend([ad_info["ad_id"] for ad_info in wrong_domain_ads[i:i+len(batch)]])
                except Exception as e:
                    failed.append({"batch_start": i, "batch_size": len(batch), "error": str(e)[:200]})

        if not request.dry_run and removed:
            _auto_changelog(
                category="optimization",
                title=f"Guard: removed {len(removed)} wrong-domain ads account-wide",
                description=f"Scanned {total_scanned} RSAs across {len(campaigns_seen)} campaigns. "
                           f"Found {len(wrong_domain_ads)} wrong-domain, removed {len(removed)}, failed {len(failed)}.",
            )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "campaigns_scanned": len(campaigns_seen),
            "total_rsas_scanned": total_scanned,
            "wrong_domain_count": len(wrong_domain_ads),
            "wrong_domain_ads": wrong_domain_ads[:50],  # limit output
            "removed": len(removed),
            "failed": failed,
        }

    # ------------------------------------------------------------------
    # Guard: Remove NOT_ELIGIBLE sitelinks account-wide
    # ------------------------------------------------------------------

    class BatchRemoveNotEligibleSitelinksRequest(BaseModel):
        """Input for batch_remove_not_eligible_sitelinks tool."""
        campaign_id: Optional[str] = Field(None, description="Filter to specific campaign (None=all ENABLED campaigns)")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_remove_not_eligible_sitelinks(request: BatchRemoveNotEligibleSitelinksRequest) -> dict:
        """
        Find and REMOVE NOT_ELIGIBLE sitelinks across account or specific campaign.

        Scans ad_group_asset links where primary_status=NOT_ELIGIBLE and
        field_type=SITELINK. Removes the asset link (not the underlying asset).

        Use dry_run=true first to see what would be removed.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        campaign_filter = f"AND campaign.id = {request.campaign_id}" if request.campaign_id else "AND campaign.status = 'ENABLED'"

        query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   ad_group.id, ad_group.name, ad_group.status,
                   ad_group_asset.resource_name,
                   ad_group_asset.field_type,
                   ad_group_asset.primary_status,
                   ad_group_asset.primary_status_reasons
            FROM ad_group_asset
            WHERE ad_group_asset.field_type = 'SITELINK'
              AND ad_group_asset.primary_status = 'NOT_ELIGIBLE'
              AND ad_group.status = 'ENABLED'
              {campaign_filter}
        """
        results = _execute_gaql(customer_id, query, page_size=1000)

        not_eligible = []
        removed = []
        failed = []

        for row in results:
            rn = str(_safe_get_value(row, "ad_group_asset.resource_name", ""))
            camp_id = str(_safe_get_value(row, "campaign.id", ""))
            camp_name = str(_safe_get_value(row, "campaign.name", ""))
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            ag_name = str(_safe_get_value(row, "ad_group.name", ""))

            not_eligible.append({
                "resource_name": rn, "campaign_id": camp_id,
                "campaign_name": camp_name, "ad_group_name": ag_name,
            })

        # Batch remove NOT_ELIGIBLE sitelinks (up to 5000 ops per mutate call)
        if not request.dry_run and not_eligible:
            service = google_ads_client.get_service("AdGroupAssetService")
            ops = []
            for item in not_eligible:
                rn = item["resource_name"]
                if rn:
                    op = google_ads_client.get_type("AdGroupAssetOperation")
                    op.remove = rn
                    ops.append(op)

            BATCH_SIZE = 1000
            for i in range(0, len(ops), BATCH_SIZE):
                batch = ops[i:i+BATCH_SIZE]
                try:
                    _retry_with_backoff(
                        service.mutate_ad_group_assets,
                        customer_id=customer_id, operations=batch
                    )
                    removed.extend([item["resource_name"] for item in not_eligible[i:i+len(batch)]])
                except Exception as e:
                    failed.append({"batch_start": i, "batch_size": len(batch), "error": str(e)[:200]})

        if not request.dry_run and removed:
            _auto_changelog(
                category="optimization",
                title=f"Guard: removed {len(removed)} NOT_ELIGIBLE sitelinks",
                description=f"Found {len(not_eligible)} NOT_ELIGIBLE sitelinks, removed {len(removed)}, failed {len(failed)}.",
                campaign_id=request.campaign_id,
            )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "not_eligible_count": len(not_eligible),
            "not_eligible_sample": not_eligible[:20],  # limit output
            "removed": len(removed),
            "failed": failed,
        }

    # ------------------------------------------------------------------
    # Guard: Remove NOT_ELIGIBLE ads (disapproved RSAs) account-wide
    # ------------------------------------------------------------------

    class BatchRemoveNotEligibleAdsRequest(BaseModel):
        """Input for batch_remove_not_eligible_ads tool."""
        campaign_id: Optional[str] = Field(None, description="Filter to specific campaign (None=all ENABLED Search campaigns)")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_remove_not_eligible_ads(request: BatchRemoveNotEligibleAdsRequest) -> dict:
        """
        Find and REMOVE DISAPPROVED ads across account or specific campaign.

        Scans all ENABLED Search campaigns for ads where
        policy_summary.approval_status = DISAPPROVED. Catches:
        - "One website per ad group" (wrong domain RSAs)
        - "Destination not working"
        - "Non-family safe" (prohibited content)

        Permanently REMOVES these ads (not pause — remove).
        Use dry_run=true first to see what would be removed.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        campaign_filter = f"AND campaign.id = {request.campaign_id}" if request.campaign_id else ""

        query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   ad_group.id, ad_group.name, ad_group.status,
                   ad_group_ad.ad.id, ad_group_ad.ad.type,
                   ad_group_ad.status,
                   ad_group_ad.policy_summary.approval_status,
                   ad_group_ad.policy_summary.policy_topic_entries
            FROM ad_group_ad
            WHERE campaign.status = 'ENABLED'
              AND campaign.advertising_channel_type = 'SEARCH'
              AND ad_group.status = 'ENABLED'
              AND ad_group_ad.status = 'ENABLED'
              AND ad_group_ad.policy_summary.approval_status = 'DISAPPROVED'
              {campaign_filter}
        """
        results = _execute_gaql(customer_id, query, page_size=10000)

        disapproved_ads = []
        removed = []
        failed = []

        # Parse policy topics per ad
        by_policy = {}
        by_campaign = {}

        for row in results:
            camp_id = str(_safe_get_value(row, "campaign.id", ""))
            camp_name = str(_safe_get_value(row, "campaign.name", ""))
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            ag_name = str(_safe_get_value(row, "ad_group.name", ""))
            ad_id = str(_safe_get_value(row, "ad_group_ad.ad.id", ""))

            # Extract policy topics
            topics = []
            try:
                entries = row.ad_group_ad.policy_summary.policy_topic_entries
                for entry in entries:
                    t = str(entry.topic)
                    topics.append(t)
                    by_policy[t] = by_policy.get(t, 0) + 1
            except Exception:
                topics = ["UNKNOWN"]

            by_campaign[camp_name] = by_campaign.get(camp_name, 0) + 1

            disapproved_ads.append({
                "campaign_id": camp_id, "campaign_name": camp_name,
                "ad_group_id": ag_id, "ad_group_name": ag_name,
                "ad_id": ad_id, "policy_topics": topics,
            })

        # Batch remove disapproved ads
        if not request.dry_run and disapproved_ads:
            service = google_ads_client.get_service("AdGroupAdService")
            ops = []
            for ad_info in disapproved_ads:
                op = google_ads_client.get_type("AdGroupAdOperation")
                op.remove = service.ad_group_ad_path(customer_id, ad_info["ad_group_id"], ad_info["ad_id"])
                ops.append(op)

            BATCH_SIZE = 1000
            for i in range(0, len(ops), BATCH_SIZE):
                batch = ops[i:i+BATCH_SIZE]
                try:
                    _retry_with_backoff(
                        service.mutate_ad_group_ads,
                        customer_id=customer_id, operations=batch
                    )
                    removed.extend([a["ad_id"] for a in disapproved_ads[i:i+len(batch)]])
                except Exception as e:
                    failed.append({"batch_start": i, "batch_size": len(batch), "error": str(e)[:200]})

        if not request.dry_run and removed:
            _auto_changelog(
                category="optimization",
                title=f"Guard: removed {len(removed)} DISAPPROVED ads",
                description=f"Found {len(disapproved_ads)} disapproved ads, removed {len(removed)}, failed {len(failed)}. By policy: {by_policy}",
            )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "disapproved_count": len(disapproved_ads),
            "by_policy": by_policy,
            "by_campaign": by_campaign,
            "sample": disapproved_ads[:20],
            "removed": len(removed),
            "failed": failed,
        }

    # ------------------------------------------------------------------
    # Guard: Remove NOT_ELIGIBLE images account-wide
    # ------------------------------------------------------------------

    class BatchRemoveNotEligibleImagesRequest(BaseModel):
        """Input for batch_remove_not_eligible_images tool."""
        campaign_id: Optional[str] = Field(None, description="Filter to specific campaign (None=all ENABLED campaigns)")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_remove_not_eligible_images(request: BatchRemoveNotEligibleImagesRequest) -> dict:
        """
        Find and REMOVE NOT_ELIGIBLE image assets across account or specific campaign.

        Scans ad_group_asset links where primary_status=NOT_ELIGIBLE and
        field_type=AD_IMAGE. Removes the asset LINK (not the underlying asset).
        Only scans ENABLED ad groups in ENABLED campaigns.

        Use dry_run=true first to see what would be removed.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        campaign_filter = f"AND campaign.id = {request.campaign_id}" if request.campaign_id else "AND campaign.status = 'ENABLED'"

        query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   ad_group.id, ad_group.name, ad_group.status,
                   ad_group_asset.resource_name,
                   ad_group_asset.field_type,
                   ad_group_asset.primary_status,
                   ad_group_asset.primary_status_reasons
            FROM ad_group_asset
            WHERE ad_group_asset.field_type = 'AD_IMAGE'
              AND ad_group_asset.primary_status = 'NOT_ELIGIBLE'
              AND ad_group.status = 'ENABLED'
              {campaign_filter}
        """
        results = _execute_gaql(customer_id, query, page_size=10000)

        not_eligible = []
        removed = []
        failed = []
        by_campaign = {}

        for row in results:
            rn = str(_safe_get_value(row, "ad_group_asset.resource_name", ""))
            camp_id = str(_safe_get_value(row, "campaign.id", ""))
            camp_name = str(_safe_get_value(row, "campaign.name", ""))
            ag_name = str(_safe_get_value(row, "ad_group.name", ""))

            by_campaign[camp_name] = by_campaign.get(camp_name, 0) + 1

            not_eligible.append({
                "resource_name": rn, "campaign_id": camp_id,
                "campaign_name": camp_name, "ad_group_name": ag_name,
            })

        # Batch remove NOT_ELIGIBLE images
        if not request.dry_run and not_eligible:
            service = google_ads_client.get_service("AdGroupAssetService")
            ops = []
            for item in not_eligible:
                rn = item["resource_name"]
                if rn:
                    op = google_ads_client.get_type("AdGroupAssetOperation")
                    op.remove = rn
                    ops.append(op)

            BATCH_SIZE = 1000
            for i in range(0, len(ops), BATCH_SIZE):
                batch = ops[i:i+BATCH_SIZE]
                try:
                    _retry_with_backoff(
                        service.mutate_ad_group_assets,
                        customer_id=customer_id, operations=batch
                    )
                    removed.extend([item["resource_name"] for item in not_eligible[i:i+len(batch)]])
                except Exception as e:
                    failed.append({"batch_start": i, "batch_size": len(batch), "error": str(e)[:200]})

        if not request.dry_run and removed:
            _auto_changelog(
                category="optimization",
                title=f"Guard: removed {len(removed)} NOT_ELIGIBLE images",
                description=f"Found {len(not_eligible)} NOT_ELIGIBLE images, removed {len(removed)}, failed {len(failed)}. By campaign: {by_campaign}",
            )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "not_eligible_count": len(not_eligible),
            "by_campaign": by_campaign,
            "sample": not_eligible[:20],
            "removed": len(removed),
            "failed": failed,
        }

    # ------------------------------------------------------------------
    # batch_warmup_market_intel — Pre-warm DataForSEO/Botster market data
    # ------------------------------------------------------------------
    @mcp_app.tool()
    async def batch_warmup_market_intel(request: MarketIntelWarmupRequest) -> dict:
        """
        Pre-warm market intelligence data (DataForSEO/Botster) for product handles.

        Warms trends, competitor ads, keyword suggestions, and CPC/volume data
        for each product handle. This data is used as input for GPT ad copy generation.

        Run BEFORE batch_warmup_cache for faster GPT generation (no live DataForSEO
        API calls during GPT copy generation).

        Recommended flow for new country:
        1. batch_warmup_market_intel(CC)                        — warm DataForSEO data (15-20min)
        2. batch_sync_from_api(CC, campaign_id)                 — load feed + sync
        3. batch_analyze_assets(CC, campaign_id)                — score existing assets
        4. batch_warmup_cache(CC, campaign_id, max_products=10) — GPT ad copy
        5. batch_setup_all(CC, campaign_id)                     — create ads in Google Ads

        Parameters:
        - country_code: ISO 2-letter code (HR, TR, RO, PL, etc.)
        - product_handles: empty = discover ALL from XML feed (recommended for new country)
        - batch_size: parallel workers 1-10 (default 3)
        - skip_cached: skip handles with fresh cache (default True)
        - force_refresh: force re-fetch even if cached (default False)
        - timeout: per-worker timeout 60-600s (default 180)
        - bots: subset of [trends, competitor_ads, suggestions, cpc_volume] (None = all 4)

        Timing: ~25-50s per uncached product, ~0.03s per cached.
        68 products with batch_size=3: ~15-20 minutes.
        """
        MCP_HARD_LIMIT = 55  # seconds — leave 5s buffer for MCP 60s timeout
        tool_start = time.time()

        country_code = request.country_code.strip().upper()

        # NEW_PRODUCTS: auto-discover handles from NP_FEEDS BW feeds if not provided
        product_handles = list(request.product_handles)
        if not product_handles and request.feed_type == "new_products":
            product_handles = _load_np_feed_handles(country_code, request.shop_domain)
            if not product_handles:
                return {"status": "error", "message": f"No NP_FEEDS BW feed handles found for {country_code}" + (f" / {request.shop_domain}" if request.shop_domain else "")}
            _debug("market_intel", f"Auto-discovered {len(product_handles)} NP handles for {country_code}")

        payload: Dict[str, Any] = {
            "country_code": country_code,
            "product_handles": product_handles,
            "batch_size": request.batch_size,
            "skip_cached": request.skip_cached,
            "force_refresh": request.force_refresh,
            "timeout": request.timeout,
        }
        if request.bots is not None:
            payload["bots"] = request.bots
        # NEW_PRODUCTS support: pass feed_type and shop_domain to PHP endpoint
        if request.feed_type is not None:
            payload["feed_type"] = request.feed_type
        if request.shop_domain is not None:
            payload["shop_domain"] = request.shop_domain

        # POST to create async job
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        try:
            payload_json = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                BATCH_WARM_DFO_URL,
                data=payload_json,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=BATCH_WARM_DFO_POST_TIMEOUT, context=ssl_ctx) as resp:
                job_response = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            return {"status": "error", "message": f"Failed to create market intel warm job: {str(e)[:300]}"}

        job_id = job_response.get("job_id")
        if not job_id:
            return {"status": "error", "message": "No job_id in response", "response": job_response}

        # Poll in a tight loop until MCP timeout approaches
        poll_url = f"{BATCH_WARM_DFO_URL}?job_id={job_id}"
        last_status = None

        while True:
            remaining = MCP_HARD_LIMIT - (time.time() - tool_start)
            if remaining < BATCH_WARM_DFO_POLL_INTERVAL + 8:
                break
            time.sleep(BATCH_WARM_DFO_POLL_INTERVAL)

            try:
                poll_req = urllib.request.Request(poll_url, method="GET")
                with urllib.request.urlopen(poll_req, timeout=BATCH_WARM_DFO_POLL_TIMEOUT, context=ssl_ctx) as resp:
                    last_status = json.loads(resp.read().decode("utf-8"))
            except Exception:
                continue  # Retry on transient errors

            job_status = last_status.get("status", "")

            if job_status == "completed":
                return {
                    "status": "completed",
                    "job_id": job_id,
                    "country_code": country_code,
                    "provider": last_status.get("provider"),
                    "total_discovered": last_status.get("total_discovered"),
                    "skipped_cached": last_status.get("skipped_cached"),
                    "total": last_status.get("total"),
                    "processed": last_status.get("processed"),
                    "ok": last_status.get("ok"),
                    "partial": last_status.get("partial"),
                    "fail": last_status.get("fail"),
                    "timeout_count": last_status.get("timeout_count"),
                    "total_time_sec": last_status.get("total_time_sec"),
                    "memory_peak_mb": last_status.get("memory_peak_mb"),
                    "results_summary": [
                        {
                            "handle": r.get("handle", ""),
                            "status": r.get("status", ""),
                            "bots_cached": r.get("bots_cached", 0),
                            "bots_failed": r.get("bots_failed", []),
                            "time_ms": r.get("time_ms", 0),
                            "from_cache": r.get("from_cache", False),
                        }
                        for r in last_status.get("results", [])
                    ],
                }

        # MCP timeout approaching — return job_id for manual polling
        processed = last_status.get("processed", 0) if last_status else 0
        total = last_status.get("total", 0) if last_status else 0

        return {
            "status": "submitted",
            "message": f"Job {job_id} is running ({processed}/{total} processed). "
                       f"Use batch_warmup_market_intel_status(job_id='{job_id}') to check progress.",
            "job_id": job_id,
            "country_code": country_code,
            "total": total,
            "processed": processed,
            "ok": last_status.get("ok", 0) if last_status else 0,
        }

    # ------------------------------------------------------------------
    # batch_warmup_market_intel_status — Poll DFO warm job status
    # ------------------------------------------------------------------
    @mcp_app.tool()
    async def batch_warmup_market_intel_status(request: MarketIntelWarmupStatusRequest) -> dict:
        """
        Check status of a market intel warm job created by batch_warmup_market_intel.

        Returns current progress: processed/total, ok/partial/fail/timeout,
        per-product results with bot details.

        Call this every 15-30s after batch_warmup_market_intel returns status="submitted".
        When status is "completed", the job is done.
        """
        job_id = request.job_id.strip()
        if not job_id:
            return {"status": "error", "message": "job_id is required"}

        poll_url = f"{BATCH_WARM_DFO_URL}?job_id={job_id}"

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        try:
            poll_req = urllib.request.Request(poll_url, method="GET")
            with urllib.request.urlopen(poll_req, timeout=BATCH_WARM_DFO_POLL_TIMEOUT, context=ssl_ctx) as resp:
                status_data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            return {"status": "error", "message": f"Failed to poll job {job_id}: {str(e)[:200]}"}

        # Enrich with computed fields
        job_status = status_data.get("status", "unknown")
        processed = status_data.get("processed", 0)
        total = status_data.get("total", 0)

        if total > 0:
            status_data["percent"] = round(processed / total * 100, 1)

        if job_status == "processing" and processed > 0:
            # Estimate remaining time from started_at
            started = status_data.get("started_at", "")
            if started:
                try:
                    from datetime import datetime
                    start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    elapsed_s = (datetime.now(start_dt.tzinfo) - start_dt).total_seconds()
                    if elapsed_s > 0 and processed > 0:
                        avg_per_product = elapsed_s / processed
                        remaining = (total - processed) * avg_per_product
                        status_data["eta_seconds"] = round(remaining)
                except Exception:
                    pass

        return status_data

    # ------------------------------------------------------------------
    # batch_list_market_intel_jobs — List recent DFO warm jobs
    # ------------------------------------------------------------------
    @mcp_app.tool()
    async def batch_list_market_intel_jobs() -> dict:
        """
        List recent market intelligence warm jobs.

        Returns the 20 most recent DataForSEO/Botster warm jobs with summary info.
        Useful for checking if a country's market intel is already warmed.
        """
        list_url = f"{BATCH_WARM_DFO_URL}?list"

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        try:
            req = urllib.request.Request(list_url, method="GET")
            with urllib.request.urlopen(req, timeout=BATCH_WARM_DFO_POLL_TIMEOUT, context=ssl_ctx) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            return {"status": "error", "message": f"Failed to list market intel jobs: {str(e)[:200]}"}

    # ------------------------------------------------------------------
    # batch_update_campaign_settings — Rename + budget + ROAS + network
    # ------------------------------------------------------------------
    class UpdateCampaignSettingsRequest(BaseModel):
        customer_id: str = Field(default=CUSTOMER_ID, description="Customer ID (from config)")
        campaign_id: str = Field(description="Campaign ID")
        campaign_name: Optional[str] = Field(default=None, description="New campaign name")
        daily_budget_micros: Optional[int] = Field(default=None, description="Daily budget in micros (5000000 = 5 GBP)")
        bidding_strategy: Optional[str] = Field(default=None, description="MAXIMIZE_CONVERSION_VALUE, MAXIMIZE_CONVERSIONS, TARGET_CPA, MANUAL_CPC")
        target_roas: Optional[float] = Field(default=None, description="Target ROAS (e.g. 1.6 for 160%)")
        target_cpa_micros: Optional[int] = Field(default=None, description="Target CPA in micros")
        target_search_network: Optional[bool] = Field(default=None, description="Enable/disable Search Network partners")
        target_content_network: Optional[bool] = Field(default=None, description="Enable/disable Display Network")

    @mcp_app.tool()
    async def batch_update_campaign_settings(request: UpdateCampaignSettingsRequest) -> dict:
        """
        Update campaign settings: name, budget, bidding strategy + ROAS, network settings.

        Combines multiple mutations into a single API call for efficiency.
        Use this after creating a campaign to apply standard settings, or to fix
        existing campaigns.

        Standard NEW_PRODUCTS settings:
        - Budget: 5,000,000 micros (5 GBP/day)
        - Bidding: MAXIMIZE_CONVERSION_VALUE with target_roas=1.6 (160%)
        - Search Network: disabled (target_search_network=false)
        """
        import google_ads_mcp as _gads
        from google.protobuf import field_mask_pb2

        try:
            _gads._ensure_client()
            customer_id = _gads._format_customer_id(request.customer_id)
            google_ads_client = _gads.google_ads_client
            campaign_service = google_ads_client.get_service("CampaignService")
            changes = []

            resource_name = campaign_service.campaign_path(customer_id, request.campaign_id)

            # Build a SINGLE operation with all campaign-level changes merged
            # (Google Ads API rejects multiple operations on the same resource)
            op = google_ads_client.get_type("CampaignOperation")
            c = op.update
            c.resource_name = resource_name
            mask_paths = []

            # --- Campaign name ---
            if request.campaign_name:
                c.name = request.campaign_name
                mask_paths.append("name")
                changes.append(f"name → '{request.campaign_name}'")

            # --- Network settings ---
            if request.target_search_network is not None:
                c.network_settings.target_search_network = request.target_search_network
                mask_paths.append("network_settings.target_search_network")
                changes.append(f"search_network → {request.target_search_network}")
            if request.target_content_network is not None:
                c.network_settings.target_content_network = request.target_content_network
                mask_paths.append("network_settings.target_content_network")
                changes.append(f"content_network → {request.target_content_network}")

            # --- Bidding strategy ---
            if request.bidding_strategy or request.target_roas is not None:
                strategy = (request.bidding_strategy or "MAXIMIZE_CONVERSION_VALUE").upper()
                if strategy == "MAXIMIZE_CONVERSION_VALUE":
                    roas = request.target_roas if request.target_roas is not None else 0
                    c.maximize_conversion_value.target_roas = roas
                    mask_paths.append("maximize_conversion_value.target_roas")
                    changes.append(f"bidding → MAXIMIZE_CONVERSION_VALUE, target_roas={roas}")
                elif strategy == "MAXIMIZE_CONVERSIONS":
                    cpa = request.target_cpa_micros or 0
                    c.maximize_conversions.target_cpa_micros = cpa
                    mask_paths.append("maximize_conversions.target_cpa_micros")
                    changes.append(f"bidding → MAXIMIZE_CONVERSIONS, target_cpa={cpa}")
                elif strategy == "TARGET_CPA":
                    cpa = request.target_cpa_micros or 0
                    c.target_cpa.target_cpa_micros = cpa
                    mask_paths.append("target_cpa.target_cpa_micros")
                    changes.append(f"bidding → TARGET_CPA, target_cpa={cpa}")
                elif strategy == "MANUAL_CPC":
                    c.manual_cpc.enhanced_cpc_enabled = True
                    mask_paths.append("manual_cpc.enhanced_cpc_enabled")
                    changes.append("bidding → MANUAL_CPC (enhanced)")

            # Execute single merged campaign mutation
            campaign_result = None
            if mask_paths:
                op.update_mask = field_mask_pb2.FieldMask(paths=mask_paths)
                campaign_result = campaign_service.mutate_campaigns(
                    customer_id=customer_id,
                    operations=[op]
                )

            # --- Budget (separate service) ---
            budget_result = None
            if request.daily_budget_micros is not None:
                # First, find the budget resource name
                budget_query = f"""
                    SELECT campaign_budget.resource_name, campaign_budget.amount_micros
                    FROM campaign_budget
                    WHERE campaign.id = {request.campaign_id}
                """
                budget_rows = _gads._execute_gaql(customer_id, budget_query, page_size=1)
                if budget_rows:
                    budget_rn = None
                    for row in budget_rows:
                        if hasattr(row, 'campaign_budget'):
                            budget_rn = row.campaign_budget.resource_name
                            break
                    if budget_rn:
                        budget_service = google_ads_client.get_service("CampaignBudgetService")
                        bop = google_ads_client.get_type("CampaignBudgetOperation")
                        b = bop.update
                        b.resource_name = budget_rn
                        b.amount_micros = request.daily_budget_micros
                        bop.update_mask = field_mask_pb2.FieldMask(paths=["amount_micros"])
                        budget_result = budget_service.mutate_campaign_budgets(
                            customer_id=customer_id, operations=[bop]
                        )
                        changes.append(f"budget → {request.daily_budget_micros / 1_000_000:.2f}/day")

            return {
                "status": "success",
                "campaign_id": request.campaign_id,
                "changes": changes,
                "campaign_mutations": len(operations),
                "budget_updated": budget_result is not None,
            }

        except Exception as e:
            return {"status": "error", "message": f"Failed to update campaign settings: {str(e)[:500]}"}

    # ------------------------------------------------------------------
    # batch_verify_campaign_settings — Post-creation guard
    # ------------------------------------------------------------------
    class VerifyCampaignRequest(BaseModel):
        customer_id: str = Field(default=CUSTOMER_ID, description="Customer ID (from config)")
        campaign_id: str = Field(description="Campaign ID")
        country_code: str = Field(description="Country code (e.g. DE, PL)")
        brand: str = Field(default="HLE", description="Brand: HLE, HLP, VGL")
        auto_fix: bool = Field(default=False, description="Auto-fix issues found")

    @mcp_app.tool()
    async def batch_verify_campaign_settings(request: VerifyCampaignRequest) -> dict:
        """
        Post-creation guard: verify campaign has correct settings.

        Checks:
        1. Campaign name follows convention: {CC} | {Country} | Search | NEW_PRODUCTS | {Brand} | CLAUDE_MCP - last updated: {date}
        2. Budget is 5,000,000 micros (5/day)
        3. Bidding is MAXIMIZE_CONVERSION_VALUE with target_roas = 1.6 (160%)
        4. Search Network is DISABLED (target_search_network = false)
        5. Content Network is DISABLED (target_content_network = false)

        If auto_fix=True, automatically corrects any issues found.
        """
        import google_ads_mcp as _gads2
        customer_id = _gads2._format_customer_id(request.customer_id)
        cc = request.country_code.upper()

        # Expected values
        expected_budget = 5_000_000  # 5 GBP/day
        expected_roas = 1.6
        expected_search_network = False
        expected_content_network = False

        query = f"""
            SELECT campaign.name, campaign.status, campaign.bidding_strategy_type,
                   campaign.maximize_conversion_value.target_roas,
                   campaign_budget.amount_micros,
                   campaign.network_settings.target_google_search,
                   campaign.network_settings.target_search_network,
                   campaign.network_settings.target_content_network
            FROM campaign
            WHERE campaign.id = {request.campaign_id}
        """
        rows = _gads2._execute_gaql(customer_id, query, page_size=1)
        if not rows:
            return {"status": "error", "message": f"Campaign {request.campaign_id} not found"}

        row = rows[0]
        issues = []
        fixes_needed = {}

        # 1. Check name convention
        actual_name = row.campaign.name
        expected_name_prefix = f"{cc} | {COUNTRY_NAMES_EN.get(cc, cc)} | Search | NEW_PRODUCTS | {request.brand} | CLAUDE_MCP"
        if not actual_name.startswith(expected_name_prefix):
            new_name = _get_np_campaign_name(cc, "Search", request.brand)
            issues.append(f"NAME: '{actual_name}' should start with '{expected_name_prefix}'")
            fixes_needed["campaign_name"] = new_name

        # 2. Check budget
        actual_budget = row.campaign_budget.amount_micros
        if actual_budget != expected_budget:
            issues.append(f"BUDGET: {actual_budget / 1_000_000:.2f}/day, expected {expected_budget / 1_000_000:.2f}/day")
            fixes_needed["daily_budget_micros"] = expected_budget

        # 3. Check bidding strategy + ROAS
        # bidding_strategy_type can be enum int (11) or name string depending on proto_plus
        actual_strategy_raw = row.campaign.bidding_strategy_type
        actual_strategy = str(actual_strategy_raw)
        # MAXIMIZE_CONVERSION_VALUE = 11 in the BiddingStrategyTypeEnum
        is_mcv = ("MAXIMIZE_CONVERSION_VALUE" in actual_strategy or str(actual_strategy_raw) == "11"
                  or (hasattr(actual_strategy_raw, 'value') and actual_strategy_raw.value == 11)
                  or actual_strategy_raw == 11)
        actual_roas = row.campaign.maximize_conversion_value.target_roas
        if not is_mcv:
            issues.append(f"BIDDING: {actual_strategy}, expected MAXIMIZE_CONVERSION_VALUE")
            fixes_needed["bidding_strategy"] = "MAXIMIZE_CONVERSION_VALUE"
            fixes_needed["target_roas"] = expected_roas
        elif abs(actual_roas - expected_roas) > 0.01:
            issues.append(f"ROAS: {actual_roas:.2f} ({actual_roas*100:.0f}%), expected {expected_roas:.2f} ({expected_roas*100:.0f}%)")
            fixes_needed["target_roas"] = expected_roas

        # 4. Check Search Network
        actual_search_net = row.campaign.network_settings.target_search_network
        if actual_search_net != expected_search_network:
            issues.append(f"SEARCH_NETWORK: {actual_search_net}, expected {expected_search_network}")
            fixes_needed["target_search_network"] = expected_search_network

        # 5. Check Content Network
        actual_content_net = row.campaign.network_settings.target_content_network
        if actual_content_net != expected_content_network:
            issues.append(f"CONTENT_NETWORK: {actual_content_net}, expected {expected_content_network}")
            fixes_needed["target_content_network"] = expected_content_network

        result = {
            "status": "success",
            "campaign_id": request.campaign_id,
            "country_code": cc,
            "issues_found": len(issues),
            "issues": issues,
            "all_ok": len(issues) == 0,
        }

        # Auto-fix if requested
        if request.auto_fix and fixes_needed:
            fix_request = UpdateCampaignSettingsRequest(
                customer_id=request.customer_id,
                campaign_id=request.campaign_id,
                **fixes_needed
            )
            fix_result = await batch_update_campaign_settings(fix_request)
            result["auto_fix_result"] = fix_result
            result["fixed"] = fix_result.get("status") == "success"

        return result

    # ------------------------------------------------------------------
    # Time-of-Day & Day-of-Week Performance Analysis
    # ------------------------------------------------------------------

    # ── Time Analysis: Core function (callable from batch_analytics.py orchestrator) ──

    class TimeAnalysisRequest(BaseModel):
        """Input for batch_time_analysis tool."""
        country_code: str = Field(..., description="Country code (e.g., TR, RO, PL)")
        campaign_ids: Optional[List[str]] = Field(
            None, description="Campaign IDs to analyze. If None, auto-discovers all ENABLED Search campaigns for the country."
        )
        start_date: str = Field(..., description="Start date YYYY-MM-DD")
        end_date: str = Field(..., description="End date YYYY-MM-DD")
        min_impressions_hour: int = Field(
            50, description="Minimum impressions per hour slot to include in analysis"
        )
        roas_threshold_low: float = Field(
            1.0, description="ROAS below this = waste (underperforming hours)"
        )
        roas_threshold_high: float = Field(
            1.5, description="ROAS above this = peak (overperforming hours)"
        )
        generate_schedules: bool = Field(
            True, description="If true, generates recommended ad schedule bid modifiers"
        )

    @mcp_app.tool()
    async def batch_time_analysis(request: TimeAnalysisRequest) -> dict:
        """
        Full time-of-day and day-of-week performance analysis for Search campaigns.

        Analyzes hourly and daily patterns in clicks, conversions, CTR, CPA, ROAS.
        Identifies peak hours, waste hours, and generates ad schedule recommendations.

        Returns full analysis + ready-to-use schedule definitions for
        google_ads_create_ad_schedule tool.
        """
        return _batch_time_analysis_core(
            country_code=request.country_code,
            campaign_ids=request.campaign_ids,
            start_date=request.start_date,
            end_date=request.end_date,
            min_impressions_hour=request.min_impressions_hour,
            roas_threshold_low=request.roas_threshold_low,
            roas_threshold_high=request.roas_threshold_high,
            generate_schedules=request.generate_schedules,
        )

    # ------------------------------------------------------------------
    # batch_remove_ad_groups - Permanently REMOVE ad groups (for DESTINATION_NOT_WORKING reset)
    # ------------------------------------------------------------------

    class BatchRemoveAdGroupsRequest(BaseModel):
        """Input for batch_remove_ad_groups tool."""
        campaign_id: str = Field(..., description="Campaign ID containing the ad groups")
        ad_group_ids: list = Field(..., description="List of ad group IDs to permanently REMOVE")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_remove_ad_groups(request: BatchRemoveAdGroupsRequest) -> dict:
        """
        Permanently REMOVE ad groups by ID. This is irreversible.

        Use this when ad groups are stuck with DESTINATION_NOT_WORKING policy cache
        and new RSAs cannot be created. After removing, use batch_setup_products
        to create fresh ad groups with new RSAs.

        WARNING: This permanently deletes the ad group and ALL its ads, keywords,
        and asset links. Use dry_run=true first to preview.
        """
        from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)
        ad_group_service = google_ads_client.get_service("AdGroupService")

        removed = []
        failed = []

        for ag_id in request.ad_group_ids:
            ag_id_str = str(ag_id)
            if request.dry_run:
                removed.append({"ad_group_id": ag_id_str, "status": "would_remove"})
                continue

            try:
                operation = google_ads_client.get_type("AdGroupOperation")
                resource_name = ad_group_service.ad_group_path(customer_id, ag_id_str)
                operation.remove = resource_name

                ad_group_service.mutate_ad_groups(
                    customer_id=customer_id,
                    operations=[operation]
                )
                removed.append({"ad_group_id": ag_id_str, "status": "removed"})
            except Exception as e:
                failed.append({"ad_group_id": ag_id_str, "error": str(e)[:300]})

        if not request.dry_run and removed:
            _auto_changelog(
                category="optimization",
                title=f"Nuked {len(removed)} ad groups in campaign {request.campaign_id}",
                description=f"Permanently removed {len(removed)} ad groups to reset DESTINATION_NOT_WORKING policy cache. Failed: {len(failed)}",
            )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "campaign_id": request.campaign_id,
            "removed": removed,
            "failed": failed,
            "total_removed": len(removed),
            "total_failed": len(failed),
            "next_step": "Run batch_setup_products to recreate ad groups with fresh RSAs" if not request.dry_run else "Set dry_run=false to execute",
        }

    # ------------------------------------------------------------------
    # batch_fix_sitelinks - Find and replace sitelinks with bad URLs
    # ------------------------------------------------------------------
    class BatchFixSitelinksRequest(BaseModel):
        """Input for batch_fix_sitelinks tool."""
        country_code: str
        campaign_id: str
        dry_run: bool = True
        max_products: int = Field(0, description="Limit products (0=all)")

    @mcp_app.tool()
    async def batch_fix_sitelinks(request: BatchFixSitelinksRequest) -> dict:
        """
        Find and replace sitelinks with bad (non-existent) URLs.

        Scans ENABLED ad groups for sitelinks whose final_url does NOT contain
        '/products/'. Removes those links, creates new sitelinks using product
        URL (from RSA) with #anchor appended.
        Use dry_run=true first.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)
        cc = request.country_code.upper()
        config = COUNTRY_CONFIG.get(cc, COUNTRY_CONFIG.get("DEFAULT", {}))
        lang = config.get("language_code", "en")

        # Step 1: Get all sitelink assets linked to ENABLED ad groups
        gaql = f"""
            SELECT campaign.id, ad_group.id, ad_group.name, ad_group.status,
                   ad_group_asset.resource_name,
                   asset.id, asset.sitelink_asset.link_text,
                   asset.sitelink_asset.description1, asset.sitelink_asset.description2,
                   asset.final_urls
            FROM ad_group_asset
            WHERE campaign.id = {request.campaign_id}
              AND ad_group.status = 'ENABLED'
              AND ad_group_asset.field_type = 'SITELINK'
              AND ad_group_asset.status != 'REMOVED'
        """
        sitelink_rows = _execute_gaql(CUSTOMER_ID, gaql, page_size=1000)

        # Step 2: Get RSA final_urls per ad group
        gaql_rsa = f"""
            SELECT campaign.id, ad_group.id, ad_group.status,
                   ad_group_ad.ad.final_urls
            FROM ad_group_ad
            WHERE campaign.id = {request.campaign_id}
              AND ad_group.status = 'ENABLED'
              AND ad_group_ad.status = 'ENABLED'
              AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
        """
        rsa_rows = _execute_gaql(CUSTOMER_ID, gaql_rsa, page_size=1000)

        ag_product_url = {}
        for row in rsa_rows:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            raw_urls = str(_safe_get_value(row, "ad_group_ad.ad.final_urls", ""))
            url_match = re.search(r'https?://[^\s\'">\]]+/products/[^\s\'">\]]+', raw_urls)
            if url_match and ag_id:
                ag_product_url[ag_id] = url_match.group(0)

        # Step 3: Identify bad sitelinks (URL doesn't contain /products/)
        bad_sitelinks = []
        good_count = 0
        for row in sitelink_rows:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            ag_name = str(_safe_get_value(row, "ad_group.name", ""))
            resource_name = str(_safe_get_value(row, "ad_group_asset.resource_name", ""))
            link_text = str(_safe_get_value(row, "asset.sitelink_asset.link_text", ""))
            desc1 = str(_safe_get_value(row, "asset.sitelink_asset.description1", ""))
            desc2 = str(_safe_get_value(row, "asset.sitelink_asset.description2", ""))
            raw_urls = str(_safe_get_value(row, "asset.final_urls", ""))
            url_match = re.search(r'https?://[^\s\'">\]]+', raw_urls)
            current_url = url_match.group(0) if url_match else ""

            if "/products/" not in current_url and resource_name:
                bad_sitelinks.append({
                    "ag_id": ag_id, "ag_name": ag_name,
                    "resource_name": resource_name,
                    "link_text": link_text, "desc1": desc1, "desc2": desc2,
                    "current_url": current_url,
                })
            else:
                good_count += 1

        from collections import defaultdict
        ag_bad = defaultdict(list)
        for sl in bad_sitelinks:
            ag_bad[sl["ag_id"]].append(sl)

        ag_ids_to_fix = list(ag_bad.keys())
        if request.max_products > 0:
            ag_ids_to_fix = ag_ids_to_fix[:request.max_products]

        results = {
            "total_sitelinks_scanned": len(sitelink_rows),
            "good_sitelinks": good_count,
            "bad_sitelinks": len(bad_sitelinks),
            "ad_groups_affected": len(ag_bad),
            "ad_groups_to_fix": len(ag_ids_to_fix),
            "dry_run": request.dry_run,
            "removed": 0, "created": 0, "errors": [], "fixed_ad_groups": [],
        }

        if request.dry_run:
            preview = []
            for ag_id in ag_ids_to_fix[:20]:
                sls = ag_bad[ag_id]
                product_url = ag_product_url.get(ag_id, "UNKNOWN")
                preview.append({
                    "ag_name": sls[0]["ag_name"], "ag_id": ag_id,
                    "bad_sitelinks": len(sls), "product_url": product_url,
                    "sample_bad_urls": [s["current_url"] for s in sls[:4]],
                    "would_create_urls": [
                        f"{product_url}{'&' if '?' in product_url else '?'}sitelink={s['link_text'].lower().replace(' ', '-')[:20]}"
                        for s in sls[:4]
                    ] if product_url != "UNKNOWN" else [],
                })
            results["preview"] = preview
            return results

        # Step 4: Remove bad, create new
        service = google_ads_client.get_service("AdGroupAssetService")

        for ag_id in ag_ids_to_fix:
            sls = ag_bad[ag_id]
            product_url = ag_product_url.get(ag_id)
            if not product_url:
                results["errors"].append(f"AG {ag_id} ({sls[0]['ag_name']}): no RSA URL found, skipped")
                continue

            try:
                # Remove bad sitelink links
                remove_ops = []
                for sl in sls:
                    op = google_ads_client.get_type("AdGroupAssetOperation")
                    op.remove = sl["resource_name"]
                    remove_ops.append(op)

                _retry_with_backoff(
                    service.mutate_ad_group_assets,
                    customer_id=customer_id, operations=remove_ops
                )
                results["removed"] += len(remove_ops)

                # Create new sitelinks from ANCHOR_SITELINK_TEMPLATES
                anchor_templates = ANCHOR_SITELINK_TEMPLATES.get(lang, ANCHOR_SITELINK_TEMPLATES.get("en", []))
                new_sitelinks = []
                for tmpl in anchor_templates[:4]:
                    lt = tmpl["link_text"].replace("{discount}", "50")[:25]
                    d1 = tmpl["desc1"].replace("{price}", "").replace("{currency}", "").strip()[:35]
                    d2 = tmpl["desc2"][:35]
                    suffix = tmpl["url_suffix"]
                    if suffix.startswith("#"):
                        sl_url = f"{product_url}{suffix}"
                    elif suffix.startswith("?"):
                        separator = "&" if "?" in product_url else "?"
                        sl_url = f"{product_url}{separator}{suffix.lstrip('?')}"
                    else:
                        sl_url = f"{product_url}?sitelink={suffix}"
                    new_sitelinks.append({
                        "link_text": lt, "description1": d1, "description2": d2,
                        "final_urls": [sl_url],
                    })

                _create_sitelinks(ag_id, new_sitelinks)
                results["created"] += len(new_sitelinks)
                results["fixed_ad_groups"].append({
                    "ag_id": ag_id, "ag_name": sls[0]["ag_name"],
                    "removed": len(sls), "created": len(new_sitelinks),
                })
            except Exception as e:
                results["errors"].append(f"AG {ag_id} ({sls[0]['ag_name']}): {str(e)[:150]}")

        if results["removed"] > 0:
            _auto_changelog(
                category="optimization",
                title=f"Fixed {results['removed']} bad sitelinks in {len(results['fixed_ad_groups'])} ad groups",
                description=f"Campaign {request.campaign_id}: removed {results['removed']} sitelinks with fake URLs, created {results['created']} with correct product URLs.",
                campaign_id=request.campaign_id, country_code=cc,
            )

        return results

    # ------------------------------------------------------------------
    # batch_fix_empty_groups - Detect and fix ad groups without active ads
    # Finds ENABLED ad groups with zero ENABLED ads. Classifies them:
    # ENABLE_AD: has PAUSED+APPROVED ad -> enable it
    # CREATE_RSA: all ads REMOVED but good historical ROI -> create new RSA
    # PAUSE_GROUP: zero value in lookback period -> pause the group
    # ------------------------------------------------------------------
    class BatchFixEmptyGroupsRequest(BaseModel):
        """Input for batch_fix_empty_groups tool."""
        country_code: Optional[str] = Field(None, description="Country code (None=all countries)")
        campaign_id: Optional[str] = Field(None, description="Campaign ID (None=all ENABLED Search campaigns)")
        dry_run: bool = True
        roi_threshold: float = Field(0.5, description="Min ROI to justify creating new RSA")
        lookback_days: int = Field(180, description="Days to look back for ROI calculation")
        max_groups: int = Field(0, description="Limit groups to process (0=all)")

    @mcp_app.tool()
    async def batch_fix_empty_groups(request: BatchFixEmptyGroupsRequest) -> dict:
        """
        Detect and fix ad groups without active ads.

        Scans all ENABLED ad groups in ENABLED campaigns, finds those with
        zero ENABLED ads, and classifies them into actions:
        - ENABLE_AD: Has PAUSED+APPROVED ad -> enable it
        - CREATE_RSA: All ads REMOVED but good ROI in lookback -> create new RSA
        - PAUSE_GROUP: Zero impressions + zero conversions -> pause the group
        - REVIEW: Has traffic but no conversions -> leave for human review

        DOMAIN RULE: When creating new RSAs, the URL domain is extracted from
        existing REMOVED ads in the same ad group. If that fails, uses the
        campaign's dominant domain (from other ad groups' RSAs). Never mixes
        domains within a campaign.

        Use dry_run=true first to preview.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        # Build date range
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=request.lookback_days)).strftime("%Y-%m-%d")

        # Step 1: Find campaigns to scan
        if request.campaign_id:
            campaign_filter = f"campaign.id = {request.campaign_id}"
        elif request.country_code:
            campaign_filter = f"campaign.name LIKE '{request.country_code.upper()} %'"
        else:
            campaign_filter = "campaign.advertising_channel_type = 'SEARCH'"

        # Step 2: Get all ENABLED ad groups with their ad counts
        # Query ENABLED ads per ad group
        gaql_ads = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   ad_group.id, ad_group.name, ad_group.status,
                   ad_group_ad.ad.id, ad_group_ad.status,
                   ad_group_ad.policy_summary.approval_status,
                   ad_group_ad.ad.final_urls
            FROM ad_group_ad
            WHERE {campaign_filter}
              AND campaign.status = 'ENABLED'
              AND ad_group.status = 'ENABLED'
              AND ad_group_ad.status != 'REMOVED'
        """
        ad_rows = _execute_gaql(CUSTOMER_ID, gaql_ads, page_size=1000)

        # Build map: ag_id -> {ads: [...], campaign info}
        ag_map = {}
        for row in ad_rows:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            if not ag_id:
                continue
            if ag_id not in ag_map:
                ag_map[ag_id] = {
                    "ag_name": str(_safe_get_value(row, "ad_group.name", "")),
                    "campaign_id": str(_safe_get_value(row, "campaign.id", "")),
                    "campaign_name": str(_safe_get_value(row, "campaign.name", "")),
                    "ads": [],
                }
            ad_status_raw = _safe_get_value(row, "ad_group_ad.status", "")
            ad_status = ad_status_raw.name if hasattr(ad_status_raw, 'name') else str(ad_status_raw)
            approval_raw = _safe_get_value(row, "ad_group_ad.policy_summary.approval_status", "")
            approval = approval_raw.name if hasattr(approval_raw, 'name') else str(approval_raw)
            final_urls = str(_safe_get_value(row, "ad_group_ad.ad.final_urls", ""))
            url_match = re.search(r'https?://[^\s\'">\]]+', final_urls)
            ag_map[ag_id]["ads"].append({
                "ad_id": str(_safe_get_value(row, "ad_group_ad.ad.id", "")),
                "status": ad_status,
                "approval": approval,
                "url": url_match.group(0) if url_match else "",
            })

        # Also find ad groups with NO ads at all (all REMOVED)
        gaql_all_ag = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   ad_group.id, ad_group.name, ad_group.status
            FROM ad_group
            WHERE {campaign_filter}
              AND campaign.status = 'ENABLED'
              AND ad_group.status = 'ENABLED'
        """
        all_ag_rows = _execute_gaql(CUSTOMER_ID, gaql_all_ag, page_size=1000)

        for row in all_ag_rows:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            if ag_id and ag_id not in ag_map:
                ag_map[ag_id] = {
                    "ag_name": str(_safe_get_value(row, "ad_group.name", "")),
                    "campaign_id": str(_safe_get_value(row, "campaign.id", "")),
                    "campaign_name": str(_safe_get_value(row, "campaign.name", "")),
                    "ads": [],  # No non-REMOVED ads
                }

        # Step 3: Identify empty groups (no ENABLED ads)
        empty_groups = {}
        for ag_id, info in ag_map.items():
            enabled_ads = [a for a in info["ads"] if "ENABLED" in a["status"]]
            if not enabled_ads:
                empty_groups[ag_id] = info

        if not empty_groups:
            return {"status": "success", "message": "No empty ad groups found", "total_scanned": len(ag_map), "empty_groups": 0}

        # Apply max_groups limit
        ag_ids_to_process = list(empty_groups.keys())
        if request.max_groups > 0:
            ag_ids_to_process = ag_ids_to_process[:request.max_groups]

        # Step 4: Get historical performance for empty groups
        perf_map = {}
        for i in range(0, len(ag_ids_to_process), 50):
            chunk = ag_ids_to_process[i:i+50]
            ids_str = ", ".join(chunk)
            gaql_perf = f"""
                SELECT ad_group.id, metrics.conversions_value, metrics.cost_micros,
                       metrics.impressions, metrics.conversions
                FROM ad_group
                WHERE ad_group.id IN ({ids_str})
                  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
                  AND metrics.impressions > 0
            """
            try:
                perf_rows = _execute_gaql(CUSTOMER_ID, gaql_perf, page_size=1000)
                for row in perf_rows:
                    ag_id = str(_safe_get_value(row, "ad_group.id", ""))
                    if ag_id not in perf_map:
                        perf_map[ag_id] = {"cv": 0, "cost": 0, "impr": 0, "conv": 0}
                    perf_map[ag_id]["cv"] += float(_safe_get_value(row, "metrics.conversions_value", 0) or 0)
                    perf_map[ag_id]["cost"] += float(_safe_get_value(row, "metrics.cost_micros", 0) or 0) / 1e6
                    perf_map[ag_id]["impr"] += int(_safe_get_value(row, "metrics.impressions", 0) or 0)
                    perf_map[ag_id]["conv"] += float(_safe_get_value(row, "metrics.conversions", 0) or 0)
            except Exception:
                pass  # Some chunks may fail if IDs are invalid

        # Step 5: Classify each empty group
        actions = {"ENABLE_AD": [], "CREATE_RSA": [], "PAUSE_GROUP": [], "REVIEW": []}

        for ag_id in ag_ids_to_process:
            info = empty_groups[ag_id]
            perf = perf_map.get(ag_id, {"cv": 0, "cost": 0, "impr": 0, "conv": 0})
            roi = perf["cv"] / perf["cost"] if perf["cost"] > 0 else (999 if perf["cv"] > 0 else 0)

            # Check for PAUSED + APPROVED ads
            paused_approved = [a for a in info["ads"] if "PAUSED" in a["status"] and a["approval"] in ("APPROVED", "APPROVED_LIMITED")]

            entry = {
                "ag_id": ag_id,
                "ag_name": info["ag_name"],
                "campaign_name": info["campaign_name"],
                "campaign_id": info["campaign_id"],
                "roi": round(roi, 2),
                "cv": round(perf["cv"], 2),
                "cost": round(perf["cost"], 2),
                "impr": perf["impr"],
            }

            if paused_approved:
                entry["ad_id"] = paused_approved[0]["ad_id"]
                entry["ad_url"] = paused_approved[0]["url"]
                actions["ENABLE_AD"].append(entry)
            elif roi >= request.roi_threshold:
                # Find URL from any ad (including PAUSED/DISAPPROVED)
                any_url = ""
                for a in info["ads"]:
                    if a["url"] and "/products/" in a["url"]:
                        any_url = a["url"]
                        break
                entry["product_url"] = any_url
                actions["CREATE_RSA"].append(entry)
            elif perf["impr"] == 0 and perf["cv"] == 0:
                actions["PAUSE_GROUP"].append(entry)
            else:
                actions["REVIEW"].append(entry)

        # Step 6: Execute (or dry_run preview)
        results = {
            "total_scanned": len(ag_map),
            "empty_groups": len(empty_groups),
            "processed": len(ag_ids_to_process),
            "dry_run": request.dry_run,
            "enabled_ads": 0,
            "created_rsas": 0,
            "paused_groups": 0,
            "review_needed": len(actions["REVIEW"]),
            "errors": [],
            "actions_detail": {
                "ENABLE_AD": [{"ag": e["ag_name"], "roi": e["roi"]} for e in actions["ENABLE_AD"][:20]],
                "CREATE_RSA": [{"ag": e["ag_name"], "roi": e["roi"], "url": e.get("product_url", "")[:60]} for e in actions["CREATE_RSA"][:20]],
                "PAUSE_GROUP": len(actions["PAUSE_GROUP"]),
                "REVIEW": [{"ag": e["ag_name"], "impr": e["impr"]} for e in actions["REVIEW"][:10]],
            },
        }

        if request.dry_run:
            return results

        # Execute ENABLE_AD
        for entry in actions["ENABLE_AD"]:
            try:
                from google_ads_mcp import google_ads_client as gac
                service = gac.get_service("AdGroupAdService")
                op = gac.get_type("AdGroupAdOperation")
                op.update.resource_name = f"customers/{customer_id}/adGroupAds/{entry['ag_id']}~{entry['ad_id']}"
                op.update.status = gac.enums.AdGroupAdStatusEnum.ENABLED
                gac.get_type("FieldMask")
                from google.protobuf import field_mask_pb2
                op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
                _retry_with_backoff(service.mutate_ad_group_ads, customer_id=customer_id, operations=[op])
                results["enabled_ads"] += 1
            except Exception as e:
                results["errors"].append(f"Enable ad {entry['ag_name']}: {str(e)[:100]}")

        # Execute CREATE_RSA
        for entry in actions["CREATE_RSA"]:
            product_url = entry.get("product_url", "")
            if not product_url:
                # Try to get URL from REMOVED ads
                try:
                    gaql_removed = f"""
                        SELECT ad_group_ad.ad.final_urls FROM ad_group_ad
                        WHERE ad_group.id = {entry['ag_id']}
                    """
                    removed_rows = _execute_gaql(CUSTOMER_ID, gaql_removed, page_size=5)
                    for rr in removed_rows:
                        raw = str(_safe_get_value(rr, "ad_group_ad.ad.final_urls", ""))
                        m = re.search(r'https?://[^\s\'">\]]+/products/[^\s\'">\]]+', raw)
                        if m:
                            product_url = m.group(0)
                            # Found a valid product URL
                            break
                except Exception:
                    pass

            if not product_url:
                results["errors"].append(f"CREATE_RSA {entry['ag_name']}: no product URL found, skipped")
                continue

            # Ensure we use the correct domain (pharm > express)
            # Extract domain from URL
            from urllib.parse import urlparse
            parsed = urlparse(product_url)
            domain = parsed.netloc

            product_name = entry["ag_name"]
            try:
                _create_rsa(
                    ad_group_id=entry["ag_id"],
                    headlines=[
                        f"{product_name} Original", "Official Store", "Verified Product",
                        "Fast Delivery", "Premium Quality", "Cash on Delivery",
                        "Premium Product", "Order Now Online", "Quality Guaranteed",
                        "Free Shipping", "100% Authentic", "Best Price Online",
                        "Trusted Product", "Real Results", "Free Support",
                    ],
                    descriptions=[
                        f"{product_name} from the official store. Fast delivery.",
                        f"Natural {product_name} formula. Quality guaranteed.",
                        f"Order {product_name} online. Cash on delivery available.",
                        f"Premium {product_name}. Free shipping available now.",
                    ],
                    final_urls=[product_url],
                )
                results["created_rsas"] += 1
            except Exception as e:
                results["errors"].append(f"CREATE_RSA {entry['ag_name']}: {str(e)[:100]}")

        # Execute PAUSE_GROUP
        service_ag = google_ads_client.get_service("AdGroupService")
        for entry in actions["PAUSE_GROUP"]:
            try:
                op = google_ads_client.get_type("AdGroupOperation")
                op.update.resource_name = f"customers/{customer_id}/adGroups/{entry['ag_id']}"
                op.update.status = google_ads_client.enums.AdGroupStatusEnum.PAUSED
                from google.protobuf import field_mask_pb2
                op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
                _retry_with_backoff(service_ag.mutate_ad_groups, customer_id=customer_id, operations=[op])
                results["paused_groups"] += 1
            except Exception as e:
                results["errors"].append(f"PAUSE {entry['ag_name']}: {str(e)[:100]}")

        if results["enabled_ads"] + results["created_rsas"] + results["paused_groups"] > 0:
            _auto_changelog(
                category="optimization",
                title=f"Fixed {results['enabled_ads']+results['created_rsas']} empty groups, paused {results['paused_groups']}",
                description=f"Enabled {results['enabled_ads']} paused ads, created {results['created_rsas']} new RSAs, paused {results['paused_groups']} dead groups.",
                campaign_id=request.campaign_id,
                country_code=request.country_code,
            )

        return results

    # ------------------------------------------------------------------
    # Tool: Sync ALL negative keywords to local DB for analysis
    # ------------------------------------------------------------------

    class BatchSyncNegativesRequest(BaseModel):
        """Input for batch_sync_negatives tool."""
        force_refresh: bool = Field(False, description="Force re-sync even if recently synced")

    @mcp_app.tool()
    async def batch_sync_negatives(request: BatchSyncNegativesRequest) -> dict:
        """
        Sync ALL negative keywords from shared lists + account-level to local SQLite DB.

        Fetches from:
        1. All ENABLED shared negative keyword sets (with status tracking)
        2. All REMOVED shared sets (marked as inactive)
        3. Account-level negative keywords list
        4. Stores in negative_keywords table for fast local analysis

        Use this before running whitelist conflict checks.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)
        db = _get_db()
        total_synced = 0
        sources = []

        # Step 1: Get ALL shared sets (ENABLED + REMOVED for tracking)
        ss_query = """
            SELECT shared_set.id, shared_set.name, shared_set.status, shared_set.member_count
            FROM shared_set
            WHERE shared_set.type = 'NEGATIVE_KEYWORDS'
        """
        ss_results = _execute_gaql(customer_id, ss_query, page_size=100)

        shared_sets = []
        for row in ss_results:
            ss_id = str(_safe_get_value(row, "shared_set.id", ""))
            ss_name = str(_safe_get_value(row, "shared_set.name", ""))
            ss_status = str(_safe_get_value(row, "shared_set.status", ""))
            member_count = int(_safe_get_value(row, "shared_set.member_count", 0) or 0)
            is_active = 1 if ss_status in ("ENABLED", "2") else 0
            if ss_id and member_count > 0:
                shared_sets.append({
                    "id": ss_id, "name": ss_name,
                    "is_active": is_active, "member_count": member_count,
                })

        # Step 2: Fetch keywords from each shared set
        for ss in shared_sets:
            try:
                sc_query = f"""
                    SELECT shared_criterion.criterion_id,
                           shared_criterion.keyword.text,
                           shared_criterion.keyword.match_type
                    FROM shared_criterion
                    WHERE shared_set.id = {ss['id']}
                      AND shared_criterion.type = 'KEYWORD'
                """
                sc_results = _execute_gaql(customer_id, sc_query, page_size=10000)

                records = []
                for row in sc_results:
                    crit_id = str(_safe_get_value(row, "shared_criterion.criterion_id", ""))
                    kw_text = str(_safe_get_value(row, "shared_criterion.keyword.text", ""))
                    match_type = str(_safe_get_value(row, "shared_criterion.keyword.match_type", ""))
                    if crit_id and kw_text:
                        records.append({
                            "criterion_id": crit_id,
                            "keyword_text": kw_text,
                            "match_type": match_type,
                            "source_type": "shared",
                            "source_name": ss["name"],
                            "shared_set_id": ss["id"],
                            "campaign_id": None,
                            "is_active": ss["is_active"],
                        })

                if records:
                    count = db.bulk_upsert_negative_keywords(records)
                    total_synced += count
                    sources.append({
                        "name": ss["name"],
                        "id": ss["id"],
                        "is_active": ss["is_active"],
                        "synced": count,
                        "expected": ss["member_count"],
                    })
            except Exception as e:
                sources.append({
                    "name": ss["name"],
                    "id": ss["id"],
                    "error": str(e)[:200],
                })

        # Step 3: Get account-level negatives
        try:
            acct_query = """
                SELECT shared_set.id, shared_set.name
                FROM shared_set
                WHERE shared_set.name = 'account-level negative keywords list'
                  AND shared_set.status = 'ENABLED'
            """
            acct_results = _execute_gaql(customer_id, acct_query, page_size=5)
            for row in acct_results:
                acct_ss_id = str(_safe_get_value(row, "shared_set.id", ""))
                if acct_ss_id:
                    acct_sc_query = f"""
                        SELECT shared_criterion.criterion_id,
                               shared_criterion.keyword.text,
                               shared_criterion.keyword.match_type
                        FROM shared_criterion
                        WHERE shared_set.id = {acct_ss_id}
                          AND shared_criterion.type = 'KEYWORD'
                    """
                    acct_sc_results = _execute_gaql(customer_id, acct_sc_query, page_size=10000)
                    records = []
                    for sc_row in acct_sc_results:
                        crit_id = str(_safe_get_value(sc_row, "shared_criterion.criterion_id", ""))
                        kw_text = str(_safe_get_value(sc_row, "shared_criterion.keyword.text", ""))
                        match_type = str(_safe_get_value(sc_row, "shared_criterion.keyword.match_type", ""))
                        if crit_id and kw_text:
                            records.append({
                                "criterion_id": crit_id,
                                "keyword_text": kw_text,
                                "match_type": match_type,
                                "source_type": "account",
                                "source_name": "Account negative",
                                "shared_set_id": acct_ss_id,
                                "is_active": 1,
                            })
                    if records:
                        count = db.bulk_upsert_negative_keywords(records)
                        total_synced += count
                        sources.append({
                            "name": "Account-level negatives",
                            "id": acct_ss_id,
                            "is_active": 1,
                            "synced": count,
                        })
        except Exception:
            pass

        stats = db.get_negative_stats()

        _auto_changelog(
            category="config",
            title=f"Synced {total_synced} negative keywords to local DB",
            description=f"Sources: {len(sources)}. Stats: {stats['total']} total, {stats['active']} active.",
        )

        return {
            "status": "success",
            "total_synced": total_synced,
            "sources": sources,
            "db_stats": stats,
        }

    # ------------------------------------------------------------------
    # Tool: Remove keywords from shared negative keyword lists
    # ------------------------------------------------------------------

    class BatchRemoveSharedCriterionRequest(BaseModel):
        """Input for batch_remove_shared_criterion tool."""
        shared_set_id: str = Field(..., description="Shared set ID (e.g. '10816179504' for account-level negatives)")
        criterion_ids: List[str] = Field(..., description="List of shared criterion IDs to remove")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_remove_shared_criterion(request: BatchRemoveSharedCriterionRequest) -> dict:
        """
        Remove keywords from a shared negative keyword list by criterion_id.

        Works for any shared set including the special 'account-level negative keywords list'.
        Use this to unblock product brand keywords that are incorrectly blocked by shared negatives.

        Steps:
        1. Verify the criterion IDs exist in the shared set
        2. Remove them via SharedCriterionService

        IMPORTANT: Removing from a shared list affects ALL campaigns using that list.
        Use dry_run=true first to preview.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        # Step 1: Verify criterion IDs exist in the shared set
        crit_id_csv = ", ".join(str(c) for c in request.criterion_ids)
        query = f"""
            SELECT shared_set.id, shared_set.name,
                   shared_criterion.criterion_id,
                   shared_criterion.keyword.text,
                   shared_criterion.keyword.match_type
            FROM shared_criterion
            WHERE shared_set.id = {request.shared_set_id}
              AND shared_criterion.criterion_id IN ({crit_id_csv})
              AND shared_criterion.type = 'KEYWORD'
        """
        results = _execute_gaql(customer_id, query, page_size=1000)

        found = []
        shared_set_name = ""
        for row in results:
            crit_id = str(_safe_get_value(row, "shared_criterion.criterion_id", ""))
            kw_text = str(_safe_get_value(row, "shared_criterion.keyword.text", ""))
            match_type = str(_safe_get_value(row, "shared_criterion.keyword.match_type", ""))
            shared_set_name = str(_safe_get_value(row, "shared_set.name", ""))
            found.append({
                "criterion_id": crit_id,
                "keyword_text": kw_text,
                "match_type": match_type,
                "resource_name": f"customers/{customer_id}/sharedCriteria/{request.shared_set_id}~{crit_id}",
            })

        not_found = [c for c in request.criterion_ids if c not in [f["criterion_id"] for f in found]]

        # Step 2: Remove (if not dry_run)
        removed = []
        failed = []

        if not request.dry_run and found:
            service = google_ads_client.get_service("SharedCriterionService")
            ops = []
            for item in found:
                op = google_ads_client.get_type("SharedCriterionOperation")
                op.remove = item["resource_name"]
                ops.append(op)

            try:
                _retry_with_backoff(
                    service.mutate_shared_criteria,
                    customer_id=customer_id, operations=ops
                )
                removed = [f["criterion_id"] for f in found]
            except Exception as e:
                failed.append({"error": str(e)[:300]})

        if not request.dry_run and removed:
            kw_list = ", ".join(f'"{f["keyword_text"]}"' for f in found if f["criterion_id"] in removed)
            _auto_changelog(
                category="optimization",
                title=f"Removed {len(removed)} keywords from shared set '{shared_set_name}'",
                description=f"Shared set {request.shared_set_id}: removed {kw_list}",
                severity="warning",
            )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "shared_set_id": request.shared_set_id,
            "shared_set_name": shared_set_name,
            "found_count": len(found),
            "found": found,
            "not_found": not_found,
            "removed": len(removed),
            "failed": failed,
        }

    # ------------------------------------------------------------------
    # Tool: Remove campaign-level negative keywords by criterion_id
    # ------------------------------------------------------------------

    class BatchRemoveCampaignNegativeKeywordsRequest(BaseModel):
        """Input for batch_remove_campaign_negative_keywords tool."""
        campaign_id: str = Field(..., description="Campaign ID")
        criterion_ids: List[str] = Field(..., description="List of campaign criterion IDs to remove")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_remove_campaign_negative_keywords(request: BatchRemoveCampaignNegativeKeywordsRequest) -> dict:
        """
        Remove campaign-level negative keywords by criterion_id.

        Queries GAQL to verify the criterion IDs exist and are negative keywords,
        then removes them via CampaignCriterionService.

        Use dry_run=true first to preview what would be removed.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        # Step 1: Verify criterion IDs exist and are negative keywords
        crit_id_csv = ", ".join(str(c) for c in request.criterion_ids)
        query = f"""
            SELECT campaign.id, campaign.name,
                   campaign_criterion.criterion_id,
                   campaign_criterion.keyword.text,
                   campaign_criterion.keyword.match_type,
                   campaign_criterion.negative
            FROM campaign_criterion
            WHERE campaign.id = {request.campaign_id}
              AND campaign_criterion.criterion_id IN ({crit_id_csv})
              AND campaign_criterion.negative = TRUE
              AND campaign_criterion.type = 'KEYWORD'
        """
        results = _execute_gaql(customer_id, query, page_size=1000)

        found = []
        for row in results:
            crit_id = str(_safe_get_value(row, "campaign_criterion.criterion_id", ""))
            kw_text = str(_safe_get_value(row, "campaign_criterion.keyword.text", ""))
            match_type = str(_safe_get_value(row, "campaign_criterion.keyword.match_type", ""))
            camp_name = str(_safe_get_value(row, "campaign.name", ""))
            found.append({
                "criterion_id": crit_id,
                "keyword_text": kw_text,
                "match_type": match_type,
                "campaign_name": camp_name,
            })

        not_found = [c for c in request.criterion_ids if c not in [f["criterion_id"] for f in found]]

        # Step 2: Remove (if not dry_run)
        removed = []
        failed = []

        if not request.dry_run and found:
            service = google_ads_client.get_service("CampaignCriterionService")
            ops = []
            for item in found:
                op = google_ads_client.get_type("CampaignCriterionOperation")
                op.remove = service.campaign_criterion_path(
                    customer_id, request.campaign_id, item["criterion_id"]
                )
                ops.append(op)

            BATCH_SIZE = 1000
            for i in range(0, len(ops), BATCH_SIZE):
                batch = ops[i:i + BATCH_SIZE]
                try:
                    _retry_with_backoff(
                        service.mutate_campaign_criteria,
                        customer_id=customer_id, operations=batch
                    )
                    removed.extend([item["criterion_id"] for item in found[i:i + len(batch)]])
                except Exception as e:
                    failed.append({"batch_start": i, "error": str(e)[:200]})

        if not request.dry_run and removed:
            kw_list = ", ".join(f'"{f["keyword_text"]}" ({f["match_type"]})' for f in found if f["criterion_id"] in removed)
            _auto_changelog(
                category="optimization",
                title=f"Removed {len(removed)} campaign negative keywords",
                description=f"Campaign {request.campaign_id}: removed negatives: {kw_list}",
                campaign_id=request.campaign_id,
            )

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "found_count": len(found),
            "found": found,
            "not_found": not_found,
            "removed": len(removed),
            "failed": failed,
        }

    # ------------------------------------------------------------------
    # Tool: Find and resolve negative keyword conflicts
    # ------------------------------------------------------------------

    class BatchFixNegativeConflictsRequest(BaseModel):
        """Input for batch_fix_negative_conflicts tool."""
        campaign_id: str = Field(..., description="Campaign ID to scan for conflicts")
        roi_threshold: float = Field(50.0, description="ROI % threshold: >= this -> remove negative, < this -> pause keyword")
        days_back: int = Field(90, description="Days to look back for keyword performance data")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_fix_negative_conflicts(request: BatchFixNegativeConflictsRequest) -> dict:
        """
        Find and fix negative keyword conflicts in a campaign.

        Scans campaign-level negative keywords, finds positive keywords they block,
        evaluates ROI of blocked keywords, and takes action:
        - ROI >= threshold: remove the negative keyword (it blocks profitable traffic)
        - ROI < threshold or zero data: pause the positive keyword (not profitable)

        Handles campaign-level negatives. Use dry_run=true first to preview.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, google_ads_client

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        # Step 1a: Get campaign-level negative keywords
        all_negatives = []  # unified list: {text, match_type, level, source, criterion_id, shared_set_id?}

        neg_query = f"""
            SELECT campaign_criterion.criterion_id,
                   campaign_criterion.keyword.text,
                   campaign_criterion.keyword.match_type,
                   campaign_criterion.negative
            FROM campaign_criterion
            WHERE campaign.id = {request.campaign_id}
              AND campaign_criterion.negative = TRUE
              AND campaign_criterion.type = 'KEYWORD'
        """
        neg_results = _execute_gaql(customer_id, neg_query, page_size=1000)

        for row in neg_results:
            all_negatives.append({
                "criterion_id": str(_safe_get_value(row, "campaign_criterion.criterion_id", "")),
                "text": str(_safe_get_value(row, "campaign_criterion.keyword.text", "")).lower(),
                "match_type": str(_safe_get_value(row, "campaign_criterion.keyword.match_type", "")),
                "level": "campaign",
                "source": "Campaign negative",
                "shared_set_id": None,
            })

        # Step 1b: Get ACTIVE shared negative keyword lists linked to this campaign
        # CRITICAL: filter by campaign_shared_set.status = 'ENABLED' to exclude REMOVED lists
        shared_sets_query = f"""
            SELECT shared_set.id, shared_set.name, campaign_shared_set.status
            FROM campaign_shared_set
            WHERE campaign.id = {request.campaign_id}
              AND shared_set.type = 'NEGATIVE_KEYWORDS'
              AND campaign_shared_set.status = 'ENABLED'
        """
        try:
            ss_results = _execute_gaql(customer_id, shared_sets_query, page_size=100)
            shared_sets = []
            for row in ss_results:
                ss_id = str(_safe_get_value(row, "shared_set.id", ""))
                ss_name = str(_safe_get_value(row, "shared_set.name", ""))
                if ss_id:
                    shared_sets.append({"id": ss_id, "name": ss_name})

            # Query keywords from each shared set
            for ss in shared_sets:
                sc_query = f"""
                    SELECT shared_criterion.criterion_id,
                           shared_criterion.keyword.text,
                           shared_criterion.keyword.match_type
                    FROM shared_criterion
                    WHERE shared_set.id = {ss['id']}
                      AND shared_criterion.type = 'KEYWORD'
                """
                try:
                    sc_results = _execute_gaql(customer_id, sc_query, page_size=10000)
                    for row in sc_results:
                        all_negatives.append({
                            "criterion_id": str(_safe_get_value(row, "shared_criterion.criterion_id", "")),
                            "text": str(_safe_get_value(row, "shared_criterion.keyword.text", "")).lower(),
                            "match_type": str(_safe_get_value(row, "shared_criterion.keyword.match_type", "")),
                            "level": "shared",
                            "source": f"Shared: {ss['name']}",
                            "shared_set_id": ss["id"],
                        })
                except Exception:
                    pass
        except Exception:
            pass  # Shared set queries are optional enhancement

        # Step 1c: Get account-level negative keywords list
        # Google Ads has a special shared set "account-level negative keywords list"
        # that applies to ALL campaigns without campaign_shared_set links
        try:
            acct_neg_query = """
                SELECT shared_set.id, shared_set.name, shared_set.status
                FROM shared_set
                WHERE shared_set.name = 'account-level negative keywords list'
                  AND shared_set.status = 'ENABLED'
            """
            acct_results = _execute_gaql(customer_id, acct_neg_query, page_size=5)
            for row in acct_results:
                acct_ss_id = str(_safe_get_value(row, "shared_set.id", ""))
                if acct_ss_id:
                    acct_sc_query = f"""
                        SELECT shared_criterion.criterion_id,
                               shared_criterion.keyword.text,
                               shared_criterion.keyword.match_type
                        FROM shared_criterion
                        WHERE shared_set.id = {acct_ss_id}
                          AND shared_criterion.type = 'KEYWORD'
                    """
                    try:
                        acct_sc_results = _execute_gaql(customer_id, acct_sc_query, page_size=10000)
                        for sc_row in acct_sc_results:
                            all_negatives.append({
                                "criterion_id": str(_safe_get_value(sc_row, "shared_criterion.criterion_id", "")),
                                "text": str(_safe_get_value(sc_row, "shared_criterion.keyword.text", "")).lower(),
                                "match_type": str(_safe_get_value(sc_row, "shared_criterion.keyword.match_type", "")),
                                "level": "account",
                                "source": "Account negative",
                                "shared_set_id": acct_ss_id,
                            })
                    except Exception:
                        pass
        except Exception:
            pass  # Account-level negatives are optional

        if not all_negatives:
            return {"status": "success", "message": "No negative keywords found (campaign, shared, or account level)", "conflicts": []}

        # Step 2a: Get ALL positive keywords from ENABLED campaign + ENABLED ad groups only
        # Using ad_group_criterion (no date filter) — returns every keyword regardless of metrics
        kw_all_query = f"""
            SELECT campaign.status, ad_group.id, ad_group.name, ad_group.status,
                   ad_group_criterion.criterion_id,
                   ad_group_criterion.keyword.text,
                   ad_group_criterion.keyword.match_type,
                   ad_group_criterion.status
            FROM ad_group_criterion
            WHERE campaign.id = {request.campaign_id}
              AND campaign.status = 'ENABLED'
              AND ad_group.status = 'ENABLED'
              AND ad_group_criterion.status = 'ENABLED'
              AND ad_group_criterion.type = 'KEYWORD'
              AND ad_group_criterion.negative = FALSE
        """
        kw_all_results = _execute_gaql(customer_id, kw_all_query, page_size=10000)

        # Build keyword map from ALL keywords (zero metrics initially)
        keywords = {}
        for row in kw_all_results:
            crit_id = str(_safe_get_value(row, "ad_group_criterion.criterion_id", ""))
            kw_text = str(_safe_get_value(row, "ad_group_criterion.keyword.text", "")).lower()
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            ag_name = str(_safe_get_value(row, "ad_group.name", ""))
            status = str(_safe_get_value(row, "ad_group_criterion.status", ""))
            match_type = str(_safe_get_value(row, "ad_group_criterion.keyword.match_type", ""))

            key = f"{ag_id}_{crit_id}"
            if key not in keywords:
                keywords[key] = {
                    "criterion_id": crit_id, "text": kw_text,
                    "match_type": match_type, "status": status,
                    "ad_group_id": ag_id, "ad_group_name": ag_name,
                    "impressions": 0, "clicks": 0,
                    "cost_micros": 0, "conversions": 0.0, "conversions_value": 0.0,
                }

        # Step 2b: Enrich with performance metrics (only keywords that had traffic)
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=request.days_back)).strftime("%Y-%m-%d")

        kw_metrics_query = f"""
            SELECT ad_group.id,
                   ad_group_criterion.criterion_id,
                   metrics.impressions, metrics.clicks,
                   metrics.cost_micros, metrics.conversions,
                   metrics.conversions_value
            FROM keyword_view
            WHERE campaign.id = {request.campaign_id}
              AND ad_group_criterion.status != 'REMOVED'
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
        try:
            kw_metrics_results = _execute_gaql(customer_id, kw_metrics_query, page_size=10000)
            for row in kw_metrics_results:
                crit_id = str(_safe_get_value(row, "ad_group_criterion.criterion_id", ""))
                ag_id = str(_safe_get_value(row, "ad_group.id", ""))
                key = f"{ag_id}_{crit_id}"
                if key in keywords:
                    kw = keywords[key]
                    kw["impressions"] += int(_safe_get_value(row, "metrics.impressions", 0) or 0)
                    kw["clicks"] += int(_safe_get_value(row, "metrics.clicks", 0) or 0)
                    kw["cost_micros"] += int(_safe_get_value(row, "metrics.cost_micros", 0) or 0)
                    kw["conversions"] += float(_safe_get_value(row, "metrics.conversions", 0) or 0)
                    kw["conversions_value"] += float(_safe_get_value(row, "metrics.conversions_value", 0) or 0)
        except Exception:
            pass  # Metrics enrichment is optional — conflicts still detected from step 2a

        # Step 3: Detect conflicts (optimized for large negative lists)
        import re

        def _clean_kw(text):
            """Strip bracket/quote decorators from keyword text."""
            return re.sub(r'^[\[\]"]+|[\[\]"]+$', '', text.lower().strip()).strip()

        def _match_type_key(mt):
            nm = str(mt).upper().replace("KEYWORDMATCHTYPEENUM.", "").replace("KEYWORDMATCHTYPE.", "")
            if nm in ("EXACT", "2"):
                return "EXACT"
            elif nm in ("PHRASE", "3"):
                return "PHRASE"
            elif nm in ("BROAD", "4"):
                return "BROAD"
            return "UNKNOWN"

        # Pre-index negatives by match type for O(1) lookups where possible
        # MATCHING RULES (aligned with Google Ads actual behavior):
        # - EXACT: negative text == keyword text (after cleaning brackets/quotes)
        # - PHRASE: negative text appears as SUBSTRING in keyword text
        #   Google's PHRASE negative is aggressive — "arthro" blocks "arthrovia",
        #   "neo" blocks "neo collagen". This is substring containment, NOT word-boundary.
        # - BROAD: ALL words of negative appear anywhere in keyword (as substrings)
        exact_negs = {}   # text → [neg, neg, ...]
        phrase_negs = []   # list of (neg_text, first_3_chars, neg)
        broad_negs = []    # list of (word_list, neg)

        for neg in all_negatives:
            mt = _match_type_key(neg["match_type"])
            neg_text = neg["text"].lower().strip()
            if not neg_text:
                continue
            if mt == "EXACT":
                exact_negs.setdefault(neg_text, []).append(neg)
            elif mt == "PHRASE":
                # Pre-extract first 3 chars for fast pre-filter
                prefix = neg_text[:3] if len(neg_text) >= 3 else neg_text
                phrase_negs.append((neg_text, prefix, neg))
            elif mt == "BROAD":
                neg_words = neg_text.split()
                broad_negs.append((neg_words, neg))

        conflicts = []

        for key, kw in keywords.items():
            kw_clean = _clean_kw(kw["text"])
            kw_words = set(kw_clean.split())

            matched_negs = []

            # EXACT: O(1) dict lookup
            if kw_clean in exact_negs:
                matched_negs.extend(exact_negs[kw_clean])

            # PHRASE: substring containment (Google's actual behavior)
            # "arthro" blocks "arthrovia", "neo" blocks "neo collagen"
            for neg_text, prefix, neg in phrase_negs:
                # Fast pre-check: first 3 chars of negative must appear in keyword
                if prefix not in kw_clean:
                    continue
                # Full substring check
                if neg_text in kw_clean:
                    matched_negs.append(neg)

            # BROAD: each word of negative must appear as substring in keyword
            for neg_words, neg in broad_negs:
                if all(nw in kw_clean for nw in neg_words):
                    matched_negs.append(neg)

            # Build conflict entries for all matched negatives
            if matched_negs:
                cost = kw["cost_micros"] / 1_000_000.0
                conv_value = kw["conversions_value"]
                roi = ((conv_value - cost) / cost * 100) if cost > 0 else 0.0

                for neg in matched_negs:
                    if roi >= request.roi_threshold and neg["level"] == "campaign":
                        action = "REMOVE_NEGATIVE"
                    else:
                        action = "PAUSE_KEYWORD"

                    conflicts.append({
                        "keyword_text": kw["text"],
                        "keyword_criterion_id": kw["criterion_id"],
                        "keyword_match_type": kw["match_type"],
                        "keyword_status": kw["status"],
                        "ad_group_id": kw["ad_group_id"],
                        "ad_group_name": kw["ad_group_name"],
                        "negative_text": neg["text"],
                        "negative_criterion_id": neg["criterion_id"],
                        "negative_match_type": neg["match_type"],
                        "negative_level": neg["level"],
                        "negative_source": neg["source"],
                        "impressions": kw["impressions"],
                        "clicks": kw["clicks"],
                        "cost": round(cost, 2),
                        "conversions": kw["conversions"],
                        "conversions_value": round(conv_value, 2),
                        "roi": round(roi, 1),
                        "action": action,
                    })

        # Step 4: Classify actions
        remove_negatives = [c for c in conflicts if c["action"] == "REMOVE_NEGATIVE"]
        pause_keywords = [c for c in conflicts if c["action"] == "PAUSE_KEYWORD"]

        neg_ids_to_remove = list({c["negative_criterion_id"] for c in remove_negatives})
        kw_to_pause = [(c["ad_group_id"], c["keyword_criterion_id"]) for c in pause_keywords
                       if c["keyword_status"] in ("ENABLED", "KeywordMatchType.ENABLED", "2")]

        # Step 5: Execute (if not dry_run)
        negatives_removed = []
        keywords_paused = []
        errors = []

        if not request.dry_run:
            # Remove high-ROI negatives
            if neg_ids_to_remove:
                service = google_ads_client.get_service("CampaignCriterionService")
                ops = []
                for crit_id in neg_ids_to_remove:
                    op = google_ads_client.get_type("CampaignCriterionOperation")
                    op.remove = service.campaign_criterion_path(
                        customer_id, request.campaign_id, crit_id
                    )
                    ops.append(op)

                try:
                    _retry_with_backoff(
                        service.mutate_campaign_criteria,
                        customer_id=customer_id, operations=ops
                    )
                    negatives_removed = neg_ids_to_remove
                except Exception as e:
                    errors.append(f"Failed to remove negatives: {str(e)[:200]}")

            # Pause low-ROI keywords
            if kw_to_pause:
                from google.protobuf import field_mask_pb2
                ag_crit_service = google_ads_client.get_service("AdGroupCriterionService")
                ops = []
                for ag_id, crit_id in kw_to_pause:
                    op = google_ads_client.get_type("AdGroupCriterionOperation")
                    criterion = op.update
                    criterion.resource_name = ag_crit_service.ad_group_criterion_path(
                        customer_id, ag_id, crit_id
                    )
                    criterion.status = google_ads_client.enums.AdGroupCriterionStatusEnum.PAUSED
                    op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
                    ops.append(op)

                BATCH_SIZE = 1000
                for i in range(0, len(ops), BATCH_SIZE):
                    batch = ops[i:i + BATCH_SIZE]
                    try:
                        _retry_with_backoff(
                            ag_crit_service.mutate_ad_group_criteria,
                            customer_id=customer_id, operations=batch
                        )
                        keywords_paused.extend([kw_to_pause[j] for j in range(i, min(i + len(batch), len(kw_to_pause)))])
                    except Exception as e:
                        errors.append(f"Failed to pause keywords batch {i}: {str(e)[:200]}")

        if not request.dry_run and (negatives_removed or keywords_paused):
            _auto_changelog(
                category="optimization",
                title=f"Fixed {len(conflicts)} negative keyword conflicts",
                description=(
                    f"Campaign {request.campaign_id}: "
                    f"removed {len(negatives_removed)} negatives (ROI>={request.roi_threshold}%), "
                    f"paused {len(keywords_paused)} keywords (low/zero ROI). "
                    f"Errors: {len(errors)}"
                ),
                campaign_id=request.campaign_id,
            )

        # Build source breakdown
        by_source = {}
        for c in conflicts:
            src = c.get("negative_source", c.get("negative_level", "unknown"))
            by_source[src] = by_source.get(src, 0) + 1

        campaign_neg_count = sum(1 for n in all_negatives if n["level"] == "campaign")
        shared_neg_count = sum(1 for n in all_negatives if n["level"] == "shared")

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "negatives": {
                "campaign": campaign_neg_count,
                "shared": shared_neg_count,
                "total": len(all_negatives),
            },
            "total_positive_keywords": len(keywords),
            "total_conflicts": len(conflicts),
            "conflicts_by_source": by_source,
            "action_summary": {
                "remove_negatives": len(neg_ids_to_remove),
                "pause_keywords": len(kw_to_pause),
            },
            "high_roi_conflicts": remove_negatives[:20],
            "low_roi_conflicts_sample": pause_keywords[:20],
            "executed": {
                "negatives_removed": len(negatives_removed),
                "keywords_paused": len(keywords_paused),
                "errors": errors,
            },
        }

    # ============================================================================================
    # batch_enable_paused_keywords — Mass re-enable PAUSED keywords matching brand filters
    # ============================================================================================

    class BatchEnablePausedKeywordsRequest(BaseModel):
        """Input for batch_enable_paused_keywords tool."""
        keyword_filter: List[str] = Field(..., description="Brand name fragments to match (e.g. ['widget', 'gadget', 'premium'])")
        campaign_id: Optional[str] = Field(None, description="Filter to specific campaign. None = all ENABLED campaigns")
        match_mode: str = Field("contains", description="Match mode: 'prefix' (Google-style word prefix), 'exact', or 'contains' (default)")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_enable_paused_keywords(request: BatchEnablePausedKeywordsRequest) -> dict:
        """
        Mass enable PAUSED keywords matching brand name filters in ENABLED campaigns + ENABLED ad groups.

        Use after removing negative keywords from shared lists to reactivate
        brand keywords that were previously paused due to conflicts.

        Supports three match modes:
        - 'contains': keyword text contains any filter string (default, safest)
        - 'prefix': Google-style word prefix match (e.g. 'arthro' matches 'arthrovia')
        - 'exact': exact match only

        Pagination: Fetches ALL paused keywords using criterion_id-based pagination (>1000 rows).
        Batching: Enables up to 1000 keywords per mutate call.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client
        from google.protobuf import field_mask_pb2

        _ensure_client()
        from google_ads_mcp import google_ads_client
        customer_id = _format_customer_id(CUSTOMER_ID)

        filters_lower = [f.lower().strip() for f in request.keyword_filter if f.strip()]
        if not filters_lower:
            return {"status": "error", "message": "keyword_filter must contain at least one non-empty string"}

        # --- Helper: match keyword against filters ---
        def _matches(kw_text: str, mode: str) -> list:
            """Return list of matching filter strings."""
            kw_lower = kw_text.lower().strip()
            kw_words = kw_lower.split()
            matched = []
            for filt in filters_lower:
                if mode == "exact":
                    if kw_lower == filt:
                        matched.append(filt)
                elif mode == "prefix":
                    # Google-style: each filter word is prefix of corresponding kw word
                    filt_words = filt.split()
                    if len(filt_words) == 1:
                        if any(w.startswith(filt_words[0]) for w in kw_words):
                            matched.append(filt)
                    else:
                        for start in range(len(kw_words) - len(filt_words) + 1):
                            if all(kw_words[start + i].startswith(filt_words[i]) for i in range(len(filt_words))):
                                matched.append(filt)
                                break
                else:  # contains (default)
                    if filt in kw_lower:
                        matched.append(filt)
            return matched

        # --- Step 1: Fetch ALL paused keywords with pagination ---
        _debug(DebugCat.KEYWORDS, f"Fetching PAUSED keywords (campaign_id={request.campaign_id}, filters={len(filters_lower)})")

        campaign_filter = f"AND campaign.id = {request.campaign_id}" if request.campaign_id else ""

        all_paused = []
        last_criterion_id = 0
        PAGE_SIZE = 10000  # GAQL max

        while True:
            query = f"""
                SELECT campaign.id, campaign.name, campaign.status,
                       ad_group.id, ad_group.name, ad_group.status,
                       ad_group_criterion.criterion_id,
                       ad_group_criterion.keyword.text,
                       ad_group_criterion.keyword.match_type,
                       ad_group_criterion.status
                FROM ad_group_criterion
                WHERE campaign.status = 'ENABLED'
                  AND ad_group.status = 'ENABLED'
                  AND ad_group_criterion.status = 'PAUSED'
                  AND ad_group_criterion.type = 'KEYWORD'
                  AND ad_group_criterion.negative = FALSE
                  AND ad_group_criterion.criterion_id > {last_criterion_id}
                  {campaign_filter}
                ORDER BY ad_group_criterion.criterion_id ASC
                LIMIT {PAGE_SIZE}
            """
            try:
                results = _execute_gaql(customer_id, query, page_size=PAGE_SIZE)
            except Exception as e:
                return {"status": "error", "message": f"GAQL query failed: {str(e)[:300]}"}

            page_count = 0
            for row in results:
                page_count += 1
                crit_id = str(_safe_get_value(row, "ad_group_criterion.criterion_id", ""))
                kw_text = str(_safe_get_value(row, "ad_group_criterion.keyword.text", ""))
                match_type = str(_safe_get_value(row, "ad_group_criterion.keyword.match_type", ""))
                ag_id = str(_safe_get_value(row, "ad_group.id", ""))
                ag_name = str(_safe_get_value(row, "ad_group.name", ""))
                camp_id = str(_safe_get_value(row, "campaign.id", ""))
                camp_name = str(_safe_get_value(row, "campaign.name", ""))

                all_paused.append({
                    "criterion_id": crit_id,
                    "keyword_text": kw_text,
                    "match_type": match_type,
                    "ad_group_id": ag_id,
                    "ad_group_name": ag_name,
                    "campaign_id": camp_id,
                    "campaign_name": camp_name,
                })
                last_criterion_id = int(crit_id)

            _debug(DebugCat.KEYWORDS, f"Page fetched: {page_count} rows (last_id={last_criterion_id})")
            if page_count < PAGE_SIZE:
                break  # No more pages

        _debug(DebugCat.KEYWORDS, f"Total PAUSED keywords fetched: {len(all_paused)}")

        # --- Step 2: Filter by keyword_filter ---
        matched_keywords = []
        for kw in all_paused:
            matches = _matches(kw["keyword_text"], request.match_mode)
            if matches:
                kw["matched_filters"] = matches
                matched_keywords.append(kw)

        _debug(DebugCat.KEYWORDS, f"Matched {len(matched_keywords)} keywords for {len(filters_lower)} filters")

        if not matched_keywords:
            return {
                "status": "success",
                "dry_run": request.dry_run,
                "total_paused_scanned": len(all_paused),
                "matched": 0,
                "enabled": 0,
                "message": f"No PAUSED keywords matched filters: {request.keyword_filter}",
            }

        # --- Step 3: Enable keywords (if not dry_run) ---
        enabled_count = 0
        errors = []

        if not request.dry_run:
            ag_crit_service = google_ads_client.get_service("AdGroupCriterionService")
            ops = []
            for kw in matched_keywords:
                op = google_ads_client.get_type("AdGroupCriterionOperation")
                criterion = op.update
                criterion.resource_name = ag_crit_service.ad_group_criterion_path(
                    customer_id, kw["ad_group_id"], kw["criterion_id"]
                )
                criterion.status = google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
                op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
                ops.append(op)

            # Batch in chunks of 1000
            BATCH_SIZE = 1000
            for i in range(0, len(ops), BATCH_SIZE):
                batch = ops[i:i + BATCH_SIZE]
                try:
                    _retry_with_backoff(
                        ag_crit_service.mutate_ad_group_criteria,
                        customer_id=customer_id, operations=batch
                    )
                    enabled_count += len(batch)
                    _rate_limiter.record_mutation(len(batch))
                except Exception as e:
                    errors.append(f"Batch {i}-{i+len(batch)}: {str(e)[:200]}")

            # Changelog
            filter_summary = ", ".join(filters_lower[:10])
            if len(filters_lower) > 10:
                filter_summary += f" (+{len(filters_lower)-10} more)"
            _auto_changelog(
                category="optimization",
                title=f"Enabled {enabled_count} paused keywords matching [{filter_summary}]",
                description=(
                    f"Filters: {filters_lower}. Mode: {request.match_mode}. "
                    f"Scanned {len(all_paused)} paused keywords, matched {len(matched_keywords)}, "
                    f"enabled {enabled_count}. Errors: {len(errors)}"
                ),
                campaign_id=request.campaign_id,
                severity="warning",
            )

        # --- Build per-campaign summary ---
        by_campaign = {}
        for kw in matched_keywords:
            key = f"{kw['campaign_id']}|{kw['campaign_name']}"
            if key not in by_campaign:
                by_campaign[key] = {"campaign_id": kw["campaign_id"], "campaign_name": kw["campaign_name"], "count": 0, "keywords": []}
            by_campaign[key]["count"] += 1
            if len(by_campaign[key]["keywords"]) < 5:
                by_campaign[key]["keywords"].append(f"{kw['keyword_text']} [{kw['match_type']}]")

        # --- Build per-filter summary ---
        by_filter = {}
        for kw in matched_keywords:
            for f in kw.get("matched_filters", []):
                by_filter[f] = by_filter.get(f, 0) + 1

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "total_paused_scanned": len(all_paused),
            "matched": len(matched_keywords),
            "enabled": enabled_count,
            "errors": errors,
            "by_campaign": list(by_campaign.values()),
            "by_filter": by_filter,
            "sample_keywords": [
                {
                    "keyword": kw["keyword_text"],
                    "match_type": kw["match_type"],
                    "ad_group": kw["ad_group_name"],
                    "campaign": kw["campaign_name"],
                    "matched_filters": kw.get("matched_filters", []),
                }
                for kw in matched_keywords[:30]
            ],
        }

    # ============================================================================================
    # batch_enable_paused_ad_groups — Enable PAUSED ad groups that have ENABLED keywords/ads
    # ============================================================================================

    class BatchEnablePausedAdGroupsRequest(BaseModel):
        """Input for batch_enable_paused_ad_groups tool."""
        campaign_id: Optional[str] = Field(None, description="Filter to specific campaign. None = all ENABLED campaigns")
        require_enabled_keywords: bool = Field(True, description="Only enable ad groups that have at least 1 ENABLED keyword")
        require_enabled_ads: bool = Field(True, description="Only enable ad groups that have at least 1 ENABLED ad")
        name_filter: Optional[List[str]] = Field(None, description="Optional: only enable ad groups whose name contains any of these strings")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")

    @mcp_app.tool()
    async def batch_enable_paused_ad_groups(request: BatchEnablePausedAdGroupsRequest) -> dict:
        """
        Enable PAUSED ad groups in ENABLED campaigns that have active keywords and/or ads.

        Use after batch_enable_paused_keywords to reactivate ad groups whose keywords
        were re-enabled. Only enables ad groups that meet the criteria:
        - At least 1 ENABLED keyword (if require_enabled_keywords=True)
        - At least 1 ENABLED ad (if require_enabled_ads=True)
        - Name matches filter (if name_filter provided)

        Ad groups with zero ENABLED keywords AND zero ENABLED ads are left PAUSED.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client
        from google.protobuf import field_mask_pb2

        _ensure_client()
        from google_ads_mcp import google_ads_client
        customer_id = _format_customer_id(CUSTOMER_ID)

        campaign_filter = f"AND campaign.id = {request.campaign_id}" if request.campaign_id else ""

        # --- Step 1: Get PAUSED ad groups ---
        query_ag = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   ad_group.id, ad_group.name, ad_group.status
            FROM ad_group
            WHERE campaign.status = 'ENABLED'
              AND ad_group.status = 'PAUSED'
              {campaign_filter}
        """
        try:
            results_ag = _execute_gaql(customer_id, query_ag, page_size=10000)
        except Exception as e:
            return {"status": "error", "message": f"GAQL failed: {str(e)[:300]}"}

        paused_groups = []
        for row in results_ag:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            ag_name = str(_safe_get_value(row, "ad_group.name", ""))
            camp_id = str(_safe_get_value(row, "campaign.id", ""))
            camp_name = str(_safe_get_value(row, "campaign.name", ""))
            paused_groups.append({
                "ad_group_id": ag_id,
                "ad_group_name": ag_name,
                "campaign_id": camp_id,
                "campaign_name": camp_name,
            })

        _debug(DebugCat.AD_GROUPS, f"Found {len(paused_groups)} PAUSED ad groups")

        # Apply name_filter if provided
        if request.name_filter:
            name_filters_lower = [f.lower() for f in request.name_filter if f.strip()]
            paused_groups = [
                ag for ag in paused_groups
                if any(f in ag["ad_group_name"].lower() for f in name_filters_lower)
            ]
            _debug(DebugCat.AD_GROUPS, f"After name filter: {len(paused_groups)} ad groups")

        if not paused_groups:
            return {
                "status": "success",
                "dry_run": request.dry_run,
                "total_paused": 0,
                "eligible": 0,
                "enabled": 0,
                "message": "No PAUSED ad groups found matching criteria",
            }

        # --- Step 2: Check which have ENABLED keywords and/or ads ---
        ag_ids = [ag["ad_group_id"] for ag in paused_groups]

        # Check keywords (in batches of 100 ad group IDs for GAQL)
        ag_has_enabled_kw = set()
        ag_has_enabled_ad = set()

        if request.require_enabled_keywords:
            CHUNK = 100
            for i in range(0, len(ag_ids), CHUNK):
                chunk = ag_ids[i:i + CHUNK]
                ag_id_list = ", ".join(chunk)
                kw_query = f"""
                    SELECT ad_group.id, ad_group_criterion.criterion_id, ad_group_criterion.status
                    FROM ad_group_criterion
                    WHERE ad_group.id IN ({ag_id_list})
                      AND ad_group_criterion.status = 'ENABLED'
                      AND ad_group_criterion.type = 'KEYWORD'
                      AND ad_group_criterion.negative = FALSE
                    LIMIT 10000
                """
                try:
                    kw_results = _execute_gaql(customer_id, kw_query, page_size=10000)
                    for row in kw_results:
                        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
                        ag_has_enabled_kw.add(ag_id)
                except Exception:
                    pass

        if request.require_enabled_ads:
            CHUNK = 100
            for i in range(0, len(ag_ids), CHUNK):
                chunk = ag_ids[i:i + CHUNK]
                ag_id_list = ", ".join(chunk)
                ad_query = f"""
                    SELECT ad_group.id, ad_group_ad.ad.id, ad_group_ad.status
                    FROM ad_group_ad
                    WHERE ad_group.id IN ({ag_id_list})
                      AND ad_group_ad.status = 'ENABLED'
                    LIMIT 10000
                """
                try:
                    ad_results = _execute_gaql(customer_id, ad_query, page_size=10000)
                    for row in ad_results:
                        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
                        ag_has_enabled_ad.add(ag_id)
                except Exception:
                    pass

        # --- Step 3: Filter eligible ad groups ---
        eligible = []
        skipped_no_kw = []
        skipped_no_ad = []

        for ag in paused_groups:
            ag_id = ag["ad_group_id"]
            has_kw = (not request.require_enabled_keywords) or (ag_id in ag_has_enabled_kw)
            has_ad = (not request.require_enabled_ads) or (ag_id in ag_has_enabled_ad)

            if has_kw and has_ad:
                eligible.append(ag)
            else:
                if not has_kw:
                    skipped_no_kw.append(ag["ad_group_name"])
                if not has_ad:
                    skipped_no_ad.append(ag["ad_group_name"])

        _debug(DebugCat.AD_GROUPS, f"Eligible to enable: {len(eligible)}, skipped (no kw): {len(skipped_no_kw)}, skipped (no ad): {len(skipped_no_ad)}")

        # --- Step 4: Enable eligible ad groups ---
        enabled_count = 0
        errors = []

        if not request.dry_run and eligible:
            ad_group_service = google_ads_client.get_service("AdGroupService")
            ops = []
            for ag in eligible:
                op = google_ads_client.get_type("AdGroupOperation")
                ad_group = op.update
                ad_group.resource_name = ad_group_service.ad_group_path(
                    customer_id, ag["ad_group_id"]
                )
                ad_group.status = google_ads_client.enums.AdGroupStatusEnum.ENABLED
                op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
                ops.append(op)

            BATCH_SIZE = 1000
            for i in range(0, len(ops), BATCH_SIZE):
                batch = ops[i:i + BATCH_SIZE]
                try:
                    _retry_with_backoff(
                        ad_group_service.mutate_ad_groups,
                        customer_id=customer_id, operations=batch
                    )
                    enabled_count += len(batch)
                    _rate_limiter.record_mutation(len(batch))
                except Exception as e:
                    errors.append(f"Batch {i}: {str(e)[:200]}")

            _auto_changelog(
                category="optimization",
                title=f"Enabled {enabled_count} PAUSED ad groups with active keywords/ads",
                description=(
                    f"Campaign: {request.campaign_id or 'ALL'}. "
                    f"Scanned {len(paused_groups)} paused, {len(eligible)} eligible, "
                    f"enabled {enabled_count}. Skipped: {len(skipped_no_kw)} no kw, {len(skipped_no_ad)} no ad."
                ),
                campaign_id=request.campaign_id,
                severity="warning",
            )

        # --- Build per-campaign summary ---
        by_campaign = {}
        for ag in eligible:
            key = ag["campaign_id"]
            if key not in by_campaign:
                by_campaign[key] = {"campaign_id": ag["campaign_id"], "campaign_name": ag["campaign_name"], "count": 0, "ad_groups": []}
            by_campaign[key]["count"] += 1
            if len(by_campaign[key]["ad_groups"]) < 5:
                by_campaign[key]["ad_groups"].append(ag["ad_group_name"])

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "total_paused_scanned": len(paused_groups),
            "eligible": len(eligible),
            "enabled": enabled_count,
            "skipped": {
                "no_enabled_keywords": len(skipped_no_kw),
                "no_enabled_ads": len(skipped_no_ad),
                "sample_skipped_no_kw": skipped_no_kw[:10],
                "sample_skipped_no_ad": skipped_no_ad[:10],
            },
            "errors": errors,
            "by_campaign": list(by_campaign.values()),
            "sample_enabled": [
                {
                    "ad_group": ag["ad_group_name"],
                    "ad_group_id": ag["ad_group_id"],
                    "campaign": ag["campaign_name"],
                }
                for ag in eligible[:20]
            ],
        }

    # ============================================================================================
    # batch_fix_match_types — Fix keywords with wrong match type notation (brackets/quotes/BMM)
    # ============================================================================================

    class MatchTypeFixEntry(BaseModel):
        """Single keyword fix entry."""
        ad_group_id: str = Field(..., description="Ad group ID")
        criterion_id: str = Field(..., description="Criterion ID of keyword to pause")
        clean_text: str = Field(..., description="Cleaned keyword text (no brackets/quotes/plus)")
        match_type: str = Field("PHRASE", description="Match type: BROAD, PHRASE, EXACT")
        skip_create: bool = Field(False, description="If true, only pause (clean version already exists)")

    class BatchFixMatchTypesRequest(BaseModel):
        """Input for batch_fix_match_types tool."""
        keywords: List[MatchTypeFixEntry] = Field(..., description="List of keywords to fix")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")
        action: str = Field("PAUSE", description="Action for existing keywords: PAUSE (default) or ENABLE")

    @mcp_app.tool()
    async def batch_fix_match_types(request: BatchFixMatchTypesRequest) -> dict:
        """
        Batch keyword status changer + optional keyword creator.

        Modes:
        - action='PAUSE' (default): Pause keywords and optionally create replacements with clean text
        - action='ENABLE': Enable paused keywords (set skip_create=true for all entries)

        For PAUSE mode with skip_create=false, also creates new keywords with clean_text.
        Use dry_run=true first to preview changes.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client
        from google.protobuf import field_mask_pb2

        _ensure_client()
        from google_ads_mcp import google_ads_client
        customer_id = _format_customer_id(CUSTOMER_ID)

        action = request.action.upper()
        target_keywords = request.keywords
        to_create = [k for k in request.keywords if not k.skip_create] if action == "PAUSE" else []

        if request.dry_run:
            return {
                "status": "success",
                "dry_run": True,
                "action": action,
                "total_keywords": len(target_keywords),
                f"will_{action.lower()}": len(target_keywords),
                "will_create": len(to_create),
                "sample": [
                    {"ag": k.ad_group_id, "crit": k.criterion_id, "text": k.clean_text, "match": k.match_type}
                    for k in target_keywords[:10]
                ],
            }

        # --- Step 1: Change keyword status (PAUSE or ENABLE) ---
        changed_count = 0
        change_errors = []

        status_enum = (google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
                       if action == "ENABLE"
                       else google_ads_client.enums.AdGroupCriterionStatusEnum.PAUSED)

        ag_crit_service = google_ads_client.get_service("AdGroupCriterionService")
        ops = []
        for kw in target_keywords:
            op = google_ads_client.get_type("AdGroupCriterionOperation")
            criterion = op.update
            criterion.resource_name = ag_crit_service.ad_group_criterion_path(
                customer_id, kw.ad_group_id, kw.criterion_id
            )
            criterion.status = status_enum
            op.update_mask = field_mask_pb2.FieldMask(paths=["status"])
            ops.append(op)

        BATCH_SIZE = 1000
        for i in range(0, len(ops), BATCH_SIZE):
            batch = ops[i:i + BATCH_SIZE]
            try:
                _retry_with_backoff(
                    ag_crit_service.mutate_ad_group_criteria,
                    customer_id=customer_id, operations=batch
                )
                changed_count += len(batch)
                _rate_limiter.record_mutation(len(batch))
            except Exception as e:
                change_errors.append(f"{action} batch {i}-{i+len(batch)}: {str(e)[:200]}")

        # --- Step 2: CREATE new keywords with clean text (only in PAUSE mode) ---
        created_count = 0
        create_errors = []

        if to_create:
            create_ops = []
            for kw in to_create:
                op = google_ads_client.get_type("AdGroupCriterionOperation")
                criterion = op.create
                criterion.ad_group = google_ads_client.get_service("AdGroupService").ad_group_path(
                    customer_id, kw.ad_group_id
                )
                criterion.keyword.text = kw.clean_text
                match_enum = google_ads_client.enums.KeywordMatchTypeEnum
                if kw.match_type == "EXACT":
                    criterion.keyword.match_type = match_enum.EXACT
                elif kw.match_type == "BROAD":
                    criterion.keyword.match_type = match_enum.BROAD
                else:
                    criterion.keyword.match_type = match_enum.PHRASE
                criterion.status = google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED
                create_ops.append(op)

            for i in range(0, len(create_ops), BATCH_SIZE):
                batch = create_ops[i:i + BATCH_SIZE]
                try:
                    _retry_with_backoff(
                        ag_crit_service.mutate_ad_group_criteria,
                        customer_id=customer_id, operations=batch
                    )
                    created_count += len(batch)
                    _rate_limiter.record_mutation(len(batch))
                except Exception as e:
                    create_errors.append(f"Create batch {i}-{i+len(batch)}: {str(e)[:200]}")

        return {
            "status": "success" if not change_errors and not create_errors else "partial",
            "dry_run": False,
            "action": action,
            f"{action.lower()}d": changed_count,
            "created": created_count,
            f"{action.lower()}_errors": change_errors,
            "create_errors": create_errors,
        }

    # -----------------------------------------------------------------------
    # batch_implement_promotion_assets — Create promotion assets for campaigns
    # -----------------------------------------------------------------------
    # Country code → language code mapping
    COUNTRY_LANGUAGE_MAP = {
        "PL": "pl", "HU": "hu", "RO": "ro", "TR": "tr", "TH": "th",
        "GR": "el", "CZ": "cs", "SK": "sk", "BG": "bg", "HR": "hr",
        "SI": "sl", "EE": "et", "LT": "lt", "LV": "lv", "RS": "sr",
        "BA": "hr", "MD": "ro", "UA": "uk", "DE": "de", "AT": "de",
        "CH": "fr", "LU": "fr", "BE": "fr", "FR": "fr", "IT": "it",
        "ES": "es", "PT": "es", "MX": "es", "AR": "es", "CO": "es",
        "EC": "es", "PE": "es", "GT": "es", "PH": "en", "ID": "id",
        "MY": "ms", "BD": "en", "NG": "en", "KE": "en", "UG": "en",
        "EG": "ar", "MA": "fr", "CI": "fr", "AZ": "tr", "UZ": "ru",
        "QA": "ar", "CY": "el", "US": "en", "NZ": "en", "AE": "en",
    }

    # Country code → promotion texts (max 20 chars each) — localized, high-CTR
    COUNTRY_PROMO_TEXTS = {
        "PL": ["Rabat 50%", "Zniżka -50%", "Promocja -50%"],
        "HU": ["50% kedvezmény", "Akció -50%", "Félárú ajánlat"],
        "RO": ["Reducere 50%", "Ofertă -50%", "Promoție -50%"],
        "TR": ["İndirim %50", "%50 Fırsat", "Kampanya %50"],
        "TH": ["ลด 50%", "โปรลด 50%", "ลดครึ่งราคา"],
        "GR": ["Έκπτωση 50%", "Προσφορά -50%", "-50% Τώρα"],
        "CZ": ["Sleva 50%", "Akce -50%", "Výprodej -50%"],
        "SK": ["Zľava 50%", "Akcia -50%", "Výpredaj -50%"],
        "BG": ["Отстъпка 50%", "Намаление -50%", "Промоция -50%"],
        "HR": ["Popust 50%", "Akcija -50%", "Sniženje -50%"],
        "SI": ["Popust 50%", "Akcija -50%", "Razprodaja -50%"],
        "EE": ["Allahindlus 50%", "Sooduspakkumine", "Pakkumine -50%"],
        "LT": ["Nuolaida 50%", "Akcija -50%", "Pasiūlymas -50%"],
        "LV": ["Atlaide 50%", "Akcija -50%", "Piedāvājums -50%"],
        "RS": ["Popust 50%", "Akcija -50%", "Sniženje -50%"],
        "BA": ["Popust 50%", "Akcija -50%", "Sniženje -50%"],
        "MD": ["Reducere 50%", "Ofertă -50%", "Promoție -50%"],
        "UA": ["Знижка 50%", "Акція -50%", "Розпродаж -50%"],
        "DE": ["50% Rabatt", "Angebot -50%", "Aktion -50%"],
        "AT": ["50% Rabatt", "Angebot -50%", "Aktion -50%"],
        "CH": ["Remise 50%", "Offre -50%", "Promo -50%"],
        "LU": ["Remise 50%", "Offre -50%", "Promo -50%"],
        "BE": ["Remise 50%", "Offre -50%", "Promo -50%"],
        "FR": ["Remise 50%", "Offre -50%", "Promo -50%"],
        "IT": ["Sconto 50%", "Offerta -50%", "Promozione -50%"],
        "ES": ["Descuento 50%", "Oferta -50%", "Promo -50%"],
        "PT": ["Desconto 50%", "Oferta -50%", "Promoção -50%"],
        "MX": ["Descuento 50%", "Oferta -50%", "Promo -50%"],
        "AR": ["Descuento 50%", "Oferta -50%", "Promo -50%"],
        "CO": ["Descuento 50%", "Oferta -50%", "Promo -50%"],
        "EC": ["Descuento 50%", "Oferta -50%", "Promo -50%"],
        "PE": ["Descuento 50%", "Oferta -50%", "Promo -50%"],
        "GT": ["Descuento 50%", "Oferta -50%", "Promo -50%"],
        "PH": ["50% Off Sale", "Half Price Deal", "Save 50% Today"],
        "ID": ["Diskon 50%", "Promo -50%", "Hemat 50%"],
        "MY": ["Diskaun 50%", "Tawaran -50%", "Promosi -50%"],
        "BD": ["50% Off Sale", "Half Price Deal", "Save 50% Today"],
        "NG": ["50% Off Sale", "Half Price Deal", "Save 50% Today"],
        "KE": ["50% Off Sale", "Half Price Deal", "Save 50% Today"],
        "UG": ["50% Off Sale", "Half Price Deal", "Save 50% Today"],
        "EG": ["خصم 50%", "عرض -50%", "تخفيض 50%"],
        "MA": ["Remise 50%", "Offre -50%", "Promo -50%"],
        "CI": ["Remise 50%", "Offre -50%", "Promo -50%"],
        "AZ": ["50% endirim", "Aksiya -50%", "Təklif -50%"],
        "UZ": ["50% chegirma", "Aksiya -50%", "Taklif -50%"],
        "QA": ["خصم 50%", "عرض -50%", "تخفيض 50%"],
        "CY": ["Έκπτωση 50%", "Προσφορά -50%", "-50% Τώρα"],
        "US": ["50% Off Sale", "Half Price Deal", "Save 50% Today"],
        "NZ": ["50% Off Sale", "Half Price Deal", "Save 50% Today"],
        "AE": ["50% Off Sale", "Half Price Deal", "Save 50% Today"],
    }

    # Occasions to rotate across campaigns (seasonal mix)
    PROMO_OCCASIONS = [
        "SPRING_SALE",
        "SUMMER_SALE",
        "END_OF_SEASON",
        "WOMENS_DAY",
    ]

    class PromoCampaignEntry(BaseModel):
        """Single campaign to get promotion asset."""
        campaign_id: str = Field(..., description="Campaign ID")
        campaign_name: str = Field("", description="Campaign name (for country detection)")

    class BatchImplementPromotionAssetsRequest(BaseModel):
        """Input for batch_implement_promotion_assets tool."""
        campaigns: List[PromoCampaignEntry] = Field(..., description="List of campaigns")
        percent_off: int = Field(50, description="Percentage discount (default: 50)")
        dry_run: bool = Field(True, description="Preview only (default: true for safety)")
        occasions: List[str] = Field(
            default_factory=lambda: ["SPRING_SALE", "SUMMER_SALE", "END_OF_SEASON", "WOMENS_DAY"],
            description="Occasions to rotate across campaigns"
        )

    @mcp_app.tool()
    async def batch_implement_promotion_assets(request: BatchImplementPromotionAssetsRequest) -> dict:
        """
        Create promotion assets (50% off) for multiple campaigns at once.
        Auto-detects country/language from campaign name, fetches final_urls
        from existing ads, and creates localized promotion assets linked at campaign level.
        Each campaign gets multiple promotion assets with different promotional texts
        and rotating occasions. Use dry_run=true first to preview.
        """
        from google_ads_mcp import _execute_gaql, _safe_get_value, _format_customer_id, _ensure_client, _track_api_call
        from google_ads_mcp import _link_assets_to_entity
        _ensure_client()
        from google_ads_mcp import google_ads_client

        customer_id = CUSTOMER_ID
        results = []
        errors = []
        total_created = 0
        total_linked = 0

        def detect_country(name: str) -> str:
            """Extract 2-letter country code from campaign name."""
            name = name.strip()
            # Pattern: "XX |" or "XX -" at start
            m = re.match(r'^([A-Z]{2})\s*[\|\-]', name)
            if m:
                return m.group(1)
            # Pattern: "BW (Product) - Romania" style
            if "Romania" in name:
                return "RO"
            # Try to find known country patterns
            for code in COUNTRY_LANGUAGE_MAP:
                if f"({code})" in name.upper() or f"| {code} " in name.upper():
                    return code
            return "US"  # default fallback

        def get_final_urls_for_campaign(cid: str) -> list:
            """Get final URLs from existing ads in a campaign."""
            query = f"""
                SELECT ad_group_ad.ad.final_urls
                FROM ad_group_ad
                WHERE campaign.id = {cid}
                  AND ad_group_ad.status != 'REMOVED'
                LIMIT 10
            """
            try:
                rows = _execute_gaql(customer_id, query)
                urls = set()
                for row in rows:
                    ad = row.ad_group_ad.ad
                    for url in ad.final_urls:
                        urls.add(url)
                if urls:
                    return list(urls)[:3]
            except Exception as e:
                _log.warning(f"Failed to get final_urls for campaign {cid}: {e}")
            # Fallback
            return ["https://www.example.com"]

        asset_service = google_ads_client.get_service("AssetService")
        campaign_asset_service = google_ads_client.get_service("CampaignAssetService")

        for idx, camp in enumerate(request.campaigns):
            cname = camp.campaign_name
            cid = camp.campaign_id
            country = detect_country(cname)
            lang = COUNTRY_LANGUAGE_MAP.get(country, "en")
            promo_texts = COUNTRY_PROMO_TEXTS.get(country, ["50% Off Sale", "Half Price Deal", "Save 50% Today"])

            # Get final URLs
            final_urls = get_final_urls_for_campaign(cid)

            # Pick occasion (rotate)
            occasion_list = request.occasions if request.occasions else PROMO_OCCASIONS
            occasion = occasion_list[idx % len(occasion_list)]

            camp_result = {
                "campaign_id": cid,
                "campaign_name": cname[:60],
                "country": country,
                "language": lang,
                "occasion": occasion,
                "promo_texts": promo_texts,
                "final_urls": final_urls,
                "assets_created": 0,
                "errors": [],
            }

            if request.dry_run:
                camp_result["status"] = "dry_run"
                results.append(camp_result)
                continue

            # Create one promotion asset per promo_text
            for promo_text in promo_texts:
                try:
                    # Ensure promotion_target <= 20 chars
                    promo_target = promo_text[:20]

                    asset_operation = google_ads_client.get_type("AssetOperation")
                    asset = asset_operation.create
                    asset.promotion_asset.promotion_target = promo_target
                    asset.promotion_asset.language_code = lang
                    # percent_off in micros: 1000 = 1%, so 50% = 50000
                    asset.promotion_asset.percent_off = request.percent_off * 1000

                    # Set occasion
                    try:
                        asset.promotion_asset.occasion = getattr(
                            google_ads_client.enums.PromotionExtensionOccasionEnum, occasion
                        )
                    except Exception:
                        pass  # skip occasion if enum not found

                    # Set final URLs
                    for url in final_urls:
                        asset.final_urls.append(url)

                    result = _retry_with_backoff(
                        asset_service.mutate_assets,
                        customer_id=customer_id,
                        operations=[asset_operation]
                    )
                    _track_api_call('MUTATE_ASSET')
                    _rate_limiter.record_mutation(1)
                    asset_resource_name = result.results[0].resource_name

                    # Link to campaign
                    link_op = google_ads_client.get_type("CampaignAssetOperation")
                    link = link_op.create
                    link.asset = asset_resource_name
                    link.campaign = campaign_asset_service.campaign_path(customer_id, cid)
                    link.field_type = google_ads_client.enums.AssetFieldTypeEnum.PROMOTION

                    _retry_with_backoff(
                        campaign_asset_service.mutate_campaign_assets,
                        customer_id=customer_id,
                        operations=[link_op]
                    )
                    _track_api_call('MUTATE_LINK')
                    _rate_limiter.record_mutation(1)

                    camp_result["assets_created"] += 1
                    total_created += 1
                    total_linked += 1

                except Exception as e:
                    err_msg = f"Promo '{promo_text}' for campaign {cid}: {str(e)[:200]}"
                    camp_result["errors"].append(err_msg)
                    errors.append(err_msg)

            camp_result["status"] = "success" if not camp_result["errors"] else "partial"
            results.append(camp_result)

            # Small delay between campaigns to avoid rate limits
            if idx > 0 and idx % 5 == 0:
                time.sleep(1)

        return {
            "status": "dry_run" if request.dry_run else ("success" if not errors else "partial"),
            "total_campaigns": len(request.campaigns),
            "total_assets_created": total_created,
            "total_links_created": total_linked,
            "total_errors": len(errors),
            "dry_run": request.dry_run,
            "percent_off": request.percent_off,
            "campaigns": results,
            "errors_summary": errors[:20] if errors else [],
        }

    # ===================================================================
    # SHOPPING & MERCHANT CENTER TOOLS (merged from v3.9.2)
    # ===================================================================

    # -----------------------------------------------------------------------
    # batch_check_obligatory — Cross-reference OBLIGATORY_GLOBAL whitelist
    # against Merchant Center shopping_product to find missing/blocked products
    # -----------------------------------------------------------------------
    class BatchCheckObligatoryRequest(BaseModel):
        """Request model for checking OBLIGATORY products visibility"""
        customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID (from config)")
        country_code: str = Field(..., description="Country code (e.g., 'GR', 'PL', 'RO'). Use 'ALL' to check all countries with OBLIGATORY handles.")
        obligatory_file_path: str = Field(
            "dat/WHITELIST/HANDMADE/OBLIGATORY_GLOBAL.txt",
            description="Path to OBLIGATORY_GLOBAL.txt relative to project root"
        )
        blacklist_dir: str = Field(
            "dat/BLACKLIST/gMerchant",
            description="Path to gMerchant blacklist directory relative to project root"
        )
        include_eligible_details: bool = Field(
            False, description="Include details of eligible products (verbose)"
        )

    @mcp_app.tool()
    async def batch_check_obligatory(request: BatchCheckObligatoryRequest) -> dict:
        """
        Cross-reference OBLIGATORY_GLOBAL whitelist against Merchant Center products.

        Checks which OBLIGATORY products are:
        - ELIGIBLE in Merchant Center (visible in Shopping)
        - NOT_ELIGIBLE (in feed but blocked/disapproved)
        - MISSING (not in feed at all — possibly blacklisted or removed)

        Also reads local blacklist files to identify if missing products were blacklisted.
        Cost: 1 GAQL query per country checked.
        """
        from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client, _track_api_call
        from collections import defaultdict

        result = {
            "status": "success",
            "countries": {},
            "summary": {},
        }

        obligatory_file = request.obligatory_file_path
        _project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        possible_paths = [
            obligatory_file,
            os.path.join(_project_root, obligatory_file),
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), obligatory_file),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', obligatory_file),
        ]

        obligatory_handles = []
        for p in possible_paths:
            if os.path.isfile(p):
                with open(p, 'r') as f:
                    obligatory_handles = [line.strip() for line in f if line.strip()]
                break

        if not obligatory_handles:
            return {"status": "error", "message": f"OBLIGATORY file not found. Tried: {possible_paths}"}

        obligatory_by_cc = defaultdict(set)
        for h in obligatory_handles:
            parts = h.rsplit('-', 1)
            if len(parts) == 2:
                cc = parts[1].upper()
                obligatory_by_cc[cc].add(h)

        target_cc = request.country_code.upper()
        if target_cc == 'ALL':
            countries_to_check = sorted(obligatory_by_cc.keys())
        else:
            countries_to_check = [target_cc]
            if target_cc not in obligatory_by_cc:
                return {"status": "error", "message": f"No OBLIGATORY handles found for {target_cc}"}

        blacklist_dir_path = request.blacklist_dir
        _project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        possible_bl_paths = [
            blacklist_dir_path,
            os.path.join(_project_root, blacklist_dir_path),
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), blacklist_dir_path),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', blacklist_dir_path),
        ]

        bl_dir = None
        for p in possible_bl_paths:
            if os.path.isdir(p):
                bl_dir = p
                break

        blacklisted_variant_count = {}
        if bl_dir:
            for fname in os.listdir(bl_dir):
                if fname.startswith('.') or not fname.endswith('.txt'):
                    continue
                name = fname[:-4]
                if len(name) == 2 and name.isupper():
                    fpath = os.path.join(bl_dir, fname)
                    with open(fpath, 'r', errors='ignore') as f:
                        count = sum(1 for line in f if line.strip() and not line.startswith('='))
                    blacklisted_variant_count[name] = count

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        total_obligatory = 0
        total_found = 0
        total_eligible = 0
        total_not_eligible = 0
        total_missing = 0

        for cc in countries_to_check:
            merchant_id = MERCHANT_IDS.get(cc)
            if not merchant_id:
                result["countries"][cc] = {
                    "status": "skipped",
                    "reason": "No merchant_id configured",
                    "obligatory_count": len(obligatory_by_cc[cc]),
                }
                continue

            try:
                query = (
                    f"SELECT shopping_product.item_id, shopping_product.custom_attribute0, "
                    f"shopping_product.custom_attribute4, shopping_product.status, "
                    f"shopping_product.feed_label "
                    f"FROM shopping_product "
                    f"WHERE shopping_product.merchant_center_id = {merchant_id} "
                    f"AND shopping_product.channel = 'ONLINE' "
                    f"LIMIT 10000"
                )

                ga_service = google_ads_client.get_service("GoogleAdsService")
                search_request = google_ads_client.get_type("SearchGoogleAdsRequest")
                search_request.customer_id = customer_id
                search_request.query = query
                search_request.page_size = 10000

                products_by_handle = {}
                stream = ga_service.search_stream(request=search_request)
                for batch in stream:
                    for row in batch.results:
                        sp = row.shopping_product
                        handle = sp.custom_attribute0 if sp.custom_attribute0 else ""
                        status = sp.status.name if sp.status else "UNKNOWN"
                        item_id = sp.item_id if sp.item_id else ""
                        feed_label = sp.feed_label if sp.feed_label else ""

                        if handle:
                            if handle not in products_by_handle:
                                products_by_handle[handle] = {
                                    "status": status,
                                    "variants": [item_id],
                                    "feed_label": feed_label,
                                    "custom_label4": sp.custom_attribute4 if sp.custom_attribute4 else "",
                                }
                            else:
                                products_by_handle[handle]["variants"].append(item_id)
                                if status == "ELIGIBLE":
                                    products_by_handle[handle]["status"] = "ELIGIBLE"

                _track_api_call('GAQL_SHOPPING_PRODUCT')

                cc_obligatory = obligatory_by_cc[cc]
                found = set()
                eligible = set()
                not_eligible = set()
                missing = set()
                not_eligible_details = []

                for handle in sorted(cc_obligatory):
                    if handle in products_by_handle:
                        found.add(handle)
                        if products_by_handle[handle]["status"] == "ELIGIBLE":
                            eligible.add(handle)
                        else:
                            not_eligible.add(handle)
                            not_eligible_details.append({
                                "handle": handle,
                                "status": products_by_handle[handle]["status"],
                                "variants": len(products_by_handle[handle]["variants"]),
                                "feed_label": products_by_handle[handle]["feed_label"],
                            })
                    else:
                        missing.add(handle)

                cc_result = {
                    "status": "success",
                    "merchant_id": merchant_id,
                    "obligatory_count": len(cc_obligatory),
                    "total_products_in_mc": len(products_by_handle),
                    "found_in_mc": len(found),
                    "eligible": len(eligible),
                    "not_eligible": len(not_eligible),
                    "missing_from_mc": len(missing),
                    "blacklisted_variants": blacklisted_variant_count.get(cc, 0),
                    "missing_handles": sorted(missing),
                    "not_eligible_details": not_eligible_details,
                }

                if request.include_eligible_details:
                    cc_result["eligible_handles"] = sorted(eligible)

                result["countries"][cc] = cc_result

                total_obligatory += len(cc_obligatory)
                total_found += len(found)
                total_eligible += len(eligible)
                total_not_eligible += len(not_eligible)
                total_missing += len(missing)

            except Exception as e:
                result["countries"][cc] = {
                    "status": "error",
                    "message": str(e),
                    "obligatory_count": len(cc_obligatory),
                }

        result["summary"] = {
            "countries_checked": len(countries_to_check),
            "total_obligatory_handles": total_obligatory,
            "total_found_in_mc": total_found,
            "total_eligible": total_eligible,
            "total_not_eligible": total_not_eligible,
            "total_missing": total_missing,
            "visibility_rate": f"{(total_eligible/total_obligatory*100):.1f}%" if total_obligatory > 0 else "N/A",
        }

        return result

    # -----------------------------------------------------------------------
    # batch_shopping_listing_groups — List product groups in Shopping campaigns
    # -----------------------------------------------------------------------
    class BatchShoppingListingGroupsRequest(BaseModel):
        """Request model for listing Shopping campaign product groups"""
        customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID (from config)")
        country_code: str = Field(..., description="Country code (e.g., 'GR', 'PL')")
        campaign_id: Optional[str] = Field(None, description="Specific campaign ID. If not set, finds all Shopping campaigns for the country.")

    @mcp_app.tool()
    async def batch_shopping_listing_groups(request: BatchShoppingListingGroupsRequest) -> dict:
        """
        List all product groups (listing groups) in Shopping campaigns for a country.

        Shows the full product group tree: root subdivisions, custom_label subdivisions,
        individual product bids, and performance metrics via product_group_view.
        Useful for understanding campaign structure before optimizing bids or groups.
        Cost: 2-3 GAQL queries.
        """
        from google_ads_mcp import _ensure_client, _format_customer_id, google_ads_client, _track_api_call

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)
        cc = request.country_code.upper()
        merchant_id = MERCHANT_IDS.get(cc)

        if not merchant_id:
            return {"status": "error", "message": f"No merchant_id for {cc}"}

        result = {
            "status": "success",
            "country": cc,
            "campaigns": [],
        }

        try:
            if request.campaign_id:
                campaign_filter = f"AND campaign.id = {request.campaign_id}"
            else:
                campaign_filter = f"AND campaign.shopping_setting.merchant_id = {merchant_id}"

            campaign_query = (
                f"SELECT campaign.id, campaign.name, campaign.status, "
                f"campaign.shopping_setting.merchant_id, campaign.shopping_setting.feed_label "
                f"FROM campaign "
                f"WHERE campaign.advertising_channel_type = 'SHOPPING' "
                f"AND campaign.status != 'REMOVED' "
                f"{campaign_filter} "
                f"LIMIT 20"
            )

            ga_service = google_ads_client.get_service("GoogleAdsService")
            search_request = google_ads_client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = campaign_query
            search_request.page_size = 20

            campaigns = []
            stream = ga_service.search_stream(request=search_request)
            for batch in stream:
                for row in batch.results:
                    c = row.campaign
                    campaigns.append({
                        "id": str(c.id),
                        "name": c.name,
                        "status": c.status.name,
                        "merchant_id": str(c.shopping_setting.merchant_id),
                        "feed_label": c.shopping_setting.feed_label if c.shopping_setting.feed_label else "",
                    })
            _track_api_call('GAQL_CAMPAIGNS')

            for camp in campaigns:
                pg_query = (
                    f"SELECT ad_group.id, ad_group.name, ad_group.status, "
                    f"ad_group_criterion.criterion_id, "
                    f"ad_group_criterion.listing_group.type, "
                    f"ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
                    f"ad_group_criterion.listing_group.case_value.product_custom_attribute.value, "
                    f"ad_group_criterion.listing_group.parent_ad_group_criterion, "
                    f"ad_group_criterion.cpc_bid_micros, "
                    f"metrics.impressions, metrics.clicks, metrics.cost_micros, "
                    f"metrics.conversions, metrics.conversions_value "
                    f"FROM product_group_view "
                    f"WHERE campaign.id = {camp['id']} "
                    f"AND segments.date DURING LAST_30_DAYS "
                    f"ORDER BY metrics.impressions DESC "
                    f"LIMIT 500"
                )

                search_request2 = google_ads_client.get_type("SearchGoogleAdsRequest")
                search_request2.customer_id = customer_id
                search_request2.query = pg_query
                search_request2.page_size = 500

                product_groups = []
                try:
                    stream2 = ga_service.search_stream(request=search_request2)
                    for batch in stream2:
                        for row in batch.results:
                            ag = row.ad_group
                            agc = row.ad_group_criterion
                            m = row.metrics
                            lg = agc.listing_group

                            pg = {
                                "ad_group_id": str(ag.id),
                                "ad_group_name": ag.name,
                                "ad_group_status": ag.status.name,
                                "criterion_id": str(agc.criterion_id),
                                "type": lg.type_.name,
                                "cpc_bid": agc.cpc_bid_micros / 1_000_000 if agc.cpc_bid_micros else 0,
                                "impressions": m.impressions,
                                "clicks": m.clicks,
                                "cost": m.cost_micros / 1_000_000 if m.cost_micros else 0,
                                "conversions": round(m.conversions, 2),
                                "conv_value": round(m.conversions_value, 2),
                            }

                            if lg.case_value and lg.case_value.product_custom_attribute:
                                pca = lg.case_value.product_custom_attribute
                                pg["dimension"] = f"custom_label_{pca.index.name.replace('INDEX', '').lower()}" if pca.index else ""
                                pg["value"] = pca.value if pca.value else ""

                            if lg.parent_ad_group_criterion:
                                pg["parent_criterion"] = lg.parent_ad_group_criterion.split("~")[-1] if "~" in lg.parent_ad_group_criterion else ""

                            product_groups.append(pg)

                    _track_api_call('GAQL_PRODUCT_GROUP_VIEW')
                except Exception as e:
                    product_groups = [{"error": str(e)}]

                camp["product_groups"] = product_groups
                camp["product_groups_count"] = len(product_groups)

            result["campaigns"] = campaigns

        except Exception as e:
            return {"status": "error", "message": str(e)}

        return result

    # -----------------------------------------------------------------------
    # batch_merchant_gc — Merchant Center Garbage Collection
    # -----------------------------------------------------------------------
    APPEAL_DESCRIPTIONS = {
        "Alcoholic beverages",
        "Sexual interests in personalized advertising",
        "Restricted adult content",
        "Adult-oriented content",
        "Dangerous products (Tobacco products and related equipment)",
        "Tobacco products & related equipment",
        "Tobacco products and related equipment",
        "Guns and Parts",
        "Guns and parts",
        "Personalized advertising: legal restrictions",
    }

    SKIP_DESCRIPTIONS = {
        "Image not retrieved (crawl pending)",
        "Image not processed",
        "Unable to show image",
        "Missing product image",
    }

    class BatchMerchantGCRequest(BaseModel):
        """Request model for Merchant Center Garbage Collection"""
        customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID (from config)")
        merchant_id: Optional[str] = Field(
            None,
            description="Specific sub-account Merchant ID. If not set, scans ALL sub-accounts under both MCAs."
        )
        country_code: Optional[str] = Field(
            None,
            description="Filter to specific country code (e.g., 'RO', 'PL'). Matches against product feed_label or target_country."
        )
        dry_run: bool = Field(
            True,
            description="Preview blacklist/appeal actions without writing files (default True for safety)"
        )
        max_products_per_account: int = Field(
            1000,
            description="Max products to scan per sub-account (pagination limit)"
        )
        include_product_details: bool = Field(
            False,
            description="Include full issue details per product in output (verbose)"
        )
        refresh_feeds_after_blacklist: bool = Field(
            True,
            description="Trigger feed refresh via MC API after writing blacklist files"
        )

    @mcp_app.tool()
    async def batch_merchant_gc(request: BatchMerchantGCRequest) -> dict:
        """
        Merchant Center Garbage Collection — scan sub-accounts for DISAPPROVED products.

        Two actions based on disapproval reason:
        1. BLACKLIST: Products with policy violations that cannot be appealed (Healthcare/Prescription drugs,
           Dangerous products, etc.) → adds offerId to dat/BLACKLIST/gMerchant/{domain}.txt and triggers feed refresh.
        2. APPEAL: Products with appealable violations (Alcoholic beverages, Tobacco, Adult content,
           Personalized advertising restrictions, Guns) → generates MC UI links for manual review requests.

        Mirrors the logic of the PHP gMerchantGC worker but runs via MCP.
        Cost: 1-2 API calls per sub-account.
        """
        import google_ads_mcp as _gam
        _gam._ensure_merchant_client()
        merchant_center_client = _gam.merchant_center_client

        _mca = _config.get('merchant_center', {}).get('mca_accounts', {})
        MCA_HLP = _mca.get('HLP', '')
        MCA_HLE = _mca.get('HLE', '')

        result = {
            "status": "success",
            "dry_run": request.dry_run,
            "scan_timestamp": __import__('datetime').datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
            "accounts_scanned": 0,
            "products_checked": 0,
            "to_blacklist": [],
            "to_appeal": [],
            "already_blacklisted": 0,
            "skipped": 0,
            "errors": [],
            "blacklist_files_updated": [],
            "feeds_refreshed": [],
            "summary": {},
        }

        # Navigate up from skills/google-mcp-universal/ to project root (4 levels)
        _project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        blacklist_dir = os.path.join(_project_root, "dat", "BLACKLIST", "gMerchant")

        sub_accounts = []

        if request.merchant_id:
            sub_accounts.append({
                "merchant_id": request.merchant_id,
                "name": f"Direct: {request.merchant_id}",
            })
        else:
            for mca_id in [MCA_HLP, MCA_HLE]:
                try:
                    resp = merchant_center_client.accounts().list(merchantId=mca_id, maxResults=250).execute()
                    for acc in resp.get("resources", []):
                        acc_id = str(acc.get("id", ""))
                        acc_name = acc.get("name", "")
                        sub_accounts.append({
                            "merchant_id": acc_id,
                            "name": acc_name,
                        })
                except Exception as e:
                    result["errors"].append({"mca_id": mca_id, "error": f"Failed to list sub-accounts: {str(e)}"})

        existing_blacklists = {}

        for sa in sub_accounts:
            mid = sa["merchant_id"]

            try:
                all_statuses = []
                page_token = None
                fetched = 0

                while fetched < request.max_products_per_account:
                    batch_size = min(250, request.max_products_per_account - fetched)
                    params = {"merchantId": mid, "maxResults": batch_size}
                    if page_token:
                        params["pageToken"] = page_token

                    resp = merchant_center_client.productstatuses().list(**params).execute()

                    statuses = resp.get("resources", [])
                    all_statuses.extend(statuses)
                    fetched += len(statuses)

                    page_token = resp.get("nextPageToken")
                    if not page_token or len(statuses) == 0:
                        break

                result["accounts_scanned"] += 1
                result["products_checked"] += len(all_statuses)

                domain_for_bl = None
                for ps in all_statuses:
                    link = ps.get("link", "")
                    if link:
                        from urllib.parse import urlparse
                        parsed = urlparse(link)
                        domain_for_bl = parsed.netloc
                        break

                if not domain_for_bl:
                    domain_for_bl = f"merchant_{mid}"

                bl_file = os.path.join(blacklist_dir, f"{domain_for_bl}.txt")
                if domain_for_bl not in existing_blacklists:
                    existing_blacklists[domain_for_bl] = set()
                    if os.path.exists(bl_file):
                        with open(bl_file, "r") as f:
                            for line in f:
                                line = line.strip()
                                if line and not line.startswith("="):
                                    existing_blacklists[domain_for_bl].add(line)

                for ps in all_statuses:
                    offer_id = ps.get("productId", "").split(":")[-1] if ":" in ps.get("productId", "") else ps.get("productId", "")
                    title = ps.get("title", "")
                    product_id_full = ps.get("productId", "")

                    if request.country_code:
                        dest_statuses = ps.get("destinationStatuses", [])
                        country_match = False
                        for ds in dest_statuses:
                            countries = ds.get("disapprovedCountries", []) + ds.get("approvedCountries", []) + ds.get("pendingCountries", [])
                            if request.country_code.upper() in countries:
                                country_match = True
                                break
                        if not country_match and request.country_code.upper() in product_id_full.upper():
                            country_match = True
                        if not country_match:
                            continue

                    issues = ps.get("itemLevelIssues", [])
                    blacklist_reasons = []
                    appeal_reasons = []

                    for issue in issues:
                        reporting_ctx = issue.get("destination", "") or issue.get("reportingContext", "")
                        severity = issue.get("servability", "") or issue.get("severity", "")
                        description = issue.get("description", "")

                        is_disapproved = "disapproved" in severity.lower() or severity == "DISAPPROVED"
                        is_shopping = "Shopping" in reporting_ctx or "SHOPPING" in reporting_ctx

                        if not (is_disapproved and is_shopping):
                            continue

                        if description in SKIP_DESCRIPTIONS:
                            result["skipped"] += 1
                            continue

                        if description in APPEAL_DESCRIPTIONS:
                            appeal_reasons.append(description)
                        else:
                            blacklist_reasons.append(description)

                    if blacklist_reasons:
                        already = offer_id in existing_blacklists.get(domain_for_bl, set())
                        if already:
                            result["already_blacklisted"] += 1
                        else:
                            entry = {
                                "merchant_id": mid,
                                "offer_id": offer_id,
                                "title": title,
                                "domain": domain_for_bl,
                                "reasons": blacklist_reasons,
                                "action": "BLACKLIST",
                            }
                            if request.include_product_details:
                                entry["all_issues"] = issues
                            result["to_blacklist"].append(entry)

                    if appeal_reasons:
                        mc_link = f"https://merchants.google.com/mc/items/details?a={mid}&offerId={offer_id}"
                        entry = {
                            "merchant_id": mid,
                            "offer_id": offer_id,
                            "title": title,
                            "domain": domain_for_bl,
                            "reasons": appeal_reasons,
                            "action": "APPEAL",
                            "mc_review_link": mc_link,
                        }
                        if request.include_product_details:
                            entry["all_issues"] = issues
                        result["to_appeal"].append(entry)

            except Exception as e:
                result["errors"].append({"merchant_id": mid, "error": str(e)})

        if not request.dry_run and result["to_blacklist"]:
            by_domain = {}
            for entry in result["to_blacklist"]:
                domain = entry["domain"]
                if domain not in by_domain:
                    by_domain[domain] = []
                by_domain[domain].append(entry["offer_id"])

            header = f"============== UPDATE: {result['scan_timestamp']} gMerchantGC_MCP =============="

            for domain, offer_ids in by_domain.items():
                bl_file = os.path.join(blacklist_dir, f"{domain}.txt")
                try:
                    os.makedirs(os.path.dirname(bl_file), exist_ok=True)
                    with open(bl_file, "a") as f:
                        f.write(header + "\n")
                        for oid in offer_ids:
                            f.write(oid + "\n")
                    result["blacklist_files_updated"].append({
                        "file": bl_file,
                        "new_entries": len(offer_ids),
                    })
                except Exception as e:
                    result["errors"].append({"domain": domain, "error": f"Failed to write blacklist: {str(e)}"})

            if request.refresh_feeds_after_blacklist:
                affected_merchants = set(e["merchant_id"] for e in result["to_blacklist"])
                for mid in affected_merchants:
                    try:
                        feeds_resp = merchant_center_client.datafeeds().list(merchantId=mid).execute()
                        for feed in feeds_resp.get("resources", []):
                            feed_id = str(feed.get("id", ""))
                            if feed_id:
                                try:
                                    merchant_center_client.datafeeds().fetchnow(merchantId=mid, datafeedId=feed_id).execute()
                                    result["feeds_refreshed"].append({"merchant_id": mid, "feed_id": feed_id})
                                except Exception as fe:
                                    result["errors"].append({"merchant_id": mid, "feed_id": feed_id, "error": f"Feed refresh failed: {str(fe)}"})
                    except Exception as e:
                        result["errors"].append({"merchant_id": mid, "error": f"Failed to list feeds: {str(e)}"})

        result["summary"] = {
            "accounts_scanned": result["accounts_scanned"],
            "products_checked": result["products_checked"],
            "to_blacklist": len(result["to_blacklist"]),
            "to_appeal": len(result["to_appeal"]),
            "already_blacklisted": result["already_blacklisted"],
            "skipped": result["skipped"],
            "blacklist_files_updated": len(result["blacklist_files_updated"]),
            "feeds_refreshed": len(result["feeds_refreshed"]),
            "errors": len(result["errors"]),
        }

        if len(result["to_blacklist"]) > 100:
            result["to_blacklist"] = result["to_blacklist"][:100]
            result["to_blacklist_truncated"] = True
        if len(result["to_appeal"]) > 100:
            result["to_appeal"] = result["to_appeal"][:100]
            result["to_appeal_truncated"] = True

        return result

    # -----------------------------------------------------------------------
    # batch_shopping_bid_optimizer — Auto-optimize CPC bids per product group
    # -----------------------------------------------------------------------
    class BatchShoppingBidOptimizerRequest(BaseModel):
        """Request model for Shopping bid optimization"""
        customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID (from config)")
        country_code: str = Field(..., description="Country code (e.g., 'GR', 'PL')")
        campaign_id: str = Field(..., description="Shopping campaign ID")
        target_roas: float = Field(1.6, description="Target ROAS (1.6 = 160%). Products above get bid increase, below get decrease.")
        min_impressions: int = Field(100, description="Min impressions to be considered for optimization (skip low-data nodes)")
        days: int = Field(30, description="Lookback period in days (7, 14, 30, 90)")
        max_bid_micros: int = Field(5000000, description="Maximum CPC bid in micros (default 5.00)")
        min_bid_micros: int = Field(10000, description="Minimum CPC bid in micros (default 0.01)")
        bid_increase_pct: float = Field(0.15, description="Bid increase percentage for good performers (0.15 = +15%)")
        bid_decrease_pct: float = Field(0.20, description="Bid decrease percentage for poor performers (0.20 = -20%)")
        dry_run: bool = Field(True, description="Preview changes without applying (default True for safety)")

    @mcp_app.tool()
    async def batch_shopping_bid_optimizer(request: BatchShoppingBidOptimizerRequest) -> dict:
        """
        Auto-optimize CPC bids on Shopping campaign product groups based on ROAS.

        Queries product_group_view for UNIT nodes with performance metrics,
        calculates ROAS per node, proposes bid increases (good ROAS) or decreases (poor ROAS).
        Applies changes via google_ads_update_shopping_listing_group_bids if not dry_run.
        Cost: 1 GAQL query + N mutate calls.
        """
        from google_ads_mcp import (
            _ensure_client, _format_customer_id, google_ads_client, _track_api_call,
            google_ads_update_shopping_listing_group_bids, ShoppingListingGroupBidRequest
        )

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        date_ranges = {7: "LAST_7_DAYS", 14: "LAST_14_DAYS", 30: "LAST_30_DAYS", 90: "LAST_90_DAYS"}
        date_range = date_ranges.get(request.days, "LAST_30_DAYS")

        query = (
            f"SELECT ad_group.id, ad_group.name, "
            f"ad_group_criterion.criterion_id, "
            f"ad_group_criterion.listing_group.type, "
            f"ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
            f"ad_group_criterion.listing_group.case_value.product_custom_attribute.value, "
            f"ad_group_criterion.cpc_bid_micros, "
            f"metrics.impressions, metrics.clicks, metrics.cost_micros, "
            f"metrics.conversions, metrics.conversions_value "
            f"FROM product_group_view "
            f"WHERE campaign.id = {request.campaign_id} "
            f"AND ad_group_criterion.listing_group.type = 'UNIT' "
            f"AND segments.date DURING {date_range} "
            f"ORDER BY metrics.impressions DESC "
            f"LIMIT 1000"
        )

        ga_service = google_ads_client.get_service("GoogleAdsService")
        search_request = google_ads_client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query
        search_request.page_size = 1000

        nodes = []
        try:
            stream = ga_service.search_stream(request=search_request)
            for batch in stream:
                for row in batch.results:
                    agc = row.ad_group_criterion
                    m = row.metrics
                    lg = agc.listing_group

                    value = ""
                    dimension = ""
                    if lg.case_value and lg.case_value.product_custom_attribute:
                        pca = lg.case_value.product_custom_attribute
                        dimension = f"custom_label_{pca.index.name.replace('INDEX', '').lower()}" if pca.index else ""
                        value = pca.value if pca.value else "(everything else)"

                    nodes.append({
                        "ad_group_id": str(row.ad_group.id),
                        "ad_group_name": row.ad_group.name,
                        "criterion_id": str(agc.criterion_id),
                        "dimension": dimension,
                        "value": value,
                        "current_bid_micros": agc.cpc_bid_micros,
                        "current_bid": agc.cpc_bid_micros / 1_000_000 if agc.cpc_bid_micros else 0,
                        "impressions": m.impressions,
                        "clicks": m.clicks,
                        "cost_micros": m.cost_micros,
                        "cost": m.cost_micros / 1_000_000 if m.cost_micros else 0,
                        "conversions": round(m.conversions, 2),
                        "conv_value": round(m.conversions_value, 2),
                    })
            _track_api_call('GAQL_PRODUCT_GROUP_VIEW')
        except Exception as e:
            return {"status": "error", "message": f"GAQL error: {str(e)}"}

        increase_proposals = []
        decrease_proposals = []
        skip_low_data = []

        for node in nodes:
            if node["impressions"] < request.min_impressions:
                skip_low_data.append(node)
                continue

            cost = node["cost"]
            conv_value = node["conv_value"]
            roas = conv_value / cost if cost > 0 else 0
            node["roas"] = round(roas, 2)

            current_bid = node["current_bid_micros"]
            if not current_bid or current_bid == 0:
                continue

            if roas >= request.target_roas:
                new_bid = int(current_bid * (1 + request.bid_increase_pct))
                new_bid = min(new_bid, request.max_bid_micros)
                if new_bid != current_bid:
                    node["new_bid_micros"] = new_bid
                    node["new_bid"] = new_bid / 1_000_000
                    node["action"] = "INCREASE"
                    increase_proposals.append(node)
            elif roas < request.target_roas and cost > 0:
                new_bid = int(current_bid * (1 - request.bid_decrease_pct))
                new_bid = max(new_bid, request.min_bid_micros)
                if new_bid != current_bid:
                    node["new_bid_micros"] = new_bid
                    node["new_bid"] = new_bid / 1_000_000
                    node["action"] = "DECREASE"
                    decrease_proposals.append(node)

        all_proposals = increase_proposals + decrease_proposals
        applied = 0
        errors = []

        if not request.dry_run and all_proposals:
            by_ag = {}
            for p in all_proposals:
                ag_id = p["ad_group_id"]
                if ag_id not in by_ag:
                    by_ag[ag_id] = []
                by_ag[ag_id].append({
                    "criterion_id": p["criterion_id"],
                    "cpc_bid_micros": p["new_bid_micros"],
                })

            for ag_id, bid_updates in by_ag.items():
                try:
                    update_req = ShoppingListingGroupBidRequest(
                        customer_id=CUSTOMER_ID,
                        ad_group_id=ag_id,
                        bid_updates=bid_updates,
                    )
                    await google_ads_update_shopping_listing_group_bids(update_req)
                    applied += len(bid_updates)
                except Exception as e:
                    errors.append({"ad_group_id": ag_id, "error": str(e)})

        return {
            "status": "success",
            "dry_run": request.dry_run,
            "campaign_id": request.campaign_id,
            "date_range": date_range,
            "target_roas": request.target_roas,
            "total_unit_nodes": len(nodes),
            "skipped_low_data": len(skip_low_data),
            "increase_proposals": len(increase_proposals),
            "decrease_proposals": len(decrease_proposals),
            "applied": applied,
            "errors": errors,
            "proposals": [
                {
                    "ad_group": p["ad_group_name"],
                    "value": p.get("value", ""),
                    "roas": p.get("roas", 0),
                    "current_bid": p["current_bid"],
                    "new_bid": p.get("new_bid", 0),
                    "action": p.get("action", ""),
                    "impressions": p["impressions"],
                    "clicks": p["clicks"],
                    "cost": p["cost"],
                    "conv_value": p["conv_value"],
                }
                for p in all_proposals[:50]
            ],
        }

    # -----------------------------------------------------------------------
    # batch_shopping_exclude_products — Exclude products from Shopping campaign
    # -----------------------------------------------------------------------
    class BatchShoppingExcludeProductsRequest(BaseModel):
        """Request model for excluding products from Shopping campaigns"""
        customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID (from config)")
        country_code: str = Field(..., description="Country code (e.g., 'GR', 'PL')")
        campaign_id: str = Field(..., description="Shopping campaign ID")
        ad_group_id: str = Field(..., description="Ad group ID containing the listing group tree")
        exclude_handles: List[str] = Field(..., description="Product handles to exclude (custom_label_0 values)")
        dimension: str = Field("custom_label_0", description="Dimension to partition by (usually custom_label_0 for handles)")
        active_bid_micros: int = Field(100000, description="CPC bid for active (non-excluded) products in micros (default 0.10)")
        dry_run: bool = Field(True, description="Preview changes without applying (default True for safety)")

    @mcp_app.tool()
    async def batch_shopping_exclude_products(request: BatchShoppingExcludeProductsRequest) -> dict:
        """
        Exclude specific products from a Shopping campaign by setting their bids to minimum.

        Rebuilds the listing group tree: excluded handles get bid=1 micro (won't win auctions),
        active products keep the specified bid. Uses atomic tree rebuild to avoid broken states.
        Cost: 1 GAQL query + 1 atomic mutate.
        """
        from google_ads_mcp import (
            _ensure_client, _format_customer_id, google_ads_client, _track_api_call,
            google_ads_rebuild_shopping_listing_group_tree, RebuildShoppingListingGroupTreeRequest
        )

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)

        query = (
            f"SELECT ad_group_criterion.criterion_id, "
            f"ad_group_criterion.listing_group.type, "
            f"ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
            f"ad_group_criterion.listing_group.case_value.product_custom_attribute.value, "
            f"ad_group_criterion.cpc_bid_micros "
            f"FROM ad_group_criterion "
            f"WHERE campaign.id = {request.campaign_id} "
            f"AND ad_group.id = {request.ad_group_id} "
            f"AND ad_group_criterion.listing_group.type = 'UNIT' "
            f"LIMIT 1000"
        )

        ga_service = google_ads_client.get_service("GoogleAdsService")
        search_req = google_ads_client.get_type("SearchGoogleAdsRequest")
        search_req.customer_id = customer_id
        search_req.query = query
        search_req.page_size = 1000

        existing_handles = set()
        try:
            stream = ga_service.search_stream(request=search_req)
            for batch in stream:
                for row in batch.results:
                    agc = row.ad_group_criterion
                    lg = agc.listing_group
                    if lg.case_value and lg.case_value.product_custom_attribute:
                        pca = lg.case_value.product_custom_attribute
                        if pca.value:
                            existing_handles.add(pca.value)
            _track_api_call('GAQL_LISTING_GROUPS')
        except Exception as e:
            return {"status": "error", "message": f"GAQL error: {str(e)}"}

        exclude_set = set(h.lower() for h in request.exclude_handles)

        groups = []
        excluded_count = 0
        active_count = 0

        for handle in sorted(existing_handles):
            if handle.lower() in exclude_set:
                groups.append({"value": handle, "cpc_bid_micros": 1})
                excluded_count += 1
            else:
                groups.append({"value": handle, "cpc_bid_micros": request.active_bid_micros})
                active_count += 1

        for handle in request.exclude_handles:
            if handle.lower() not in {h.lower() for h in existing_handles}:
                groups.append({"value": handle, "cpc_bid_micros": 1})
                excluded_count += 1

        not_found = [h for h in request.exclude_handles if h.lower() not in {eh.lower() for eh in existing_handles}]

        result = {
            "status": "success",
            "dry_run": request.dry_run,
            "campaign_id": request.campaign_id,
            "ad_group_id": request.ad_group_id,
            "existing_handles": len(existing_handles),
            "excluded_count": excluded_count,
            "active_count": active_count,
            "not_in_tree": not_found,
            "total_groups_in_rebuild": len(groups),
        }

        if not request.dry_run:
            try:
                rebuild_req = RebuildShoppingListingGroupTreeRequest(
                    customer_id=CUSTOMER_ID,
                    ad_group_id=request.ad_group_id,
                    root_dimension=request.dimension,
                    groups=groups,
                    everything_else_bid_micros=request.active_bid_micros,
                )
                rebuild_result = await google_ads_rebuild_shopping_listing_group_tree(rebuild_req)
                result["rebuild_result"] = rebuild_result
            except Exception as e:
                result["status"] = "error"
                result["error"] = str(e)

        return result

    # -----------------------------------------------------------------------
    # batch_shopping_clone_campaign — Clone Shopping campaign to another country
    # -----------------------------------------------------------------------
    class BatchShoppingCloneCampaignRequest(BaseModel):
        """Request model for cloning Shopping campaign cross-country"""
        customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID (from config)")
        source_campaign_id: str = Field(..., description="Source Shopping campaign ID to clone from")
        target_country_code: str = Field(..., description="Target country code (e.g., 'IT', 'FR')")
        target_campaign_name: Optional[str] = Field(None, description="Target campaign name. Auto-generated if not set.")
        daily_budget_micros: int = Field(5000000, description="Daily budget in micros (default 5.00 GBP)")
        bid_multiplier: float = Field(1.0, description="Multiply all bids by this factor (e.g., 0.8 = 80% of source bids)")
        feed_label: Optional[str] = Field(None, description="Feed label for target (e.g., 'ITFEEDNEW'). Auto-generated as {CC}FEEDNEW if not set.")
        dry_run: bool = Field(True, description="Preview clone plan without creating (default True for safety)")

    @mcp_app.tool()
    async def batch_shopping_clone_campaign(request: BatchShoppingCloneCampaignRequest) -> dict:
        """
        Clone a Shopping campaign structure to another country.

        Reads source campaign: ad groups, listing group trees, bids.
        Creates new campaign + budget + ad groups + Shopping ads + listing group trees
        for the target country with adjusted merchant_id and optional bid multiplier.
        Creates campaign as PAUSED. Cost: 3-5 GAQL queries + N mutate calls.
        """
        from google_ads_mcp import (
            _ensure_client, _format_customer_id, google_ads_client, _track_api_call,
            google_ads_create_campaign_budget, CreateCampaignBudgetRequest,
            google_ads_create_shopping_campaign, CreateShoppingCampaignRequest,
            google_ads_create_shopping_ad_group, CreateShoppingAdGroupRequest,
            google_ads_create_shopping_ad, CreateShoppingAdRequest,
            google_ads_rebuild_shopping_listing_group_tree, RebuildShoppingListingGroupTreeRequest,
        )

        _ensure_client()
        customer_id = _format_customer_id(CUSTOMER_ID)
        cc = request.target_country_code.upper()

        target_merchant = MERCHANT_IDS.get(cc)
        if not target_merchant:
            return {"status": "error", "message": f"No merchant_id for target country {cc}"}

        camp_query = (
            f"SELECT campaign.id, campaign.name, campaign.status, "
            f"campaign.shopping_setting.merchant_id, campaign.shopping_setting.feed_label, "
            f"campaign.shopping_setting.sales_country "
            f"FROM campaign WHERE campaign.id = {request.source_campaign_id}"
        )

        ga_service = google_ads_client.get_service("GoogleAdsService")
        search_req = google_ads_client.get_type("SearchGoogleAdsRequest")
        search_req.customer_id = customer_id
        search_req.query = camp_query
        search_req.page_size = 1

        source_camp = None
        stream = ga_service.search_stream(request=search_req)
        for batch in stream:
            for row in batch.results:
                c = row.campaign
                source_camp = {
                    "id": str(c.id),
                    "name": c.name,
                    "merchant_id": str(c.shopping_setting.merchant_id),
                    "feed_label": c.shopping_setting.feed_label or "",
                    "sales_country": c.shopping_setting.sales_country or "",
                }
        _track_api_call('GAQL_CAMPAIGN_DETAIL')

        if not source_camp:
            return {"status": "error", "message": f"Source campaign {request.source_campaign_id} not found"}

        ag_query = (
            f"SELECT ad_group.id, ad_group.name, ad_group.status, ad_group.cpc_bid_micros "
            f"FROM ad_group "
            f"WHERE campaign.id = {request.source_campaign_id} "
            f"AND ad_group.status != 'REMOVED' "
            f"LIMIT 50"
        )
        search_req2 = google_ads_client.get_type("SearchGoogleAdsRequest")
        search_req2.customer_id = customer_id
        search_req2.query = ag_query
        search_req2.page_size = 50

        source_ad_groups = []
        stream2 = ga_service.search_stream(request=search_req2)
        for batch in stream2:
            for row in batch.results:
                ag = row.ad_group
                source_ad_groups.append({
                    "id": str(ag.id),
                    "name": ag.name,
                    "status": ag.status.name,
                    "cpc_bid_micros": ag.cpc_bid_micros,
                })
        _track_api_call('GAQL_AD_GROUPS')

        for ag in source_ad_groups:
            lg_query = (
                f"SELECT ad_group_criterion.criterion_id, "
                f"ad_group_criterion.listing_group.type, "
                f"ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
                f"ad_group_criterion.listing_group.case_value.product_custom_attribute.value, "
                f"ad_group_criterion.listing_group.parent_ad_group_criterion, "
                f"ad_group_criterion.cpc_bid_micros "
                f"FROM ad_group_criterion "
                f"WHERE campaign.id = {request.source_campaign_id} "
                f"AND ad_group.id = {ag['id']} "
                f"AND ad_group_criterion.listing_group.type != 'UNKNOWN' "
                f"LIMIT 500"
            )
            search_req3 = google_ads_client.get_type("SearchGoogleAdsRequest")
            search_req3.customer_id = customer_id
            search_req3.query = lg_query
            search_req3.page_size = 500

            listing_groups = []
            try:
                stream3 = ga_service.search_stream(request=search_req3)
                for batch in stream3:
                    for row in batch.results:
                        agc = row.ad_group_criterion
                        lg = agc.listing_group
                        node = {
                            "criterion_id": str(agc.criterion_id),
                            "type": lg.type_.name,
                            "cpc_bid_micros": agc.cpc_bid_micros,
                            "parent": lg.parent_ad_group_criterion if lg.parent_ad_group_criterion else None,
                        }
                        if lg.case_value and lg.case_value.product_custom_attribute:
                            pca = lg.case_value.product_custom_attribute
                            node["dimension_index"] = pca.index.name if pca.index else ""
                            node["value"] = pca.value if pca.value else ""
                        listing_groups.append(node)
                _track_api_call('GAQL_LISTING_GROUPS')
            except Exception as e:
                listing_groups = [{"error": str(e)}]

            ag["listing_groups"] = listing_groups

        target_feed_label = request.feed_label or f"{cc}FEEDNEW"
        country_names_en = {
            "PL": "Poland", "DE": "Germany", "ES": "Spain", "IT": "Italy",
            "CZ": "Czech Republic", "SK": "Slovakia", "BG": "Bulgaria", "AT": "Austria",
            "HU": "Hungary", "FR": "France", "GR": "Greece", "RO": "Romania",
            "TR": "Turkey", "HR": "Croatia", "SI": "Slovenia", "LT": "Lithuania",
            "EE": "Estonia", "LV": "Latvia", "UA": "Ukraine", "NL": "Netherlands",
            "BE": "Belgium", "SE": "Sweden", "UK": "United Kingdom", "US": "United States",
        }
        country_name = country_names_en.get(cc, cc)
        target_name = request.target_campaign_name or f"{cc} | {country_name} | Shopping | NEW_PRODUCTS | CLONE | CLAUDE_MCP"

        clone_plan = {
            "status": "success",
            "dry_run": request.dry_run,
            "source": source_camp,
            "target": {
                "country": cc,
                "campaign_name": target_name,
                "merchant_id": target_merchant,
                "feed_label": target_feed_label,
                "daily_budget_micros": request.daily_budget_micros,
                "bid_multiplier": request.bid_multiplier,
            },
            "ad_groups_to_clone": len(source_ad_groups),
            "ad_group_details": [
                {
                    "name": ag["name"],
                    "listing_groups_count": len(ag["listing_groups"]),
                    "default_bid": ag["cpc_bid_micros"],
                    "adjusted_bid": int(ag["cpc_bid_micros"] * request.bid_multiplier) if ag["cpc_bid_micros"] else 0,
                }
                for ag in source_ad_groups
            ],
        }

        if request.dry_run:
            return clone_plan

        created = {"campaign_id": None, "ad_groups": [], "errors": []}

        try:
            budget_req = CreateCampaignBudgetRequest(
                customer_id=CUSTOMER_ID,
                name=f"{target_name} Budget",
                amount_micros=request.daily_budget_micros,
            )
            budget_result = await google_ads_create_campaign_budget(budget_req)
            budget_rn = budget_result.get("budget_resource_name", "")

            camp_req = CreateShoppingCampaignRequest(
                customer_id=CUSTOMER_ID,
                name=target_name,
                budget_resource_name=budget_rn,
                merchant_id=int(target_merchant),
                sales_country=cc,
                status="PAUSED",
                feed_label=target_feed_label,
            )
            camp_result = await google_ads_create_shopping_campaign(camp_req)
            new_campaign_id = camp_result.get("campaign_id", "")
            created["campaign_id"] = new_campaign_id

            for ag in source_ad_groups:
                try:
                    adjusted_bid = int(ag["cpc_bid_micros"] * request.bid_multiplier) if ag["cpc_bid_micros"] else 100000

                    ag_req = CreateShoppingAdGroupRequest(
                        customer_id=CUSTOMER_ID,
                        campaign_id=new_campaign_id,
                        name=ag["name"],
                        cpc_bid_micros=adjusted_bid,
                    )
                    ag_result = await google_ads_create_shopping_ad_group(ag_req)
                    new_ag_id = ag_result.get("ad_group_id", "")

                    ad_req = CreateShoppingAdRequest(
                        customer_id=CUSTOMER_ID,
                        ad_group_id=new_ag_id,
                    )
                    await google_ads_create_shopping_ad(ad_req)

                    root_nodes = [n for n in ag["listing_groups"] if n.get("type") == "SUBDIVISION" and not n.get("parent")]
                    unit_nodes = [n for n in ag["listing_groups"] if n.get("type") == "UNIT" and n.get("value")]

                    if unit_nodes:
                        first_idx = unit_nodes[0].get("dimension_index", "INDEX0")
                        dim_map = {"INDEX0": "custom_label_0", "INDEX1": "custom_label_1",
                                   "INDEX2": "custom_label_2", "INDEX3": "custom_label_3",
                                   "INDEX4": "custom_label_4"}
                        root_dim = dim_map.get(first_idx, "custom_label_0")

                        groups = []
                        for n in unit_nodes:
                            bid = int(n["cpc_bid_micros"] * request.bid_multiplier) if n.get("cpc_bid_micros") else adjusted_bid
                            bid = max(bid, 10000)
                            groups.append({"value": n["value"], "cpc_bid_micros": bid})

                        try:
                            rebuild_req = RebuildShoppingListingGroupTreeRequest(
                                customer_id=CUSTOMER_ID,
                                ad_group_id=new_ag_id,
                                root_dimension=root_dim,
                                groups=groups,
                                everything_else_bid_micros=adjusted_bid,
                            )
                            await google_ads_rebuild_shopping_listing_group_tree(rebuild_req)
                        except Exception as e:
                            created["errors"].append({"ad_group": ag["name"], "step": "listing_groups", "error": str(e)})

                    created["ad_groups"].append({
                        "name": ag["name"],
                        "new_id": new_ag_id,
                        "listing_groups_cloned": len(unit_nodes),
                    })

                except Exception as e:
                    created["errors"].append({"ad_group": ag["name"], "error": str(e)})

        except Exception as e:
            created["errors"].append({"step": "campaign_creation", "error": str(e)})

        clone_plan["created"] = created
        return clone_plan

    return [
        "batch_setup_products", "batch_upgrade_to_ai", "batch_audit_campaign", "batch_generate_ad_copy",
        "batch_status", "batch_missing", "batch_sync_from_api", "batch_enable_feed_ad_groups",
        "batch_logs", "batch_clear_cache", "batch_get_instructions",
        "batch_setup_all", "batch_setup_progress", "batch_enhance_images", "batch_cleanup_stale",
        "batch_dashboard", "batch_setup_parallel",
        "batch_api_quota", "batch_add_images_only",
        "batch_guardrails_report", "batch_check_eligibility", "batch_validate_guardrails",
        "batch_global_dashboard",
        "batch_feed_prices", "batch_rsa_price_audit",
        "batch_warmup_cache", "batch_warmup_status",
        "batch_remove_wrong_domain_ads", "batch_remove_not_eligible_sitelinks",
        "batch_remove_not_eligible_ads", "batch_remove_not_eligible_images",
        "batch_warmup_market_intel", "batch_warmup_market_intel_status", "batch_list_market_intel_jobs",
        "batch_update_campaign_settings", "batch_verify_campaign_settings",
        "batch_time_analysis", "batch_remove_ad_groups", "batch_fix_sitelinks",
        "batch_fix_empty_groups",
        "batch_sync_negatives", "batch_remove_shared_criterion",
        "batch_remove_campaign_negative_keywords", "batch_fix_negative_conflicts",
        "batch_enable_paused_keywords", "batch_enable_paused_ad_groups",
        "batch_fix_match_types",
        "batch_implement_promotion_assets",
        "batch_check_obligatory", "batch_shopping_listing_groups",
        "batch_merchant_gc", "batch_shopping_bid_optimizer",
        "batch_shopping_exclude_products", "batch_shopping_clone_campaign",
    ]
