"""
Batch Intelligence — Cross-Campaign Keyword Miner + Category Intelligence.

Provides:
- batch_keyword_research: Mine keywords from ALL historical campaigns for a product
- batch_category_keywords: Share high-ROI keywords across products in same category
- Auto-enrichment of batch_setup_products with proven keywords (ROI > 50%)

Product Category Taxonomy:
  electronics, fashion, home_garden, beauty, sports, food, toys, pet, automotive, other
"""

import json
import re
import threading
import time
import unicodedata
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from accounts_config import DEFAULT_CUSTOMER_ID as CUSTOMER_ID, resolve_customer_id


# ---------------------------------------------------------------------------
# Script-based language filter for cross-country keyword deduplication
# ---------------------------------------------------------------------------
# Map country codes to their expected Unicode script(s).
# Keywords containing characters from WRONG scripts are rejected.
_COUNTRY_SCRIPTS = {
    # Latin-script countries
    "PL": {"Latin"}, "RO": {"Latin"}, "TR": {"Latin"}, "HU": {"Latin"},
    "FR": {"Latin"}, "DE": {"Latin"}, "IT": {"Latin"}, "ES": {"Latin"},
    "CZ": {"Latin"}, "SK": {"Latin"}, "HR": {"Latin"}, "SI": {"Latin"},
    "LT": {"Latin"}, "LV": {"Latin"}, "EE": {"Latin"}, "PT": {"Latin"},
    "NL": {"Latin"}, "SE": {"Latin"}, "NO": {"Latin"}, "DK": {"Latin"},
    "FI": {"Latin"}, "AL": {"Latin"}, "BA": {"Latin"}, "ME": {"Latin"},
    "XK": {"Latin"}, "MK": {"Latin"},
    # Cyrillic
    "BG": {"Cyrillic", "Latin"}, "UA": {"Cyrillic", "Latin"},
    "RS": {"Cyrillic", "Latin"}, "RU": {"Cyrillic", "Latin"},
    # Greek
    "GR": {"Greek", "Latin"},
    # Arabic
    "AE": {"Arabic", "Latin"}, "SA": {"Arabic", "Latin"},
    "EG": {"Arabic", "Latin"}, "DZ": {"Arabic", "Latin"},
    "MA": {"Arabic", "Latin"}, "BH": {"Arabic", "Latin"},
    "QA": {"Arabic", "Latin"}, "OM": {"Arabic", "Latin"},
    "LB": {"Arabic", "Latin"},
    # Thai
    "TH": {"Thai", "Latin"},
    # Other — Latin + local
    "JP": {"Han", "Hiragana", "Katakana", "Latin"},
    "KR": {"Hangul", "Latin"},
    "IN": {"Devanagari", "Latin"},
    "BD": {"Bengali", "Latin"},
}


# Language-specific word blacklist for Latin-script countries.
# These words are UNIQUE to one language and should NOT appear in other countries' keywords.
# Used to filter cross-country keyword contamination (e.g., Romanian keywords on Polish campaigns).
_LANGUAGE_WORD_BLACKLIST: Dict[str, List[str]] = {
    "RO": ["comandă", "cumpără", "produs", "preț", "reducere", "livrare", "ofertă", "calitate", "original", "magazin"],
    "PL": ["zamów", "kup", "produkt", "cena", "wysyłka", "opinie", "sklep", "oferta", "promocja", "jakość"],
    "TR": ["sipariş", "satın al", "ürün", "fiyat", "kargo", "indirim", "mağaza", "kalite", "orijinal"],
    "HU": ["rendelés", "vásárlás", "termék", "ár", "szállítás", "akció", "bolt", "vélemény", "minőség"],
    "FR": ["commander", "acheter", "produit", "prix", "livraison", "offre", "qualité", "original", "boutique"],
    "DE": ["bestellen", "kaufen", "produkt", "preis", "versand", "angebot", "qualität", "original", "shop"],
    "IT": ["ordinare", "comprare", "prodotto", "prezzo", "spedizione", "offerta", "qualità", "originale", "negozio"],
    "ES": ["comprar", "pedido", "producto", "precio", "envío", "oferta", "calidad", "original", "tienda"],
    "BG": ["поръчай", "доставка", "кутия", "официален", "натурален"],
    "GR": ["παραγγείλτε", "αποστολή", "κουτί", "επίσημο"],
    "EN": ["buy", "order", "product", "price", "shipping", "deal", "quality", "original", "shop", "review"],
}


def _keyword_has_foreign_words(keyword_text: str, target_country: str) -> bool:
    """
    Check if a Latin-script keyword contains language-specific words from OTHER countries.
    Returns True if foreign words detected (keyword should be REJECTED).
    This catches cross-country contamination that _keyword_matches_script misses
    (since all these languages use Latin script).
    """
    cc = target_country.upper()
    kw_lower = keyword_text.lower()

    for lang_cc, blacklist in _LANGUAGE_WORD_BLACKLIST.items():
        if lang_cc == cc:
            continue  # Skip own language
        for word in blacklist:
            if word.lower() in kw_lower:
                return True  # Foreign word found
    return False


def _keyword_matches_script(keyword_text: str, target_country: str) -> bool:
    """
    Check if a keyword's characters are compatible with the target country's script.
    Returns True if the keyword is safe to use, False if it contains foreign script chars.
    Brand names (single words, all ASCII) are always allowed.
    """
    allowed = _COUNTRY_SCRIPTS.get(target_country.upper())
    if not allowed:
        return True  # No script info → allow

    # Brand names: short, all ASCII letters → always OK
    words = keyword_text.strip().split()
    if len(words) <= 2 and all(c.isascii() for c in keyword_text):
        return True

    # Check each non-ASCII, non-digit, non-punctuation character
    for ch in keyword_text:
        if ch.isascii() or ch.isdigit() or ch in " -.,!?/'\"()&+%#@":
            continue
        cat = unicodedata.category(ch)
        if cat.startswith("L"):  # Letter
            script = unicodedata.name(ch, "").split()[0] if unicodedata.name(ch, "") else ""
            # Map some name prefixes to script names
            script_map = {
                "LATIN": "Latin", "CYRILLIC": "Cyrillic", "GREEK": "Greek",
                "ARABIC": "Arabic", "THAI": "Thai", "CJK": "Han",
                "HIRAGANA": "Hiragana", "KATAKANA": "Katakana", "HANGUL": "Hangul",
                "DEVANAGARI": "Devanagari", "BENGALI": "Bengali",
            }
            detected = None
            for prefix, sname in script_map.items():
                if script.upper().startswith(prefix):
                    detected = sname
                    break
            if detected and detected not in allowed:
                return False  # Foreign script character found
    return True
from pydantic import BaseModel, Field

from batch_db import BatchDB

# Background job registry (shared with batch_optimizer via import)
_mining_jobs: Dict[str, Dict[str, Any]] = {}
_mining_jobs_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Product → Category mapping (heuristic based on product name patterns)
# ---------------------------------------------------------------------------
CATEGORY_PATTERNS: Dict[str, List[str]] = {
    "electronics": [
        "phone", "laptop", "headphone", "speaker", "charger", "cable",
        "screen", "monitor", "keyboard", "mouse", "tablet", "watch",
        "smartwatch", "earbuds", "router", "usb",
    ],
    "fashion": [
        "shirt", "dress", "jacket", "shoe", "sneaker", "boot", "hat",
        "watch", "ring", "necklace", "sweater", "jeans", "coat", "tie",
        "scarf", "glove", "socks", "belt", "vest",
    ],
    "home_garden": [
        "lamp", "pillow", "blanket", "chair", "table", "shelf", "plant",
        "tool", "drill", "garden", "sofa", "bed", "rug", "curtain",
        "pot", "pan", "sink", "cabinet", "shelf",
    ],
    "beauty": [
        "cream", "serum", "shampoo", "perfume", "lipstick", "mascara",
        "lotion", "soap", "brush", "foundation", "moisturizer", "cleanser",
        "conditioner", "sunscreen", "eye shadow", "concealer",
    ],
    "sports": [
        "protein", "dumbbell", "yoga", "fitness", "running", "cycling",
        "mat", "glove", "ball", "racket", "jersey", "shorts", "shoes",
        "band", "rope", "weight",
    ],
    "food": [
        "coffee", "tea", "snack", "organic", "vitamin", "honey", "spice",
        "oil", "chocolate", "pasta", "rice", "cereal", "flour", "sugar",
        "juice", "smoothie", "nuts",
    ],
    "toys": [
        "puzzle", "lego", "doll", "game", "robot", "toy", "plush",
        "board game", "action figure", "building block", "card game",
    ],
    "pet": [
        "dog", "cat", "pet food", "collar", "leash", "aquarium", "bird",
        "treats", "bed", "toy", "cage", "bowl", "brush",
    ],
    "automotive": [
        "car", "tire", "brake", "filter", "oil", "wiper", "battery",
        "seat cover", "floor mat", "headlight", "mirror", "windshield",
    ],
}

# Country-specific keyword suffix patterns to mine
KEYWORD_SUFFIX_PATTERNS: Dict[str, List[str]] = {
    "RO": ["pret", "catena", "farmacia", "original", "pareri", "prospect",
            "dr max", "forum", "romania", "comanda"],
    "TR": ["fiyat", "eczane", "orijinal", "yorumlar", "siparis",
            "nereden alinir", "kullananlar"],
    "HU": ["ár", "vélemények", "gyógyszertár", "rendelés", "dm",
            "hol kapható", "fórum"],
    "PL": ["cena", "apteka", "opinie", "skład", "forum", "gdzie kupić",
            "allegro", "rossmann"],
    "GR": ["τιμή", "φαρμακείο", "κριτικές", "παραγγελία", "αγορά"],
    "BG": ["цена", "аптека", "мнения", "поръчка", "форум"],
    "HR": ["cijena", "ljekarna", "iskustva", "narudžba", "forum"],
    "SK": ["cena", "lekáreň", "recenzie", "objednávka", "dr max"],
    "CZ": ["cena", "lékárna", "recenze", "objednávka", "dr max"],
    "ES": ["precio", "farmacia", "opiniones", "comprar", "original"],
    "IT": ["prezzo", "farmacia", "recensioni", "acquisto", "originale"],
    "DE": ["preis", "apotheke", "erfahrungen", "kaufen", "original"],
    "FR": ["prix", "pharmacie", "avis", "acheter", "original"],
}

# Customer ID (shared with batch_optimizer)
# Mapping: XML feed <g:product_type> values → our internal category names
# This is the AUTHORITATIVE source; heuristic classify_product() is fallback only.
FEED_PRODUCT_TYPE_MAP: Dict[str, str] = {
    # Electronics
    "electronics": "electronics",
    "electronics > phones": "electronics",
    "electronics > laptops": "electronics",
    "electronics > headphones": "electronics",
    "electronics > speakers": "electronics",
    "electronics > chargers": "electronics",
    "electronics > cables": "electronics",
    "electronics > monitors": "electronics",
    # Fashion
    "fashion": "fashion",
    "fashion > clothing": "fashion",
    "fashion > shoes": "fashion",
    "fashion > accessories": "fashion",
    "fashion > watches": "fashion",
    "fashion > jewelry": "fashion",
    # Home & Garden
    "home & garden": "home_garden",
    "home & garden > furniture": "home_garden",
    "home & garden > decor": "home_garden",
    "home & garden > tools": "home_garden",
    "home & garden > plants": "home_garden",
    # Beauty
    "beauty": "beauty",
    "beauty > skincare": "beauty",
    "beauty > makeup": "beauty",
    "beauty > haircare": "beauty",
    "beauty > fragrances": "beauty",
    # Sports
    "sports": "sports",
    "sports > fitness": "sports",
    "sports > outdoor": "sports",
    "sports > equipment": "sports",
    # Food & Beverages
    "food": "food",
    "food > beverages": "food",
    "food > snacks": "food",
    "food > organic": "food",
    "food > gourmet": "food",
    # Toys
    "toys": "toys",
    "toys > games": "toys",
    "toys > puzzles": "toys",
    "toys > action figures": "toys",
    # Pet Supplies
    "pet": "pet",
    "pet > pet food": "pet",
    "pet > pet accessories": "pet",
    "pet > pet toys": "pet",
    # Automotive
    "automotive": "automotive",
    "automotive > parts": "automotive",
    "automotive > accessories": "automotive",
    "automotive > maintenance": "automotive",
}


def _get_db() -> BatchDB:
    """Import shared DB instance from batch_optimizer."""
    from batch_optimizer import _get_db
    return _get_db()


def classify_product(handle: str, feed_product_type: str = "") -> str:
    """Classify a product into a category.

    Priority:
    1. feed_product_type (from XML <g:product_type>) — authoritative
    2. handle-based heuristic (CATEGORY_PATTERNS) — fallback
    """
    # Priority 1: Use feed product_type if available
    if feed_product_type:
        pt_lower = feed_product_type.lower().strip()
        if pt_lower in FEED_PRODUCT_TYPE_MAP:
            return FEED_PRODUCT_TYPE_MAP[pt_lower]
        # Try partial match (e.g., "Wireless Headphones" → "electronics")
        for key, category in FEED_PRODUCT_TYPE_MAP.items():
            if key in pt_lower:
                return category

    # Priority 2: Handle-based heuristic (fallback)
    h = handle.lower()
    for category, patterns in CATEGORY_PATTERNS.items():
        for pat in patterns:
            if pat in h:
                return category
    return "other"


def _normalize_product_name(handle: str) -> str:
    """Normalize handle to product base name for cross-campaign matching.
    'running-shoes-us' -> 'running shoes', 'wireless-headphones-uk' -> 'wireless headphones'
    """
    name = re.sub(r"-[a-z]{2}$", "", handle)  # remove country suffix
    name = re.sub(r"-\d+-[a-z]{3}$", "", name)  # remove price suffix like -120-ron
    name = re.sub(r"-(low|full|free|premium)$", "", name)  # remove variant suffix
    return name.replace("-", " ").lower().strip()


def _parse_keyword_rows(results: list, product_name: str, country_code: str, _safe_get_value) -> List[Dict[str, Any]]:
    """Parse GAQL keyword_view rows into standardized keyword dicts.
    Includes conversions_value for true ROAS calculation."""
    keywords: List[Dict[str, Any]] = []
    for row in results:
        kw_text = str(_safe_get_value(row, "ad_group_criterion.keyword.text", ""))
        match_type = str(_safe_get_value(row, "ad_group_criterion.keyword.match_type", ""))
        impressions = int(_safe_get_value(row, "metrics.impressions", 0))
        clicks = int(_safe_get_value(row, "metrics.clicks", 0))
        conversions = float(_safe_get_value(row, "metrics.conversions", 0))
        conversion_value = float(_safe_get_value(row, "metrics.conversions_value", 0))
        cost_micros = int(_safe_get_value(row, "metrics.cost_micros", 0))
        quality_score = _safe_get_value(row, "ad_group_criterion.quality_info.quality_score", None)
        campaign_name = str(_safe_get_value(row, "campaign.name", ""))
        campaign_id = str(_safe_get_value(row, "campaign.id", ""))

        ctr = (clicks / impressions * 100) if impressions > 0 else 0
        cpc = (cost_micros / clicks) if clicks > 0 else 0
        cost_approx = cost_micros / 1_000_000

        # True ROAS from conversion value (preferred over approximation)
        roas = (conversion_value / cost_approx) if cost_approx > 0 else 0
        # Legacy ROI approximation as fallback
        roi_percent = ((conversions * 100) / cost_approx - 100) if cost_approx > 0 else 0

        keywords.append({
            "product_name": product_name,
            "country_code": country_code,
            "keyword_text": kw_text,
            "match_type": match_type.replace("MatchType.", ""),
            "source_campaign_id": campaign_id,
            "source_campaign_name": campaign_name,
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "conversion_value": round(conversion_value, 2),
            "cost_micros": cost_micros,
            "quality_score": int(quality_score) if quality_score else None,
            "ctr": round(ctr, 2),
            "conv_rate": round(conversions / clicks * 100, 2) if clicks > 0 else 0,
            "cpc_micros": round(cpc),
            "roas": round(roas, 2),
            "roi_percent": round(roi_percent, 1),
        })
    return keywords


def _discover_ad_groups_by_url(product_handle: str, customer_id: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    Discover ALL ad group IDs across the ENTIRE account where RSA final_urls
    contain the product handle. Returns list of (ad_group_id, campaign_id).

    This is the primary discovery method — finds the product everywhere it
    was ever advertised, regardless of ad group naming conventions.
    E.g., 'duoslim-pl' in URL → finds ad groups in ANY campaign.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    cid = resolve_customer_id(customer_id)
    # Search for the handle in ad final URLs across all campaigns
    # ad_group_ad resource lets us query final_urls
    query = f"""
        SELECT ad_group.id, campaign.id
        FROM ad_group_ad
        WHERE ad_group_ad.ad.final_urls CONTAINS '{product_handle}'
          AND ad_group_ad.status != 'REMOVED'
    """
    try:
        results = _execute_gaql(cid, query, page_size=500)
    except Exception:
        return []

    seen: set = set()
    pairs: List[Tuple[str, str]] = []
    for row in results:
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        cmp_id = str(_safe_get_value(row, "campaign.id", ""))
        if ag_id and cmp_id:
            key = f"{ag_id}|{cmp_id}"
            if key not in seen:
                seen.add(key)
                pairs.append((ag_id, cmp_id))

    return pairs


def mine_keywords_for_product(
    product_handle: str,
    country_code: str,
    min_roi_percent: float = 50.0,
    days_back: int = 90,
    feed_product_type: str = "",
    customer_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Mine keywords from ALL campaigns (active + paused) for a product.
    Returns structured dict with keyword recommendations.

    Uses TWO discovery methods:
    1. URL-based: Finds ad groups where RSA final_urls contain the product handle
       (most reliable — catches ALL campaigns regardless of naming)
    2. Name-based: Finds ad groups whose name matches the product name
       (fallback for legacy campaigns)

    Args:
        days_back: Number of days to look back for data (default 90).
                   Shorter windows show more recent (often better) ROI.
                   Use 30 for recent, 180 for broader history, 0 for all-time.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    cid = resolve_customer_id(customer_id)
    db = _get_db()
    product_name = _normalize_product_name(product_handle)
    category = classify_product(product_handle, feed_product_type=feed_product_type)

    # Date filter for GAQL
    if days_back > 0:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        date_filter = f"AND segments.date BETWEEN '{start_date}' AND '{end_date}'"
    else:
        date_filter = ""

    all_keywords: List[Dict[str, Any]] = []
    discovered_ag_ids: set = set()   # Track all discovered ad group IDs

    # ── METHOD 1: URL-based discovery (PRIMARY) ──────────────────────────
    # Find ALL ad groups where the product handle appears in ad final_urls
    url_pairs = _discover_ad_groups_by_url(product_handle, customer_id=cid)

    if url_pairs:
        # Build list of ad_group IDs for GAQL IN clause
        ag_ids = list(set(pair[0] for pair in url_pairs))
        discovered_ag_ids.update(ag_ids)

        # Query keywords from all discovered ad groups in chunks
        # (GAQL IN clause has limits, so chunk by 50)
        for chunk_start in range(0, len(ag_ids), 50):
            chunk = ag_ids[chunk_start:chunk_start + 50]
            ag_id_list = ", ".join(chunk)

            query = f"""
                SELECT ad_group_criterion.keyword.text,
                       ad_group_criterion.keyword.match_type,
                       ad_group_criterion.quality_info.quality_score,
                       metrics.impressions, metrics.clicks,
                       metrics.conversions, metrics.conversions_value,
                       metrics.cost_micros,
                       ad_group.name, ad_group.id,
                       campaign.name, campaign.id
                FROM keyword_view
                WHERE ad_group.id IN ({ag_id_list})
                  AND metrics.impressions > 0
                  {date_filter}
                ORDER BY metrics.conversions DESC
            """
            try:
                results = _execute_gaql(cid, query, page_size=500)
            except Exception:
                results = []

            all_keywords.extend(_parse_keyword_rows(results, product_name, country_code, _safe_get_value))

    # ── METHOD 2: Name-based discovery (FALLBACK) ────────────────────────
    # Search by ad group name matching the product name
    search_terms = [product_name]
    if " " in product_name:
        search_terms.append(product_name.replace(" ", "-"))

    for search_term in search_terms:
        query = f"""
            SELECT ad_group_criterion.keyword.text,
                   ad_group_criterion.keyword.match_type,
                   ad_group_criterion.quality_info.quality_score,
                   metrics.impressions, metrics.clicks,
                   metrics.conversions, metrics.conversions_value,
                   metrics.cost_micros,
                   ad_group.name, ad_group.id,
                   campaign.name, campaign.id
            FROM keyword_view
            WHERE ad_group.name LIKE '%{search_term}%'
              AND metrics.impressions > 0
              {date_filter}
            ORDER BY metrics.conversions DESC
        """
        try:
            results = _execute_gaql(cid, query, page_size=200)
        except Exception:
            results = []

        # Only add keywords from ad groups we haven't already discovered via URL
        for row in results:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            if ag_id not in discovered_ag_ids:
                discovered_ag_ids.add(ag_id)
                all_keywords.extend(_parse_keyword_rows([row], product_name, country_code, _safe_get_value))

    # Deduplicate: aggregate same keyword+match_type across campaigns
    aggregated: Dict[str, Dict[str, Any]] = {}
    for kw in all_keywords:
        key = f"{kw['keyword_text']}|{kw['match_type']}"
        if key not in aggregated:
            aggregated[key] = {**kw, "source_campaigns": [kw["source_campaign_name"]]}
        else:
            agg = aggregated[key]
            agg["impressions"] += kw["impressions"]
            agg["clicks"] += kw["clicks"]
            agg["conversions"] += kw["conversions"]
            agg["conversion_value"] = agg.get("conversion_value", 0) + kw.get("conversion_value", 0)
            agg["cost_micros"] += kw["cost_micros"]
            if kw["source_campaign_name"] not in agg["source_campaigns"]:
                agg["source_campaigns"].append(kw["source_campaign_name"])
            if kw["quality_score"] and (not agg["quality_score"] or kw["quality_score"] > agg["quality_score"]):
                agg["quality_score"] = kw["quality_score"]

    # Recalculate aggregated metrics (with true ROAS from conversion_value)
    for key, agg in aggregated.items():
        imp = agg["impressions"]
        clk = agg["clicks"]
        conv = agg["conversions"]
        conv_val = agg.get("conversion_value", 0)
        cost = agg["cost_micros"]
        cost_approx = cost / 1_000_000
        agg["ctr"] = round(clk / imp * 100, 2) if imp > 0 else 0
        agg["conv_rate"] = round(conv / clk * 100, 2) if clk > 0 else 0
        agg["cpc_micros"] = round(cost / clk) if clk > 0 else 0
        agg["roas"] = round(conv_val / cost_approx, 2) if cost_approx > 0 else 0
        agg["roi_percent"] = round((conv * 100) / cost_approx - 100, 1) if cost_approx > 0 else 0

    # Store all in DB
    db.bulk_upsert_keyword_intel(all_keywords)

    # Filter by ROI threshold and conversions
    recommended = []
    for kw in sorted(aggregated.values(), key=lambda x: x["conversions"], reverse=True):
        if kw["conversions"] >= 1.0 and kw["roi_percent"] >= min_roi_percent:
            recommended.append(kw)

    # Also mine category keywords
    category_kws = _get_category_keywords_for_product(product_name, category, country_code, customer_id=cid)

    # Collect unique campaign IDs from discovered ad groups
    url_campaign_ids = list(set(pair[1] for pair in url_pairs)) if url_pairs else []

    return {
        "product_handle": product_handle,
        "product_name": product_name,
        "category": category,
        "country_code": country_code,
        "total_historical_keywords": len(all_keywords),
        "unique_keywords": len(aggregated),
        "recommended_keywords": recommended[:30],
        "category_keywords": category_kws[:15],
        "roi_threshold": min_roi_percent,
        "discovery": {
            "url_based_ad_groups": len(url_pairs) if url_pairs else 0,
            "url_based_campaigns": url_campaign_ids,
            "total_ad_groups_discovered": len(discovered_ag_ids),
        },
    }


def _accumulate_category_kw(
    row: Any,
    _safe_get_value: Any,
    category_products: List[str],
    product_name: str,
    category_kws: Dict[str, Dict[str, Any]],
) -> None:
    """Parse a single GAQL keyword_view row and accumulate into category_kws dict.
    Skips brand-specific keywords. Uses conversions_value for true ROAS."""
    kw_text = str(_safe_get_value(row, "ad_group_criterion.keyword.text", "")).lower()
    match_type = str(_safe_get_value(row, "ad_group_criterion.keyword.match_type", ""))
    conversions = float(_safe_get_value(row, "metrics.conversions", 0))
    conversion_value = float(_safe_get_value(row, "metrics.conversions_value", 0))
    clicks = int(_safe_get_value(row, "metrics.clicks", 0))
    cost_micros = int(_safe_get_value(row, "metrics.cost_micros", 0))
    impressions = int(_safe_get_value(row, "metrics.impressions", 0))
    ag_name = str(_safe_get_value(row, "ad_group.name", ""))

    # Skip brand-specific keywords (exact product names)
    for pat in category_products:
        if pat.lower() in kw_text and pat.lower() != product_name:
            return

    cost_approx = cost_micros / 1_000_000
    roi = ((conversions * 100) / cost_approx - 100) if cost_approx > 0 else 0
    roas = (conversion_value / cost_approx) if cost_approx > 0 else 0
    ctr = (clicks / impressions * 100) if impressions > 0 else 0

    key = f"{kw_text}|{match_type}"
    if key not in category_kws:
        category_kws[key] = {
            "keyword_text": kw_text,
            "match_type": match_type.replace("MatchType.", ""),
            "avg_conversions": conversions,
            "avg_conversion_value": round(conversion_value, 2),
            "avg_ctr": round(ctr, 2),
            "avg_roi": round(roi, 1),
            "avg_roas": round(roas, 2),
            "source_products": ag_name,
        }
    else:
        existing = category_kws[key]
        existing["avg_conversions"] += conversions
        existing["avg_conversion_value"] = round(
            existing.get("avg_conversion_value", 0) + conversion_value, 2
        )
        existing["source_products"] += f", {ag_name}"
        # Recalculate aggregated ROAS
        total_cost = (existing.get("_total_cost", 0) + cost_approx)
        total_val = existing["avg_conversion_value"]
        existing["avg_roas"] = round(total_val / total_cost, 2) if total_cost > 0 else 0
        existing["_total_cost"] = total_cost


def _get_category_keywords_for_product(
    product_name: str,
    category: str,
    country_code: str,
    customer_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Find keywords from OTHER products in the same category that could work
    for this product. E.g., if 'wireless-headphones' works in electronics category,
    'gaming-headphones' might benefit from similar generic keywords.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    cid = resolve_customer_id(customer_id)
    db = _get_db()

    # Check DB cache first
    cached = db.get_category_keywords(category, country_code, min_conversions=1.0)
    if cached:
        # Filter out keywords that are specific to other products (brand names)
        generic = [kw for kw in cached if product_name not in kw["keyword_text"].lower()]
        if generic:
            return generic

    # Mine category keywords from all campaigns matching the country
    # DUAL APPROACH: URL-based discovery (primary) + name-based fallback
    category_products = CATEGORY_PATTERNS.get(category, [])
    if not category_products:
        return []

    category_kws: Dict[str, Dict[str, Any]] = {}
    discovered_ag_ids: set = set()

    for product_pattern in category_products[:10]:  # Limit to avoid too many queries
        # ── PRIMARY: URL-based discovery ──
        url_pairs = _discover_ad_groups_by_url(product_pattern, customer_id=cid)
        url_ag_ids = [p[0] for p in url_pairs]
        discovered_ag_ids.update(url_ag_ids)

        if url_ag_ids:
            # Query keywords from URL-discovered ad groups in chunks
            for i in range(0, len(url_ag_ids), 50):
                chunk = url_ag_ids[i:i+50]
                ag_filter = ",".join(f"'{ag}'" for ag in chunk)
                query = f"""
                    SELECT ad_group_criterion.keyword.text,
                           ad_group_criterion.keyword.match_type,
                           metrics.impressions, metrics.clicks,
                           metrics.conversions, metrics.conversions_value,
                           metrics.cost_micros,
                           ad_group.name, ad_group.id, campaign.name
                    FROM keyword_view
                    WHERE ad_group.id IN ({ag_filter})
                      AND metrics.conversions > 2
                    ORDER BY metrics.conversions DESC
                """
                try:
                    results = _execute_gaql(cid, query, page_size=100)
                except Exception:
                    results = []

                for row in results:
                    _accumulate_category_kw(row, _safe_get_value, category_products, product_name, category_kws)

        # ── FALLBACK: Name-based discovery for products not found by URL ──
        query = f"""
            SELECT ad_group_criterion.keyword.text,
                   ad_group_criterion.keyword.match_type,
                   metrics.impressions, metrics.clicks,
                   metrics.conversions, metrics.conversions_value,
                   metrics.cost_micros,
                   ad_group.name, ad_group.id, campaign.name
            FROM keyword_view
            WHERE ad_group.name LIKE '%{product_pattern}%'
              AND campaign.name LIKE '%{country_code}%'
              AND metrics.conversions > 2
            ORDER BY metrics.conversions DESC
        """
        try:
            results = _execute_gaql(cid, query, page_size=50)
        except Exception:
            results = []

        for row in results:
            ag_id = str(_safe_get_value(row, "ad_group.id", ""))
            if ag_id not in discovered_ag_ids:
                discovered_ag_ids.add(ag_id)
                _accumulate_category_kw(row, _safe_get_value, category_products, product_name, category_kws)

    # Store in DB
    for kw_data in category_kws.values():
        db.upsert_category_keyword(
            category=category,
            country_code=country_code,
            keyword_text=kw_data["keyword_text"],
            match_type=kw_data["match_type"],
            avg_conversions=kw_data["avg_conversions"],
            avg_ctr=kw_data["avg_ctr"],
            avg_roi=kw_data["avg_roi"],
            source_products=kw_data["source_products"][:500],
        )

    # Clean internal tracking fields and return sorted by conversions
    for kw_data in category_kws.values():
        kw_data.pop("_total_cost", None)
    return sorted(category_kws.values(), key=lambda x: x["avg_conversions"], reverse=True)


def discover_product_everywhere(product_handle: str, customer_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Discover ALL ad groups and campaigns where a product appears in ad URLs.
    Public API for cross-module use (e.g., by _fetch_ai_generated_copy in batch_optimizer).

    Returns:
        {
            "ad_group_ids": ["123", "456", ...],
            "campaign_ids": ["789", "012", ...],
            "pairs": [("ag_id", "campaign_id"), ...],
        }
    """
    pairs = _discover_ad_groups_by_url(product_handle, customer_id=customer_id)
    ag_ids = list(set(p[0] for p in pairs))
    cmp_ids = list(set(p[1] for p in pairs))
    return {
        "ad_group_ids": ag_ids,
        "campaign_ids": cmp_ids,
        "pairs": pairs,
    }


def get_keywords_for_setup(
    product_handle: str,
    country_code: str,
    ad_group_id: str,
    min_roi: float = 0.0,
    max_keywords: int = 15,
    feed_product_type: str = "",
) -> List[Dict[str, str]]:
    """
    Get keyword recommendations for batch_setup_products integration.
    Returns list of {text, match_type} ready for Google Ads API.

    Priority:
    1. Historical keywords from target country (by conversions, ignoring ROI)
    2. Cross-country keywords if <5 found in target country
    3. Category keywords with proven conversions
    4. Fallback: brand name [EXACT] + [PHRASE]

    Args:
        min_roi: ROI filter. Default 0 (disabled) — many countries lack
                 conversion_value data so ROI is unreliable.
        feed_product_type: Value from XML feed <g:product_type> (e.g., "Electronics > Headphones").
                          Takes priority over handle-based heuristic for category assignment.
    """
    db = _get_db()
    product_name = _normalize_product_name(product_handle)
    category = classify_product(product_handle, feed_product_type=feed_product_type)

    keywords: List[Dict[str, str]] = []
    seen: set = set()

    # 1. Check DB for keyword intelligence — target country first
    cached_kws = db.get_keyword_intel(product_name, country_code, min_roi=min_roi)

    for kw in cached_kws:
        key = f"{kw['keyword_text']}|{kw['match_type']}"
        if key not in seen and len(keywords) < max_keywords:
            keywords.append({
                "text": kw["keyword_text"],
                "match_type": kw["match_type"],
                "source": "historical",
                "conversions": kw.get("conversions", 0),
                "roi": kw.get("roi_percent", 0),
                "roas": kw.get("roas", 0),
                "country": country_code,
            })
            seen.add(key)

    # 2. Cross-country fallback: if <5 keywords from target country,
    #    pull from ALL countries for this product and deduplicate
    if len(keywords) < 5:
        cross_kws = db.get_keyword_intel(
            product_name, country_code=country_code,
            min_roi=min_roi, cross_country=True, limit=80
        )
        # Deduplicate: same keyword_text from multiple countries → keep highest conversions
        best_by_text: dict = {}
        for kw in cross_kws:
            txt = kw["keyword_text"].lower()
            existing = best_by_text.get(txt)
            if not existing or kw.get("conversions", 0) > existing.get("conversions", 0):
                best_by_text[txt] = kw

        for kw in sorted(best_by_text.values(), key=lambda x: x.get("conversions", 0), reverse=True):
            key = f"{kw['keyword_text']}|{kw['match_type']}"
            if key not in seen and len(keywords) < max_keywords:
                # Script filter: reject keywords with foreign characters
                if not _keyword_matches_script(kw["keyword_text"], country_code):
                    continue
                # Language word filter: reject Latin-script keywords from wrong language
                if _keyword_has_foreign_words(kw["keyword_text"], country_code):
                    continue
                keywords.append({
                    "text": kw["keyword_text"],
                    "match_type": kw["match_type"],
                    "source": "cross_country",
                    "conversions": kw.get("conversions", 0),
                    "roi": kw.get("roi_percent", 0),
                    "roas": kw.get("roas", 0),
                    "country": kw.get("country_code", "??"),
                })
                seen.add(key)

    # 3. Add category keywords if we have room
    if len(keywords) < max_keywords:
        cat_kws = db.get_category_keywords(category, country_code, min_conversions=3.0)
        for kw in cat_kws:
            key = f"{kw['keyword_text']}|{kw['match_type']}"
            if key not in seen and len(keywords) < max_keywords:
                # Don't add category keywords that are other product brands
                if product_name.split()[0] not in kw["keyword_text"].lower():
                    keywords.append({
                        "text": kw["keyword_text"],
                        "match_type": kw["match_type"],
                        "source": "category",
                        "conversions": kw.get("avg_conversions", 0),
                        "roi": kw.get("avg_roi", 0),
                    })
                    seen.add(key)

    # 4. Fallback: always ensure brand name keywords exist
    # CRITICAL: Only EXACT and PHRASE. BROAD match is BLOCKED to prevent budget waste.
    brand_base = product_name.split()[0] if " " in product_name else product_name
    for mt in ["EXACT", "PHRASE"]:
        key = f"{brand_base}|{mt}"
        if key not in seen and len(keywords) < max_keywords:
            keywords.append({
                "text": brand_base,
                "match_type": mt,
                "source": "brand_fallback",
                "conversions": 0,
                "roi": 0,
            })
            seen.add(key)

    # Also add full product name if different from brand_base
    if product_name != brand_base:
        key = f"{product_name}|EXACT"
        if key not in seen and len(keywords) < max_keywords:
            keywords.append({
                "text": product_name,
                "match_type": "EXACT",
                "source": "brand_fallback",
                "conversions": 0,
                "roi": 0,
            })
            seen.add(key)

    return keywords


# ---------------------------------------------------------------------------
# Country-Wide Keyword Mining (Search + PMax)
# ---------------------------------------------------------------------------

# Google Ads API enum → string mapping
# campaign.advertising_channel_type returns integer enums
_CHANNEL_TYPE_MAP = {
    0: "UNSPECIFIED",
    1: "UNKNOWN",
    2: "SEARCH",
    3: "DISPLAY",
    4: "SHOPPING",  # Legacy
    5: "HOTEL",
    6: "SHOPPING",  # Standard Shopping
    7: "VIDEO",
    8: "MULTI_CHANNEL",
    9: "PERFORMANCE_MAX",
    10: "LOCAL",
    11: "SMART",
    12: "SMART_SHOPPING",  # Deprecated
    13: "LOCAL_SERVICES",
    14: "DEMAND_GEN",
    15: "TRAVEL",
}

_CAMPAIGN_STATUS_MAP = {
    0: "UNSPECIFIED",
    1: "UNKNOWN",
    2: "ENABLED",
    3: "PAUSED",
    4: "REMOVED",
}


def _parse_enum(value, enum_map: dict) -> str:
    """Parse Google Ads protobuf enum value to string.
    Handles: int (2), str('2'), str('SEARCH'), str('AdvertisingChannelType.SEARCH').
    """
    s = str(value)
    # Already a string name? Return as-is (from GAQL text results)
    if s in enum_map.values():
        return s
    # Try integer lookup
    try:
        return enum_map.get(int(s), s)
    except (ValueError, TypeError):
        pass
    # Try extracting from enum repr like "AdvertisingChannelType.SEARCH"
    if "." in s:
        suffix = s.rsplit(".", 1)[-1]
        if suffix in enum_map.values():
            return suffix
    return s


def _find_all_campaigns_for_country(country_code: str, customer_id: Optional[str] = None, include_paused: bool = True) -> List[Dict[str, Any]]:
    """
    Find ALL campaigns (Search + PMax + Shopping) matching a country code.
    Returns list of {id, name, type, status}.
    Campaign naming convention: starts with country code (e.g., "PL ...", "TR ...").
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    cid = resolve_customer_id(customer_id)
    status_filter = "" if include_paused else "AND campaign.status = 'ENABLED'"
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status,
               campaign.advertising_channel_type
        FROM campaign
        WHERE campaign.status != 'REMOVED'
          AND campaign.name LIKE '%{country_code}%'
          {status_filter}
    """
    try:
        results = _execute_gaql(cid, query, page_size=500)
    except Exception:
        return []

    campaigns = []
    for row in results:
        cmp_id = str(_safe_get_value(row, "campaign.id", ""))
        cmp_name = str(_safe_get_value(row, "campaign.name", ""))
        raw_type = _safe_get_value(row, "campaign.advertising_channel_type", "")
        raw_status = _safe_get_value(row, "campaign.status", "")
        cmp_type = _parse_enum(raw_type, _CHANNEL_TYPE_MAP)
        cmp_status = _parse_enum(raw_status, _CAMPAIGN_STATUS_MAP)
        if cmp_id:
            campaigns.append({
                "id": cmp_id,
                "name": cmp_name,
                "type": cmp_type,
                "status": cmp_status,
            })
    return campaigns


def _build_ag_to_handle_map(campaign_id: str, customer_id: Optional[str] = None) -> Dict[str, str]:
    """
    Build ad_group_id → product_handle map for a campaign.
    Uses RSA final_urls to extract handle from /products/{handle}.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    cid = resolve_customer_id(customer_id)
    query = f"""
        SELECT campaign.id, ad_group.id, ad_group_ad.ad.final_urls
        FROM ad_group_ad
        WHERE campaign.id = {campaign_id}
          AND ad_group_ad.status != 'REMOVED'
          AND ad_group.status != 'REMOVED'
    """
    try:
        results = _execute_gaql(cid, query, page_size=1000)
    except Exception:
        return {}

    mapping: Dict[str, str] = {}
    for row in results:
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        urls = str(_safe_get_value(row, "ad_group_ad.ad.final_urls", ""))
        handle_match = re.search(r'/products/([a-z0-9][a-z0-9\-]+?)(?:\?|$)', urls)
        if ag_id and handle_match:
            mapping[ag_id] = handle_match.group(1)
    return mapping


def _mine_search_terms_from_search_campaign(
    campaign_id: str,
    campaign_name: str,
    country_code: str,
    ag_handle_map: Dict[str, str],
    days_back: int = 0,
    customer_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Mine converting search terms from a Search campaign using search_term_view.
    Maps each search term to a product handle via ad_group → URL → handle.

    Returns list of keyword dicts ready for DB storage.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    cid = resolve_customer_id(customer_id)
    # search_term_view REQUIRES segments.date in WHERE clause
    if days_back > 0:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    else:
        # All-time: use a far-back start date
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = "2020-01-01"

    query = f"""
        SELECT campaign.id, ad_group.id,
               search_term_view.search_term,
               metrics.impressions, metrics.clicks,
               metrics.conversions, metrics.conversions_value,
               metrics.cost_micros
        FROM search_term_view
        WHERE campaign.id = {campaign_id}
          AND segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND metrics.conversions > 0
        ORDER BY metrics.conversions DESC
        LIMIT 500
    """
    try:
        results = _execute_gaql(cid, query, page_size=500)
    except Exception:
        return []

    keywords: List[Dict[str, Any]] = []
    for row in results:
        search_term = str(_safe_get_value(row, "search_term_view.search_term", ""))
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        impressions = int(_safe_get_value(row, "metrics.impressions", 0))
        clicks = int(_safe_get_value(row, "metrics.clicks", 0))
        conversions = float(_safe_get_value(row, "metrics.conversions", 0))
        conv_value = float(_safe_get_value(row, "metrics.conversions_value", 0))
        cost_micros = int(_safe_get_value(row, "metrics.cost_micros", 0))

        if not search_term:
            continue

        # Map to product handle
        product_handle = ag_handle_map.get(ag_id, "")
        product_name = _normalize_product_name(product_handle) if product_handle else "unknown"

        cost_approx = cost_micros / 1_000_000
        ctr = (clicks / impressions * 100) if impressions > 0 else 0
        roas = (conv_value / cost_approx) if cost_approx > 0 else 0
        roi_percent = ((conversions * 100) / cost_approx - 100) if cost_approx > 0 else 0

        keywords.append({
            "product_name": product_name,
            "product_handle": product_handle,
            "country_code": country_code,
            "keyword_text": search_term,
            "match_type": "SEARCH_TERM",  # Marks this as actual search term, not keyword
            "source_campaign_id": campaign_id,
            "source_campaign_name": campaign_name,
            "source_type": "search",
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "conversion_value": round(conv_value, 2),
            "cost_micros": cost_micros,
            "quality_score": None,
            "ctr": round(ctr, 2),
            "conv_rate": round(conversions / clicks * 100, 2) if clicks > 0 else 0,
            "cpc_micros": round(cost_micros / clicks) if clicks > 0 else 0,
            "roas": round(roas, 2),
            "roi_percent": round(roi_percent, 1),
        })

    return keywords


def _mine_search_terms_from_pmax_campaign(
    campaign_id: str,
    campaign_name: str,
    country_code: str,
    customer_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Mine search term insights from a Performance Max campaign.
    Uses campaign_search_term_insight resource (requires single campaign filter).

    Note: PMax returns category_label (search category), not individual search terms.
    Still valuable for understanding what query categories drive conversions.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    cid = resolve_customer_id(customer_id)
    query = f"""
        SELECT campaign_search_term_insight.category_label,
               metrics.impressions, metrics.clicks,
               metrics.conversions, metrics.conversions_value,
               metrics.cost_micros
        FROM campaign_search_term_insight
        WHERE campaign_search_term_insight.campaign_id = {campaign_id}
          AND metrics.conversions > 0
        ORDER BY metrics.conversions DESC
        LIMIT 200
    """
    try:
        results = _execute_gaql(cid, query, page_size=200)
    except Exception:
        # PMax insight may not be available for all campaigns
        return []

    keywords: List[Dict[str, Any]] = []
    for row in results:
        category = str(_safe_get_value(row, "campaign_search_term_insight.category_label", ""))
        impressions = int(_safe_get_value(row, "metrics.impressions", 0))
        clicks = int(_safe_get_value(row, "metrics.clicks", 0))
        conversions = float(_safe_get_value(row, "metrics.conversions", 0))
        conv_value = float(_safe_get_value(row, "metrics.conversions_value", 0))
        cost_micros = int(_safe_get_value(row, "metrics.cost_micros", 0))

        if not category:
            continue

        cost_approx = cost_micros / 1_000_000
        ctr = (clicks / impressions * 100) if impressions > 0 else 0
        roas = (conv_value / cost_approx) if cost_approx > 0 else 0

        keywords.append({
            "product_name": "pmax_aggregate",
            "product_handle": "",
            "country_code": country_code,
            "keyword_text": category,
            "match_type": "PMAX_CATEGORY",  # Marks this as PMax category insight
            "source_campaign_id": campaign_id,
            "source_campaign_name": campaign_name,
            "source_type": "pmax",
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "conversion_value": round(conv_value, 2),
            "cost_micros": cost_micros,
            "quality_score": None,
            "ctr": round(ctr, 2),
            "conv_rate": round(conversions / clicks * 100, 2) if clicks > 0 else 0,
            "cpc_micros": round(cost_micros / clicks) if clicks > 0 else 0,
            "roas": round(roas, 2),
            "roi_percent": round(((conversions * 100) / cost_approx - 100), 1) if cost_approx > 0 else 0,
        })

    return keywords


def _mine_keyword_view_from_search_campaign(
    campaign_id: str,
    campaign_name: str,
    country_code: str,
    ag_handle_map: Dict[str, str],
    days_back: int = 0,
    customer_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Mine converting keywords (keyword_view) from a Search campaign.
    Complements search_term_view — keyword_view shows configured keywords + quality scores.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    cid = resolve_customer_id(customer_id)
    date_filter = ""
    if days_back > 0:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        date_filter = f"AND segments.date BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
        SELECT campaign.id, ad_group.id,
               ad_group_criterion.keyword.text,
               ad_group_criterion.keyword.match_type,
               ad_group_criterion.quality_info.quality_score,
               metrics.impressions, metrics.clicks,
               metrics.conversions, metrics.conversions_value,
               metrics.cost_micros
        FROM keyword_view
        WHERE campaign.id = {campaign_id}
          AND metrics.conversions > 0
          {date_filter}
        ORDER BY metrics.conversions DESC
        LIMIT 500
    """
    try:
        results = _execute_gaql(cid, query, page_size=500)
    except Exception:
        return []

    keywords: List[Dict[str, Any]] = []
    for row in results:
        kw_text = str(_safe_get_value(row, "ad_group_criterion.keyword.text", ""))
        match_type = str(_safe_get_value(row, "ad_group_criterion.keyword.match_type", ""))
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        quality_score = _safe_get_value(row, "ad_group_criterion.quality_info.quality_score", None)
        impressions = int(_safe_get_value(row, "metrics.impressions", 0))
        clicks = int(_safe_get_value(row, "metrics.clicks", 0))
        conversions = float(_safe_get_value(row, "metrics.conversions", 0))
        conv_value = float(_safe_get_value(row, "metrics.conversions_value", 0))
        cost_micros = int(_safe_get_value(row, "metrics.cost_micros", 0))

        if not kw_text:
            continue

        product_handle = ag_handle_map.get(ag_id, "")
        product_name = _normalize_product_name(product_handle) if product_handle else "unknown"

        cost_approx = cost_micros / 1_000_000
        ctr = (clicks / impressions * 100) if impressions > 0 else 0
        roas = (conv_value / cost_approx) if cost_approx > 0 else 0
        roi_percent = ((conversions * 100) / cost_approx - 100) if cost_approx > 0 else 0

        keywords.append({
            "product_name": product_name,
            "product_handle": product_handle,
            "country_code": country_code,
            "keyword_text": kw_text,
            "match_type": match_type.replace("MatchType.", ""),
            "source_campaign_id": campaign_id,
            "source_campaign_name": campaign_name,
            "source_type": "keyword",
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "conversion_value": round(conv_value, 2),
            "cost_micros": cost_micros,
            "quality_score": int(quality_score) if quality_score else None,
            "ctr": round(ctr, 2),
            "conv_rate": round(conversions / clicks * 100, 2) if clicks > 0 else 0,
            "cpc_micros": round(cost_micros / clicks) if clicks > 0 else 0,
            "roas": round(roas, 2),
            "roi_percent": round(roi_percent, 1),
        })

    return keywords


def mine_country_keywords(
    country_code: str,
    days_back: int = 0,
    min_conversions: float = 1.0,
) -> Dict[str, Any]:
    """
    Mine ALL converting keywords from ALL campaigns (Search + PMax) for a country.

    Flow:
    1. Find all Search + PMax campaigns matching country_code
    2. For each Search campaign:
       a. Build ad_group → product_handle map (from RSA final_urls)
       b. Query search_term_view (actual search queries → conversions)
       c. Query keyword_view (configured keywords + quality scores)
    3. For each PMax campaign:
       a. Query campaign_search_term_insight (category-level search data)
    4. Aggregate all keywords by (keyword_text, product_handle)
    5. Store permanently in keyword_intelligence DB

    Args:
        country_code: Country code (e.g., PL, RO, TR)
        days_back: Days to look back (0 = all-time, recommended for full mining)
        min_conversions: Minimum conversions to include in results

    Returns:
        Summary with counts per campaign type and top keywords per product.
    """
    t0 = time.time()
    db = _get_db()
    cc = country_code.upper()

    # 1. Find ALL campaigns for this country
    campaigns = _find_all_campaigns_for_country(cc, include_paused=True)
    if not campaigns:
        return {
            "status": "error",
            "message": f"No campaigns found for country code '{cc}'",
        }

    search_campaigns = [c for c in campaigns if "SEARCH" in c["type"]]
    pmax_campaigns = [c for c in campaigns if "PERFORMANCE_MAX" in c["type"]]
    shopping_campaigns = [c for c in campaigns if "SHOPPING" in c["type"]]

    all_keywords: List[Dict[str, Any]] = []
    stats = {
        "campaigns_found": len(campaigns),
        "search_campaigns": len(search_campaigns),
        "pmax_campaigns": len(pmax_campaigns),
        "shopping_campaigns": len(shopping_campaigns),
        "search_terms_mined": 0,
        "keywords_mined": 0,
        "pmax_insights_mined": 0,
        "campaigns_processed": [],
    }

    # 2. Mine Search campaigns
    for cmp in search_campaigns:
        cmp_id = cmp["id"]
        cmp_name = cmp["name"]

        # Build ad_group → handle mapping for this campaign
        ag_handle_map = _build_ag_to_handle_map(cmp_id)

        # Mine search terms (actual user queries)
        search_terms = _mine_search_terms_from_search_campaign(
            cmp_id, cmp_name, cc, ag_handle_map, days_back
        )
        all_keywords.extend(search_terms)
        stats["search_terms_mined"] += len(search_terms)

        # Mine keyword_view (configured keywords with quality scores)
        kw_view = _mine_keyword_view_from_search_campaign(
            cmp_id, cmp_name, cc, ag_handle_map, days_back
        )
        all_keywords.extend(kw_view)
        stats["keywords_mined"] += len(kw_view)

        stats["campaigns_processed"].append({
            "id": cmp_id,
            "name": cmp_name,
            "type": "SEARCH",
            "status": cmp["status"],
            "search_terms": len(search_terms),
            "keywords": len(kw_view),
            "handles_mapped": len(ag_handle_map),
        })

    # 3. Mine PMax campaigns
    for cmp in pmax_campaigns:
        cmp_id = cmp["id"]
        cmp_name = cmp["name"]

        pmax_insights = _mine_search_terms_from_pmax_campaign(cmp_id, cmp_name, cc)
        all_keywords.extend(pmax_insights)
        stats["pmax_insights_mined"] += len(pmax_insights)

        stats["campaigns_processed"].append({
            "id": cmp_id,
            "name": cmp_name,
            "type": "PMAX",
            "status": cmp["status"],
            "pmax_insights": len(pmax_insights),
        })

    # 4. Store all in DB (permanent cache)
    # Filter out extra fields not in DB schema before storing
    db_records = []
    for kw in all_keywords:
        db_records.append({
            "product_name": kw["product_name"],
            "country_code": kw["country_code"],
            "keyword_text": kw["keyword_text"],
            "match_type": kw["match_type"],
            "source_campaign_id": kw["source_campaign_id"],
            "source_campaign_name": kw["source_campaign_name"],
            "impressions": kw["impressions"],
            "clicks": kw["clicks"],
            "conversions": kw["conversions"],
            "conversion_value": kw.get("conversion_value", 0),
            "cost_micros": kw["cost_micros"],
            "quality_score": kw.get("quality_score"),
            "ctr": kw["ctr"],
            "conv_rate": kw["conv_rate"],
            "cpc_micros": kw["cpc_micros"],
            "roas": kw["roas"],
            "roi_percent": kw["roi_percent"],
        })
    db.bulk_upsert_keyword_intel(db_records)

    # 5. Aggregate by product handle for the summary
    product_keywords: Dict[str, Dict[str, Any]] = {}
    for kw in all_keywords:
        handle = kw.get("product_handle", "") or kw.get("product_name", "unknown")
        if handle not in product_keywords:
            product_keywords[handle] = {
                "handle": handle,
                "product_name": kw["product_name"],
                "total_keywords": 0,
                "total_conversions": 0,
                "total_conversion_value": 0,
                "total_cost_micros": 0,
                "top_keywords": [],
            }
        pk = product_keywords[handle]
        pk["total_keywords"] += 1
        pk["total_conversions"] += kw["conversions"]
        pk["total_conversion_value"] += kw.get("conversion_value", 0)
        pk["total_cost_micros"] += kw["cost_micros"]

    # Add top 5 keywords per product
    kw_by_product: Dict[str, List[Dict[str, Any]]] = {}
    for kw in all_keywords:
        handle = kw.get("product_handle", "") or kw.get("product_name", "unknown")
        kw_by_product.setdefault(handle, []).append(kw)

    for handle, kws in kw_by_product.items():
        top = sorted(kws, key=lambda x: x["conversions"], reverse=True)[:5]
        if handle in product_keywords:
            product_keywords[handle]["top_keywords"] = [
                {
                    "text": k["keyword_text"],
                    "type": k["match_type"],
                    "source": k.get("source_type", ""),
                    "conv": k["conversions"],
                    "roas": k["roas"],
                }
                for k in top
            ]
            # Round totals
            pk = product_keywords[handle]
            pk["total_conversions"] = round(pk["total_conversions"], 1)
            pk["total_conversion_value"] = round(pk["total_conversion_value"], 2)
            total_cost = pk["total_cost_micros"] / 1_000_000
            pk["overall_roas"] = round(pk["total_conversion_value"] / total_cost, 2) if total_cost > 0 else 0
            del pk["total_cost_micros"]  # Don't expose raw micros

    # Sort products by conversions DESC
    sorted_products = sorted(
        product_keywords.values(),
        key=lambda x: x["total_conversions"],
        reverse=True,
    )

    duration = round(time.time() - t0, 1)

    return {
        "status": "success",
        "country_code": cc,
        "days_back": days_back if days_back > 0 else "all-time",
        "duration_seconds": duration,
        "total_keywords_mined": len(all_keywords),
        "total_keywords_stored": len(db_records),
        "stats": stats,
        "products": sorted_products[:50],  # Top 50 products
    }


# ---------------------------------------------------------------------------
# MCP Tool Registration
# ---------------------------------------------------------------------------

class MineCountryKeywordsRequest(BaseModel):
    """Input for batch_mine_country_keywords tool."""
    country_code: str = Field(..., description="Country code (e.g., PL, RO, TR, HU)")
    days_back: int = Field(0, description="Days to look back (0 = all-time). Use 90 for recent, 365 for last year.")
    min_conversions: float = Field(1.0, description="Minimum conversions to include")
    customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID.")

class MineKeywordsProgressRequest(BaseModel):
    """Input for batch_mine_keywords_progress tool."""
    job_id: str = Field(..., description="Job ID returned by batch_mine_country_keywords")

class KeywordResearchRequest(BaseModel):
    """Input for batch_keyword_research tool."""
    country_code: str = Field(..., description="Country code (e.g., RO, TR, PL)")
    product_handle: str = Field(..., description="Product handle (e.g., running-shoes-us)")
    min_roi_percent: float = Field(50.0, description="Minimum ROI % threshold for recommendations")
    days_back: int = Field(90, description="Days to look back (default 90). Use 30 for recent, 180 for broader, 0 for all-time")
    customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID.")

class CategoryKeywordsRequest(BaseModel):
    """Input for batch_category_keywords tool."""
    country_code: str = Field(..., description="Country code")
    category: str = Field(..., description=f"Product category: {', '.join(CATEGORY_PATTERNS.keys())}")
    min_conversions: float = Field(3.0, description="Min total conversions filter")
    customer_id: str = Field(default=CUSTOMER_ID, description="Google Ads Customer ID.")


def _mine_country_keywords_worker(
    job_id: str,
    country_code: str,
    days_back: int,
    min_conversions: float,
    search_campaigns: List[Dict],
    pmax_campaigns: List[Dict],
    total_campaigns: int,
    customer_id: Optional[str] = None,
) -> None:
    """Background worker for mining keywords across all campaigns."""
    cid = resolve_customer_id(customer_id)
    try:
        db = _get_db()
        all_keywords: List[Dict[str, Any]] = []
        stats = {
            "campaigns_found": total_campaigns,
            "search_campaigns": len(search_campaigns),
            "pmax_campaigns": len(pmax_campaigns),
            "search_terms_mined": 0,
            "keywords_mined": 0,
            "pmax_insights_mined": 0,
            "campaigns_processed": [],
        }

        total_to_process = len(search_campaigns) + len(pmax_campaigns)
        processed = 0

        # Mine Search campaigns
        for cmp in search_campaigns:
            cmp_id = cmp["id"]
            cmp_name = cmp["name"]

            with _mining_jobs_lock:
                if job_id in _mining_jobs:
                    _mining_jobs[job_id].update({
                        "current_campaign": cmp_name,
                        "current_phase": "search_terms",
                        "processed": processed,
                        "percent": round(processed / total_to_process * 100) if total_to_process else 0,
                    })

            # Build ad_group → handle mapping
            ag_handle_map = _build_ag_to_handle_map(cmp_id, customer_id=cid)

            # Mine search terms
            search_terms = _mine_search_terms_from_search_campaign(
                cmp_id, cmp_name, country_code, ag_handle_map, days_back, customer_id=cid
            )
            all_keywords.extend(search_terms)
            stats["search_terms_mined"] += len(search_terms)

            # Mine keyword_view
            kw_view = _mine_keyword_view_from_search_campaign(
                cmp_id, cmp_name, country_code, ag_handle_map, days_back, customer_id=cid
            )
            all_keywords.extend(kw_view)
            stats["keywords_mined"] += len(kw_view)

            stats["campaigns_processed"].append({
                "id": cmp_id, "name": cmp_name, "type": "SEARCH",
                "status": cmp["status"],
                "search_terms": len(search_terms), "keywords": len(kw_view),
                "handles_mapped": len(ag_handle_map),
            })
            processed += 1

        # Mine PMax campaigns
        for cmp in pmax_campaigns:
            cmp_id = cmp["id"]
            cmp_name = cmp["name"]

            with _mining_jobs_lock:
                if job_id in _mining_jobs:
                    _mining_jobs[job_id].update({
                        "current_campaign": cmp_name,
                        "current_phase": "pmax_insight",
                        "processed": processed,
                        "percent": round(processed / total_to_process * 100) if total_to_process else 0,
                    })

            pmax_insights = _mine_search_terms_from_pmax_campaign(cmp_id, cmp_name, country_code, customer_id=cid)
            all_keywords.extend(pmax_insights)
            stats["pmax_insights_mined"] += len(pmax_insights)

            stats["campaigns_processed"].append({
                "id": cmp_id, "name": cmp_name, "type": "PMAX",
                "status": cmp["status"],
                "pmax_insights": len(pmax_insights),
            })
            processed += 1

        # Store in DB
        db_records = []
        for kw in all_keywords:
            db_records.append({
                "product_name": kw["product_name"],
                "country_code": kw["country_code"],
                "keyword_text": kw["keyword_text"],
                "match_type": kw["match_type"],
                "source_campaign_id": kw["source_campaign_id"],
                "source_campaign_name": kw["source_campaign_name"],
                "impressions": kw["impressions"],
                "clicks": kw["clicks"],
                "conversions": kw["conversions"],
                "conversion_value": kw.get("conversion_value", 0),
                "cost_micros": kw["cost_micros"],
                "quality_score": kw.get("quality_score"),
                "ctr": kw["ctr"],
                "conv_rate": kw["conv_rate"],
                "cpc_micros": kw["cpc_micros"],
                "roas": kw["roas"],
                "roi_percent": kw["roi_percent"],
            })
        db.bulk_upsert_keyword_intel(db_records)

        # Build product summary
        product_keywords: Dict[str, Dict[str, Any]] = {}
        for kw in all_keywords:
            handle = kw.get("product_handle", "") or kw.get("product_name", "unknown")
            if handle not in product_keywords:
                product_keywords[handle] = {
                    "handle": handle, "product_name": kw["product_name"],
                    "total_keywords": 0, "total_conversions": 0,
                    "total_conversion_value": 0, "total_cost_micros": 0,
                    "top_keywords": [],
                }
            pk = product_keywords[handle]
            pk["total_keywords"] += 1
            pk["total_conversions"] += kw["conversions"]
            pk["total_conversion_value"] += kw.get("conversion_value", 0)
            pk["total_cost_micros"] += kw["cost_micros"]

        kw_by_product: Dict[str, List[Dict[str, Any]]] = {}
        for kw in all_keywords:
            handle = kw.get("product_handle", "") or kw.get("product_name", "unknown")
            kw_by_product.setdefault(handle, []).append(kw)

        for handle, kws in kw_by_product.items():
            top = sorted(kws, key=lambda x: x["conversions"], reverse=True)[:5]
            if handle in product_keywords:
                product_keywords[handle]["top_keywords"] = [
                    {"text": k["keyword_text"], "type": k["match_type"],
                     "source": k.get("source_type", ""), "conv": k["conversions"],
                     "roas": k["roas"]}
                    for k in top
                ]
                pk = product_keywords[handle]
                pk["total_conversions"] = round(pk["total_conversions"], 1)
                pk["total_conversion_value"] = round(pk["total_conversion_value"], 2)
                total_cost = pk["total_cost_micros"] / 1_000_000
                pk["overall_roas"] = round(pk["total_conversion_value"] / total_cost, 2) if total_cost > 0 else 0
                del pk["total_cost_micros"]

        sorted_products = sorted(
            product_keywords.values(), key=lambda x: x["total_conversions"], reverse=True
        )

        # Final update
        with _mining_jobs_lock:
            if job_id in _mining_jobs:
                _mining_jobs[job_id].update({
                    "status": "completed",
                    "processed": total_to_process,
                    "percent": 100,
                    "current_campaign": "",
                    "current_phase": "done",
                    "total_keywords_mined": len(all_keywords),
                    "total_keywords_stored": len(db_records),
                    "stats": stats,
                    "products": sorted_products[:50],
                })

    except Exception as e:
        with _mining_jobs_lock:
            if job_id in _mining_jobs:
                _mining_jobs[job_id].update({
                    "status": "error",
                    "error": str(e),
                    "current_phase": "failed",
                })


def register_intelligence_tools(mcp_app):
    """Register keyword intelligence MCP tools."""

    @mcp_app.tool()
    async def batch_mine_country_keywords(request: MineCountryKeywordsRequest) -> dict:
        """
        Mine ALL converting keywords from ALL campaigns (Search + PMax) for a country.

        ASYNC mode — spawns a background thread and returns a job_id
        immediately (<1s). Poll progress with batch_mine_keywords_progress(job_id).

        Scans every Search and Performance Max campaign matching the country code,
        extracts search terms (actual user queries) and keywords that generated
        conversions, maps them to product handles via ad_group → URL mapping,
        and stores PERMANENTLY in the keyword_intelligence DB.

        Data sources:
        - Search campaigns: search_term_view (actual queries) + keyword_view (configured keywords)
        - PMax campaigns: campaign_search_term_insight (category-level search data)
        - Shopping campaigns: NOT available via API (product-level only)

        Use days_back=0 for full historical mining (recommended for first run).
        Results are cached permanently — run periodically to refresh.
        """
        cc = request.country_code.upper()
        cid = resolve_customer_id(getattr(request, 'customer_id', None))

        # 1. Find campaigns (quick — single GAQL call)
        campaigns = _find_all_campaigns_for_country(cc, cid, include_paused=True)
        if not campaigns:
            return {"status": "error", "message": f"No campaigns found for '{cc}'"}

        search_campaigns = [c for c in campaigns if "SEARCH" in c["type"]]
        pmax_campaigns = [c for c in campaigns if "PERFORMANCE_MAX" in c["type"]]
        total_to_process = len(search_campaigns) + len(pmax_campaigns)

        if total_to_process == 0:
            return {
                "status": "success",
                "message": f"Found {len(campaigns)} campaigns but none are Search or PMax",
                "campaigns_found": len(campaigns),
            }

        # 2. Spawn background worker
        job_id = uuid.uuid4().hex[:16]

        initial_progress = {
            "job_id": job_id,
            "country_code": cc,
            "days_back": request.days_back if request.days_back > 0 else "all-time",
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total": total_to_process,
            "processed": 0,
            "percent": 0,
            "status": "running",
            "current_campaign": "",
            "current_phase": "starting",
            "search_campaigns": len(search_campaigns),
            "pmax_campaigns": len(pmax_campaigns),
        }

        with _mining_jobs_lock:
            _mining_jobs[job_id] = initial_progress.copy()

        worker = threading.Thread(
            target=_mine_country_keywords_worker,
            args=(
                job_id, cc, request.days_back, request.min_conversions,
                search_campaigns, pmax_campaigns, len(campaigns), cid,
            ),
            daemon=True,
            name=f"mine_kw_{cc}_{job_id[:8]}",
        )
        worker.start()

        return {
            "status": "submitted",
            "job_id": job_id,
            "message": f"Background job started. Mining {len(search_campaigns)} Search + {len(pmax_campaigns)} PMax campaigns for {cc}. Poll with batch_mine_keywords_progress(job_id='{job_id}').",
            "search_campaigns": len(search_campaigns),
            "pmax_campaigns": len(pmax_campaigns),
            "total_campaigns": len(campaigns),
        }

    @mcp_app.tool()
    async def batch_mine_keywords_progress(request: MineKeywordsProgressRequest) -> dict:
        """
        Check status of a batch_mine_country_keywords background job.

        Returns current progress: processed/total campaigns, keywords found,
        current campaign being processed, and percent complete.

        Call this every 15-30s after batch_mine_country_keywords returns status="submitted".
        When status is "completed", the job is done and results are included.
        """
        with _mining_jobs_lock:
            progress = _mining_jobs.get(request.job_id)

        if not progress:
            return {"status": "error", "message": f"Job {request.job_id} not found"}

        return progress

    @mcp_app.tool()
    async def batch_keyword_research(request: KeywordResearchRequest) -> dict:
        """
        Mine keywords from ALL historical campaigns for a product.

        Searches across all Search campaigns (active and paused) to find
        keywords that converted for this product. Aggregates data, calculates ROI,
        and recommends keywords meeting the ROI threshold.

        Also finds related category keywords from similar products.
        Use before batch_setup_products to get proven keywords instead of guessing.
        """
        t0 = time.time()
        cid = resolve_customer_id(getattr(request, 'customer_id', None))
        result = mine_keywords_for_product(
            product_handle=request.product_handle,
            country_code=request.country_code.upper(),
            min_roi_percent=request.min_roi_percent,
            days_back=request.days_back,
            customer_id=cid,
        )
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return {"status": "success", **result}

    @mcp_app.tool()
    async def batch_category_keywords(request: CategoryKeywordsRequest) -> dict:
        """
        Get keywords shared across products in the same category.

        Finds proven converting keywords that work for similar products
        (e.g., similar products share keywords like 'best price', 'free shipping').
        Useful for new products without their own keyword history.
        """
        db = _get_db()
        kws = db.get_category_keywords(
            request.category, request.country_code.upper(),
            min_conversions=request.min_conversions,
        )
        return {
            "status": "success",
            "category": request.category,
            "country_code": request.country_code.upper(),
            "keywords": kws,
            "count": len(kws),
        }

    return ["batch_mine_country_keywords", "batch_mine_keywords_progress", "batch_keyword_research", "batch_category_keywords"]
