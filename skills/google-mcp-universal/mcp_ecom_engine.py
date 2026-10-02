"""
E-com Engine Module — Producthero Labelizer + Channable Rules + PMax Strategy

Combines data-science product segmentation (Heroes/Villains/Sidekicks/Zombies)
with feed transformation rules and campaign architecture recommendations.

Tools:
  - ecom_labelizer_run: Classify products by performance (Producthero matrix)
  - ecom_channable_transform: Apply IF-THEN feed rules to product data
  - ecom_pmax_strategy: Generate campaign architecture from Labelizer output
  - ecom_feed_compare: Compare two merchant feeds to find unique products
  - ecom_title_optimizer: Generate SEO-optimized titles for product feeds
"""

import json
import re
import csv
import io
import statistics
import ssl
import urllib.request
from typing import Optional, List
from collections import defaultdict
from pydantic import BaseModel, Field
from datetime import datetime, timedelta

import google_ads_mcp as _gam

from google_ads_mcp import (
    mcp,
    _ensure_client,
    _ensure_merchant_client,
    _format_customer_id,
    _track_api_call,
    _execute_gaql,
    _safe_get_value,
    merchant_center_client,
)
from accounts_config import load_config, get_customer_id, get_domains, get_merchant_ids

# Outbound HTTPS to your own endpoints keeps certificate verification on.
_ssl_ctx = ssl.create_default_context()

# ============================================================================
# BACKEND DATA INTEGRATION — optional order/conversion CSV from your own backend
# ============================================================================
# config.json:
#   "backend_conversions": {
#     "url": "https://backend.example.com/orders.csv?from={date_start}&to={date_end}&country={country}",
#     "columns": {"handle": "productHandle", "value": "ConversionValue", "partner": "partnerIdent",
#                 "click_id": "sub1", "order_id": "orderControlTransID"}
#   }
# The CSV needs one row per order; "click_id" holds the Google click ID when the order came from an ad.

_DEFAULT_BACKEND_COLUMNS = {
    "handle": "productHandle", "value": "ConversionValue", "partner": "partnerIdent",
    "click_id": "sub1", "order_id": "orderControlTransID",
}


def _backend_config() -> dict:
    return load_config().get("backend_conversions", {}) or {}


def _fetch_backend_conversions(country_code: str, days_back: int = 180) -> dict:
    """
    Fetch real conversion data from your backend (config "backend_conversions") for a given country.
    Returns dict keyed by normalized product handle → {count, total_value, orders}.
    """
    cfg = _backend_config()
    template = cfg.get("url", "")
    if not template:
        return {"_error": "Backend cross-validation is not configured: set backend_conversions.url in config.json"}
    cols = {**_DEFAULT_BACKEND_COLUMNS, **(cfg.get("columns") or {})}
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    url = template.format(date_start=start_date.strftime('%Y-%m-%d'), date_end=end_date.strftime('%Y-%m-%d'),
                          country=country_code.upper())

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'EcomEngine/1.0'})
        with urllib.request.urlopen(req, timeout=30, context=_ssl_ctx) as response:
            raw_data = response.read().decode('utf-8')
    except Exception as e:
        return {"_error": f"Backend fetch failed: {str(e)}", "_url": url}

    # Parse CSV
    products = defaultdict(lambda: {
        'count': 0,
        'total_value': 0.0,
        'orders': [],
        'partners': defaultdict(int),
        'gads_attributed': 0,  # orders with gclid in sub1
        'gads_value': 0.0,
    })

    try:
        reader = csv.DictReader(io.StringIO(raw_data))
        for row in reader:
            handle = row.get(cols['handle'], '').strip()
            if not handle:
                continue

            value = float(row.get(cols['value'], 0) or 0)
            partner = row.get(cols['partner'], '')
            gclid = row.get(cols['click_id'], '').strip()

            # Normalize handle: "vitamin-c-hu" → "vitamin c"
            norm_handle = _normalize_product_handle(handle, country_code)

            p = products[norm_handle]
            p['count'] += 1
            p['total_value'] += value
            p['partners'][partner] += 1

            if gclid and len(gclid) > 10:  # Valid gclid
                p['gads_attributed'] += 1
                p['gads_value'] += value

            # Keep first 5 order IDs for reference
            order_id = row.get(cols['order_id'], '')
            if len(p['orders']) < 5:
                p['orders'].append(order_id)

    except Exception as e:
        return {"_error": f"CSV parse failed: {str(e)}", "_url": url}

    # Convert defaultdicts to regular dicts for JSON serialization
    result = {}
    for handle, data in products.items():
        result[handle] = {
            'count': data['count'],
            'total_value': round(data['total_value'], 2),
            'gads_attributed': data['gads_attributed'],
            'gads_value': round(data['gads_value'], 2),
            'partners': dict(data['partners']),
            'sample_orders': data['orders'],
        }

    return result


def _normalize_product_handle(handle: str, country_code: str = "") -> str:
    """
    Normalize backend product handle to match Google Ads product titles.
    'vitamin-c-hu' → 'vitamin c'
    'sleep-well-plus-de' → 'sleep well+'
    """
    h = handle.lower().strip()
    # Remove country suffix
    cc = country_code.lower()
    if cc and h.endswith(f'-{cc}'):
        h = h[:-len(cc)-1]
    # Replace hyphens with spaces
    h = h.replace('-', ' ')
    # Common product name fixes
    h = h.replace('plus', '+')
    h = h.replace(' low price', '')
    h = h.replace(' full price', '')
    h = h.replace(' middle price', '')
    h = h.replace(' mid price', '')
    return h.strip()


def _match_backend_to_gads(backend_data: dict, gads_products: dict) -> dict:
    """
    Match backend product handles to Google Ads product titles.
    Returns merged data with both sources.
    """
    matches = {}
    unmatched_backend = {}

    for handle, bdata in backend_data.items():
        if handle.startswith('_'):  # skip _error keys
            continue

        matched = False
        for item_id, gdata in gads_products.items():
            gads_title_norm = _normalize_product_title(gdata.get('title', '')).lower()
            # Try various matching strategies
            if (handle in gads_title_norm or
                gads_title_norm in handle or
                _fuzzy_product_match(handle, gads_title_norm)):
                matches[item_id] = {
                    'gads': gdata,
                    'backend': bdata,
                    'backend_handle': handle,
                    'match_type': 'exact' if handle == gads_title_norm else 'fuzzy',
                }
                matched = True
                break

        if not matched:
            unmatched_backend[handle] = bdata

    return {'matches': matches, 'unmatched_backend': unmatched_backend}


def _fuzzy_product_match(handle: str, title: str) -> bool:
    """Simple fuzzy match: check if main words overlap."""
    handle_words = set(handle.split())
    title_words = set(title.split())
    # Remove very common words
    stopwords = {'low', 'price', 'full', 'mid', 'the', 'and', 'for', 'caps', 'gel', 'cream'}
    handle_words -= stopwords
    title_words -= stopwords
    if not handle_words or not title_words:
        return False
    overlap = handle_words & title_words
    return len(overlap) >= max(1, min(len(handle_words), len(title_words)) * 0.6)


# ============================================================================
# REQUEST MODELS
# ============================================================================

class LabelizerRequest(BaseModel):
    """Producthero Labelizer: classify products into performance segments."""
    customer_id: str = Field(..., description="Google Ads Customer ID")
    campaign_ids: Optional[List[str]] = Field(
        None, description="Campaign IDs to analyze. If empty, analyzes all Shopping + PMax campaigns."
    )
    country_code: str = Field(
        "", description="Country code (e.g., 'HU', 'PL', 'DE') for backend conversion data. If provided and backend_conversions is configured, fetches real conversions from your backend to cross-validate Google Ads attribution."
    )
    target_roas: float = Field(
        2.0, description="Target ROAS threshold for Hero/Villain classification"
    )
    days_back: int = Field(
        180, description="Performance lookback window in days (default: 180 for statistical significance)", ge=7, le=180
    )
    min_clicks_zombie: int = Field(
        5, description="Products with fewer clicks than this are Zombies"
    )
    include_custom_labels: bool = Field(
        True, description="If true, generates Custom Label assignments for Merchant Center"
    )


class ChannableRulesRequest(BaseModel):
    """Channable-style IF-THEN rules engine for feed transformation."""
    customer_id: str = Field(..., description="Google Ads Customer ID")
    merchant_id: str = Field(..., description="Merchant Center ID to pull products from")
    rules: List[str] = Field(
        ...,
        description=(
            "List of transformation rules in natural language or pseudo-code. "
            "Examples: 'IF brand IS EMPTY THEN extract_from_title', "
            "'IF price < 20 THEN exclude', "
            "'IF title CONTAINS \"low price\" THEN custom_label_1 = \"discount\"'"
        )
    )
    feed_label: Optional[str] = Field(
        None, description="Filter products by feed_label (e.g., 'DE', 'PL')"
    )
    dry_run: bool = Field(
        True, description="If true, show what would change without applying. Set false to apply via MC API."
    )


class PMaxStrategyRequest(BaseModel):
    """Generate PMax/Shopping campaign architecture from Labelizer output."""
    customer_id: str = Field(..., description="Google Ads Customer ID")
    campaign_ids: Optional[List[str]] = Field(
        None, description="Campaign IDs to analyze (defaults to all Shopping+PMax)"
    )
    target_roas: float = Field(2.0, description="Target ROAS")
    days_back: int = Field(180, description="Lookback window (default: 180 days)", ge=7, le=180)
    daily_budget_gbp: float = Field(
        50.0, description="Total daily budget across all segments in GBP"
    )


class FeedCompareRequest(BaseModel):
    """Compare products between two merchant accounts."""
    customer_id: str = Field(..., description="Google Ads Customer ID")
    merchant_a_id: str = Field(..., description="First Merchant Center ID (e.g., your main store)")
    merchant_b_id: str = Field(..., description="Second Merchant Center ID (e.g., a second store or brand)")
    merchant_a_name: str = Field("Merchant A", description="Label for first merchant")
    merchant_b_name: str = Field("Merchant B", description="Label for second merchant")
    only_eligible: bool = Field(
        False, description="Only compare ELIGIBLE products"
    )


class TitleOptimizerRequest(BaseModel):
    """SEO title optimizer for product feeds."""
    customer_id: str = Field(..., description="Google Ads Customer ID")
    merchant_id: str = Field(..., description="Merchant Center ID")
    feed_label: Optional[str] = Field(None, description="Filter by feed label")
    industry: str = Field(
        "health_supplements",
        description="Industry for title template selection",
        enum=["health_supplements", "fashion", "electronics", "home", "beauty", "other"]
    )
    max_products: int = Field(50, description="Max products to optimize", ge=1, le=200)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _normalize_product_title(title: str) -> str:
    """Normalize title for cross-merchant comparison."""
    t = title.lower().strip()
    # Strip pricing variants
    t = re.sub(r'\s*\d+\s*[€£$].*$', '', t)
    t = re.sub(r'\s*\d+\s*(eur|gbp|usd|pln|czk|ron|huf).*$', '', t, flags=re.IGNORECASE)
    # Strip review/composition suffixes (multilingual)
    t = re.sub(r'\s*-\s*(reseñas|bewertungen|recenze|recensioni|avis|hodnotenia|opinie|recenzii).*$', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*-\s*(composición|zusammensetzung|složení|composizione|composition|skład|compoziție).*$', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*-\s*(offizielle|oficjalna|oficial).*$', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*-\s*(niedrigster|lowest|najniższa).*$', '', t, flags=re.IGNORECASE)
    # Strip variant qualifiers
    for suffix in ['low price', 'low', 'full price', 'full', 'mid price', 'middle price',
                   'adult', 'caps', 'capsules', 'cream', 'gel', 'spray', 'turbo',
                   'forte', 'plus', 'premium', 'pro', 'max', 'mini']:
        t = re.sub(rf'\s+{re.escape(suffix)}$', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*\(low\s*price\).*$', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*\(off\)\s*', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*\(.*?price.*?\)', '', t, flags=re.IGNORECASE)
    return t.strip().strip('-').strip()


def _classify_product(product: dict, target_roas: float, median_clicks: float,
                      median_cost: float, min_clicks_zombie: int) -> str:
    """Classify a single product into Producthero segment."""
    clicks = product.get('clicks', 0)
    cost = product.get('cost', 0)
    conv_value = product.get('conversion_value', 0)
    roas = conv_value / cost if cost > 0 else 0

    if clicks < min_clicks_zombie:
        return 'ZOMBIE'
    elif roas >= target_roas and clicks >= median_clicks:
        return 'HERO'
    elif roas >= target_roas and clicks < median_clicks:
        return 'SIDEKICK'
    elif roas < target_roas and clicks >= median_clicks:
        return 'VILLAIN'
    else:
        # Low clicks, low ROAS but above zombie threshold
        return 'ZOMBIE'


# ============================================================================
# MCP TOOLS
# ============================================================================


@mcp.tool()
async def ecom_labelizer_run(request: LabelizerRequest) -> str:
    """
    🏷️ Producthero Labelizer 2.0: Classify products into Hero/Villain/Sidekick/Zombie segments.

    Pulls shopping product performance data from Google Ads, calculates ROAS per product,
    and classifies each into a performance bucket using the Producthero matrix:
    - 🏆 HERO: High spend + Above-target ROAS (scale these)
    - 📉 VILLAIN: High spend + Below-target ROAS (reduce bids or exclude)
    - 🐣 SIDEKICK: Low spend + Above-target ROAS (increase visibility)
    - 🧟 ZOMBIE: Near-zero clicks (algorithmic blind spot — needs reactivation)

    Returns segment statistics, product lists per segment, and Custom Label assignments.
    """
    _ensure_client()
    cid = _format_customer_id(request.customer_id)
    _track_api_call("gaql_query")

    # Build campaign filter
    campaign_filter = ""
    if request.campaign_ids:
        ids = ", ".join(request.campaign_ids)
        campaign_filter = f"AND campaign.id IN ({ids})"

    # Build merchant filter — if country_code provided, filter to that country's merchant
    merchant_filter = ""
    if request.country_code:
        _merchant_id = _MERCHANT_IDS.get(request.country_code.upper())
        if _merchant_id:
            merchant_filter = f"AND segments.product_merchant_id = {_merchant_id}"

    # Build date range using explicit dates (GAQL DURING only supports fixed periods)
    from datetime import datetime as dt, timedelta
    end_date = dt.now()
    start_date = end_date - timedelta(days=request.days_back)
    date_range = f"segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'"

    # Query shopping product performance
    query = f"""
        SELECT
            segments.product_item_id,
            segments.product_title,
            segments.product_merchant_id,
            segments.product_feed_label,
            segments.product_type_l1,
            campaign.name,
            campaign.id,
            metrics.clicks,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM shopping_performance_view
        WHERE {date_range}
            {campaign_filter}
            {merchant_filter}
        ORDER BY metrics.cost_micros DESC
        LIMIT 1000
    """

    try:
        results = _execute_gaql(cid, query, page_size=1000)
    except Exception as e:
        return json.dumps({"error": f"GAQL failed: {str(e)}"})

    if not results:
        return json.dumps({"error": "No shopping performance data found for the given criteria."})

    # Parse results into product-level aggregation
    # NOTE: _execute_gaql returns protobuf GoogleAdsRow objects, not dicts.
    # Use _safe_get_value(row, "path.to.field") for attribute access.
    products = {}
    for row in results:
        item_id = _safe_get_value(row, "segments.product_item_id", "unknown")

        if item_id not in products:
            products[item_id] = {
                'item_id': item_id,
                'title': str(_safe_get_value(row, "segments.product_title", "")),
                'merchant_id': str(_safe_get_value(row, "segments.product_merchant_id", "")),
                'feed_label': str(_safe_get_value(row, "segments.product_feed_label", "")),
                'product_type': str(_safe_get_value(row, "segments.product_type_l1", "")),
                'campaigns': set(),
                'clicks': 0,
                'impressions': 0,
                'cost': 0.0,
                'conversions': 0.0,
                'conversion_value': 0.0,
            }

        p = products[item_id]
        camp_name = str(_safe_get_value(row, "campaign.name", ""))
        p['campaigns'].add(camp_name)
        p['clicks'] += int(_safe_get_value(row, "metrics.clicks", 0))
        p['impressions'] += int(_safe_get_value(row, "metrics.impressions", 0))
        p['cost'] += int(_safe_get_value(row, "metrics.cost_micros", 0)) / 1_000_000
        p['conversions'] += float(_safe_get_value(row, "metrics.conversions", 0))
        p['conversion_value'] += float(_safe_get_value(row, "metrics.conversions_value", 0))

    # Calculate medians for classification
    all_clicks = [p['clicks'] for p in products.values() if p['clicks'] > 0]
    all_costs = [p['cost'] for p in products.values() if p['cost'] > 0]

    median_clicks = statistics.median(all_clicks) if all_clicks else 10
    median_cost = statistics.median(all_costs) if all_costs else 1.0

    # Classify each product
    segments = {'HERO': [], 'VILLAIN': [], 'SIDEKICK': [], 'ZOMBIE': []}
    for p in products.values():
        p['campaigns'] = list(p['campaigns'])  # Convert set to list for JSON
        roas = p['conversion_value'] / p['cost'] if p['cost'] > 0 else 0
        p['roas'] = round(roas, 2)
        segment = _classify_product(p, request.target_roas, median_clicks, median_cost,
                                    request.min_clicks_zombie)
        p['segment'] = segment
        segments[segment].append(p)

    # Sort each segment
    for seg_name in segments:
        if seg_name == 'HERO':
            segments[seg_name].sort(key=lambda x: x['conversion_value'], reverse=True)
        elif seg_name == 'VILLAIN':
            segments[seg_name].sort(key=lambda x: x['cost'], reverse=True)
        elif seg_name == 'SIDEKICK':
            segments[seg_name].sort(key=lambda x: x['roas'], reverse=True)
        else:
            segments[seg_name].sort(key=lambda x: x['impressions'], reverse=True)

    # Segment statistics
    seg_stats = {}
    for seg_name, prods in segments.items():
        total_cost = sum(p['cost'] for p in prods)
        total_revenue = sum(p['conversion_value'] for p in prods)
        total_conversions = sum(p['conversions'] for p in prods)
        seg_stats[seg_name] = {
            'count': len(prods),
            'total_cost': round(total_cost, 2),
            'total_revenue': round(total_revenue, 2),
            'total_conversions': round(total_conversions, 1),
            'avg_roas': round(total_revenue / total_cost, 2) if total_cost > 0 else 0,
            'cost_share_pct': 0,  # calculated below
            'revenue_share_pct': 0,
        }

    total_account_cost = sum(s['total_cost'] for s in seg_stats.values())
    total_account_revenue = sum(s['total_revenue'] for s in seg_stats.values())

    for s in seg_stats.values():
        s['cost_share_pct'] = round(s['total_cost'] / total_account_cost * 100, 1) if total_account_cost > 0 else 0
        s['revenue_share_pct'] = round(s['total_revenue'] / total_account_revenue * 100, 1) if total_account_revenue > 0 else 0

    # Generate Custom Label assignments
    custom_labels = {}
    if request.include_custom_labels:
        for seg_name, prods in segments.items():
            for p in prods:
                custom_labels[p['item_id']] = {
                    'custom_label_0': seg_name,
                    'title': p['title'],
                    'roas': p['roas'],
                    'cost': round(p['cost'], 2),
                }

    # Action recommendations
    actions = {
        'exclude_from_pmax': [
            {'item_id': p['item_id'], 'title': p['title'], 'cost': round(p['cost'], 2), 'roas': p['roas']}
            for p in segments['VILLAIN'][:20]
        ],
        'increase_visibility': [
            {'item_id': p['item_id'], 'title': p['title'], 'roas': p['roas'], 'clicks': p['clicks']}
            for p in segments['SIDEKICK'][:20]
        ],
        'zombie_reactivation': [
            {'item_id': p['item_id'], 'title': p['title'], 'impressions': p['impressions']}
            for p in segments['ZOMBIE'][:20]
        ],
    }

    # ================================================================
    # BACKEND CROSS-VALIDATION — real conversion data from your backend (optional)
    # ================================================================
    backend_section = None
    if request.country_code:
        try:
            backend_raw = _fetch_backend_conversions(request.country_code, request.days_back)

            if '_error' in backend_raw:
                backend_section = {
                    'status': 'error',
                    'message': backend_raw['_error'],
                    'url': backend_raw.get('_url', ''),
                }
            else:
                # Build a simplified gads_products dict for matching
                gads_for_match = {}
                for item_id, p in products.items():
                    gads_for_match[item_id] = {
                        'title': p['title'],
                        'cost': p['cost'],
                        'conversions': p['conversions'],
                        'conversion_value': p['conversion_value'],
                        'clicks': p['clicks'],
                        'segment': p['segment'],
                    }

                match_result = _match_backend_to_gads(backend_raw, gads_for_match)
                matched = match_result['matches']
                unmatched = match_result['unmatched_backend']

                # Calculate discrepancies
                discrepancies = []
                gads_overreporting = []
                gads_underreporting = []
                backend_only_sales = []

                for item_id, mdata in matched.items():
                    gads_conv = mdata['gads'].get('conversions', 0)
                    gads_value = mdata['gads'].get('conversion_value', 0)
                    backend_count = mdata['backend']['count']
                    backend_value = mdata['backend']['total_value']
                    gads_attr_count = mdata['backend']['gads_attributed']
                    gads_attr_value = mdata['backend']['gads_value']

                    entry = {
                        'item_id': item_id,
                        'title': mdata['gads'].get('title', ''),
                        'backend_handle': mdata['backend_handle'],
                        'gads_segment': mdata['gads'].get('segment', ''),
                        'gads_conversions': round(gads_conv, 1),
                        'gads_conv_value': round(gads_value, 2),
                        'backend_total_orders': backend_count,
                        'backend_total_value': backend_value,
                        'backend_gads_attributed': gads_attr_count,
                        'backend_gads_value': gads_attr_value,
                        'match_type': mdata['match_type'],
                    }

                    # Detect discrepancies (>20% difference)
                    if gads_conv > 0 and backend_count > 0:
                        ratio = gads_conv / backend_count
                        if ratio > 1.2:
                            entry['discrepancy'] = 'GADS_OVERREPORTS'
                            entry['ratio'] = round(ratio, 2)
                            gads_overreporting.append(entry)
                        elif ratio < 0.8:
                            entry['discrepancy'] = 'GADS_UNDERREPORTS'
                            entry['ratio'] = round(ratio, 2)
                            gads_underreporting.append(entry)
                    elif gads_conv == 0 and backend_count > 0:
                        entry['discrepancy'] = 'BACKEND_ONLY'
                        entry['note'] = f'Backend shows {backend_count} orders ({backend_value} value) but GAds reports 0 conversions'
                        backend_only_sales.append(entry)

                    if entry.get('discrepancy'):
                        discrepancies.append(entry)

                # Products in backend with no GAds match at all
                unmatched_with_sales = []
                for handle, bdata in unmatched.items():
                    if bdata['count'] > 0:
                        unmatched_with_sales.append({
                            'backend_handle': handle,
                            'orders': bdata['count'],
                            'total_value': bdata['total_value'],
                            'gads_attributed': bdata['gads_attributed'],
                            'gads_value': bdata['gads_value'],
                            'sample_orders': bdata.get('sample_orders', []),
                        })
                unmatched_with_sales.sort(key=lambda x: x['total_value'], reverse=True)

                # Backend summary stats
                total_backend_orders = sum(d['count'] for d in backend_raw.values())
                total_backend_value = sum(d['total_value'] for d in backend_raw.values())
                total_backend_gads = sum(d['gads_attributed'] for d in backend_raw.values())
                total_backend_gads_value = sum(d['gads_value'] for d in backend_raw.values())

                backend_section = {
                    'status': 'success',
                    'country_code': request.country_code.upper(),
                    'period': f'Last {request.days_back} days',
                    'summary': {
                        'backend_total_orders': total_backend_orders,
                        'backend_total_value': round(total_backend_value, 2),
                        'backend_gads_attributed_orders': total_backend_gads,
                        'backend_gads_attributed_value': round(total_backend_gads_value, 2),
                        'gads_reported_conversions': round(sum(p['conversions'] for p in products.values()), 1),
                        'gads_reported_value': round(sum(p['conversion_value'] for p in products.values()), 2),
                        'products_matched': len(matched),
                        'products_unmatched_in_backend': len(unmatched_with_sales),
                    },
                    'attribution_accuracy': {
                        'gads_overreporting': len(gads_overreporting),
                        'gads_underreporting': len(gads_underreporting),
                        'backend_only_sales': len(backend_only_sales),
                        'total_discrepancies': len(discrepancies),
                    },
                    'discrepancies': discrepancies[:30],
                    'backend_only_products': unmatched_with_sales[:20],
                    'segment_reclassification_candidates': [
                        {
                            'item_id': d['item_id'],
                            'title': d['title'],
                            'current_segment': d['gads_segment'],
                            'reason': (
                                f"GAds says {d['gads_conversions']} conv / {d['gads_conv_value']} value, "
                                f"but backend shows {d['backend_total_orders']} orders / {d['backend_total_value']} value"
                            ),
                        }
                        for d in discrepancies
                        if d.get('discrepancy') in ('GADS_UNDERREPORTS', 'BACKEND_ONLY')
                    ][:15],
                }
        except Exception as e:
            backend_section = {
                'status': 'error',
                'message': f'Backend integration failed: {str(e)}',
            }

    result = {
        'status': 'success',
        'analysis_period': f'Last {request.days_back} days',
        'target_roas': request.target_roas,
        'total_products_analyzed': len(products),
        'classification_thresholds': {
            'median_clicks': round(median_clicks, 1),
            'median_cost': round(median_cost, 2),
            'min_clicks_zombie': request.min_clicks_zombie,
        },
        'segment_statistics': seg_stats,
        'segments': {
            'HERO': [{'item_id': p['item_id'], 'title': p['title'], 'roas': p['roas'],
                       'cost': round(p['cost'], 2), 'revenue': round(p['conversion_value'], 2),
                       'clicks': p['clicks']} for p in segments['HERO'][:30]],
            'VILLAIN': [{'item_id': p['item_id'], 'title': p['title'], 'roas': p['roas'],
                         'cost': round(p['cost'], 2), 'revenue': round(p['conversion_value'], 2),
                         'clicks': p['clicks']} for p in segments['VILLAIN'][:30]],
            'SIDEKICK': [{'item_id': p['item_id'], 'title': p['title'], 'roas': p['roas'],
                          'cost': round(p['cost'], 2), 'revenue': round(p['conversion_value'], 2),
                          'clicks': p['clicks']} for p in segments['SIDEKICK'][:30]],
            'ZOMBIE': [{'item_id': p['item_id'], 'title': p['title'], 'impressions': p['impressions'],
                        'clicks': p['clicks']} for p in segments['ZOMBIE'][:30]],
        },
        'actions': actions,
        'custom_label_assignments': custom_labels if request.include_custom_labels else 'disabled',
    }

    # Attach backend cross-validation if available
    if backend_section is not None:
        result['backend_cross_validation'] = backend_section

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def ecom_channable_transform(request: ChannableRulesRequest) -> str:
    """
    🔄 Channable Rules Engine: Apply IF-THEN transformation rules to product feed data.

    Pulls products from Merchant Center via GAQL, applies transformation rules,
    and returns a diff showing what would change. Rules support:
    - Field extraction (IF brand IS EMPTY THEN extract_from_title)
    - Price logic (IF price < X THEN exclude / custom_label = "discount")
    - Title cleanup (NORMALIZE titles, REMOVE html entities)
    - Custom label assignment (IF category = X THEN custom_label_1 = Y)
    - Conditional exclusion (IF cost > X AND conversions = 0 THEN exclude)

    In dry_run mode, shows proposed changes. In apply mode, updates via Merchant Center API.
    """
    _ensure_client()
    cid = _format_customer_id(request.customer_id)
    _track_api_call("gaql_query")

    # Query products from the merchant
    feed_filter = f"AND shopping_product.feed_label = '{request.feed_label}'" if request.feed_label else ""

    query = f"""
        SELECT
            shopping_product.item_id,
            shopping_product.title,
            shopping_product.feed_label,
            shopping_product.status,
            shopping_product.merchant_center_id
        FROM shopping_product
        WHERE shopping_product.merchant_center_id = {request.merchant_id}
            {feed_filter}
        LIMIT 500
    """

    try:
        results = _execute_gaql(cid, query, page_size=500)
    except Exception as e:
        return json.dumps({"error": f"Failed to query products: {str(e)}"})

    if not results:
        return json.dumps({"error": "No products found for this merchant/feed_label combination."})

    # Parse products (protobuf rows → dicts)
    products = []
    for row in results:
        products.append({
            'item_id': str(_safe_get_value(row, "shopping_product.item_id", "")),
            'title': str(_safe_get_value(row, "shopping_product.title", "")),
            'feed_label': str(_safe_get_value(row, "shopping_product.feed_label", "")),
            'status': str(_safe_get_value(row, "shopping_product.status", "")),
            'merchant_id': str(_safe_get_value(row, "shopping_product.merchant_center_id", "")),
        })

    # Parse and apply rules
    changes = []
    excluded = []
    custom_labels = {}

    for rule_text in request.rules:
        rule_lower = rule_text.lower().strip()

        # Rule: NORMALIZE_TITLES
        if 'normalize' in rule_lower and 'title' in rule_lower:
            for p in products:
                original = p['title']
                cleaned = re.sub(r'&amp;', '&', original)
                cleaned = re.sub(r'&lt;', '<', cleaned)
                cleaned = re.sub(r'&gt;', '>', cleaned)
                cleaned = re.sub(r'&#\d+;', '', cleaned)
                cleaned = re.sub(r'\s+', ' ', cleaned).strip()
                # Title case normalization
                if cleaned.isupper():
                    cleaned = cleaned.title()
                if cleaned != original:
                    changes.append({
                        'item_id': p['item_id'],
                        'rule': rule_text,
                        'field': 'title',
                        'old_value': original,
                        'new_value': cleaned,
                    })

        # Rule: IF price < X THEN exclude
        elif 'price' in rule_lower and 'exclude' in rule_lower:
            match = re.search(r'price\s*[<>]=?\s*(\d+)', rule_lower)
            if match:
                threshold = int(match.group(1))
                op = '<' if '<' in rule_lower else '>'
                for p in products:
                    # Extract price from title if available (some feeds carry the price in the title)
                    price_match = re.search(r'(\d+)\s*[€£$]', p['title'])
                    if price_match:
                        price = int(price_match.group(1))
                        if (op == '<' and price < threshold) or (op == '>' and price > threshold):
                            excluded.append({
                                'item_id': p['item_id'],
                                'title': p['title'],
                                'detected_price': price,
                                'rule': rule_text,
                            })

        # Rule: IF title CONTAINS X THEN custom_label = Y
        elif 'contains' in rule_lower and 'custom_label' in rule_lower:
            match = re.search(r'contains\s*["\'](.+?)["\']\s*then\s*custom_label_?(\d?)\s*=\s*["\'](.+?)["\']',
                              rule_lower)
            if match:
                search_term = match.group(1)
                label_num = match.group(2) or '0'
                label_value = match.group(3)
                for p in products:
                    if search_term.lower() in p['title'].lower():
                        custom_labels[p['item_id']] = {
                            'title': p['title'],
                            f'custom_label_{label_num}': label_value,
                            'rule': rule_text,
                        }

        # Rule: IF brand IS EMPTY THEN extract_from_title
        elif 'brand' in rule_lower and 'empty' in rule_lower and 'extract' in rule_lower:
            for p in products:
                # Try to extract brand as first word of title
                words = p['title'].split()
                if words:
                    potential_brand = words[0]
                    changes.append({
                        'item_id': p['item_id'],
                        'rule': rule_text,
                        'field': 'brand',
                        'old_value': '(empty)',
                        'new_value': potential_brand,
                    })

        # Rule: IF status = NOT_ELIGIBLE THEN flag
        elif 'not_eligible' in rule_lower or 'not eligible' in rule_lower:
            for p in products:
                if p['status'] == 'NOT_ELIGIBLE':
                    changes.append({
                        'item_id': p['item_id'],
                        'rule': rule_text,
                        'field': 'status_flag',
                        'old_value': p['status'],
                        'new_value': 'FLAGGED_FOR_REVIEW',
                    })

        else:
            changes.append({
                'item_id': 'N/A',
                'rule': rule_text,
                'field': 'WARNING',
                'old_value': '',
                'new_value': f'Rule not recognized. Supported: NORMALIZE titles, IF price <> X THEN exclude, IF title CONTAINS X THEN custom_label = Y, IF brand IS EMPTY THEN extract_from_title',
            })

    result = {
        'status': 'success',
        'mode': 'DRY_RUN' if request.dry_run else 'APPLY',
        'total_products_scanned': len(products),
        'rules_applied': len(request.rules),
        'summary': {
            'field_changes': len([c for c in changes if c['field'] != 'WARNING']),
            'products_to_exclude': len(excluded),
            'custom_label_updates': len(custom_labels),
            'warnings': len([c for c in changes if c['field'] == 'WARNING']),
        },
        'changes': changes[:50],
        'exclusions': excluded[:30],
        'custom_label_updates': dict(list(custom_labels.items())[:30]),
    }

    if not request.dry_run:
        result['apply_note'] = (
            "To apply custom_label changes, use merchant_center_update_custom_labels tool "
            "with the item IDs and label values from this result."
        )

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def ecom_pmax_strategy(request: PMaxStrategyRequest) -> str:
    """
    🎯 PMax/Shopping Campaign Architecture Generator.

    Runs the Labelizer internally, then generates a concrete campaign structure:
    1. PMax "The Engine" (Heroes + Sidekicks) — high budget, scale focus
    2. PMax "Zombie Awakener" — low tROAS, forces Google to test dormant products
    3. Shopping Standard "The Optimizer" — manual CPC for Villains with high margin potential
    4. Exclusion list — products to remove from PMax immediately

    Returns campaign blueprints with budget splits, bidding strategies, and product assignments.
    """
    # First, run the Labelizer
    labelizer_req = LabelizerRequest(
        customer_id=request.customer_id,
        campaign_ids=request.campaign_ids,
        target_roas=request.target_roas,
        days_back=request.days_back,
        min_clicks_zombie=5,
        include_custom_labels=False,
    )

    labelizer_result_str = await ecom_labelizer_run(labelizer_req)
    labelizer_data = json.loads(labelizer_result_str)

    if labelizer_data.get('error'):
        return json.dumps({"error": f"Labelizer failed: {labelizer_data['error']}"})

    seg_stats = labelizer_data.get('segment_statistics', {})
    segments = labelizer_data.get('segments', {})

    # Budget allocation logic
    total_budget = request.daily_budget_gbp
    hero_count = seg_stats.get('HERO', {}).get('count', 0)
    sidekick_count = seg_stats.get('SIDEKICK', {}).get('count', 0)
    villain_count = seg_stats.get('VILLAIN', {}).get('count', 0)
    zombie_count = seg_stats.get('ZOMBIE', {}).get('count', 0)
    total = hero_count + sidekick_count + villain_count + zombie_count or 1

    # Budget split: Engine gets 60%, Optimizer 25%, Zombie Awakener 15%
    engine_budget = round(total_budget * 0.60, 2)
    optimizer_budget = round(total_budget * 0.25, 2)
    zombie_budget = round(total_budget * 0.15, 2)

    # Campaign blueprints
    blueprints = {
        'pmax_the_engine': {
            'name_template': '{CC} | PMax | Engine | Heroes+Sidekicks',
            'campaign_type': 'PERFORMANCE_MAX',
            'daily_budget': engine_budget,
            'bidding': {
                'strategy': 'MAXIMIZE_CONVERSION_VALUE',
                'target_roas': request.target_roas,
            },
            'product_count': hero_count + sidekick_count,
            'include_segments': ['HERO', 'SIDEKICK'],
            'product_ids': (
                [p['item_id'] for p in segments.get('HERO', [])] +
                [p['item_id'] for p in segments.get('SIDEKICK', [])]
            ),
            'rationale': (
                'Heroes drive revenue; Sidekicks have proven ROAS but need more traffic. '
                'Combining them lets Google allocate spend to proven winners while giving '
                'Sidekicks exposure through PMax audience signals.'
            ),
        },
        'shopping_the_optimizer': {
            'name_template': '{CC} | Shopping | Optimizer | Villains',
            'campaign_type': 'SHOPPING',
            'priority': 'LOW',
            'daily_budget': optimizer_budget,
            'bidding': {
                'strategy': 'MANUAL_CPC',
                'default_cpc': 0.05,
                'note': 'Start low, increase bids only for profitable products',
            },
            'product_count': villain_count,
            'include_segments': ['VILLAIN'],
            'product_ids': [p['item_id'] for p in segments.get('VILLAIN', [])],
            'rationale': (
                'Villains have high spend but low ROAS. Moving them to Standard Shopping '
                'with Manual CPC gives granular bid control. Start at 0.05 and only increase '
                'for products showing margin improvement.'
            ),
        },
        'pmax_zombie_awakener': {
            'name_template': '{CC} | PMax | Zombie Awakener',
            'campaign_type': 'PERFORMANCE_MAX',
            'daily_budget': zombie_budget,
            'bidding': {
                'strategy': 'MAXIMIZE_CONVERSION_VALUE',
                'target_roas': round(request.target_roas * 0.5, 1),
                'note': f'50% of normal tROAS ({round(request.target_roas * 0.5, 1)}) to force Google to test these products',
            },
            'product_count': zombie_count,
            'include_segments': ['ZOMBIE'],
            'product_ids': [p['item_id'] for p in segments.get('ZOMBIE', [])][:100],  # Cap at 100
            'rationale': (
                'Zombies get near-zero impressions — Google algorithm ignores them. '
                'A dedicated PMax with aggressive (low) tROAS forces the algorithm to '
                'test these products. Re-evaluate after 2 weeks: promote winners to Engine, '
                'exclude persistent non-performers.'
            ),
        },
    }

    # Exclusion list: Villains with >90 days cost and 0 conversions
    exclusion_list = [
        {'item_id': p['item_id'], 'title': p['title'], 'cost': p['cost'], 'roas': p['roas']}
        for p in segments.get('VILLAIN', [])
        if p['roas'] == 0 and p['cost'] > 5
    ]

    # Campaign actions summary
    actions = {
        'step_1_create_exclusion_list': {
            'description': 'Remove these products from ALL existing PMax campaigns immediately',
            'product_count': len(exclusion_list),
            'products': exclusion_list[:20],
        },
        'step_2_create_engine': {
            'description': f'Create PMax Engine with {hero_count + sidekick_count} products, budget {engine_budget} GBP/day',
            'use_tool': 'google_ads_create_pmax_campaign',
        },
        'step_3_create_optimizer': {
            'description': f'Create Shopping Optimizer with {villain_count} Villains, budget {optimizer_budget} GBP/day',
            'use_tool': 'google_ads_create_shopping_campaign',
        },
        'step_4_create_zombie_awakener': {
            'description': f'Create PMax Zombie Awakener with {zombie_count} products, budget {zombie_budget} GBP/day',
            'use_tool': 'google_ads_create_pmax_campaign',
        },
        'step_5_set_custom_labels': {
            'description': 'Update Custom Label 0 in Merchant Center for segmentation',
            'use_tool': 'merchant_center_update_custom_labels',
        },
    }

    result = {
        'status': 'success',
        'analysis_basis': labelizer_data.get('analysis_period', ''),
        'target_roas': request.target_roas,
        'total_daily_budget': total_budget,
        'segment_summary': seg_stats,
        'campaign_blueprints': blueprints,
        'exclusion_list': {
            'count': len(exclusion_list),
            'total_wasted_cost': round(sum(p['cost'] for p in exclusion_list), 2),
            'products': exclusion_list[:20],
        },
        'implementation_steps': actions,
    }

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def ecom_feed_compare(request: FeedCompareRequest) -> str:
    """
    🔍 Compare product catalogs between two Merchant Center accounts.

    Identifies products unique to each merchant, shared products, and eligibility gaps.
    Useful when two stores sell overlapping catalogs — find products that exist in only one of them.
    """
    _ensure_client()
    cid = _format_customer_id(request.customer_id)
    _track_api_call("gaql_query", 2)

    status_filter = "AND shopping_product.status = 'ELIGIBLE'" if request.only_eligible else ""

    # Query both merchants
    products_a = {}
    products_b = {}

    for merchant_id, storage, name in [
        (request.merchant_a_id, products_a, request.merchant_a_name),
        (request.merchant_b_id, products_b, request.merchant_b_name),
    ]:
        query = f"""
            SELECT
                shopping_product.item_id,
                shopping_product.title,
                shopping_product.feed_label,
                shopping_product.status,
                shopping_product.merchant_center_id
            FROM shopping_product
            WHERE shopping_product.merchant_center_id = {merchant_id}
                {status_filter}
            LIMIT 500
        """
        try:
            results = _execute_gaql(cid, query, page_size=500)
            for row in results:
                title = str(_safe_get_value(row, "shopping_product.title", ""))
                norm = _normalize_product_title(title)
                if norm not in storage:
                    storage[norm] = {
                        'item_id': str(_safe_get_value(row, "shopping_product.item_id", "")),
                        'title': title,
                        'feed_label': str(_safe_get_value(row, "shopping_product.feed_label", "")),
                        'status': str(_safe_get_value(row, "shopping_product.status", "")),
                        'normalized': norm,
                    }
        except Exception as e:
            return json.dumps({"error": f"Failed to query {name} ({merchant_id}): {str(e)}"})

    # Compare
    shared = set(products_a.keys()) & set(products_b.keys())
    only_a = set(products_a.keys()) - set(products_b.keys())
    only_b = set(products_b.keys()) - set(products_a.keys())

    result = {
        'status': 'success',
        'comparison': f'{request.merchant_a_name} ({request.merchant_a_id}) vs {request.merchant_b_name} ({request.merchant_b_id})',
        'eligibility_filter': 'ELIGIBLE only' if request.only_eligible else 'All statuses',
        'summary': {
            f'{request.merchant_a_name}_total': len(products_a),
            f'{request.merchant_b_name}_total': len(products_b),
            'shared_products': len(shared),
            f'only_in_{request.merchant_a_name}': len(only_a),
            f'only_in_{request.merchant_b_name}': len(only_b),
        },
        f'unique_to_{request.merchant_a_name}': sorted([
            {
                'title': products_a[n]['title'],
                'item_id': products_a[n]['item_id'],
                'feed_label': products_a[n]['feed_label'],
                'status': products_a[n]['status'],
            } for n in only_a
        ], key=lambda x: x['title'])[:50],
        f'unique_to_{request.merchant_b_name}': sorted([
            {
                'title': products_b[n]['title'],
                'item_id': products_b[n]['item_id'],
                'feed_label': products_b[n]['feed_label'],
                'status': products_b[n]['status'],
            } for n in only_b
        ], key=lambda x: x['title'])[:50],
        'shared': sorted([
            {
                'title': products_a[n]['title'],
                f'{request.merchant_a_name}_status': products_a[n]['status'],
                f'{request.merchant_b_name}_status': products_b[n]['status'],
            } for n in shared
        ], key=lambda x: x['title'])[:30],
    }

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
async def ecom_title_optimizer(request: TitleOptimizerRequest) -> str:
    """
    ✍️ SEO Title Optimizer for product feeds.

    Analyzes current product titles and generates optimized versions following the
    pattern: [Brand] + [Category] + [Key Attribute] + [SEO Keyword].

    For health supplements, focuses on condition/benefit keywords that match
    high-intent search queries.
    """
    _ensure_client()
    cid = _format_customer_id(request.customer_id)
    _track_api_call("gaql_query")

    feed_filter = f"AND shopping_product.feed_label = '{request.feed_label}'" if request.feed_label else ""

    query = f"""
        SELECT
            shopping_product.item_id,
            shopping_product.title,
            shopping_product.feed_label,
            shopping_product.status
        FROM shopping_product
        WHERE shopping_product.merchant_center_id = {request.merchant_id}
            AND shopping_product.status = 'ELIGIBLE'
            {feed_filter}
        LIMIT {request.max_products}
    """

    try:
        results = _execute_gaql(cid, query, page_size=request.max_products)
    except Exception as e:
        return json.dumps({"error": f"Failed to query products: {str(e)}"})

    if not results:
        return json.dumps({"error": "No eligible products found."})

    # Industry-specific title templates
    templates = {
        'health_supplements': {
            'pattern': '[Brand] [Product Type] - [Benefit/Condition] | [Key Ingredient] [Form]',
            'seo_suffixes': ['Buy Online', 'Official Store', 'Best Price', 'Natural Formula'],
            'condition_keywords': {
                'joints': ['joint pain relief', 'arthritis support', 'joint health'],
                'potency': ['male enhancement', 'libido booster', 'sexual wellness'],
                'weight': ['weight loss supplement', 'fat burner', 'slimming'],
                'heart': ['blood pressure support', 'cardiovascular health', 'heart supplement'],
                'diabetes': ['blood sugar support', 'glucose management', 'diabetes supplement'],
                'parasites': ['parasite cleanse', 'detox supplement', 'anti-parasitic'],
                'prostate': ['prostate health', 'prostate supplement', 'urinary support'],
                'vision': ['eye health', 'vision supplement', 'eye care'],
                'hair': ['hair growth', 'hair loss treatment', 'hair supplement'],
                'skin': ['skin care', 'anti-aging', 'skin supplement'],
            },
        },
        'fashion': {
            'pattern': '[Brand] [Category] [Style] - [Color] [Size] | [Material]',
            'seo_suffixes': ['Free Shipping', 'New Collection', 'Sale'],
        },
        'electronics': {
            'pattern': '[Brand] [Model] [Category] - [Key Spec] | [Year]',
            'seo_suffixes': ['Free Delivery', 'Official Retailer', 'Best Deal'],
        },
    }

    template_config = templates.get(request.industry, templates['health_supplements'])

    # Detect product categories based on title keywords
    category_map = {
        'joints': ['arthr', 'joint', 'flex', 'osteo', 'cartilag'],
        'potency': ['potenc', 'libid', 'erect', 'stamina'],
        'weight': ['slim', 'keto', 'diet', 'fat', 'weight'],
        'heart': ['cardio', 'heart', 'blood', 'pulse', 'hypo', 'tension'],
        'diabetes': ['diab', 'gluc', 'insul', 'sugar'],
        'parasites': ['paras', 'detox', 'vermi', 'toxo'],
        'prostate': ['prost', 'uro', 'adeno', 'cyst'],
        'vision': ['ocul', 'vision', 'eye', 'audio', 'hearing'],
        'hair': ['hair'],
        'skin': ['cream', 'derm', 'skin'],
    }

    optimized = []
    for row in results:
        title = str(_safe_get_value(row, "shopping_product.title", ""))
        item_id = str(_safe_get_value(row, "shopping_product.item_id", ""))

        # Detect category
        detected_category = 'general'
        for cat, keywords in category_map.items():
            if any(kw in title.lower() for kw in keywords):
                detected_category = cat
                break

        # Extract brand (first word, typically)
        brand = title.split()[0] if title.split() else ''

        # Generate optimized title
        condition_kws = template_config.get('condition_keywords', {}).get(detected_category, [])
        seo_kw = condition_kws[0] if condition_kws else ''

        optimized_title = f"{title}"
        if seo_kw and seo_kw.lower() not in title.lower():
            optimized_title = f"{title} - {seo_kw.title()}"

        optimized.append({
            'item_id': item_id,
            'original_title': title,
            'optimized_title': optimized_title,
            'detected_category': detected_category,
            'seo_keywords_available': condition_kws,
        })

    result = {
        'status': 'success',
        'industry': request.industry,
        'title_pattern': template_config['pattern'],
        'products_analyzed': len(optimized),
        'optimizations': optimized[:request.max_products],
    }

    return json.dumps(result, indent=2, default=str)


# ============================================================================
# PRODUCTHERO STRATEGY — REQUEST MODELS
# ============================================================================

# Merchant Center IDs and shop domains per country come from config.json
# ("merchant_center.merchant_ids", "domains").
_MERCHANT_IDS = get_merchant_ids()
_DOMAINS = get_domains()

# Producthero budget multipliers (from official documentation)
_BUDGET_MULTIPLIERS = {
    'HERO': 2.0,
    'SIDEKICK': 2.0,
    'VILLAIN': 0.5,
    'ZOMBIE': 1.5,
}

# Account currency per country (for budget calculations)
_CURRENCIES = {
    "PL": "PLN", "RO": "RON", "HU": "HUF", "CZ": "CZK",
    "TR": "TRY", "DE": "EUR", "AT": "EUR", "ES": "EUR",
    "IT": "EUR", "FR": "EUR", "GR": "EUR", "SK": "EUR",
    "AE": "AED", "AR": "ARS", "AU": "AUD", "BD": "BDT",
    "BE": "EUR", "CA": "CAD", "CO": "COP", "DZ": "DZD",
    "LB": "USD", "MA": "MAD", "MX": "MXN", "PH": "PHP",
    "TH": "THB", "UA": "UAH", "UG": "UGX", "UK": "GBP",
    "US": "USD",
}

# Min/max daily budget in local currency (micros)
_BUDGET_LIMITS = {
    "HUF": (500, 50000),     # 500-50000 HUF/day
    "PLN": (5, 500),          # 5-500 PLN/day
    "RON": (5, 500),          # 5-500 RON/day
    "CZK": (30, 3000),        # 30-3000 CZK/day
    "EUR": (1, 100),           # 1-100 EUR/day
    "GBP": (1, 100),           # 1-100 GBP/day
    "USD": (1, 100),           # 1-100 USD/day
    "TRY": (10, 1000),         # 10-1000 TRY/day
}


class LabelizerApplyRequest(BaseModel):
    """Apply Producthero labels through your label-push endpoint (config endpoints.labelizer_push)."""
    customer_id: str = Field(default_factory=get_customer_id, description="Google Ads Customer ID (defaults to the configured account)")
    country_code: str = Field(..., description="Country code (e.g., 'HU', 'PL', 'DE')")
    target_roas: float = Field(2.0, description="Target ROAS for classification")
    days_back: int = Field(180, description="Lookback window", ge=7, le=180)
    custom_label_slot: int = Field(4, description="Custom label slot (0-4). Default 4 for Producthero.", ge=0, le=4)
    dry_run: bool = Field(True, description="If true, only show what would change. Set false to push to Feed API.")
    campaign_ids: Optional[List[str]] = Field(None, description="Campaign IDs to analyze (defaults to all)")


class LabelizerDeployRequest(BaseModel):
    """Deploy Producthero 4-campaign structure from Labelizer results."""
    customer_id: str = Field(default_factory=get_customer_id, description="Google Ads Customer ID (defaults to the configured account)")
    country_code: str = Field(..., description="Country code (e.g., 'HU')")
    target_roas: float = Field(2.0, description="Target ROAS — same for ALL 4 campaigns (Producthero best practice)")
    days_back: int = Field(180, description="Lookback window for Labelizer", ge=7, le=180)
    custom_label_slot: int = Field(4, description="Which custom_label holds the segment labels (default 4 for Producthero)", ge=0, le=4)
    campaign_ids: Optional[List[str]] = Field(None, description="Source campaign IDs to analyze")
    dry_run: bool = Field(True, description="Preview campaign creation plan without creating. Set false to create.")


class SupplementalFeedRequest(BaseModel):
    """Generate a supplemental feed CSV for Merchant Center auto-refresh."""
    customer_id: str = Field(default_factory=get_customer_id, description="Google Ads Customer ID (defaults to the configured account)")
    country_code: str = Field(..., description="Country code")
    target_roas: float = Field(2.0, description="Target ROAS for classification")
    days_back: int = Field(180, description="Lookback window", ge=7, le=180)
    custom_label_slot: int = Field(4, description="Custom label slot (default 4 for Producthero)", ge=0, le=4)
    campaign_ids: Optional[List[str]] = Field(None, description="Campaign IDs to analyze")
    output_path: str = Field("", description="File path to save CSV. If empty, returns inline.")


# ============================================================================
# LABELIZER FEED API — optional label-push endpoint of your feed backend
# ============================================================================
# config.json "endpoints.labelizer_push": a URL that accepts
#   POST ?action=update  {"country_code", "custom_label_slot", "classification_params", "products": [...]}
#   GET  ?action=status&country_code=XX → {"status": "success", "in_sync": bool,
#        "servers": {"<feed backend>": {"response": {"data": {"total_labeled_products", "segment_counts", "last_update"}}}}}
# Without it, use ecom_supplemental_feed to deliver the same labels as a Merchant Center supplemental feed.

def _labelizer_url() -> str:
    return (load_config().get("endpoints", {}) or {}).get("labelizer_push", "")


def _push_labels_to_feed_api(country_code: str, custom_label_slot: int,
                              target_roas: float, products: list) -> dict:
    """
    POST labels to the configured label-push endpoint.
    Returns parsed JSON response from the endpoint.
    """
    base = _labelizer_url()
    if not base:
        return {'_error': 'endpoints.labelizer_push is not configured — use ecom_supplemental_feed instead'}
    payload = json.dumps({
        'country_code': country_code,
        'custom_label_slot': custom_label_slot,
        'classification_params': {
            'target_roas': target_roas,
            'analysis_date': datetime.now().isoformat(),
        },
        'products': products,
    })

    url = f"{base}?action=update"
    req = urllib.request.Request(
        url,
        data=payload.encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'User-Agent': 'EcomEngine/2.0',
        },
        method='POST',
    )

    try:
        with urllib.request.urlopen(req, timeout=60, context=_ssl_ctx) as response:
            raw = response.read().decode('utf-8')
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')[:500]
        return {'_error': f'HTTP {e.code}: {body}', '_url': url}
    except Exception as e:
        return {'_error': f'Request failed: {str(e)}', '_url': url}


def _get_labelizer_status(country_code: str) -> dict:
    """GET label status from the configured label-push endpoint."""
    base = _labelizer_url()
    if not base:
        return {'_error': 'endpoints.labelizer_push is not configured'}
    url = f"{base}?action=status&country_code={country_code}"
    req = urllib.request.Request(url, headers={'User-Agent': 'EcomEngine/2.0'})
    try:
        with urllib.request.urlopen(req, timeout=15, context=_ssl_ctx) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        return {'_error': str(e)}


def _resolve_handles_from_mc(merchant_id: str, item_ids: list) -> dict:
    """
    Resolve item_id → product_handle by reading customLabel0 from MC.
    Returns dict: {item_id: product_handle}.
    customLabel0 is set to the Shopify handle by the feed generator.
    """
    import sys

    try:
        _ensure_merchant_client()
        mc = _gam.merchant_center_client
        if not mc:
            print("[LABELIZER] MC client is None after _ensure_merchant_client()", file=sys.stderr)
            return {}
    except Exception as e:
        print(f"[LABELIZER] Failed to init MC client: {e}", file=sys.stderr)
        return {}

    handle_map = {}
    pages_fetched = 0
    # Batch via list products with pagination
    try:
        page_token = None
        while True:
            resp = mc.products().list(
                merchantId=merchant_id,
                maxResults=250,
                **({"pageToken": page_token} if page_token else {})
            ).execute()
            pages_fetched += 1
            resources = resp.get('resources', [])
            for prod in resources:
                offer_id = prod.get('offerId', '')
                cl0 = prod.get('customLabel0', '')
                if offer_id and cl0:
                    handle_map[offer_id] = cl0
            page_token = resp.get('nextPageToken')
            if not page_token:
                break
        print(f"[LABELIZER] MC resolve: {pages_fetched} pages, {len(handle_map)} handles mapped", file=sys.stderr)
    except Exception as e:
        print(f"[LABELIZER] MC list failed after {pages_fetched} pages ({len(handle_map)} mapped): {e}", file=sys.stderr)

    return handle_map


# ============================================================================
# TOOL: APPLY LABELS VIA FEED API
# ============================================================================

@mcp.tool()
async def ecom_labelizer_apply(request: LabelizerApplyRequest) -> str:
    """
    🏷️ Apply Producthero segment labels through your label-push endpoint (config endpoints.labelizer_push).

    Runs the Labelizer to classify products, maps item_ids to product_handles
    (via MC customLabel0), aggregates metrics per handle, and POSTs them to the
    configured endpoint, which writes the labels into your primary feed.

    Labels appear as custom_label_4 in the next feed XML generation.
    No supplemental feed needed — labels go directly into the primary feed.

    Use dry_run=true first to preview, then dry_run=false to push.
    """
    _ensure_client()
    cc = request.country_code.upper()
    merchant_id = _MERCHANT_IDS.get(cc)
    if not merchant_id:
        return json.dumps({"error": f"No merchant ID configured for country {cc}"})

    # Step 1: Run Labelizer to get classifications
    labelizer_req = LabelizerRequest(
        customer_id=request.customer_id,
        campaign_ids=request.campaign_ids,
        country_code=cc,
        target_roas=request.target_roas,
        days_back=request.days_back,
        min_clicks_zombie=5,
        include_custom_labels=True,
    )
    labelizer_result_str = await ecom_labelizer_run(labelizer_req)
    labelizer_data = json.loads(labelizer_result_str)

    if labelizer_data.get('error'):
        return json.dumps({"error": f"Labelizer failed: {labelizer_data['error']}"})

    # Step 2: Collect all item_ids from segments
    item_updates = []
    for seg_name in ['HERO', 'VILLAIN', 'SIDEKICK', 'ZOMBIE']:
        products_in_seg = labelizer_data.get('segments', {}).get(seg_name, [])
        for p in products_in_seg:
            item_id = p.get('item_id', '')
            if not item_id or item_id == 'unknown':
                continue
            item_updates.append({
                'item_id': item_id,
                'title': p.get('title', ''),
                'segment': seg_name,
                'roas': p.get('roas', 0),
                'cost': p.get('cost', 0),
                'clicks': p.get('clicks', 0),
                'impressions': p.get('impressions', 0),
                'conversions': p.get('conversions', 0),
                'conversion_value': p.get('conversion_value', 0),
            })

    if not item_updates:
        return json.dumps({"error": "No products classified. Check campaign data."})

    # Step 3: Resolve item_id → product_handle via MC customLabel0
    all_item_ids = [u['item_id'] for u in item_updates]
    handle_map = _resolve_handles_from_mc(merchant_id, all_item_ids)

    # Aggregate metrics by handle (multiple variants → one handle)
    handle_data = {}
    unmapped_items = []
    for u in item_updates:
        handle = handle_map.get(u['item_id'], '')
        if not handle:
            unmapped_items.append(u['item_id'])
            continue

        if handle not in handle_data:
            handle_data[handle] = {
                'product_handle': handle,
                'segment': u['segment'],
                'clicks': 0, 'impressions': 0, 'cost': 0.0,
                'conversions': 0.0, 'conversion_value': 0.0,
                'variant_count': 0,
                'best_roas': 0.0,
            }

        h = handle_data[handle]
        h['clicks'] += u.get('clicks', 0)
        h['impressions'] += u.get('impressions', 0)
        h['cost'] += u.get('cost', 0)
        h['conversions'] += u.get('conversions', 0)
        h['conversion_value'] += u.get('conversion_value', 0)
        h['variant_count'] += 1
        # Keep the "best" segment — highest priority: HERO > SIDEKICK > VILLAIN > ZOMBIE
        seg_priority = {'HERO': 4, 'SIDEKICK': 3, 'VILLAIN': 2, 'ZOMBIE': 1}
        if seg_priority.get(u['segment'], 0) > seg_priority.get(h['segment'], 0):
            h['segment'] = u['segment']

    # Calculate aggregated ROAS per handle
    for h in handle_data.values():
        h['roas'] = round(h['conversion_value'] / h['cost'], 2) if h['cost'] > 0 else 0

    # Build API payload
    api_products = []
    for h in handle_data.values():
        api_products.append({
            'product_handle': h['product_handle'],
            'segment': h['segment'],
            'metrics': {
                'clicks': h['clicks'],
                'impressions': h['impressions'],
                'cost': round(h['cost'], 2),
                'conversions': round(h['conversions'], 1),
                'conversion_value': round(h['conversion_value'], 2),
                'roas': h['roas'],
            },
        })

    # Build segment summary
    seg_counts = defaultdict(int)
    seg_costs = defaultdict(float)
    seg_revenue = defaultdict(float)
    for h in handle_data.values():
        seg_counts[h['segment']] += 1
        seg_costs[h['segment']] += h['cost']
        seg_revenue[h['segment']] += h['conversion_value']

    result = {
        'status': 'success',
        'mode': 'DRY_RUN' if request.dry_run else 'PUSHED_TO_FEED_API',
        'country_code': cc,
        'merchant_id': merchant_id,
        'custom_label_slot': f'custom_label_{request.custom_label_slot}',
        'total_handles': len(handle_data),
        'total_item_ids': len(item_updates),
        'unmapped_items': len(unmapped_items),
        'segment_breakdown': {
            seg: {
                'count': seg_counts[seg],
                'historical_cost': round(seg_costs[seg], 2),
                'historical_revenue': round(seg_revenue[seg], 2),
                'budget_multiplier': f'{_BUDGET_MULTIPLIERS[seg]}x',
            }
            for seg in ['HERO', 'SIDEKICK', 'VILLAIN', 'ZOMBIE']
        },
        'sample_handles': [
            {
                'handle': h['product_handle'],
                'segment': h['segment'],
                'roas': h['roas'],
                'variants': h['variant_count'],
            }
            for h in sorted(handle_data.values(), key=lambda x: x['cost'], reverse=True)[:20]
        ],
    }

    # Step 4: Push to Feed API (or show dry-run)
    if not request.dry_run:
        if not api_products:
            result['error'] = 'No products with resolved handles to push.'
            return json.dumps(result, indent=2, default=str)

        push_result = _push_labels_to_feed_api(
            cc, request.custom_label_slot, request.target_roas, api_products
        )

        if push_result.get('_error'):
            result['feed_api'] = {
                'status': 'error',
                'error': push_result['_error'],
            }
        else:
            result['feed_api'] = push_result

            # Verify with status check
            status_result = _get_labelizer_status(cc)
            if not status_result.get('_error'):
                result['verification'] = status_result
    else:
        result['next_step'] = (
            f"Run with dry_run=false to push {len(api_products)} handle labels "
            f"to the label-push endpoint. Labels will appear in "
            f"custom_label_{request.custom_label_slot} in next feed generation."
        )

    # Include backend cross-validation if available
    if labelizer_data.get('backend_cross_validation'):
        result['backend_cross_validation_summary'] = {
            'status': labelizer_data['backend_cross_validation'].get('status'),
            'discrepancies': labelizer_data['backend_cross_validation'].get('attribution_accuracy', {}),
        }

    if unmapped_items:
        result['unmapped_item_ids'] = unmapped_items[:20]
        result['unmapped_note'] = (
            f"{len(unmapped_items)} item_ids could not be mapped to handles "
            f"(not found in MC customLabel0). These products were skipped."
        )

    return json.dumps(result, indent=2, default=str)


# ============================================================================
# TOOL: DEPLOY 4 PMAX CAMPAIGNS (Producthero Strategy)
# ============================================================================

@mcp.tool()
async def ecom_labelizer_deploy(request: LabelizerDeployRequest) -> str:
    """
    🚀 Deploy Producthero Labelizer Strategy: create 4 PMax campaigns segmented by performance.

    Creates the exact Producthero campaign architecture:
    1. PMax Heroes — budget = historical_cost × 2.0x (scale winners)
    2. PMax Sidekicks — budget = historical_cost × 2.0x (grow potential)
    3. PMax Villains — budget = historical_cost × 0.5x (reduce waste)
    4. PMax Zombies — budget = historical_cost × 1.5x (activate dormant)

    ALL campaigns use the SAME tROAS (Producthero best practice).
    Products are filtered via listing_group_filter on custom_label_4.

    Prerequisites: Run ecom_labelizer_apply first to push labels to Feed API.
    """
    _ensure_client()
    cc = request.country_code.upper()
    cid = _format_customer_id(request.customer_id)
    merchant_id = _MERCHANT_IDS.get(cc)
    domain = _DOMAINS.get(cc)

    if not merchant_id:
        return json.dumps({"error": f"No merchant ID for country {cc}"})
    if not domain:
        return json.dumps({"error": f"No domain configured for country {cc}"})

    # Step 1: Run Labelizer to get segment data + costs
    labelizer_req = LabelizerRequest(
        customer_id=request.customer_id,
        campaign_ids=request.campaign_ids,
        country_code=cc,
        target_roas=request.target_roas,
        days_back=request.days_back,
        min_clicks_zombie=5,
        include_custom_labels=False,
    )
    labelizer_result_str = await ecom_labelizer_run(labelizer_req)
    labelizer_data = json.loads(labelizer_result_str)

    if labelizer_data.get('error'):
        return json.dumps({"error": f"Labelizer failed: {labelizer_data['error']}"})

    seg_stats = labelizer_data.get('segment_statistics', {})
    days = request.days_back

    # Step 2: Calculate budgets using Producthero multipliers
    label_key = f"custom_label_{request.custom_label_slot}"
    campaigns_plan = []

    for seg_name in ['HERO', 'SIDEKICK', 'VILLAIN', 'ZOMBIE']:
        stats = seg_stats.get(seg_name, {})
        historical_cost = stats.get('total_cost', 0)
        product_count = stats.get('count', 0)
        total_revenue = stats.get('total_revenue', 0)
        avg_roas = stats.get('avg_roas', 0)
        multiplier = _BUDGET_MULTIPLIERS[seg_name]

        # Daily budget = (total cost over period / days) × multiplier
        historical_daily = historical_cost / days if days > 0 else 0
        proposed_daily = historical_daily * multiplier
        # Apply currency-aware min/max limits
        currency = _CURRENCIES.get(cc, 'EUR')
        budget_min, budget_max = _BUDGET_LIMITS.get(currency, (1, 100))
        proposed_daily = max(budget_min, min(budget_max, proposed_daily))
        daily_budget_micros = int(proposed_daily * 1_000_000)

        final_url = f"https://{domain}"

        campaign_name = f"{cc} | PMax | Labelizer | {seg_name.title()}s"

        campaigns_plan.append({
            'segment': seg_name,
            'campaign_name': campaign_name,
            'product_count': product_count,
            'historical_daily_cost': round(historical_daily, 2),
            'multiplier': f'{multiplier}x',
            'proposed_daily_budget': round(proposed_daily, 2),
            'daily_budget_micros': daily_budget_micros,
            'bidding': {
                'strategy': 'MAXIMIZE_CONVERSION_VALUE',
                'target_roas': request.target_roas,
            },
            'listing_group_filter': {
                'field': label_key,
                'value': seg_name,
            },
            'historical_stats': {
                'total_cost': stats.get('total_cost', 0),
                'total_revenue': total_revenue,
                'avg_roas': avg_roas,
                'cost_share_pct': stats.get('cost_share_pct', 0),
                'revenue_share_pct': stats.get('revenue_share_pct', 0),
            },
            'merchant_id': merchant_id,
            'final_url': final_url,
            'sales_country': cc,
        })

    # Step 3: Execute or dry-run
    created_campaigns = []

    if not request.dry_run:
        # Import models and functions up front
        import mcp_shopping as _ms
        from mcp_shopping import (
            CreatePMaxCampaignRequest,
            CreateAssetGroupListingFilterRequest,
        )

        for cp in campaigns_plan:
            try:
                # 3a. Create PMax campaign (PAUSED)
                pmax_req = CreatePMaxCampaignRequest(
                    customer_id=request.customer_id,
                    campaign_name=cp['campaign_name'],
                    daily_budget_micros=cp['daily_budget_micros'],
                    merchant_id=str(cp['merchant_id']),
                    sales_country=cp['sales_country'],
                    final_url=cp['final_url'],
                    status="PAUSED",
                    feed_label=None,
                )

                pmax_result = await _ms.google_ads_create_pmax_campaign(pmax_req)

                if isinstance(pmax_result, str):
                    pmax_data = json.loads(pmax_result)
                elif isinstance(pmax_result, dict):
                    pmax_data = pmax_result
                else:
                    pmax_data = {'raw': str(pmax_result)[:200]}

                campaign_id = str(pmax_data.get('campaign_id', ''))
                asset_group_id = str(pmax_data.get('asset_group_id', ''))

                # 3b. Set listing group filter on asset group
                filter_result = None
                if asset_group_id:
                    filter_kwargs = {
                        'customer_id': request.customer_id,
                        'asset_group_id': asset_group_id,
                        'filter_type': 'CUSTOM',
                        'custom_label_0': None,
                        'custom_label_1': None,
                        'custom_label_2': None,
                        'custom_label_3': None,
                        'custom_label_4': None,
                    }
                    filter_kwargs[f'custom_label_{request.custom_label_slot}'] = cp['segment']
                    filter_req = CreateAssetGroupListingFilterRequest(**filter_kwargs)
                    filter_result = await _ms.google_ads_create_listing_group_filter(filter_req)

                # 3c. Set bidding to same tROAS — already set by create_pmax via MAXIMIZE_CONVERSION_VALUE
                # but we also set explicit tROAS
                if campaign_id:
                    from google_ads_mcp import UpdateCampaignBiddingStrategyRequest
                    bid_req = UpdateCampaignBiddingStrategyRequest(
                        customer_id=request.customer_id,
                        campaign_id=campaign_id,
                        bidding_strategy='MAXIMIZE_CONVERSION_VALUE',
                        target_roas=request.target_roas,
                        target_cpa_micros=None,
                        enhanced_cpc=True,
                    )
                    await _gam.google_ads_update_campaign_bidding_strategy(bid_req)

                created_campaigns.append({
                    'segment': cp['segment'],
                    'campaign_name': cp['campaign_name'],
                    'campaign_id': campaign_id,
                    'asset_group_id': asset_group_id,
                    'daily_budget': cp['proposed_daily_budget'],
                    'status': 'CREATED_PAUSED',
                })

            except Exception as e:
                created_campaigns.append({
                    'segment': cp['segment'],
                    'campaign_name': cp['campaign_name'],
                    'error': str(e)[:200],
                    'status': 'FAILED',
                })

    # Build result
    total_daily = sum(cp['proposed_daily_budget'] for cp in campaigns_plan)
    total_monthly = total_daily * 30.4

    result = {
        'status': 'success',
        'mode': 'DRY_RUN' if request.dry_run else 'DEPLOYED',
        'country_code': cc,
        'strategy': 'Producthero Labelizer 4-Campaign Architecture',
        'key_principle': 'SAME tROAS on all 4 campaigns — budget multipliers control spend allocation',
        'target_roas': request.target_roas,
        'producthero_troas_tip': f'Set 25% below actual ROAS. If actual is {request.target_roas}, use {round(request.target_roas * 0.75, 2)}',
        'budget_summary': {
            'total_daily_budget': round(total_daily, 2),
            'total_monthly_estimate': round(total_monthly, 2),
            'currency': _CURRENCIES.get(cc, 'EUR'),
        },
        'campaigns': campaigns_plan,
    }

    if not request.dry_run:
        result['created_campaigns'] = created_campaigns
        result['next_steps'] = [
            '1. Review created campaigns in Google Ads (all PAUSED)',
            '2. Add text assets (headlines + descriptions) to each asset group',
            '3. Add image assets (logos, marketing images)',
            '4. Set geo targeting + language for each campaign',
            '5. Enable campaigns one by one',
            '6. Schedule periodic label refresh via ecom_labelizer_apply (Feed API)',
        ]
    else:
        result['next_step'] = (
            'Run with dry_run=false to create all 4 campaigns. '
            'Make sure ecom_labelizer_apply was run first to set custom labels in MC.'
        )

    return json.dumps(result, indent=2, default=str)


# ============================================================================
# TOOL: GENERATE SUPPLEMENTAL FEED CSV
# ============================================================================

@mcp.tool()
async def ecom_supplemental_feed(request: SupplementalFeedRequest) -> str:
    """
    📊 Generate Producthero-style supplemental feed CSV for Merchant Center.

    Creates a CSV file with product IDs and their Labelizer segment labels.
    This CSV can be:
    1. Hosted on your web server as a supplemental feed URL for MC to fetch daily
    2. Uploaded manually to MC as a supplemental data source
    3. Used with Content API for programmatic updates

    The feed contains: id, custom_label_0 (or chosen slot).
    MC merges this with your primary feed, automatically tagging products.

    When hosted and scheduled, labels refresh daily — products automatically
    move between campaigns as their performance changes (the core Producthero mechanic).
    """
    _ensure_client()
    cc = request.country_code.upper()
    merchant_id = _MERCHANT_IDS.get(cc)
    if not merchant_id:
        return json.dumps({"error": f"No merchant ID for country {cc}"})

    # Run Labelizer
    labelizer_req = LabelizerRequest(
        customer_id=request.customer_id,
        campaign_ids=request.campaign_ids,
        country_code=cc,
        target_roas=request.target_roas,
        days_back=request.days_back,
        min_clicks_zombie=5,
        include_custom_labels=True,
    )
    labelizer_result_str = await ecom_labelizer_run(labelizer_req)
    labelizer_data = json.loads(labelizer_result_str)

    if labelizer_data.get('error'):
        return json.dumps({"error": f"Labelizer failed: {labelizer_data['error']}"})

    # Build CSV rows
    label_key = f"custom_label_{request.custom_label_slot}"
    rows = []

    for seg_name in ['HERO', 'SIDEKICK', 'VILLAIN', 'ZOMBIE']:
        products_in_seg = labelizer_data.get('segments', {}).get(seg_name, [])
        for p in products_in_seg:
            item_id = p.get('item_id', '')
            if not item_id or item_id == 'unknown':
                continue
            rows.append({
                'id': item_id,
                label_key: seg_name,
            })

    # Generate CSV content
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=['id', label_key])
        writer.writeheader()
        writer.writerows(rows)

    csv_content = output.getvalue()

    # Save to file if path provided
    saved_path = ""
    if request.output_path:
        try:
            with open(request.output_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            saved_path = request.output_path
        except Exception as e:
            return json.dumps({"error": f"Failed to save CSV: {str(e)}"})

    # Segment stats
    seg_counts = defaultdict(int)
    for r in rows:
        seg_counts[r[label_key]] += 1

    result = {
        'status': 'success',
        'country_code': cc,
        'merchant_id': merchant_id,
        'label_field': label_key,
        'total_products': len(rows),
        'segment_breakdown': dict(seg_counts),
        'csv_format': f'id,{label_key}',
        'csv_rows_sample': csv_content[:500] if not saved_path else '(saved to file)',
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

    if saved_path:
        result['saved_to'] = saved_path
        result['hosting_instructions'] = (
            f"Upload this CSV to any web server. "
            f"Then in Google Merchant Center → Data Sources → Supplemental Sources → "
            f"Add supplemental product data → paste the hosted URL. "
            f"Set fetch schedule to DAILY. MC will auto-merge labels with your primary feed."
        )
    else:
        result['csv_content'] = csv_content
        result['next_steps'] = [
            f"1. Save this CSV and host it at a public URL (e.g., https://feeds.example.com/{cc.lower()}_labels.csv)",
            "2. In Google Merchant Center → Data Sources → Supplemental Sources → Add",
            "3. Paste the hosted URL, set fetch schedule to DAILY",
            "4. MC will merge custom_label_0 with your primary feed automatically",
            "5. Products will move between Labelizer campaigns as labels refresh",
        ]

    return json.dumps(result, indent=2, default=str)


# ============================================================================
# LABELIZER VERIFY — comprehensive pipeline health check with SQLite tracking
# ============================================================================

import sqlite3 as _sqlite3
import os as _os

_LABELIZER_DB_PATH = _os.path.join(
    _os.path.dirname(_os.path.abspath(__file__)), "labelizer_state.db"
)


def _get_labelizer_db() -> _sqlite3.Connection:
    """Get or create the labelizer state DB."""
    conn = _sqlite3.connect(_LABELIZER_DB_PATH, timeout=10)
    conn.row_factory = _sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS labelizer_country_state (
            country_code      TEXT PRIMARY KEY,
            merchant_id       TEXT,
            -- Step 1: Labels in Feed API
            labels_pushed     INTEGER DEFAULT 0,
            labels_total      INTEGER DEFAULT 0,
            labels_hero       INTEGER DEFAULT 0,
            labels_sidekick   INTEGER DEFAULT 0,
            labels_villain    INTEGER DEFAULT 0,
            labels_zombie     INTEGER DEFAULT 0,
            labels_pushed_at  TEXT,
            backends_synced   INTEGER DEFAULT 0,
            backends_total    INTEGER DEFAULT 0,
            -- Step 2: Shopping Listing Groups deployed
            deploy_approach   TEXT,
            deploy_campaign_id TEXT,
            deploy_campaign_name TEXT,
            deploy_ad_group_id TEXT,
            deploy_node_count INTEGER DEFAULT 0,
            deploy_hero_bid   INTEGER DEFAULT 0,
            deploy_sidekick_bid INTEGER DEFAULT 0,
            deploy_villain_bid INTEGER DEFAULT 0,
            deploy_zombie_bid INTEGER DEFAULT 0,
            deploy_else_bid   INTEGER DEFAULT 0,
            deployed_at       TEXT,
            -- Step 3: Traffic verification
            last_traffic_check TEXT,
            impressions_7d    INTEGER DEFAULT 0,
            clicks_7d         INTEGER DEFAULT 0,
            conversions_7d    REAL DEFAULT 0,
            cost_7d           REAL DEFAULT 0,
            -- Step 4: MC product coverage
            mc_active_products INTEGER DEFAULT 0,
            mc_disapproved    INTEGER DEFAULT 0,
            mc_pending        INTEGER DEFAULT 0,
            mc_checked_at     TEXT,
            -- Overall
            pipeline_status   TEXT DEFAULT 'NOT_STARTED',
            last_verified_at  TEXT,
            notes             TEXT
        );

        CREATE TABLE IF NOT EXISTS labelizer_verify_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT NOT NULL,
            check_type   TEXT NOT NULL,
            status       TEXT NOT NULL,
            details      TEXT,
            checked_at   TEXT DEFAULT (datetime('now'))
        );
    """)
    return conn


class LabelizerVerifyRequest(BaseModel):
    """Verify Producthero Labelizer pipeline health for a country."""
    customer_id: str = Field(default_factory=get_customer_id, description="Google Ads Customer ID (defaults to the configured account)")
    country_code: str = Field(..., description="Country code (e.g., 'HU')")
    custom_label_slot: int = Field(4, description="Custom label slot (0-4)", ge=0, le=4)
    fix: bool = Field(False, description="Auto-fix issues where possible (re-push labels, rebuild listing groups)")


@mcp.tool()
async def ecom_labelizer_verify(request: LabelizerVerifyRequest) -> str:
    """
    ✅ Verify Producthero Labelizer pipeline — comprehensive health check.

    Checks ALL steps of the pipeline:
    1. Labels in the feed backend(s) behind endpoints.labelizer_push — pushed? in sync?
    2. Shopping listing groups — deployed? correct bids? correct INDEX?
    3. Campaign traffic — impressions/clicks in last 7 days?
    4. Merchant Center — product coverage, disapprovals?

    Stores results in local SQLite (labelizer_state.db) for tracking.
    Run periodically to catch drift or broken deployments.
    """
    try:
        _ensure_client()
        customer_id = _format_customer_id(request.customer_id)
        cc = request.country_code.upper()
        slot = request.custom_label_slot
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        db = _get_labelizer_db()
        checks = []
        issues = []
        state = {}

        # ====================================================================
        # CHECK 1: Labels in the feed backend(s)
        # ====================================================================
        label_status = _get_labelizer_status(cc)
        labels_total = 0
        segment_counts = {}

        if label_status.get('status') == 'success':
            servers = label_status.get('servers', {}) or {}
            backend_data = {name: (srv or {}).get('response', {}).get('data', {}) or {} for name, srv in servers.items()}
            totals = {name: d.get('total_labeled_products', 0) for name, d in backend_data.items()}
            labels_total = max(totals.values(), default=0)
            in_sync = label_status.get('in_sync', len(set(totals.values())) <= 1)
            first = next((d for d in backend_data.values() if d), {})

            segment_counts = first.get('segment_counts', {})

            state['labels_pushed'] = 1 if labels_total > 0 else 0
            state['labels_total'] = labels_total
            state['labels_hero'] = segment_counts.get('HERO', 0)
            state['labels_sidekick'] = segment_counts.get('SIDEKICK', 0)
            state['labels_villain'] = segment_counts.get('VILLAIN', 0)
            state['labels_zombie'] = segment_counts.get('ZOMBIE', 0)
            state['labels_pushed_at'] = first.get('last_update', '')
            state['backends_synced'] = sum(1 for t in totals.values() if t > 0)
            state['backends_total'] = len(totals)

            if labels_total > 0 and in_sync:
                checks.append({'step': 'LABELS_IN_FEED', 'status': 'PASS',
                               'detail': f'{labels_total} products labeled, feed backends in sync',
                               'segments': segment_counts})
            elif labels_total > 0 and not in_sync:
                checks.append({'step': 'LABELS_IN_FEED', 'status': 'WARN',
                               'detail': f'{labels_total} labeled but backends differ: {totals}'})
                issues.append('Feed backends out of sync — re-run ecom_labelizer_apply')
            else:
                checks.append({'step': 'LABELS_IN_FEED', 'status': 'FAIL',
                               'detail': 'No labels found in feed API'})
                issues.append('No labels pushed — run /ads-labelizer-apply first')
        else:
            checks.append({'step': 'LABELS_IN_FEED', 'status': 'FAIL',
                           'detail': f'Feed API error: {label_status.get("_error", "unknown")}'})
            issues.append('Cannot reach Feed API')

        # ====================================================================
        # CHECK 2: Shopping Listing Groups deployed
        # ====================================================================
        index_name = f"INDEX{slot}"
        shopping_query = (
            f"SELECT campaign.id, campaign.name "
            f"FROM campaign "
            f"WHERE campaign.status = 'ENABLED' "
            f"AND campaign.advertising_channel_type = 'SHOPPING' "
            f"AND campaign.name LIKE '%{cc}%'"
        )
        shopping_results = _execute_gaql(customer_id, shopping_query)

        deployed_campaign = None
        deployed_ad_group = None
        listing_nodes = []

        # Collect ALL campaigns with labelizer nodes, then prefer NP > Shopping > MCPC
        _candidates = []

        for camp_row in shopping_results:
            camp_id = str(camp_row.campaign.id)
            camp_name = camp_row.campaign.name

            lg_query = (
                f"SELECT ad_group.id, ad_group.name, "
                f"ad_group_criterion.listing_group.type, "
                f"ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
                f"ad_group_criterion.listing_group.case_value.product_custom_attribute.value, "
                f"ad_group_criterion.cpc_bid_micros, "
                f"ad_group_criterion.status "
                f"FROM ad_group_criterion "
                f"WHERE campaign.id = {camp_id} "
                f"AND ad_group_criterion.type = 'LISTING_GROUP' "
                f"AND ad_group_criterion.status != 'REMOVED'"
            )
            lg_results = _execute_gaql(customer_id, lg_query)

            has_labelizer_nodes = False
            camp_nodes = []
            ad_group_id = None

            for row in lg_results:
                ad_group_id = str(row.ad_group.id)
                idx_raw = row.ad_group_criterion.listing_group.case_value.product_custom_attribute.index
                val = row.ad_group_criterion.listing_group.case_value.product_custom_attribute.value
                bid = row.ad_group_criterion.cpc_bid_micros
                lg_type = row.ad_group_criterion.listing_group.type_.name
                status = row.ad_group_criterion.status.name

                # Proto enum: INDEX0=2, INDEX1=3, INDEX2=4, INDEX3=5, INDEX4=6
                # Also check .name property and string representation
                idx_str = ''
                idx_match = False
                if idx_raw:
                    # Try .name (e.g., "INDEX4"), str() (e.g., "6" or "ProductCustomAttributeIndex.INDEX4")
                    try:
                        idx_str = idx_raw.name if hasattr(idx_raw, 'name') else str(idx_raw)
                    except Exception:
                        idx_str = str(idx_raw)
                    # Match by name, string repr, or numeric value (INDEX4 = enum value 6)
                    idx_match = (
                        index_name in idx_str
                        or index_name.lower() in idx_str.lower()
                        or (slot == 4 and idx_raw in (6, 'INDEX4'))
                        or (slot == 3 and idx_raw in (5, 'INDEX3'))
                        or (slot == 2 and idx_raw in (4, 'INDEX2'))
                        or (slot == 1 and idx_raw in (3, 'INDEX1'))
                        or (slot == 0 and idx_raw in (2, 'INDEX0'))
                    )

                if idx_match:
                    has_labelizer_nodes = True
                camp_nodes.append({
                    'type': lg_type,
                    'index': idx_str,
                    'index_raw_type': type(idx_raw).__name__,
                    'index_raw_repr': repr(idx_raw),
                    'value': val if val else '(everything else)',
                    'bid_micros': bid,
                    'status': status,
                    'idx_match': idx_match,
                })

            if has_labelizer_nodes:
                matched_nodes = [n for n in camp_nodes if n.get('idx_match', False)]
                # Priority: NP campaigns first, then by number of labelizer nodes
                name_upper = camp_name.upper()
                priority = 0
                if ' NP ' in name_upper or name_upper.endswith(' NP') or 'NP |' in name_upper:
                    priority = 2  # Highest — NP Shopping campaign
                elif 'MCPC' not in name_upper:
                    priority = 1  # Middle — regular Shopping (not MCPC)
                # priority 0 = MCPC (lowest)
                _candidates.append({
                    'campaign': {'id': camp_id, 'name': camp_name},
                    'ad_group_id': ad_group_id,
                    'nodes': matched_nodes,
                    'priority': priority,
                    'node_count': len(matched_nodes),
                })

        # Pick best candidate: highest priority, then most nodes
        if _candidates:
            _candidates.sort(key=lambda c: (c['priority'], c['node_count']), reverse=True)
            best = _candidates[0]
            deployed_campaign = best['campaign']
            deployed_ad_group = best['ad_group_id']
            listing_nodes = best['nodes']

        if deployed_campaign and len(listing_nodes) >= 5:
            found_segments = set()
            for node in listing_nodes:
                v = node['value'].upper()
                if v in ('HERO', 'SIDEKICK', 'VILLAIN', 'ZOMBIE'):
                    found_segments.add(v)

            expected = {'HERO', 'SIDEKICK', 'VILLAIN', 'ZOMBIE'}
            missing = expected - found_segments

            state['deploy_approach'] = 'SHOPPING_LISTING_GROUPS'
            state['deploy_campaign_id'] = deployed_campaign['id']
            state['deploy_campaign_name'] = deployed_campaign['name']
            state['deploy_ad_group_id'] = deployed_ad_group
            state['deploy_node_count'] = len(listing_nodes)
            state['deployed_at'] = now

            for node in listing_nodes:
                v = node['value'].upper()
                b = node.get('bid_micros', 0)
                if v == 'HERO':
                    state['deploy_hero_bid'] = b
                elif v == 'SIDEKICK':
                    state['deploy_sidekick_bid'] = b
                elif v == 'VILLAIN':
                    state['deploy_villain_bid'] = b
                elif v == 'ZOMBIE':
                    state['deploy_zombie_bid'] = b
                elif v == '(EVERYTHING ELSE)':
                    state['deploy_else_bid'] = b

            if not missing:
                checks.append({'step': 'LISTING_GROUPS_DEPLOYED', 'status': 'PASS',
                               'detail': f'All 4 segments on {index_name} in {deployed_campaign["name"]}',
                               'campaign_id': deployed_campaign['id'],
                               'ad_group_id': deployed_ad_group,
                               'nodes': listing_nodes})
            else:
                checks.append({'step': 'LISTING_GROUPS_DEPLOYED', 'status': 'FAIL',
                               'detail': f'Missing segments: {", ".join(missing)}',
                               'nodes': listing_nodes})
                issues.append(f'Missing listing group segments: {", ".join(missing)}')
        elif deployed_campaign:
            checks.append({'step': 'LISTING_GROUPS_DEPLOYED', 'status': 'WARN',
                           'detail': f'Found {index_name} nodes but only {len(listing_nodes)} (need >=5)',
                           'nodes': listing_nodes})
            issues.append('Incomplete listing group tree — rebuild needed')
            state['deploy_approach'] = 'SHOPPING_LISTING_GROUPS'
            state['deploy_campaign_id'] = deployed_campaign['id']
            state['deploy_node_count'] = len(listing_nodes)
        else:
            checks.append({'step': 'LISTING_GROUPS_DEPLOYED', 'status': 'FAIL',
                           'detail': f'No Shopping campaign with {index_name} listing groups found for {cc}'})
            issues.append(f'No Labelizer deployment found — run /ads-labelizer-deploy {cc}')
            state['deploy_approach'] = None
            state['deploy_campaign_id'] = None

        # ====================================================================
        # CHECK 3: Campaign traffic (last 7 days)
        # ====================================================================
        if deployed_campaign:
            traffic_query = (
                f"SELECT metrics.impressions, metrics.clicks, metrics.conversions, "
                f"metrics.cost_micros, metrics.conversions_value "
                f"FROM campaign "
                f"WHERE campaign.id = {deployed_campaign['id']} "
                f"AND segments.date DURING LAST_7_DAYS"
            )
            traffic_results = _execute_gaql(customer_id, traffic_query)
            imp = clicks = cost_micros = 0
            conv = conv_val = 0.0
            for row in traffic_results:
                imp += row.metrics.impressions
                clicks += row.metrics.clicks
                conv += row.metrics.conversions
                cost_micros += row.metrics.cost_micros
                conv_val += row.metrics.conversions_value

            state['impressions_7d'] = imp
            state['clicks_7d'] = clicks
            state['conversions_7d'] = conv
            state['cost_7d'] = cost_micros / 1_000_000
            state['last_traffic_check'] = now

            if imp > 0:
                checks.append({'step': 'CAMPAIGN_TRAFFIC', 'status': 'PASS',
                               'detail': f'{imp} imp, {clicks} clicks, {conv:.1f} conv, ${cost_micros/1_000_000:.2f} cost (7d)',
                               'conversions_value': conv_val})
            else:
                checks.append({'step': 'CAMPAIGN_TRAFFIC', 'status': 'FAIL',
                               'detail': 'Zero impressions in last 7 days'})
                issues.append('Campaign has no traffic — check status, budget, MC feed')
        else:
            checks.append({'step': 'CAMPAIGN_TRAFFIC', 'status': 'SKIP',
                           'detail': 'No deployed campaign to check'})

        # ====================================================================
        # CHECK 4: Merchant Center product coverage
        # ====================================================================
        # Merchant Center ID per country from config.json (falls back to GAQL below)
        _known_merchants = _MERCHANT_IDS
        try:
            merchant_id = _known_merchants.get(cc)
            if not merchant_id:
                # Fallback: try GAQL
                merchant_map_query = (
                    f"SELECT segments.product_merchant_id, segments.product_country "
                    f"FROM shopping_performance_view "
                    f"WHERE segments.product_country = '{cc}' "
                    f"LIMIT 1"
                )
                merchant_rows = _execute_gaql(customer_id, merchant_map_query)
                for row in merchant_rows:
                    merchant_id = str(row.segments.product_merchant_id)
                    break

            if merchant_id:
                state['merchant_id'] = merchant_id
                try:
                    _ensure_merchant_client()
                    mc = _gam.merchant_center_client
                    acct_status = mc.accountstatuses().get(
                        merchantId=merchant_id, accountId=merchant_id
                    ).execute()

                    active = disapproved = pending = 0
                    for prod in acct_status.get('products', []):
                        if prod.get('destination') == 'Shopping' and prod.get('country') == cc:
                            stats = prod.get('statistics', {})
                            active = int(stats.get('active', 0))
                            disapproved = int(stats.get('disapproved', 0))
                            pending = int(stats.get('pending', 0))
                            break

                    state['mc_active_products'] = active
                    state['mc_disapproved'] = disapproved
                    state['mc_pending'] = pending
                    state['mc_checked_at'] = now

                    if active > 0:
                        checks.append({'step': 'MERCHANT_CENTER', 'status': 'PASS' if disapproved == 0 else 'WARN',
                                       'detail': f'{active} active, {disapproved} disapproved, {pending} pending',
                                       'merchant_id': merchant_id})
                        if disapproved > 0:
                            issues.append(f'{disapproved} products disapproved in MC — run /ads-merchant-gc {cc}')
                    else:
                        checks.append({'step': 'MERCHANT_CENTER', 'status': 'FAIL',
                                       'detail': 'No active products in MC for Shopping',
                                       'merchant_id': merchant_id})
                        issues.append('No active MC products — check feed and MC status')
                except Exception as e:
                    checks.append({'step': 'MERCHANT_CENTER', 'status': 'WARN',
                                   'detail': f'MC API error: {str(e)[:100]}'})
            else:
                checks.append({'step': 'MERCHANT_CENTER', 'status': 'SKIP',
                               'detail': f'No merchant_id found for {cc}'})
        except Exception as e:
            checks.append({'step': 'MERCHANT_CENTER', 'status': 'WARN',
                           'detail': f'Could not resolve merchant: {str(e)[:100]}'})

        # ====================================================================
        # Determine overall pipeline status
        # ====================================================================
        statuses = [c['status'] for c in checks]
        if all(s in ('PASS',) for s in statuses):
            pipeline_status = 'HEALTHY'
        elif 'FAIL' in statuses:
            pipeline_status = 'BROKEN'
        else:
            pipeline_status = 'DEGRADED'

        state['pipeline_status'] = pipeline_status
        state['last_verified_at'] = now
        state['notes'] = '; '.join(issues) if issues else 'All checks passed'

        # ====================================================================
        # Persist to SQLite
        # ====================================================================
        cols = list(state.keys())
        placeholders = ', '.join(['?'] * (len(cols) + 1))
        col_names = ', '.join(['country_code'] + cols)
        update_clause = ', '.join([f'{c}=excluded.{c}' for c in cols])
        values = [cc] + [state[c] for c in cols]

        db.execute(
            f"INSERT INTO labelizer_country_state ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT(country_code) DO UPDATE SET {update_clause}",
            values
        )

        for check in checks:
            db.execute(
                "INSERT INTO labelizer_verify_log (country_code, check_type, status, details) VALUES (?, ?, ?, ?)",
                (cc, check['step'], check['status'], json.dumps(check, default=str))
            )

        db.commit()

        # Load ALL country states for summary
        all_countries = []
        cursor = db.execute(
            "SELECT country_code, pipeline_status, labels_total, deploy_approach, "
            "deploy_campaign_name, impressions_7d, last_verified_at "
            "FROM labelizer_country_state ORDER BY country_code"
        )
        for row in cursor:
            all_countries.append(dict(row))

        db.close()

        return json.dumps({
            'status': 'success',
            'country_code': cc,
            'pipeline_status': pipeline_status,
            'checks': checks,
            'issues': issues,
            'fix_suggestions': [f'Run: {iss}' for iss in issues] if issues else [],
            'all_countries': all_countries,
            'db_path': _LABELIZER_DB_PATH,
            'verified_at': now,
        }, indent=2, default=str)

    except Exception as e:
        return json.dumps({
            'status': 'error',
            'message': f'Verify failed: {str(e)}',
        }, indent=2)
