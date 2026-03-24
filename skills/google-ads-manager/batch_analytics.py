"""
Batch Analytics — RSA Asset Performance Tracker + Performance Intelligence.

Provides:
1. Asset performance analysis with trend history (LEARNING→GOOD→BEST tracking)
2. Cross-campaign asset scanning (auto-scan ALL campaigns for a country)
3. Funnel-stage diagnostics (impressions→CTR→conversions bottleneck detection)
4. Seasonality mining (monthly performance patterns by product category)
5. Frequency/overlap detection (products in multiple campaigns → fatigue risk)

Use: batch_analyze_assets(country_code, campaign_id) to scan a campaign
     and store asset performance data + history in batch_db.
"""

import json
import re
import time
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field

from batch_db import BatchDB
from accounts_config import get_customer_id


def _get_customer_id() -> str:
    """Get the configured customer ID, defaulting to generic placeholder if not configured."""
    try:
        return get_customer_id()
    except Exception:
        # Fallback for backward compatibility if config not available
        return "0000000000"


def _get_db() -> BatchDB:
    from batch_optimizer import _get_db
    return _get_db()


def _generalize_pattern(text: str, product_names: Optional[List[str]] = None) -> str:
    """
    Convert a specific headline to a generic pattern.
    'Premium Widget' -> '{product} Original'
    'Doar 149 Lei' -> 'Doar {price} Lei'
    'Produs Verificat' -> 'Produs Verificat' (already generic)
    """
    result = text.strip()

    # Replace product names with {product}
    if product_names:
        for name in product_names:
            if name.lower() in result.lower():
                pattern = re.compile(re.escape(name), re.IGNORECASE)
                result = pattern.sub("{product}", result)

    # Replace numeric prices with {price}
    result = re.sub(r"\b\d{2,4}\b(?=\s*(Lei|TL|Ft|Kč|zł|€|\$|RON|TRY|HUF|CZK|PLN))", "{price}", result)
    result = re.sub(r"\b\d{2,4}\b(?=\s*(lei|tl|ft))", "{price}", result, flags=re.IGNORECASE)

    # Replace KeyWord insertion with {keyword}
    result = re.sub(r"\{KeyWord:[^}]*\}", "{keyword}", result)
    result = re.sub(r"\{Keyword:[^}]*\}", "{keyword}", result)

    return result


# ---------------------------------------------------------------------------
# 1. Asset Performance Analysis (with trend history)
# ---------------------------------------------------------------------------

def analyze_campaign_assets(
    country_code: str,
    campaign_id: str,
) -> Dict[str, Any]:
    """
    Analyze RSA asset performance for a campaign using REAL METRICS.

    Instead of relying on Google's performance_label (which returns NOT_APPLICABLE
    with Explorer access tokens), we pull actual impressions/clicks/conversions/cost
    and compute our own BEST/GOOD/LEARNING/LOW labels based on CTR and conversion
    rate percentiles.

    Scoring algorithm:
    1. Aggregate metrics per unique asset text + field_type across all ad groups
    2. Calculate CTR and conversion_rate per aggregated asset
    3. Compute composite score = 0.4*CTR_percentile + 0.6*conv_rate_percentile
    4. Assign labels by percentile: top 25% → BEST, next 25% → GOOD,
       next 25% → LEARNING, bottom 25% → LOW
    5. Assets with < 100 impressions → PENDING (not enough data)
    """
    from google_ads_mcp import _execute_gaql

    db = _get_db()

    # ── Step 1: Get ad group names for pattern generalization ──
    ag_query = f"""
        SELECT campaign.id, ad_group.id, ad_group.name
        FROM ad_group
        WHERE campaign.id = {campaign_id}
          AND ad_group.status != 'REMOVED'
    """
    try:
        ag_results = _execute_gaql(_get_customer_id(), ag_query, page_size=500)
    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch ad groups: {e}"}

    product_names = []
    for row in ag_results:
        try:
            name = str(row.ad_group.name)
        except (AttributeError, TypeError):
            continue
        if name:
            product_names.append(name)

    # ── Step 2: Query assets WITH metrics ──
    # Minimal fields for speed. Filter metrics.impressions > 0 to skip new/zero assets.
    asset_query = f"""
        SELECT campaign.id, ad_group_ad_asset_view.field_type,
               asset.text_asset.text,
               metrics.impressions,
               metrics.clicks,
               metrics.conversions,
               metrics.cost_micros
        FROM ad_group_ad_asset_view
        WHERE campaign.id = {campaign_id}
          AND ad_group_ad_asset_view.field_type IN ('HEADLINE', 'DESCRIPTION')
          AND ad_group_ad_asset_view.enabled = TRUE
          AND metrics.impressions > 0
    """
    try:
        asset_results = _execute_gaql(_get_customer_id(), asset_query, page_size=1000)
    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch asset performance: {e}"}

    # Enum int → string for field_type (protobuf returns ints via getattr)
    FIELD_TYPE_INT_MAP = {2: "HEADLINE", 3: "DESCRIPTION"}

    # ── Step 3: Aggregate metrics per unique (text, field_type) ──
    # Direct protobuf attribute access for speed (no _safe_get_value overhead)
    aggregated: Dict[str, Dict[str, Any]] = {}
    total_rows = 0

    for row in asset_results:
        total_rows += 1
        try:
            # Direct protobuf access — much faster than _safe_get_value per-field
            ft_raw = row.ad_group_ad_asset_view.field_type
            text = row.asset.text_asset.text
            imp = row.metrics.impressions
            clk = row.metrics.clicks
            conv = row.metrics.conversions
            cost = row.metrics.cost_micros
        except (AttributeError, TypeError):
            continue

        if not text:
            continue

        # Resolve field_type: may be int enum or string
        if isinstance(ft_raw, int):
            field_type = FIELD_TYPE_INT_MAP.get(ft_raw, "")
        else:
            ft_str = str(ft_raw)
            field_type = FIELD_TYPE_INT_MAP.get(int(ft_str), ft_str) if ft_str.isdigit() else ft_str
        if field_type not in ("HEADLINE", "DESCRIPTION"):
            continue

        key = f"{field_type}|{text}"
        if key not in aggregated:
            aggregated[key] = {
                "text": text, "field_type": field_type,
                "impressions": 0, "clicks": 0, "conversions": 0.0, "cost_micros": 0,
                "ad_group_count": 0,
            }
        agg = aggregated[key]
        agg["impressions"] += int(imp)
        agg["clicks"] += int(clk)
        agg["conversions"] += float(conv)
        agg["cost_micros"] += int(cost)
        agg["ad_group_count"] += 1

    # ── Step 4: Calculate CTR, conversion rate, composite score ──
    MIN_IMPRESSIONS = 100  # Need at least 100 impressions for meaningful data

    scoreable_headlines: List[Dict] = []
    scoreable_descriptions: List[Dict] = []
    pending_assets: List[Dict] = []

    for key, agg in aggregated.items():
        imp = agg["impressions"]
        ctr = agg["clicks"] / imp if imp > 0 else 0
        conv_rate = agg["conversions"] / agg["clicks"] if agg["clicks"] > 0 else 0
        cpa = (agg["cost_micros"] / 1_000_000) / agg["conversions"] if agg["conversions"] > 0 else 0

        entry = {
            **agg,
            "ctr": ctr,
            "conv_rate": conv_rate,
            "cpa": cpa,
        }

        if imp < MIN_IMPRESSIONS:
            entry["computed_label"] = "PENDING"
            pending_assets.append(entry)
        elif agg["field_type"] == "HEADLINE":
            scoreable_headlines.append(entry)
        else:
            scoreable_descriptions.append(entry)

    def _assign_percentile_labels(assets: List[Dict]) -> None:
        """Assign BEST/GOOD/LEARNING/LOW based on composite score percentiles."""
        if not assets:
            return

        # Composite score: 40% CTR percentile + 60% conversion rate percentile
        # (conversion rate matters more for e-commerce)
        ctrs = sorted(set(a["ctr"] for a in assets))
        conv_rates = sorted(set(a["conv_rate"] for a in assets))

        def _percentile(value: float, sorted_vals: list) -> float:
            if len(sorted_vals) <= 1:
                return 0.5
            idx = 0
            for i, v in enumerate(sorted_vals):
                if v <= value:
                    idx = i
            return idx / (len(sorted_vals) - 1) if len(sorted_vals) > 1 else 0.5

        for a in assets:
            ctr_pct = _percentile(a["ctr"], ctrs)
            conv_pct = _percentile(a["conv_rate"], conv_rates)
            a["composite_score"] = 0.4 * ctr_pct + 0.6 * conv_pct

        # Sort by composite score descending, assign labels by quartile
        assets.sort(key=lambda x: x["composite_score"], reverse=True)
        n = len(assets)
        for i, a in enumerate(assets):
            rank_pct = i / n if n > 0 else 0
            if rank_pct < 0.25:
                a["computed_label"] = "BEST"
            elif rank_pct < 0.50:
                a["computed_label"] = "GOOD"
            elif rank_pct < 0.75:
                a["computed_label"] = "LEARNING"
            else:
                a["computed_label"] = "LOW"

    _assign_percentile_labels(scoreable_headlines)
    _assign_percentile_labels(scoreable_descriptions)

    # ── Step 5: Store in DB with computed labels (BATCH mode for speed) ──
    conn = db._get_conn()
    # Clear old data for this campaign
    conn.execute(
        "DELETE FROM asset_performance WHERE country_code=? AND campaign_id=?",
        (country_code, campaign_id)
    )

    all_scored = scoreable_headlines + scoreable_descriptions + pending_assets
    by_label = {"BEST": 0, "GOOD": 0, "LEARNING": 0, "LOW": 0, "PENDING": 0}
    patterns_seen: Dict[str, Dict[str, int]] = {}

    perf_rows = []
    hist_rows = []

    for asset in all_scored:
        label = asset["computed_label"]
        by_label[label] = by_label.get(label, 0) + 1

        pattern = _generalize_pattern(asset["text"], product_names)
        field_type = asset["field_type"]

        perf_rows.append((
            country_code, campaign_id, field_type, pattern, label,
            1, asset["impressions"], asset["clicks"], asset["conversions"],
        ))
        hist_rows.append((
            country_code, campaign_id, field_type, pattern, label,
            1, asset["impressions"], asset["clicks"], asset["conversions"], 0,
        ))

        # Track for report
        pkey = f"{field_type}|{pattern}"
        if pkey not in patterns_seen:
            patterns_seen[pkey] = {"BEST": 0, "GOOD": 0, "LEARNING": 0, "LOW": 0, "PENDING": 0}
        patterns_seen[pkey][label] = patterns_seen[pkey].get(label, 0) + 1

    # Batch insert — single commit for all rows
    conn.executemany(
        """INSERT INTO asset_performance
           (country_code, campaign_id, asset_type, headline_pattern, performance_label,
            occurrences, avg_impressions, avg_clicks, avg_conversions, cached_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(country_code, campaign_id, asset_type, headline_pattern, performance_label)
           DO UPDATE SET occurrences=occurrences+1,
           avg_impressions=excluded.avg_impressions,
           avg_clicks=excluded.avg_clicks,
           avg_conversions=excluded.avg_conversions,
           cached_at=datetime('now')""",
        perf_rows
    )
    conn.executemany(
        """INSERT INTO asset_performance_history
           (country_code, campaign_id, asset_type, headline_pattern, performance_label,
            occurrences, avg_impressions, avg_clicks, avg_conversions, avg_conversion_value)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        hist_rows
    )
    conn.commit()

    # ── Step 6: Build report (pre-compute pattern→metrics map for O(n)) ──
    # Build pattern→metrics aggregation from all_scored in single pass
    pattern_metrics_map: Dict[str, Dict] = {}
    for asset in all_scored:
        pattern = _generalize_pattern(asset["text"], product_names)
        pkey = f"{asset['field_type']}|{pattern}"
        if pkey not in pattern_metrics_map:
            pattern_metrics_map[pkey] = {"ctr_sum": 0, "conv_sum": 0, "imp_sum": 0, "count": 0}
        pm = pattern_metrics_map[pkey]
        pm["ctr_sum"] += asset.get("ctr", 0)
        pm["conv_sum"] += asset.get("conv_rate", 0)
        pm["imp_sum"] += asset["impressions"]
        pm["count"] += 1

    headline_patterns = []
    description_patterns = []

    for key, labels in patterns_seen.items():
        field_type, pattern = key.split("|", 1)
        total = sum(labels.values())
        score = labels.get("BEST", 0) * 4 + labels.get("GOOD", 0) * 2 - labels.get("LOW", 0) * 2

        pm = pattern_metrics_map.get(key, {"ctr_sum": 0, "conv_sum": 0, "imp_sum": 0, "count": 1})
        cnt = pm["count"] or 1
        entry = {
            "pattern": pattern,
            "total": total,
            "best": labels.get("BEST", 0),
            "good": labels.get("GOOD", 0),
            "learning": labels.get("LEARNING", 0),
            "low": labels.get("LOW", 0),
            "pending": labels.get("PENDING", 0),
            "score": score,
            "avg_ctr": round(pm["ctr_sum"] / cnt * 100, 2),
            "avg_conv_rate": round(pm["conv_sum"] / cnt * 100, 2),
            "total_impressions": pm["imp_sum"],
        }

        if "HEADLINE" in field_type:
            headline_patterns.append(entry)
        else:
            description_patterns.append(entry)

    headline_patterns.sort(key=lambda x: x["score"], reverse=True)
    description_patterns.sort(key=lambda x: x["score"], reverse=True)

    # Skip regressions check for speed (can be called separately)
    regressions = []

    return {
        "country_code": country_code,
        "campaign_id": campaign_id,
        "scoring_method": "metrics_based",
        "total_rows_from_api": total_rows,
        "total_unique_assets": len(aggregated),
        "scoreable_headlines": len(scoreable_headlines),
        "scoreable_descriptions": len(scoreable_descriptions),
        "pending_assets": len(pending_assets),
        "min_impressions_threshold": MIN_IMPRESSIONS,
        "label_distribution": by_label,
        "unique_headline_patterns": len(headline_patterns),
        "unique_description_patterns": len(description_patterns),
        "top_headline_patterns": headline_patterns[:20],
        "top_description_patterns": description_patterns[:10],
        "worst_headline_patterns": [p for p in headline_patterns if p["low"] > 0][:10],
        "regressions_detected": len(regressions),
        "regressing_assets": regressions[:5],
    }


# ---------------------------------------------------------------------------
# 2. Cross-Campaign Asset Scan
# ---------------------------------------------------------------------------

def analyze_all_campaigns_for_country(country_code: str) -> Dict[str, Any]:
    """
    Auto-scan ALL active campaigns for a country and collect asset performance.
    This fills asset_performance + asset_performance_history for every campaign,
    giving _fetch_ai_generated_copy() complete cross-campaign data.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    # Find all ENABLED campaigns that match this country code
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND campaign.name LIKE '%{country_code}%'
    """
    try:
        results = _execute_gaql(_get_customer_id(), query, page_size=200)
    except Exception as e:
        return {"status": "error", "message": f"Failed to list campaigns: {e}"}

    campaigns_scanned = 0
    total_assets = 0
    campaign_results = []

    for row in results:
        cmp_id = str(_safe_get_value(row, "campaign.id", ""))
        cmp_name = str(_safe_get_value(row, "campaign.name", ""))

        if not cmp_id:
            continue

        result = analyze_campaign_assets(country_code, cmp_id)
        campaigns_scanned += 1
        assets_count = result.get("total_assets_analyzed", 0)
        total_assets += assets_count

        campaign_results.append({
            "campaign_id": cmp_id,
            "campaign_name": cmp_name,
            "assets_analyzed": assets_count,
        })

    return {
        "status": "success",
        "country_code": country_code,
        "campaigns_scanned": campaigns_scanned,
        "total_assets_analyzed": total_assets,
        "campaigns": campaign_results,
    }


# ---------------------------------------------------------------------------
# 3. Funnel-Stage Diagnostics
# ---------------------------------------------------------------------------

def diagnose_product_funnels(
    country_code: str,
    campaign_id: str,
    days_back: int = 30,
) -> Dict[str, Any]:
    """
    Analyze funnel performance for each product in a campaign.
    Identifies bottlenecks: LOW_IMPRESSIONS, LOW_CTR, LOW_CONV_RATE, LOW_ROAS.
    Compares each product to campaign averages.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value

    db = _get_db()
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # Get per-ad-group performance (each ad group ~ 1 product)
    query = f"""
        SELECT ad_group.id, ad_group.name, ad_group.status,
               metrics.impressions, metrics.clicks,
               metrics.conversions, metrics.conversions_value,
               metrics.cost_micros,
               campaign.id, campaign.name
        FROM ad_group
        WHERE campaign.id = {campaign_id}
          AND ad_group.status = 'ENABLED'
          AND metrics.impressions > 0
          AND segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        results = _execute_gaql(_get_customer_id(), query, page_size=500)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    # Aggregate per ad group (may have multiple rows due to date segments)
    ag_data: Dict[str, Dict[str, Any]] = {}
    for row in results:
        ag_id = str(_safe_get_value(row, "ad_group.id", ""))
        ag_name = str(_safe_get_value(row, "ad_group.name", ""))
        if not ag_id:
            continue

        if ag_id not in ag_data:
            ag_data[ag_id] = {
                "name": ag_name, "impressions": 0, "clicks": 0,
                "conversions": 0.0, "conversion_value": 0.0, "cost_micros": 0,
            }
        ag = ag_data[ag_id]
        ag["impressions"] += int(_safe_get_value(row, "metrics.impressions", 0))
        ag["clicks"] += int(_safe_get_value(row, "metrics.clicks", 0))
        ag["conversions"] += float(_safe_get_value(row, "metrics.conversions", 0))
        ag["conversion_value"] += float(_safe_get_value(row, "metrics.conversions_value", 0))
        ag["cost_micros"] += int(_safe_get_value(row, "metrics.cost_micros", 0))

    if not ag_data:
        return {"status": "success", "products_analyzed": 0, "diagnostics": []}

    # Calculate campaign averages
    total_imp = sum(a["impressions"] for a in ag_data.values())
    total_clk = sum(a["clicks"] for a in ag_data.values())
    total_conv = sum(a["conversions"] for a in ag_data.values())
    total_value = sum(a["conversion_value"] for a in ag_data.values())
    total_cost = sum(a["cost_micros"] for a in ag_data.values())

    avg_ctr = (total_clk / total_imp * 100) if total_imp > 0 else 0
    avg_conv_rate = (total_conv / total_clk * 100) if total_clk > 0 else 0
    avg_roas = (total_value / (total_cost / 1_000_000)) if total_cost > 0 else 0

    diagnostics = []
    bottleneck_counts = {"LOW_IMPRESSIONS": 0, "LOW_CTR": 0, "LOW_CONV_RATE": 0, "LOW_ROAS": 0, "HEALTHY": 0}

    for ag_id, ag in ag_data.items():
        imp = ag["impressions"]
        clk = ag["clicks"]
        conv = ag["conversions"]
        conv_val = ag["conversion_value"]
        cost = ag["cost_micros"]

        ctr = (clk / imp * 100) if imp > 0 else 0
        conv_rate = (conv / clk * 100) if clk > 0 else 0
        roas = (conv_val / (cost / 1_000_000)) if cost > 0 else 0
        cpa = (cost / conv) if conv > 0 else 0

        # Determine bottleneck
        ctr_ratio = (ctr / avg_ctr) if avg_ctr > 0 else 1
        conv_ratio = (conv_rate / avg_conv_rate) if avg_conv_rate > 0 else 1
        roas_ratio = (roas / avg_roas) if avg_roas > 0 else 1

        if imp < 100:
            bottleneck = "LOW_IMPRESSIONS"
            details = f"Only {imp} impressions in {days_back}d — product not getting visibility"
        elif ctr_ratio < 0.5:
            bottleneck = "LOW_CTR"
            details = f"CTR {ctr:.2f}% is {ctr_ratio:.0%} of campaign avg ({avg_ctr:.2f}%) — ad creative/targeting problem"
        elif conv_ratio < 0.5 and clk >= 20:
            bottleneck = "LOW_CONV_RATE"
            details = f"Conv rate {conv_rate:.2f}% is {conv_ratio:.0%} of avg ({avg_conv_rate:.2f}%) — landing page/offer issue"
        elif roas_ratio < 0.5 and conv > 1:
            bottleneck = "LOW_ROAS"
            details = f"ROAS {roas:.2f} is {roas_ratio:.0%} of avg ({avg_roas:.2f}) — value/cost mismatch"
        else:
            bottleneck = "HEALTHY"
            details = "Performing at or above campaign averages"

        bottleneck_counts[bottleneck] = bottleneck_counts.get(bottleneck, 0) + 1

        # Derive handle from ad group name
        handle = ag["name"].lower().replace(" ", "-")
        cc_suffix = f"-{country_code.lower()}"
        if not handle.endswith(cc_suffix):
            handle = handle + cc_suffix

        diag = {
            "product_handle": handle,
            "ad_group_id": ag_id,
            "ad_group_name": ag["name"],
            "impressions": imp,
            "clicks": clk,
            "conversions": round(conv, 2),
            "conversion_value": round(conv_val, 2),
            "cost_micros": cost,
            "ctr": round(ctr, 2),
            "conv_rate": round(conv_rate, 2),
            "roas": round(roas, 2),
            "cpa_micros": round(cpa),
            "funnel_bottleneck": bottleneck,
            "diagnosis": details,
            "ctr_vs_avg": round(ctr_ratio, 2),
            "conv_rate_vs_avg": round(conv_ratio, 2),
            "roas_vs_avg": round(roas_ratio, 2),
        }
        diagnostics.append(diag)

        # Store in DB
        db.upsert_funnel_diagnostic(
            country_code=country_code, campaign_id=campaign_id,
            product_handle=handle,
            ad_group_id=ag_id,
            impressions=imp, clicks=clk, conversions=conv,
            conversion_value=conv_val, cost_micros=cost,
            ctr=round(ctr, 2), conv_rate=round(conv_rate, 2),
            roas=round(roas, 2), cpa_micros=round(cpa),
            funnel_bottleneck=bottleneck, diagnosis_details=details,
            ctr_vs_avg=round(ctr_ratio, 2),
            conv_rate_vs_avg=round(conv_ratio, 2),
            roas_vs_avg=round(roas_ratio, 2),
            period_start=start_date, period_end=end_date,
        )

    # Sort: worst bottlenecks first (by cost to show money wasted)
    diagnostics.sort(key=lambda x: (0 if x["funnel_bottleneck"] != "HEALTHY" else 1, -x["cost_micros"]))

    return {
        "status": "success",
        "country_code": country_code,
        "campaign_id": campaign_id,
        "period": f"{start_date} to {end_date}",
        "products_analyzed": len(diagnostics),
        "campaign_averages": {
            "ctr": round(avg_ctr, 2),
            "conv_rate": round(avg_conv_rate, 2),
            "roas": round(avg_roas, 2),
        },
        "bottleneck_distribution": bottleneck_counts,
        "diagnostics": diagnostics,
    }


# ---------------------------------------------------------------------------
# 4. Seasonality Mining
# ---------------------------------------------------------------------------

def mine_seasonality_patterns(
    country_code: str,
    days_back: int = 365,
) -> Dict[str, Any]:
    """
    Mine monthly performance patterns by product category.
    Analyzes the last year of data to find seasonal trends
    (e.g., outdoor gear peaks in spring, electronics peak in Q4).
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value
    from batch_intelligence import classify_product, _normalize_product_name

    db = _get_db()
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # Get monthly performance per ad group
    query = f"""
        SELECT ad_group.name,
               segments.month,
               metrics.impressions, metrics.clicks,
               metrics.conversions, metrics.conversions_value,
               metrics.cost_micros
        FROM ad_group
        WHERE campaign.status IN ('ENABLED', 'PAUSED')
          AND campaign.name LIKE '%{country_code}%'
          AND metrics.impressions > 100
          AND segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        results = _execute_gaql(_get_customer_id(), query, page_size=1000)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    # Aggregate by category + month
    monthly: Dict[str, Dict[int, Dict[str, float]]] = {}

    for row in results:
        ag_name = str(_safe_get_value(row, "ad_group.name", ""))
        month_str = str(_safe_get_value(row, "segments.month", ""))
        if not ag_name or not month_str:
            continue

        # Extract month number from GAQL month segment
        try:
            month_num = int(month_str.split("-")[1]) if "-" in month_str else int(month_str)
        except (ValueError, IndexError):
            continue

        handle = ag_name.lower().replace(" ", "-")
        category = classify_product(handle)

        if category not in monthly:
            monthly[category] = {}
        if month_num not in monthly[category]:
            monthly[category][month_num] = {
                "impressions": 0, "clicks": 0, "conversions": 0.0,
                "conversion_value": 0.0, "cost_micros": 0, "samples": 0,
            }

        m = monthly[category][month_num]
        m["impressions"] += int(_safe_get_value(row, "metrics.impressions", 0))
        m["clicks"] += int(_safe_get_value(row, "metrics.clicks", 0))
        m["conversions"] += float(_safe_get_value(row, "metrics.conversions", 0))
        m["conversion_value"] += float(_safe_get_value(row, "metrics.conversions_value", 0))
        m["cost_micros"] += int(_safe_get_value(row, "metrics.cost_micros", 0))
        m["samples"] += 1

    # Calculate rates and store in DB
    patterns_stored = 0
    category_summaries = {}

    for category, months in monthly.items():
        cat_summary = []
        for month_num, data in sorted(months.items()):
            imp = data["impressions"]
            clk = data["clicks"]
            conv = data["conversions"]
            val = data["conversion_value"]
            cost = data["cost_micros"]

            ctr = (clk / imp * 100) if imp > 0 else 0
            conv_rate = (conv / clk * 100) if clk > 0 else 0
            roas = (val / (cost / 1_000_000)) if cost > 0 else 0
            cpc = (cost / clk) if clk > 0 else 0

            db.upsert_seasonality(
                country_code=country_code, category=category, month=month_num,
                avg_ctr=round(ctr, 2), avg_conv_rate=round(conv_rate, 2),
                avg_roas=round(roas, 2), avg_cpc_micros=round(cpc),
                sample_count=data["samples"],
            )
            patterns_stored += 1

            cat_summary.append({
                "month": month_num,
                "ctr": round(ctr, 2),
                "conv_rate": round(conv_rate, 2),
                "roas": round(roas, 2),
                "samples": data["samples"],
            })

        category_summaries[category] = cat_summary

    return {
        "status": "success",
        "country_code": country_code,
        "categories_analyzed": len(monthly),
        "patterns_stored": patterns_stored,
        "seasonality": category_summaries,
    }


# ---------------------------------------------------------------------------
# 5. Frequency/Overlap Detection
# ---------------------------------------------------------------------------

def detect_product_overlap(country_code: str) -> Dict[str, Any]:
    """
    Find products running in multiple campaigns (fatigue risk).
    Uses URL-based discovery to find all campaigns per product.
    """
    from google_ads_mcp import _execute_gaql, _safe_get_value
    from batch_intelligence import _discover_ad_groups_by_url
    from batch_optimizer import XML_FEEDS

    db = _get_db()

    # Get feed handles for this country
    feed_config = XML_FEEDS.get(country_code)
    if not feed_config:
        return {"status": "error", "message": f"No feed config for {country_code}"}

    # Get all product handles from DB
    conn = db._get_conn()
    handle_rows = conn.execute(
        "SELECT DISTINCT product_handle FROM product_setup WHERE country_code=?",
        (country_code,)
    ).fetchall()
    handles = [r["product_handle"] for r in handle_rows]

    if not handles:
        return {"status": "success", "products_checked": 0, "overlaps": []}

    overlaps = []
    high_risk_count = 0

    for handle in handles[:100]:  # Cap at 100 to avoid quota burn
        pairs = _discover_ad_groups_by_url(handle)
        if len(pairs) <= 1:
            continue

        campaign_ids = list(set(p[1] for p in pairs))
        ag_count = len(pairs)

        # Assess risk level
        if ag_count >= 5 or len(campaign_ids) >= 3:
            risk = "HIGH"
            high_risk_count += 1
        elif ag_count >= 3 or len(campaign_ids) >= 2:
            risk = "MEDIUM"
        else:
            risk = "LOW"

        db.upsert_product_overlap(
            country_code=country_code, product_handle=handle,
            campaign_count=len(campaign_ids),
            active_ad_group_count=ag_count,
            campaign_ids_json=json.dumps(campaign_ids),
            overlap_risk=risk,
        )

        if risk in ("MEDIUM", "HIGH"):
            overlaps.append({
                "product_handle": handle,
                "campaigns": len(campaign_ids),
                "ad_groups": ag_count,
                "campaign_ids": campaign_ids,
                "risk": risk,
            })

    overlaps.sort(key=lambda x: (-x["ad_groups"], -x["campaigns"]))

    return {
        "status": "success",
        "country_code": country_code,
        "products_checked": len(handles),
        "products_with_overlap": len(overlaps),
        "high_risk_count": high_risk_count,
        "overlaps": overlaps[:30],
    }


# ---------------------------------------------------------------------------
# Existing helper functions
# ---------------------------------------------------------------------------

def get_winning_headlines(country_code: str, min_score: int = 2) -> List[str]:
    """
    Get list of winning headline PATTERNS for a country.
    Returns patterns like '{product} Original', 'Site Oficial România', etc.
    """
    db = _get_db()
    patterns = db.get_winning_patterns(country_code, "HEADLINE", min_occurrences=1)

    winners = []
    for p in patterns:
        score = p.get("best_count", 0) * 4 + p.get("good_count", 0) * 2 - p.get("low_count", 0) * 2
        if score >= min_score:
            winners.append(p["headline_pattern"])

    return winners


def get_winning_descriptions(country_code: str, min_score: int = 2) -> List[str]:
    """Get list of winning description patterns for a country."""
    db = _get_db()
    patterns = db.get_winning_patterns(country_code, "DESCRIPTION", min_occurrences=1)

    winners = []
    for p in patterns:
        score = p.get("best_count", 0) * 4 + p.get("good_count", 0) * 2 - p.get("low_count", 0) * 2
        if score >= min_score:
            winners.append(p["headline_pattern"])

    return winners


# ---------------------------------------------------------------------------
# Keyword Spend Analysis — Background Jobs & Helpers
# ---------------------------------------------------------------------------

# In-memory job progress (in-memory store for fast polling)
_ksa_jobs: Dict[str, Dict] = {}
_ksa_jobs_lock = threading.Lock()
_ksa_stop_flags: Dict[str, bool] = {}


def _update_ksa_job(job_id: str, db: BatchDB, **kwargs):
    """Update both in-memory progress dict and DB record."""
    with _ksa_jobs_lock:
        if job_id not in _ksa_jobs:
            _ksa_jobs[job_id] = {}
        _ksa_jobs[job_id].update(kwargs)
    # Persist critical fields to DB
    db_fields = {}
    for k in ("status", "phase", "error_message", "total_campaigns",
              "processed_campaigns", "total_keywords_fetched", "total_gaql_calls",
              "resume_checkpoint", "summary_json", "completed_at"):
        if k in kwargs:
            db_fields[k] = kwargs[k]
    if db_fields:
        try:
            db.update_keyword_spend_job(job_id, **db_fields)
        except Exception:
            pass  # Best-effort DB write


def _update_ksa_progress(job_id: str, db: BatchDB, progress: dict):
    """Update in-memory progress for fast polling."""
    with _ksa_jobs_lock:
        if job_id not in _ksa_jobs:
            _ksa_jobs[job_id] = {}
        _ksa_jobs[job_id]["progress"] = progress
    # Update DB with subset
    try:
        db.update_keyword_spend_job(
            job_id,
            processed_campaigns=progress.get("campaigns_processed", 0),
            total_keywords_fetched=progress.get("keywords_fetched", 0),
            total_gaql_calls=progress.get("gaql_calls", 0),
        )
    except Exception:
        pass


def _save_ksa_checkpoint(job_id: str, db: BatchDB, phase: str,
                          remaining_campaigns: list, period_label: str):
    """Save resume checkpoint for rate-limited or crashed jobs."""
    checkpoint = {
        "job_id": job_id,
        "phase": phase,
        "remaining_campaigns": [{"id": c["id"], "name": c["name"]}
                                 for c in remaining_campaigns],
        "period_label": period_label,
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        db.update_keyword_spend_job(
            job_id, resume_checkpoint=json.dumps(checkpoint)
        )
    except Exception:
        pass


def _parse_keyword_row(row) -> Dict:
    """Parse a GAQL result row into a flat dict."""
    try:
        return {
            "campaign_id": str(row.campaign.id),
            "campaign_name": str(row.campaign.name),
            "ad_group_id": str(row.ad_group.id),
            "ad_group_name": str(row.ad_group.name),
            "criterion_id": str(row.ad_group_criterion.criterion_id),
            "keyword_text": str(row.ad_group_criterion.keyword.text),
            "match_type": str(row.ad_group_criterion.keyword.match_type).replace(
                "KeywordMatchType.", ""
            ),
            "keyword_status": str(row.ad_group_criterion.status).replace(
                "AdGroupCriterionStatus.", ""
            ),
            "impressions": int(row.metrics.impressions),
            "clicks": int(row.metrics.clicks),
            "conversions": float(row.metrics.conversions),
            "conversions_value": float(row.metrics.conversions_value),
            "cost_micros": int(row.metrics.cost_micros),
            "segment_month": str(getattr(row.segments, "month", "")),
        }
    except Exception:
        # Fallback for unexpected row structure
        return {}


def _fetch_keyword_data_paginated(
    customer_id: str,
    campaign_id: str,
    period_start: str,
    period_end: str,
    max_pages: int = 20,
    min_impressions: int = 0,
) -> List[Dict]:
    """
    Fetch ALL keyword data for a campaign+period with automatic pagination.
    Returns deduplicated list of keyword dicts.

    Uses synchronous _execute_gaql() — same as all existing batch tools.
    """
    from google_ads_mcp import _execute_gaql

    all_rows = []
    seen_keys = set()
    cost_cutoff = None
    page = 0

    while page < max_pages:
        cost_filter = f"AND metrics.cost_micros < {cost_cutoff}" if cost_cutoff else ""
        imp_filter = (f"AND metrics.impressions >= {min_impressions}"
                      if min_impressions > 0
                      else "AND metrics.impressions > 0")

        query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   ad_group.id, ad_group.name, ad_group.status,
                   ad_group_criterion.criterion_id,
                   ad_group_criterion.keyword.text,
                   ad_group_criterion.keyword.match_type,
                   ad_group_criterion.status,
                   metrics.impressions, metrics.clicks,
                   metrics.conversions, metrics.conversions_value,
                   metrics.cost_micros,
                   segments.month
            FROM keyword_view
            WHERE campaign.id = {campaign_id}
              AND campaign.status = 'ENABLED'
              AND ad_group.status = 'ENABLED'
              AND segments.date BETWEEN '{period_start}' AND '{period_end}'
              {imp_filter}
              {cost_filter}
            ORDER BY metrics.cost_micros DESC
            LIMIT 1000
        """

        try:
            results = _execute_gaql(customer_id, query, page_size=1000)
        except Exception:
            break

        if not results:
            break

        new_rows = 0
        min_cost_this_page = float('inf')

        for row in results:
            dedup_key = (
                str(row.ad_group_criterion.criterion_id),
                str(getattr(row.segments, 'month', '')),
            )
            if dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                row_dict = _parse_keyword_row(row)
                if row_dict:
                    all_rows.append(row_dict)
                    new_rows += 1
                    if row.metrics.cost_micros < min_cost_this_page:
                        min_cost_this_page = row.metrics.cost_micros

        if len(results) < 1000:
            break
        if new_rows == 0:
            break

        cost_cutoff = min_cost_this_page
        page += 1

    return all_rows


def _bulk_insert_keyword_periods(
    conn,
    job_id: str,
    country_code: str,
    campaign: Dict,
    rows: List[Dict],
    period_label: str,
    period_start: str,
    period_end: str,
):
    """Bulk INSERT keyword data into keyword_spend_periods."""
    if not rows:
        return

    values = []
    for r in rows:
        impressions = r.get("impressions", 0)
        clicks = r.get("clicks", 0)
        conversions = r.get("conversions", 0)
        cost_micros = r.get("cost_micros", 0)
        conv_value = r.get("conversions_value", 0)

        ctr = (clicks / impressions * 100) if impressions > 0 else 0
        cpc = (cost_micros / clicks) if clicks > 0 else 0
        conv_rate = (conversions / clicks * 100) if clicks > 0 else 0
        cpa = (cost_micros / conversions) if conversions > 0 else 0
        roas = (conv_value / (cost_micros / 1_000_000)) if cost_micros > 0 else 0

        values.append((
            job_id, country_code,
            str(campaign["id"]), campaign.get("name", ""),
            str(r.get("ad_group_id", "")), r.get("ad_group_name", ""),
            str(r.get("criterion_id", "")),
            r.get("keyword_text", ""),
            r.get("match_type", ""),
            r.get("keyword_status", ""),
            r.get("quality_score"),
            period_label, period_start, period_end,
            r.get("segment_month", ""),
            impressions, clicks, conversions, conv_value, cost_micros,
            round(ctr, 2), round(cpc, 0), round(conv_rate, 2),
            round(cpa, 0), round(roas, 2),
        ))

    conn.executemany("""
        INSERT OR REPLACE INTO keyword_spend_periods (
            job_id, country_code,
            campaign_id, campaign_name,
            ad_group_id, ad_group_name,
            criterion_id, keyword_text, match_type, keyword_status,
            quality_score,
            period_label, period_start, period_end, segment_month,
            impressions, clicks, conversions, conversions_value, cost_micros,
            ctr, cpc_micros, conv_rate, cpa_micros, roas
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()


def _aggregate_keyword_comparisons(conn, job_id: str):
    """
    JOIN current + previous period data per keyword, compute deltas.
    Uses two-step LEFT JOIN + NOT EXISTS (SQLite has no FULL OUTER JOIN).
    """
    conn.execute("DELETE FROM keyword_spend_comparisons WHERE job_id = ?", (job_id,))

    # Step 1: LEFT JOIN current → previous (captures all current + matched previous)
    conn.execute("""
        INSERT INTO keyword_spend_comparisons (
            job_id, country_code, campaign_id, campaign_name,
            ad_group_id, ad_group_name,
            criterion_id, keyword_text, match_type, keyword_status,
            quality_score,
            curr_impressions, curr_clicks, curr_conversions,
            curr_conversions_value, curr_cost_micros,
            curr_ctr, curr_cpc_micros, curr_conv_rate, curr_cpa_micros, curr_roas,
            prev_impressions, prev_clicks, prev_conversions,
            prev_conversions_value, prev_cost_micros,
            prev_ctr, prev_cpc_micros, prev_conv_rate, prev_cpa_micros, prev_roas,
            delta_impressions, delta_clicks, delta_conversions,
            delta_conversions_value, delta_cost_micros,
            pct_impressions, pct_clicks, pct_conversions,
            pct_conversions_value, pct_cost_micros,
            pct_ctr, pct_cpc, pct_conv_rate, pct_cpa, pct_roas
        )
        SELECT
            c.job_id, c.country_code, c.campaign_id, c.campaign_name,
            c.ad_group_id, c.ad_group_name,
            c.criterion_id, c.keyword_text, c.match_type, c.keyword_status,
            c.quality_score,
            COALESCE(c.imp, 0), COALESCE(c.clk, 0), COALESCE(c.conv, 0),
            COALESCE(c.conv_val, 0), COALESCE(c.cost, 0),
            CASE WHEN c.imp > 0 THEN ROUND(c.clk * 100.0 / c.imp, 2) ELSE 0 END,
            CASE WHEN c.clk > 0 THEN ROUND(c.cost * 1.0 / c.clk, 0) ELSE 0 END,
            CASE WHEN c.clk > 0 THEN ROUND(c.conv * 100.0 / c.clk, 2) ELSE 0 END,
            CASE WHEN c.conv > 0 THEN ROUND(c.cost * 1.0 / c.conv, 0) ELSE 0 END,
            CASE WHEN c.cost > 0 THEN ROUND(c.conv_val / (c.cost / 1000000.0), 2) ELSE 0 END,
            COALESCE(p.imp, 0), COALESCE(p.clk, 0), COALESCE(p.conv, 0),
            COALESCE(p.conv_val, 0), COALESCE(p.cost, 0),
            CASE WHEN p.imp > 0 THEN ROUND(p.clk * 100.0 / p.imp, 2) ELSE 0 END,
            CASE WHEN p.clk > 0 THEN ROUND(p.cost * 1.0 / p.clk, 0) ELSE 0 END,
            CASE WHEN p.clk > 0 THEN ROUND(p.conv * 100.0 / p.clk, 2) ELSE 0 END,
            CASE WHEN p.conv > 0 THEN ROUND(p.cost * 1.0 / p.conv, 0) ELSE 0 END,
            CASE WHEN p.cost > 0 THEN ROUND(p.conv_val / (p.cost / 1000000.0), 2) ELSE 0 END,
            COALESCE(c.imp, 0) - COALESCE(p.imp, 0),
            COALESCE(c.clk, 0) - COALESCE(p.clk, 0),
            COALESCE(c.conv, 0) - COALESCE(p.conv, 0),
            COALESCE(c.conv_val, 0) - COALESCE(p.conv_val, 0),
            COALESCE(c.cost, 0) - COALESCE(p.cost, 0),
            CASE WHEN p.imp > 0 THEN ROUND((c.imp - p.imp) * 100.0 / p.imp, 1) ELSE NULL END,
            CASE WHEN p.clk > 0 THEN ROUND((c.clk - p.clk) * 100.0 / p.clk, 1) ELSE NULL END,
            CASE WHEN p.conv > 0 THEN ROUND((c.conv - p.conv) * 100.0 / p.conv, 1) ELSE NULL END,
            CASE WHEN p.conv_val > 0 THEN ROUND((c.conv_val - p.conv_val) * 100.0 / p.conv_val, 1) ELSE NULL END,
            CASE WHEN p.cost > 0 THEN ROUND((c.cost - p.cost) * 100.0 / p.cost, 1) ELSE NULL END,
            CASE WHEN p.imp > 0 AND p.clk > 0 AND c.imp > 0
                 THEN ROUND(((c.clk*100.0/c.imp) - (p.clk*100.0/p.imp)) * 100.0 / (p.clk*100.0/p.imp), 1)
                 ELSE NULL END,
            CASE WHEN p.clk > 0 AND c.clk > 0
                 THEN ROUND(((c.cost*1.0/c.clk) - (p.cost*1.0/p.clk)) * 100.0 / (p.cost*1.0/p.clk), 1)
                 ELSE NULL END,
            CASE WHEN p.clk > 0 AND p.conv > 0 AND c.clk > 0
                 THEN ROUND(((c.conv*100.0/c.clk) - (p.conv*100.0/p.clk)) * 100.0 / (p.conv*100.0/p.clk), 1)
                 ELSE NULL END,
            CASE WHEN p.conv > 0 AND c.conv > 0
                 THEN ROUND(((c.cost*1.0/c.conv) - (p.cost*1.0/p.conv)) * 100.0 / (p.cost*1.0/p.conv), 1)
                 ELSE NULL END,
            CASE WHEN p.cost > 0 AND p.conv_val > 0 AND c.cost > 0
                 THEN ROUND(((c.conv_val/(c.cost/1e6)) - (p.conv_val/(p.cost/1e6))) * 100.0 / (p.conv_val/(p.cost/1e6)), 1)
                 ELSE NULL END
        FROM (
            SELECT job_id, country_code, campaign_id, campaign_name,
                   ad_group_id, ad_group_name,
                   criterion_id, keyword_text, match_type, keyword_status,
                   MAX(quality_score) as quality_score,
                   SUM(impressions) as imp, SUM(clicks) as clk,
                   SUM(conversions) as conv, SUM(conversions_value) as conv_val,
                   SUM(cost_micros) as cost
            FROM keyword_spend_periods
            WHERE job_id = ? AND period_label = 'current'
            GROUP BY criterion_id, campaign_id
        ) c
        LEFT JOIN (
            SELECT job_id, country_code, campaign_id,
                   criterion_id,
                   SUM(impressions) as imp, SUM(clicks) as clk,
                   SUM(conversions) as conv, SUM(conversions_value) as conv_val,
                   SUM(cost_micros) as cost
            FROM keyword_spend_periods
            WHERE job_id = ? AND period_label = 'previous'
            GROUP BY criterion_id, campaign_id
        ) p ON c.criterion_id = p.criterion_id AND c.campaign_id = p.campaign_id
        WHERE COALESCE(c.cost, 0) + COALESCE(p.cost, 0) > 0
    """, (job_id, job_id))

    # Step 2: INSERT previous-only keywords (STOPPED — not in current period)
    conn.execute("""
        INSERT INTO keyword_spend_comparisons (
            job_id, country_code, campaign_id, campaign_name,
            ad_group_id, ad_group_name,
            criterion_id, keyword_text, match_type, keyword_status,
            quality_score,
            curr_impressions, curr_clicks, curr_conversions,
            curr_conversions_value, curr_cost_micros,
            curr_ctr, curr_cpc_micros, curr_conv_rate, curr_cpa_micros, curr_roas,
            prev_impressions, prev_clicks, prev_conversions,
            prev_conversions_value, prev_cost_micros,
            prev_ctr, prev_cpc_micros, prev_conv_rate, prev_cpa_micros, prev_roas,
            delta_impressions, delta_clicks, delta_conversions,
            delta_conversions_value, delta_cost_micros,
            pct_impressions, pct_clicks, pct_conversions,
            pct_conversions_value, pct_cost_micros,
            pct_ctr, pct_cpc, pct_conv_rate, pct_cpa, pct_roas
        )
        SELECT
            p.job_id, p.country_code, p.campaign_id, p.campaign_name,
            p.ad_group_id, p.ad_group_name,
            p.criterion_id, p.keyword_text, p.match_type, p.keyword_status,
            p.quality_score,
            0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
            COALESCE(p.imp, 0), COALESCE(p.clk, 0), COALESCE(p.conv, 0),
            COALESCE(p.conv_val, 0), COALESCE(p.cost, 0),
            CASE WHEN p.imp > 0 THEN ROUND(p.clk * 100.0 / p.imp, 2) ELSE 0 END,
            CASE WHEN p.clk > 0 THEN ROUND(p.cost * 1.0 / p.clk, 0) ELSE 0 END,
            CASE WHEN p.clk > 0 THEN ROUND(p.conv * 100.0 / p.clk, 2) ELSE 0 END,
            CASE WHEN p.conv > 0 THEN ROUND(p.cost * 1.0 / p.conv, 0) ELSE 0 END,
            CASE WHEN p.cost > 0 THEN ROUND(p.conv_val / (p.cost / 1000000.0), 2) ELSE 0 END,
            0 - COALESCE(p.imp, 0),
            0 - COALESCE(p.clk, 0),
            0 - COALESCE(p.conv, 0),
            0 - COALESCE(p.conv_val, 0),
            0 - COALESCE(p.cost, 0),
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL
        FROM (
            SELECT job_id, country_code, campaign_id, campaign_name,
                   ad_group_id, ad_group_name,
                   criterion_id, keyword_text, match_type, keyword_status,
                   MAX(quality_score) as quality_score,
                   SUM(impressions) as imp, SUM(clicks) as clk,
                   SUM(conversions) as conv, SUM(conversions_value) as conv_val,
                   SUM(cost_micros) as cost
            FROM keyword_spend_periods
            WHERE job_id = ? AND period_label = 'previous'
            GROUP BY criterion_id, campaign_id
        ) p
        WHERE NOT EXISTS (
            SELECT 1 FROM keyword_spend_periods c2
            WHERE c2.job_id = ? AND c2.period_label = 'current'
              AND c2.criterion_id = p.criterion_id
              AND c2.campaign_id = p.campaign_id
        )
        AND COALESCE(p.cost, 0) > 0
    """, (job_id, job_id))

    conn.commit()


def _classify_keyword_trends(conn, job_id: str, spike_threshold: float):
    """Classify each keyword comparison row with trend_flag, severity, category."""
    conn.row_factory = _dict_factory
    rows = conn.execute(
        "SELECT id, curr_cost_micros, prev_cost_micros, pct_cost_micros, "
        "delta_cost_micros, curr_conversions, prev_conversions, "
        "curr_roas, prev_roas, pct_conv_rate "
        "FROM keyword_spend_comparisons WHERE job_id = ?",
        (job_id,)
    ).fetchall()

    updates = []
    for row in rows:
        curr_cost = row["curr_cost_micros"] or 0
        prev_cost = row["prev_cost_micros"] or 0
        pct_cost = row["pct_cost_micros"]
        delta_cost = row["delta_cost_micros"] or 0
        curr_conv = row["curr_conversions"] or 0
        prev_conv = row["prev_conversions"] or 0

        # Trend flag
        if prev_cost == 0 and curr_cost > 0:
            trend = "NEW"
        elif curr_cost == 0 and prev_cost > 0:
            trend = "STOPPED"
        elif pct_cost is not None and pct_cost > spike_threshold:
            trend = "SPIKE"
        elif pct_cost is not None and pct_cost < -spike_threshold:
            trend = "DROP"
        else:
            trend = "STABLE"

        # Severity
        abs_pct = abs(pct_cost) if pct_cost is not None else 0
        abs_delta_usd = abs(delta_cost) / 1_000_000

        if abs_pct > 200 or abs_delta_usd > 50:
            severity = "CRITICAL"
        elif abs_pct > 100 or abs_delta_usd > 20:
            severity = "HIGH"
        elif abs_pct > spike_threshold:
            severity = "MEDIUM"
        else:
            severity = "LOW"

        # Category
        if trend == "NEW":
            category = "new_keyword"
        elif trend == "STOPPED":
            category = "dead_keyword"
        elif trend == "SPIKE" and curr_conv > prev_conv:
            category = "scaling_winner"
        elif trend == "SPIKE" and curr_conv <= prev_conv:
            category = "spend_waste"
        elif trend == "DROP" and curr_conv >= prev_conv:
            category = "efficiency_up"
        elif trend == "DROP" and curr_conv < prev_conv:
            category = "declining"
        else:
            category = "stable"

        updates.append((trend, severity, category, row["id"]))

    conn.executemany(
        "UPDATE keyword_spend_comparisons SET trend_flag = ?, severity = ?, category = ? WHERE id = ?",
        updates
    )
    conn.commit()


def _dict_factory(cursor, row):
    """SQLite row_factory that returns dicts."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _query_to_dicts(conn, query: str, params: tuple) -> List[Dict]:
    """Execute query and return list of dicts."""
    old_factory = conn.row_factory
    conn.row_factory = _dict_factory
    try:
        rows = conn.execute(query, params).fetchall()
        return rows
    finally:
        conn.row_factory = old_factory


def _format_keyword_detail(row: Dict, currency: str = "PLN") -> Dict:
    """Format a comparison row into human-readable keyword detail object."""
    def _fmt_money(micros):
        if micros is None:
            return f"{currency} 0.00"
        return f"{currency} {micros / 1_000_000:,.2f}"

    def _fmt_pct(val):
        if val is None:
            return "N/A"
        return f"{val:+.1f}%"

    def _fmt_roas(val):
        if val is None or val == 0:
            return "0.00x"
        return f"{val:.2f}x"

    return {
        "keyword_text": row.get("keyword_text", ""),
        "match_type": row.get("match_type", ""),
        "campaign_name": row.get("campaign_name", ""),
        "ad_group_name": row.get("ad_group_name", ""),
        "criterion_id": row.get("criterion_id", ""),
        "trend_flag": row.get("trend_flag", ""),
        "severity": row.get("severity", ""),
        "category": row.get("category", ""),
        "quality_score": row.get("quality_score"),
        "current": {
            "impressions": row.get("curr_impressions", 0),
            "clicks": row.get("curr_clicks", 0),
            "conversions": row.get("curr_conversions", 0),
            "conversions_value": row.get("curr_conversions_value", 0),
            "cost": _fmt_money(row.get("curr_cost_micros", 0)),
            "cost_micros": row.get("curr_cost_micros", 0),
            "ctr": f"{row.get('curr_ctr', 0):.2f}%",
            "cpc": _fmt_money(row.get("curr_cpc_micros", 0)),
            "conv_rate": f"{row.get('curr_conv_rate', 0):.2f}%",
            "cpa": _fmt_money(row.get("curr_cpa_micros", 0)),
            "roas": _fmt_roas(row.get("curr_roas", 0)),
        },
        "previous": {
            "impressions": row.get("prev_impressions", 0),
            "clicks": row.get("prev_clicks", 0),
            "conversions": row.get("prev_conversions", 0),
            "conversions_value": row.get("prev_conversions_value", 0),
            "cost": _fmt_money(row.get("prev_cost_micros", 0)),
            "cost_micros": row.get("prev_cost_micros", 0),
            "ctr": f"{row.get('prev_ctr', 0):.2f}%",
            "cpc": _fmt_money(row.get("prev_cpc_micros", 0)),
            "conv_rate": f"{row.get('prev_conv_rate', 0):.2f}%",
            "cpa": _fmt_money(row.get("prev_cpa_micros", 0)),
            "roas": _fmt_roas(row.get("prev_roas", 0)),
        },
        "delta": {
            "impressions": f"{row.get('delta_impressions', 0):+,} ({_fmt_pct(row.get('pct_impressions'))})",
            "clicks": f"{row.get('delta_clicks', 0):+,} ({_fmt_pct(row.get('pct_clicks'))})",
            "conversions": f"{row.get('delta_conversions', 0):+.1f} ({_fmt_pct(row.get('pct_conversions'))})",
            "cost": f"{_fmt_money(row.get('delta_cost_micros', 0))} ({_fmt_pct(row.get('pct_cost_micros'))})",
            "ctr_pct": _fmt_pct(row.get("pct_ctr")),
            "cpc_pct": _fmt_pct(row.get("pct_cpc")),
            "conv_rate_pct": _fmt_pct(row.get("pct_conv_rate")),
            "cpa_pct": _fmt_pct(row.get("pct_cpa")),
            "roas_pct": _fmt_pct(row.get("pct_roas")),
        },
    }


def _build_final_report(conn, job_id: str, params: dict) -> Dict:
    """Build final structured report from keyword_spend_comparisons."""
    top_n = params.get("top_n", 50)

    report = {}

    # Top spend spikes
    report["top_spikes"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND trend_flag = 'SPIKE'
        ORDER BY pct_cost_micros DESC
        LIMIT ?
    """, (job_id, top_n))

    # Top spend drops
    report["top_drops"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND trend_flag = 'DROP'
        ORDER BY pct_cost_micros ASC
        LIMIT ?
    """, (job_id, top_n))

    # Top performers (best current ROAS with meaningful spend)
    report["top_performers"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND curr_conversions > 0 AND curr_cost_micros > 1000000
        ORDER BY curr_roas DESC
        LIMIT ?
    """, (job_id, top_n))

    # Bottom performers (worst current CPA with meaningful spend)
    report["bottom_performers"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND curr_cost_micros > 5000000 AND curr_conversions > 0
        ORDER BY curr_cpa_micros DESC
        LIMIT ?
    """, (job_id, top_n))

    # Spend waste alerts
    report["spend_waste_alerts"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND category = 'spend_waste'
        ORDER BY delta_cost_micros DESC
        LIMIT ?
    """, (job_id, top_n))

    # New keywords
    report["new_keywords"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND trend_flag = 'NEW'
        ORDER BY curr_cost_micros DESC
        LIMIT ?
    """, (job_id, top_n))

    # Stopped keywords
    report["stopped_keywords"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND trend_flag = 'STOPPED'
        ORDER BY prev_cost_micros DESC
        LIMIT ?
    """, (job_id, top_n))

    # Monthly breakdown
    if params.get("include_monthly_breakdown", True):
        report["monthly_breakdown"] = _query_to_dicts(conn, """
            SELECT segment_month,
                   period_label,
                   SUM(impressions) as total_impressions,
                   SUM(clicks) as total_clicks,
                   SUM(conversions) as total_conversions,
                   SUM(conversions_value) as total_conv_value,
                   SUM(cost_micros) as total_cost_micros,
                   COUNT(DISTINCT criterion_id) as unique_keywords
            FROM keyword_spend_periods
            WHERE job_id = ?
            GROUP BY segment_month, period_label
            ORDER BY segment_month ASC
        """, (job_id,))

    # Drastic conversion changes (>50% either direction, regardless of spend)
    report["conversion_spikes"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND pct_conversions IS NOT NULL AND pct_conversions > 50
          AND (curr_conversions > 1 OR prev_conversions > 1)
        ORDER BY pct_conversions DESC
        LIMIT ?
    """, (job_id, top_n))

    report["conversion_drops"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND pct_conversions IS NOT NULL AND pct_conversions < -50
          AND prev_conversions > 1
        ORDER BY pct_conversions ASC
        LIMIT ?
    """, (job_id, top_n))

    # Bottom performers — PREVIOUS period (worst CPA in previous period, for trajectory comparison)
    report["bottom_performers_previous"] = _query_to_dicts(conn, """
        SELECT * FROM keyword_spend_comparisons
        WHERE job_id = ? AND prev_cost_micros > 5000000 AND prev_conversions > 0
        ORDER BY prev_cpa_micros DESC
        LIMIT ?
    """, (job_id, top_n))

    # Monthly top keyword ranking — last month of each period
    # Find last month labels for current and previous periods
    month_labels = _query_to_dicts(conn, """
        SELECT DISTINCT segment_month, period_label
        FROM keyword_spend_periods
        WHERE job_id = ?
        ORDER BY segment_month DESC
    """, (job_id,))

    last_current_month = None
    last_previous_month = None
    for m in month_labels:
        if m["period_label"] == "current" and not last_current_month:
            last_current_month = m["segment_month"]
        if m["period_label"] == "previous" and not last_previous_month:
            last_previous_month = m["segment_month"]

    if last_current_month:
        report["top_keywords_current_month"] = _query_to_dicts(conn, """
            SELECT criterion_id, keyword_text, campaign_name, ad_group_name,
                   SUM(cost_micros) as month_cost, SUM(clicks) as month_clicks,
                   SUM(conversions) as month_conv, SUM(impressions) as month_imp
            FROM keyword_spend_periods
            WHERE job_id = ? AND period_label = 'current' AND segment_month = ?
            GROUP BY criterion_id, campaign_id
            ORDER BY month_cost DESC
            LIMIT 20
        """, (job_id, last_current_month))
        report["current_month_label"] = last_current_month
    else:
        report["top_keywords_current_month"] = []

    if last_previous_month:
        report["top_keywords_previous_month"] = _query_to_dicts(conn, """
            SELECT criterion_id, keyword_text, campaign_name, ad_group_name,
                   SUM(cost_micros) as month_cost, SUM(clicks) as month_clicks,
                   SUM(conversions) as month_conv, SUM(impressions) as month_imp
            FROM keyword_spend_periods
            WHERE job_id = ? AND period_label = 'previous' AND segment_month = ?
            GROUP BY criterion_id, campaign_id
            ORDER BY month_cost DESC
            LIMIT 20
        """, (job_id, last_previous_month))
        report["previous_month_label"] = last_previous_month
    else:
        report["top_keywords_previous_month"] = []

    # Campaign-level rollup
    report["campaign_rollup"] = _query_to_dicts(conn, """
        SELECT campaign_id, campaign_name,
               SUM(curr_cost_micros) as curr_total_cost,
               SUM(prev_cost_micros) as prev_total_cost,
               SUM(curr_clicks) as curr_total_clicks,
               SUM(prev_clicks) as prev_total_clicks,
               SUM(curr_impressions) as curr_total_imp,
               SUM(prev_impressions) as prev_total_imp,
               SUM(curr_conversions) as curr_total_conv,
               SUM(prev_conversions) as prev_total_conv,
               SUM(curr_conversions_value) as curr_total_conv_value,
               SUM(prev_conversions_value) as prev_total_conv_value,
               COUNT(*) as keyword_count,
               SUM(CASE WHEN trend_flag = 'SPIKE' THEN 1 ELSE 0 END) as spikes,
               SUM(CASE WHEN trend_flag = 'DROP' THEN 1 ELSE 0 END) as drops,
               SUM(CASE WHEN category = 'spend_waste' THEN 1 ELSE 0 END) as waste_alerts
        FROM keyword_spend_comparisons
        WHERE job_id = ?
        GROUP BY campaign_id
        ORDER BY curr_total_cost DESC
    """, (job_id,))

    # Ad Group rollup — for TOP 3 campaigns by current spend
    top_campaign_ids = [c["campaign_id"] for c in report["campaign_rollup"][:3]]
    report["adgroup_rollup"] = {}
    for cid in top_campaign_ids:
        rows = _query_to_dicts(conn, """
            SELECT ad_group_id, ad_group_name, campaign_name,
                   SUM(curr_cost_micros) as curr_cost,
                   SUM(prev_cost_micros) as prev_cost,
                   SUM(curr_clicks) as curr_clicks,
                   SUM(prev_clicks) as prev_clicks,
                   SUM(curr_conversions) as curr_conv,
                   SUM(prev_conversions) as prev_conv,
                   SUM(curr_conversions_value) as curr_conv_value,
                   SUM(prev_conversions_value) as prev_conv_value,
                   COUNT(*) as keyword_count,
                   SUM(CASE WHEN category = 'spend_waste' THEN 1 ELSE 0 END) as waste_alerts
            FROM keyword_spend_comparisons
            WHERE job_id = ? AND campaign_id = ?
            GROUP BY ad_group_id
            ORDER BY curr_cost DESC
        """, (job_id, cid))
        # Enrich with deltas
        for r in rows:
            cc = r.get("curr_cost") or 0
            pc = r.get("prev_cost") or 0
            r["pct_cost"] = round((cc - pc) * 100.0 / pc, 1) if pc > 0 else None
            cv = r.get("curr_conv") or 0
            pv = r.get("prev_conv") or 0
            r["pct_conv"] = round((cv - pv) * 100.0 / pv, 1) if pv > 0 else None
            r["curr_cpa"] = round(cc / cv) if cv > 0 else 0
            r["prev_cpa"] = round(pc / pv) if pv > 0 else 0
            r["curr_roas"] = round(r.get("curr_conv_value", 0) / (cc / 1e6), 2) if cc > 0 else 0
            r["prev_roas"] = round(r.get("prev_conv_value", 0) / (pc / 1e6), 2) if pc > 0 else 0
        report["adgroup_rollup"][cid] = rows

    # Spend waste total estimate
    waste_total = _query_to_dicts(conn, """
        SELECT SUM(delta_cost_micros) as total_wasted_micros,
               COUNT(*) as waste_keyword_count
        FROM keyword_spend_comparisons
        WHERE job_id = ? AND category = 'spend_waste' AND delta_cost_micros > 0
    """, (job_id,))
    report["spend_waste_total"] = waste_total[0] if waste_total else {"total_wasted_micros": 0, "waste_keyword_count": 0}

    # Summary stats
    summary_rows = _query_to_dicts(conn, """
        SELECT
            COUNT(*) as total_keywords,
            SUM(CASE WHEN trend_flag != 'STABLE' THEN 1 ELSE 0 END) as keywords_with_changes,
            SUM(curr_cost_micros) as current_total_cost,
            SUM(prev_cost_micros) as previous_total_cost,
            SUM(curr_conversions) as current_total_conversions,
            SUM(prev_conversions) as previous_total_conversions,
            SUM(CASE WHEN trend_flag = 'SPIKE' THEN 1 ELSE 0 END) as spike_count,
            SUM(CASE WHEN trend_flag = 'DROP' THEN 1 ELSE 0 END) as drop_count,
            SUM(CASE WHEN trend_flag = 'STABLE' THEN 1 ELSE 0 END) as stable_count,
            SUM(CASE WHEN trend_flag = 'NEW' THEN 1 ELSE 0 END) as new_count,
            SUM(CASE WHEN trend_flag = 'STOPPED' THEN 1 ELSE 0 END) as stopped_count,
            SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
            SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high_count,
            SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) as medium_count,
            SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) as low_count,
            SUM(CASE WHEN category = 'scaling_winner' THEN 1 ELSE 0 END) as scaling_winner_count,
            SUM(CASE WHEN category = 'spend_waste' THEN 1 ELSE 0 END) as spend_waste_count,
            SUM(CASE WHEN category = 'efficiency_up' THEN 1 ELSE 0 END) as efficiency_up_count,
            SUM(CASE WHEN category = 'declining' THEN 1 ELSE 0 END) as declining_count,
            SUM(CASE WHEN category = 'new_keyword' THEN 1 ELSE 0 END) as new_keyword_count,
            SUM(CASE WHEN category = 'dead_keyword' THEN 1 ELSE 0 END) as dead_keyword_count,
            SUM(CASE WHEN category = 'stable' THEN 1 ELSE 0 END) as stable_keyword_count
        FROM keyword_spend_comparisons
        WHERE job_id = ?
    """, (job_id,))

    if summary_rows:
        s = summary_rows[0]
        curr_cost = s.get("current_total_cost", 0) or 0
        prev_cost = s.get("previous_total_cost", 0) or 0
        curr_conv = s.get("current_total_conversions", 0) or 0
        prev_conv = s.get("previous_total_conversions", 0) or 0

        spend_pct = round((curr_cost - prev_cost) * 100.0 / prev_cost, 1) if prev_cost > 0 else None
        conv_pct = round((curr_conv - prev_conv) * 100.0 / prev_conv, 1) if prev_conv > 0 else None

        report["summary"] = {
            "total_keywords_analyzed": s.get("total_keywords", 0),
            "total_keywords_with_changes": s.get("keywords_with_changes", 0),
            "current_total_spend_micros": curr_cost,
            "previous_total_spend_micros": prev_cost,
            "spend_change_pct": spend_pct,
            "current_total_conversions": round(curr_conv, 1),
            "previous_total_conversions": round(prev_conv, 1),
            "conversions_change_pct": conv_pct,
            "trend_distribution": {
                "SPIKE": s.get("spike_count", 0),
                "DROP": s.get("drop_count", 0),
                "STABLE": s.get("stable_count", 0),
                "NEW": s.get("new_count", 0),
                "STOPPED": s.get("stopped_count", 0),
            },
            "severity_distribution": {
                "CRITICAL": s.get("critical_count", 0),
                "HIGH": s.get("high_count", 0),
                "MEDIUM": s.get("medium_count", 0),
                "LOW": s.get("low_count", 0),
            },
            "category_distribution": {
                "scaling_winner": s.get("scaling_winner_count", 0),
                "spend_waste": s.get("spend_waste_count", 0),
                "efficiency_up": s.get("efficiency_up_count", 0),
                "declining": s.get("declining_count", 0),
                "new_keyword": s.get("new_keyword_count", 0),
                "dead_keyword": s.get("dead_keyword_count", 0),
                "stable": s.get("stable_keyword_count", 0),
            },
        }
    else:
        report["summary"] = {"total_keywords_analyzed": 0}

    # Format keyword details for top sections
    for section in ("top_spikes", "top_drops", "top_performers", "bottom_performers",
                     "spend_waste_alerts", "new_keywords", "stopped_keywords",
                     "conversion_spikes", "conversion_drops", "bottom_performers_previous"):
        if section in report:
            report[section] = [_format_keyword_detail(r) for r in report[section]]

    return report


def _map_adgroup_to_product(customer_id: str, campaign_id: str) -> Dict[str, str]:
    """Map ad_group_id → product_handle via RSA final_urls."""
    from google_ads_mcp import _execute_gaql
    import re

    query = f"""
        SELECT ad_group.id, ad_group.name,
               ad_group_ad.ad.final_urls
        FROM ad_group_ad
        WHERE campaign.id = {campaign_id}
          AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
          AND ad_group_ad.status != 'REMOVED'
        LIMIT 1000
    """

    mapping = {}
    try:
        results = _execute_gaql(customer_id, query, page_size=1000)
        for row in results:
            ag_id = str(row.ad_group.id)
            if ag_id in mapping:
                continue
            urls = list(row.ad_group_ad.ad.final_urls) if row.ad_group_ad.ad.final_urls else []
            if urls:
                # Extract handle from URL path
                url = urls[0]
                match = re.search(r'/product/([^/?#]+)', url)
                if match:
                    mapping[ag_id] = match.group(1)
                else:
                    # Fallback: use ad_group name
                    mapping[ag_id] = str(row.ad_group.name).lower().replace(" ", "-")
            else:
                mapping[ag_id] = str(row.ad_group.name).lower().replace(" ", "-")
    except Exception:
        pass

    return mapping


def _batch_keyword_spend_analysis_worker(
    job_id: str,
    customer_id: str,
    country_code: str,
    campaign_list: List[Dict],
    current_start: str,
    current_end: str,
    previous_start: str,
    previous_end: str,
    params: Dict,
):
    """
    Long-running background worker for keyword spend analysis.
    Populates keyword_spend_periods, then aggregates into keyword_spend_comparisons.
    Thread-safe: uses own DB connection, updates _ksa_jobs under lock.
    """
    import sqlite3

    db = BatchDB()
    conn = db._get_conn()
    conn.row_factory = sqlite3.Row

    total_campaigns = len(campaign_list)
    gaql_call_count = 0
    total_keywords = 0

    try:
        # ══════════════════════════════════════════════
        # PHASE 1: FETCH CURRENT PERIOD
        # ══════════════════════════════════════════════
        _update_ksa_job(job_id, db, phase="fetch_current", status="running")

        for i, campaign in enumerate(campaign_list):
            if _ksa_stop_flags.get(job_id):
                _save_ksa_checkpoint(job_id, db, "fetch_current", campaign_list[i:], "current")
                _update_ksa_job(job_id, db, status="stopped")
                return

            if gaql_call_count > 1400:
                _save_ksa_checkpoint(job_id, db, "fetch_current", campaign_list[i:], "current")
                _update_ksa_job(job_id, db, status="rate_limited",
                                error_message=f"GAQL call limit approached ({gaql_call_count} calls)")
                return

            rows = _fetch_keyword_data_paginated(
                customer_id=customer_id,
                campaign_id=campaign["id"],
                period_start=current_start,
                period_end=current_end,
                min_impressions=params.get("min_impressions", 10),
            )
            gaql_call_count += (len(rows) // 1000) + 1

            _bulk_insert_keyword_periods(
                conn, job_id, country_code, campaign,
                rows, period_label="current",
                period_start=current_start, period_end=current_end,
            )
            total_keywords += len(rows)

            _update_ksa_progress(job_id, db, {
                "phase": "fetch_current",
                "campaigns_processed": i + 1,
                "campaigns_total": total_campaigns * 2,
                "keywords_fetched": total_keywords,
                "gaql_calls": gaql_call_count,
                "current_campaign": campaign["name"],
                "percent": int((i + 1) / (total_campaigns * 2) * 100),
            })

            time.sleep(0.5)

        # ══════════════════════════════════════════════
        # PHASE 2: FETCH PREVIOUS PERIOD
        # ══════════════════════════════════════════════
        _update_ksa_job(job_id, db, phase="fetch_previous")

        for i, campaign in enumerate(campaign_list):
            if _ksa_stop_flags.get(job_id):
                _save_ksa_checkpoint(job_id, db, "fetch_previous", campaign_list[i:], "previous")
                _update_ksa_job(job_id, db, status="stopped")
                return

            if gaql_call_count > 1400:
                _save_ksa_checkpoint(job_id, db, "fetch_previous", campaign_list[i:], "previous")
                _update_ksa_job(job_id, db, status="rate_limited",
                                error_message=f"GAQL call limit approached ({gaql_call_count} calls)")
                return

            rows = _fetch_keyword_data_paginated(
                customer_id=customer_id,
                campaign_id=campaign["id"],
                period_start=previous_start,
                period_end=previous_end,
                min_impressions=params.get("min_impressions", 10),
            )
            gaql_call_count += (len(rows) // 1000) + 1

            _bulk_insert_keyword_periods(
                conn, job_id, country_code, campaign,
                rows, period_label="previous",
                period_start=previous_start, period_end=previous_end,
            )
            total_keywords += len(rows)

            _update_ksa_progress(job_id, db, {
                "phase": "fetch_previous",
                "campaigns_processed": total_campaigns + i + 1,
                "campaigns_total": total_campaigns * 2,
                "keywords_fetched": total_keywords,
                "gaql_calls": gaql_call_count,
                "current_campaign": campaign["name"],
                "percent": int((total_campaigns + i + 1) / (total_campaigns * 2) * 100),
            })

            time.sleep(0.5)

        # ══════════════════════════════════════════════
        # PHASE 3: AGGREGATE
        # ══════════════════════════════════════════════
        _update_ksa_job(job_id, db, phase="aggregate")
        _aggregate_keyword_comparisons(conn, job_id)

        # ══════════════════════════════════════════════
        # PHASE 4: CLASSIFY
        # ══════════════════════════════════════════════
        _update_ksa_job(job_id, db, phase="classify")
        _classify_keyword_trends(conn, job_id, params.get("spike_threshold_pct", 50.0))

        # ══════════════════════════════════════════════
        # PHASE 5: FINALIZE
        # ══════════════════════════════════════════════
        _update_ksa_job(job_id, db, phase="finalize")
        summary = _build_final_report(conn, job_id, params)

        # Save to DB
        conn.execute(
            "UPDATE keyword_spend_jobs SET summary_json = ?, status = 'completed', "
            "completed_at = datetime('now'), total_keywords_fetched = ?, total_gaql_calls = ? "
            "WHERE job_id = ?",
            (json.dumps(summary, default=str), total_keywords, gaql_call_count, job_id)
        )
        conn.commit()

        _update_ksa_job(job_id, db, status="completed", phase="done",
                         completed_at=time.strftime("%Y-%m-%d %H:%M:%S"))

        # Store full summary in memory for fast retrieval
        with _ksa_jobs_lock:
            _ksa_jobs[job_id]["summary"] = summary

    except Exception as e:
        _update_ksa_job(job_id, db, status="failed",
                         error_message=str(e), phase="error")
        _save_ksa_checkpoint(job_id, db, "crash", campaign_list, "unknown")

    finally:
        _ksa_stop_flags.pop(job_id, None)


async def analyze_keyword_spend_trends(
    customer_id: str,
    country_code: str,
    campaign_ids: List[str],
    current_period_days: int,
    previous_period_days: int,
    min_impressions: int,
    min_cost_micros: int,
    spike_threshold_pct: float,
    top_n: int,
    include_monthly_breakdown: bool,
    include_quality_scores: bool,
    include_product_mapping: bool,
) -> Dict:
    """
    Main entry point for keyword spend analysis.
    Decides sync vs async based on workload size.
    """
    from google_ads_mcp import _execute_gaql

    db = _get_db()

    # Clean old data first
    db.cleanup_old_keyword_spend_data(days_old=7)

    # Calculate date ranges
    today = datetime.now().date()
    current_end = today - timedelta(days=1)
    current_start = current_end - timedelta(days=current_period_days - 1)
    previous_end = current_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=previous_period_days - 1)

    cc = country_code.upper()

    # Discover campaigns if not provided
    if not campaign_ids:
        query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   campaign.advertising_channel_type
            FROM campaign
            WHERE campaign.status = 'ENABLED'
              AND campaign.advertising_channel_type IN ('SEARCH')
              AND campaign.name LIKE '{cc} %'
        """
        try:
            results = _execute_gaql(customer_id, query, page_size=100)
            campaign_list = []
            for row in results:
                campaign_list.append({
                    "id": str(row.campaign.id),
                    "name": str(row.campaign.name),
                })
        except Exception as e:
            return {"status": "failed", "error": f"Campaign discovery failed: {e}"}
    else:
        # Validate provided campaign IDs
        campaign_list = []
        for cid in campaign_ids:
            query = f"""
                SELECT campaign.id, campaign.name, campaign.status,
                       campaign.advertising_channel_type
                FROM campaign
                WHERE campaign.id = {cid}
            """
            try:
                results = _execute_gaql(customer_id, query, page_size=1)
                for row in results:
                    ch_type = row.campaign.advertising_channel_type
                    # advertising_channel_type returns int enum: SEARCH=2
                    is_search = (ch_type == 2
                                 or "SEARCH" in str(ch_type).upper())
                    if is_search:
                        campaign_list.append({
                            "id": str(row.campaign.id),
                            "name": str(row.campaign.name),
                        })
                    # Skip PMax/Shopping silently
            except Exception as e:
                log.warning(f"KSA: campaign validation failed for {cid}: {e}")

    if not campaign_list:
        return {
            "status": "completed",
            "summary": {"total_keywords_analyzed": 0},
            "message": f"No ENABLED Search campaigns found for {cc}.",
        }

    # Generate job ID
    job_id = f"ksa_{cc}_{uuid.uuid4().hex[:8]}"

    # Check for resumable job
    existing = db.get_latest_keyword_spend_job(cc)
    if existing and existing.get("status") == "rate_limited":
        job_id = existing["job_id"]
        # Resume logic would go here in future; for now start fresh

    # Create job record
    campaign_id_list = [c["id"] for c in campaign_list]
    db.insert_keyword_spend_job(
        job_id=job_id,
        country_code=cc,
        campaign_ids=campaign_id_list,
        current_start=str(current_start),
        current_end=str(current_end),
        previous_start=str(previous_start),
        previous_end=str(previous_end),
    )
    db.update_keyword_spend_job(job_id, total_campaigns=len(campaign_list))

    params = {
        "min_impressions": min_impressions,
        "min_cost_micros": min_cost_micros,
        "spike_threshold_pct": spike_threshold_pct,
        "top_n": top_n,
        "include_monthly_breakdown": include_monthly_breakdown,
        "include_quality_scores": include_quality_scores,
        "include_product_mapping": include_product_mapping,
    }

    # Estimate workload
    estimated_calls = len(campaign_list) * 2 * 2  # 2 periods × ~2 pages each

    # Decide: sync if ≤3 campaigns, async otherwise
    if len(campaign_list) <= 3 and estimated_calls <= 15:
        # SYNCHRONOUS — run directly
        db.update_keyword_spend_job(job_id, status="running", phase="fetch_current")

        _batch_keyword_spend_analysis_worker(
            job_id=job_id,
            customer_id=customer_id,
            country_code=cc,
            campaign_list=campaign_list,
            current_start=str(current_start),
            current_end=str(current_end),
            previous_start=str(previous_start),
            previous_end=str(previous_end),
            params=params,
        )

        # Retrieve result
        job = db.get_keyword_spend_job(job_id)
        if job and job.get("status") == "completed" and job.get("summary_json"):
            summary = json.loads(job["summary_json"])
            summary["summary"]["job_id"] = job_id
            summary["summary"]["country_code"] = cc
            summary["summary"]["current_period"] = f"{current_start} to {current_end}"
            summary["summary"]["previous_period"] = f"{previous_start} to {previous_end}"
            summary["summary"]["campaigns_analyzed"] = len(campaign_list)
            summary["summary"]["campaign_names"] = [c["name"] for c in campaign_list]
            return {
                "status": "completed",
                "job_id": job_id,
                **summary,
                "metadata": {
                    "gaql_calls_used": job.get("total_gaql_calls", 0),
                    "total_keywords_fetched": job.get("total_keywords_fetched", 0),
                    "campaigns": len(campaign_list),
                },
            }
        elif job and job.get("status") == "failed":
            return {
                "status": "failed",
                "job_id": job_id,
                "error": job.get("error_message", "Unknown error"),
            }
        else:
            return {
                "status": job.get("status", "unknown") if job else "unknown",
                "job_id": job_id,
            }
    else:
        # ASYNCHRONOUS — spawn daemon thread
        with _ksa_jobs_lock:
            _ksa_jobs[job_id] = {
                "status": "submitted",
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_campaigns": len(campaign_list),
            }

        t = threading.Thread(
            target=_batch_keyword_spend_analysis_worker,
            args=(
                job_id, customer_id, cc, campaign_list,
                str(current_start), str(current_end),
                str(previous_start), str(previous_end),
                params,
            ),
            daemon=True,
        )
        t.start()

        return {
            "status": "submitted",
            "job_id": job_id,
            "message": (
                f"Analysis started. {len(campaign_list)} campaigns, "
                f"estimated ~{estimated_calls} GAQL calls. "
                f"Poll with batch_keyword_spend_analysis_status(job_id='{job_id}')."
            ),
            "estimated_time_seconds": len(campaign_list) * 12,
            "total_campaigns": len(campaign_list),
        }


def get_keyword_spend_analysis_status(job_id: str, include_preview: bool = False) -> Dict:
    """Get status of a keyword spend analysis job."""
    db = _get_db()

    # Check in-memory first (faster)
    with _ksa_jobs_lock:
        mem = _ksa_jobs.get(job_id, {}).copy()

    progress = mem.get("progress", {})
    status = mem.get("status", "unknown")
    started_at = mem.get("started_at", "")

    # Fallback to DB
    if status == "unknown":
        job = db.get_keyword_spend_job(job_id)
        if not job:
            return {"status": "not_found", "job_id": job_id, "error": f"Job {job_id} not found."}
        status = job.get("status", "unknown")
        started_at = job.get("started_at", "")

        if status == "completed" and job.get("summary_json"):
            summary = json.loads(job["summary_json"])
            return {
                "status": "completed",
                "job_id": job_id,
                "completed_at": job.get("completed_at", ""),
                **summary,
                "metadata": {
                    "gaql_calls_used": job.get("total_gaql_calls", 0),
                    "total_keywords_fetched": job.get("total_keywords_fetched", 0),
                },
            }
        elif status == "failed":
            return {
                "status": "failed",
                "job_id": job_id,
                "error": job.get("error_message", "Unknown error"),
            }
        elif status == "rate_limited":
            return {
                "status": "rate_limited",
                "job_id": job_id,
                "message": job.get("error_message", "Rate limited"),
                "resume_checkpoint": job.get("resume_checkpoint"),
            }

    # Completed in memory
    if status == "completed" and "summary" in mem:
        return {
            "status": "completed",
            "job_id": job_id,
            **mem["summary"],
        }

    # Running — return progress
    elapsed = 0
    if started_at:
        try:
            st = datetime.strptime(started_at, "%Y-%m-%d %H:%M:%S")
            elapsed = int((datetime.now() - st).total_seconds())
        except Exception:
            pass

    pct = progress.get("percent", 0)
    est_remaining = int(elapsed / pct * (100 - pct)) if pct > 0 else 0

    result = {
        "status": status if status != "unknown" else "running",
        "job_id": job_id,
        "phase": progress.get("phase", mem.get("phase", "")),
        "progress": {
            "campaigns_processed": progress.get("campaigns_processed", 0),
            "campaigns_total": progress.get("campaigns_total", 0),
            "keywords_fetched": progress.get("keywords_fetched", 0),
            "gaql_calls": progress.get("gaql_calls", 0),
            "current_campaign": progress.get("current_campaign", ""),
            "percent": pct,
        },
        "timing": {
            "started_at": started_at,
            "elapsed_seconds": elapsed,
            "estimated_remaining_seconds": est_remaining,
        },
    }

    return result


# ---------------------------------------------------------------------------
# Location Performance Analysis
# ---------------------------------------------------------------------------

def _resolve_geo_names(geo_ids: List[int]) -> Dict[int, Dict[str, str]]:
    """Resolve geo_target_constant IDs to names. Uses DB cache (30-day TTL)."""
    from google_ads_mcp import _execute_gaql
    if not geo_ids:
        return {}

    db = _get_db()
    result = {}
    uncached = []

    conn = db._get_conn()
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    for gid in geo_ids:
        row = conn.execute(
            "SELECT id, name, canonical_name, country_code, target_type, parent_id "
            "FROM geo_target_names WHERE id = ? AND cached_at > ?",
            (gid, cutoff)
        ).fetchone()
        if row:
            result[gid] = {
                "name": row[1], "canonical_name": row[2],
                "country_code": row[3], "target_type": row[4],
                "parent_id": row[5],
            }
        else:
            uncached.append(gid)

    for i in range(0, len(uncached), 200):
        batch_ids = uncached[i:i+200]
        ids_str = ", ".join(str(x) for x in batch_ids)
        query = f"""
            SELECT geo_target_constant.id, geo_target_constant.name,
                   geo_target_constant.canonical_name,
                   geo_target_constant.country_code,
                   geo_target_constant.target_type
            FROM geo_target_constant
            WHERE geo_target_constant.id IN ({ids_str})
        """
        try:
            rows = _execute_gaql(_get_customer_id(), query, page_size=500)
            for r in rows:
                gid = _safe_val(r, "geo_target_constant.id")
                name = _safe_val(r, "geo_target_constant.name") or ""
                canonical = _safe_val(r, "geo_target_constant.canonical_name") or ""
                cc = _safe_val(r, "geo_target_constant.country_code") or ""
                ttype = _safe_val(r, "geo_target_constant.target_type") or ""
                if gid:
                    gid = int(gid)
                    result[gid] = {"name": name, "canonical_name": canonical,
                                   "country_code": cc, "target_type": ttype, "parent_id": None}
                    conn.execute(
                        "INSERT OR REPLACE INTO geo_target_names "
                        "(id, name, canonical_name, country_code, target_type, cached_at) "
                        "VALUES (?, ?, ?, ?, ?, datetime('now'))",
                        (gid, name, canonical, cc, ttype)
                    )
            conn.commit()
        except Exception:
            pass
    return result


def _safe_val(row, field):
    """Safely extract a field from GAQL result row."""
    try:
        parts = field.split(".")
        obj = row
        for p in parts:
            obj = getattr(obj, p)
        v = obj
        if hasattr(v, 'value'):
            return v.value
        return v
    except Exception:
        return None


def _classify_location(roas, avg_roas, conversions, cost):
    """Classify location performance."""
    if cost > 0 and conversions == 0:
        return "WASTE"
    if avg_roas > 0 and roas >= avg_roas * 1.3:
        return "TOP_PERFORMER"
    if avg_roas > 0 and roas <= avg_roas * 0.7:
        return "UNDERPERFORMER"
    return "AVERAGE"


# Protobuf enum mappings — Google Ads API returns integer enums, not strings
_CHANNEL_TYPE_MAP = {
    "0": "UNSPECIFIED", "1": "UNKNOWN", "2": "SEARCH", "3": "DISPLAY",
    "4": "SHOPPING", "5": "HOTEL", "6": "VIDEO", "7": "MULTI_CHANNEL",
    "8": "LOCAL", "9": "SMART", "10": "PERFORMANCE_MAX",
    "11": "LOCAL_SERVICES", "12": "DISCOVERY", "13": "TRAVEL", "14": "DEMAND_GEN",
}

_DEVICE_TYPE_MAP = {
    "0": "OTHER", "1": "OTHER", "2": "MOBILE", "3": "TABLET",
    "4": "DESKTOP", "5": "CONNECTED_TV", "6": "OTHER",
}


def _normalize_channel_type(raw: str) -> str:
    """Normalize campaign advertising_channel_type from any format to canonical string."""
    s = str(raw).strip()
    # Already a known string?
    for canonical in ("SEARCH", "DISPLAY", "SHOPPING", "PERFORMANCE_MAX",
                      "HOTEL", "VIDEO", "MULTI_CHANNEL", "LOCAL", "SMART",
                      "LOCAL_SERVICES", "DISCOVERY", "TRAVEL", "DEMAND_GEN"):
        if canonical in s.upper():
            return canonical
    # Protobuf integer enum?
    if s in _CHANNEL_TYPE_MAP:
        return _CHANNEL_TYPE_MAP[s]
    return s.upper() if s else "UNKNOWN"


def _normalize_device_type(raw: str) -> str:
    """Normalize segments.device from any format to canonical string."""
    s = str(raw).strip()
    # Strip common protobuf prefixes
    upper = s.upper()
    for prefix in ("DEVICEENUM.DEVICETYPE.", "DEVICEENUM.", "DEVICETYPE.",
                   "DEVICE_ENUM__DEVICE_TYPE__"):
        upper = upper.replace(prefix, "")
    # Already a known device string?
    if upper in ("MOBILE", "DESKTOP", "TABLET", "CONNECTED_TV"):
        return upper
    # Protobuf integer enum?
    if s in _DEVICE_TYPE_MAP:
        return _DEVICE_TYPE_MAP[s]
    return "OTHER"


def _discover_campaigns(country_code: str, campaign_types: List[str]) -> Tuple[List[str], Dict[str, Dict]]:
    """Discover ENABLED campaigns for a country filtered by type."""
    from google_ads_mcp import _execute_gaql
    type_filter = ", ".join(f"'{t}'" for t in campaign_types)
    disc_query = f"""
        SELECT campaign.id, campaign.name, campaign.status,
               campaign.advertising_channel_type
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND campaign.advertising_channel_type IN ({type_filter})
          AND campaign.name LIKE '{country_code} %'
    """
    disc_results = _execute_gaql(_get_customer_id(), disc_query, page_size=100)
    ids = []
    meta = {}
    for row in disc_results:
        cid = str(_safe_val(row, "campaign.id"))
        cname = _safe_val(row, "campaign.name") or cid
        ctype_raw = str(_safe_val(row, "campaign.advertising_channel_type") or "UNKNOWN")
        ctype = _normalize_channel_type(ctype_raw)
        ids.append(cid)
        meta[cid] = {"name": cname, "type": ctype}
    return ids, meta


def analyze_location_performance(
    country_code: str,
    campaign_ids: Optional[List[str]] = None,
    campaign_types: Optional[List[str]] = None,
    start_date: str = "",
    end_date: str = "",
    min_impressions: int = 50,
    top_n: int = 20,
    include_regions: bool = True,
    location_type: str = "both",
) -> Dict[str, Any]:
    """Analyze campaign performance by geographic location."""
    from google_ads_mcp import _execute_gaql
    cc = country_code.upper()
    if not campaign_types:
        campaign_types = ["SEARCH", "PERFORMANCE_MAX", "SHOPPING"]

    if not campaign_ids:
        campaign_ids, campaign_meta = _discover_campaigns(cc, campaign_types)
    else:
        campaign_meta = {cid: {"name": cid, "type": "UNKNOWN"} for cid in campaign_ids}
    if not campaign_ids:
        return {"status": "error", "message": f"No ENABLED campaigns found for {cc}"}

    all_locations = {}
    per_campaign = {}
    geo_ids_seen = set()
    queries_used = 0

    for cid in campaign_ids:
        camp_locations = {}

        if location_type in ("user_location", "both"):
            ulv_query = f"""
                SELECT campaign.id, campaign.name,
                       user_location_view.country_criterion_id,
                       user_location_view.targeting_location,
                       metrics.impressions, metrics.clicks,
                       metrics.conversions, metrics.conversions_value,
                       metrics.cost_micros
                FROM user_location_view
                WHERE campaign.id = {cid}
                  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
                  AND metrics.impressions > 0
                ORDER BY metrics.cost_micros DESC
            """
            try:
                ulv_rows = _execute_gaql(_get_customer_id(), ulv_query, page_size=1000)
                queries_used += 1
                for row in ulv_rows:
                    tl = str(_safe_val(row, "user_location_view.targeting_location") or "")
                    geo_id = None
                    if "geoTargetConstants/" in tl:
                        try:
                            geo_id = int(tl.split("/")[-1])
                        except ValueError:
                            pass
                    if not geo_id:
                        geo_id = _safe_val(row, "user_location_view.country_criterion_id")
                        if geo_id:
                            geo_id = int(geo_id)
                    if not geo_id:
                        continue
                    geo_ids_seen.add(geo_id)
                    impr = int(_safe_val(row, "metrics.impressions") or 0)
                    if impr < min_impressions:
                        continue
                    clicks = int(_safe_val(row, "metrics.clicks") or 0)
                    conv = float(_safe_val(row, "metrics.conversions") or 0)
                    conv_val = float(_safe_val(row, "metrics.conversions_value") or 0)
                    cost_m = int(_safe_val(row, "metrics.cost_micros") or 0)
                    if geo_id not in camp_locations:
                        camp_locations[geo_id] = {"impressions": 0, "clicks": 0, "conversions": 0.0,
                                                   "conversions_value": 0.0, "cost_micros": 0}
                    camp_locations[geo_id]["impressions"] += impr
                    camp_locations[geo_id]["clicks"] += clicks
                    camp_locations[geo_id]["conversions"] += conv
                    camp_locations[geo_id]["conversions_value"] += conv_val
                    camp_locations[geo_id]["cost_micros"] += cost_m
            except Exception:
                pass

        if location_type in ("targeted", "both"):
            gv_query = f"""
                SELECT campaign.id, campaign.name,
                       geographic_view.country_criterion_id,
                       geographic_view.location_type,
                       metrics.impressions, metrics.clicks,
                       metrics.conversions, metrics.conversions_value,
                       metrics.cost_micros
                FROM geographic_view
                WHERE campaign.id = {cid}
                  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
                  AND metrics.impressions > 0
                ORDER BY metrics.cost_micros DESC
            """
            try:
                gv_rows = _execute_gaql(_get_customer_id(), gv_query, page_size=1000)
                queries_used += 1
                for row in gv_rows:
                    geo_id = _safe_val(row, "geographic_view.country_criterion_id")
                    if geo_id:
                        geo_id = int(geo_id)
                    if not geo_id:
                        continue
                    geo_ids_seen.add(geo_id)
                    impr = int(_safe_val(row, "metrics.impressions") or 0)
                    if impr < min_impressions:
                        continue
                    clicks = int(_safe_val(row, "metrics.clicks") or 0)
                    conv = float(_safe_val(row, "metrics.conversions") or 0)
                    conv_val = float(_safe_val(row, "metrics.conversions_value") or 0)
                    cost_m = int(_safe_val(row, "metrics.cost_micros") or 0)
                    if geo_id not in camp_locations:
                        camp_locations[geo_id] = {"impressions": 0, "clicks": 0, "conversions": 0.0,
                                                   "conversions_value": 0.0, "cost_micros": 0}
                    camp_locations[geo_id]["impressions"] += impr
                    camp_locations[geo_id]["clicks"] += clicks
                    camp_locations[geo_id]["conversions"] += conv
                    camp_locations[geo_id]["conversions_value"] += conv_val
                    camp_locations[geo_id]["cost_micros"] += cost_m
            except Exception:
                pass

        for geo_id, metrics in camp_locations.items():
            if geo_id not in all_locations:
                all_locations[geo_id] = {"impressions": 0, "clicks": 0, "conversions": 0.0,
                                          "conversions_value": 0.0, "cost_micros": 0}
            for k in ("impressions", "clicks", "conversions", "conversions_value", "cost_micros"):
                all_locations[geo_id][k] += metrics[k]
        per_campaign[cid] = {"meta": campaign_meta.get(cid, {}), "locations": camp_locations}

    geo_names = _resolve_geo_names(list(geo_ids_seen))

    total_cost = sum(m["cost_micros"] for m in all_locations.values())
    total_conv = sum(m["conversions"] for m in all_locations.values())
    total_clicks = sum(m["clicks"] for m in all_locations.values())
    total_impr = sum(m["impressions"] for m in all_locations.values())
    total_conv_val = sum(m["conversions_value"] for m in all_locations.values())

    avg_ctr = (total_clicks / total_impr * 100) if total_impr > 0 else 0
    avg_cpc = (total_cost / total_clicks / 1_000_000) if total_clicks > 0 else 0
    avg_cpa = (total_cost / total_conv / 1_000_000) if total_conv > 0 else 0
    avg_roas = (total_conv_val / (total_cost / 1_000_000)) if total_cost > 0 else 0

    locations_list = []
    for geo_id, m in all_locations.items():
        cost = m["cost_micros"] / 1_000_000
        ctr = (m["clicks"] / m["impressions"] * 100) if m["impressions"] > 0 else 0
        cpc = (cost / m["clicks"]) if m["clicks"] > 0 else 0
        cpa = (cost / m["conversions"]) if m["conversions"] > 0 else 0
        roas = (m["conversions_value"] / cost) if cost > 0 else 0
        conv_rate = (m["conversions"] / m["clicks"] * 100) if m["clicks"] > 0 else 0
        spend_share = (m["cost_micros"] / total_cost * 100) if total_cost > 0 else 0
        geo_info = geo_names.get(geo_id, {})
        classification = _classify_location(roas, avg_roas, m["conversions"], cost)
        locations_list.append({
            "location_id": geo_id,
            "name": geo_info.get("name", f"ID:{geo_id}"),
            "canonical_name": geo_info.get("canonical_name", ""),
            "type": geo_info.get("target_type", "Unknown"),
            "impressions": m["impressions"], "clicks": m["clicks"],
            "conversions": round(m["conversions"], 2),
            "conversions_value": round(m["conversions_value"], 2),
            "cost": round(cost, 2),
            "ctr": round(ctr, 2), "cpc": round(cpc, 2),
            "cpa": round(cpa, 2), "roas": round(roas, 2),
            "conv_rate": round(conv_rate, 2),
            "spend_share_pct": round(spend_share, 1),
            "vs_account_avg": {
                "ctr_delta_pct": round(((ctr - avg_ctr) / avg_ctr * 100) if avg_ctr > 0 else 0, 1),
                "cpa_delta_pct": round(((cpa - avg_cpa) / avg_cpa * 100) if avg_cpa > 0 else 0, 1),
                "roas_delta_pct": round(((roas - avg_roas) / avg_roas * 100) if avg_roas > 0 else 0, 1),
            },
            "classification": classification,
        })
    locations_list.sort(key=lambda x: x["cost"], reverse=True)

    per_campaign_summary = {}
    for cid, data in per_campaign.items():
        camp_locs = []
        for geo_id, m in data["locations"].items():
            cost = m["cost_micros"] / 1_000_000
            roas = (m["conversions_value"] / cost) if cost > 0 else 0
            geo_info = geo_names.get(geo_id, {})
            camp_locs.append({"name": geo_info.get("name", f"ID:{geo_id}"),
                              "cost": round(cost, 2), "conversions": round(m["conversions"], 2),
                              "roas": round(roas, 2)})
        camp_locs.sort(key=lambda x: x["cost"], reverse=True)
        per_campaign_summary[cid] = {
            "name": data["meta"].get("name", cid),
            "type": data["meta"].get("type", "UNKNOWN"),
            "top_locations": camp_locs[:5],
            "worst_locations": sorted(camp_locs, key=lambda x: x["roas"])[:5] if camp_locs else [],
        }

    recommendations = []
    for loc in locations_list[:top_n]:
        if loc["classification"] == "TOP_PERFORMER":
            recommendations.append({"priority": "HIGH", "action": "INCREASE_BID",
                "location": loc["name"],
                "reason": f"ROAS {loc['roas']} is {loc['vs_account_avg']['roas_delta_pct']}% above avg",
                "expected_impact": "+15% conversions from +20% bid increase"})
        elif loc["classification"] == "WASTE":
            recommendations.append({"priority": "HIGH", "action": "EXCLUDE",
                "location": loc["name"],
                "reason": f"{loc['cost']} spent, 0 conversions",
                "expected_impact": f"Save {loc['cost']}/period"})
        elif loc["classification"] == "UNDERPERFORMER" and loc["cost"] > 10:
            recommendations.append({"priority": "MEDIUM", "action": "REDUCE_BID",
                "location": loc["name"],
                "reason": f"ROAS {loc['roas']} is {abs(loc['vs_account_avg']['roas_delta_pct'])}% below avg",
                "expected_impact": f"-30% bid saves ~{round(loc['cost'] * 0.3, 2)}/period"})

    top3_cost = sum(l["cost"] for l in locations_list[:3])
    top3_conv = sum(l["conversions"] for l in locations_list[:3])
    total_cost_amt = total_cost / 1_000_000 if total_cost > 0 else 1

    return {
        "status": "success", "country_code": cc,
        "period": {"start": start_date, "end": end_date},
        "campaigns_analyzed": len(campaign_ids), "campaign_types": campaign_types,
        "total_locations": len(locations_list), "queries_used": queries_used,
        "locations": locations_list[:top_n],
        "per_campaign": per_campaign_summary,
        "summary": {
            "total_locations": len(locations_list),
            "top_performers": sum(1 for l in locations_list if l["classification"] == "TOP_PERFORMER"),
            "underperformers": sum(1 for l in locations_list if l["classification"] == "UNDERPERFORMER"),
            "waste_locations": sum(1 for l in locations_list if l["classification"] == "WASTE"),
            "concentration": {
                "top_3_spend_pct": round(top3_cost / total_cost_amt * 100, 1) if total_cost_amt > 0 else 0,
                "top_3_conv_pct": round(top3_conv / total_conv * 100, 1) if total_conv > 0 else 0,
            },
            "avg_metrics": {"ctr": round(avg_ctr, 2), "cpc": round(avg_cpc, 2),
                            "cpa": round(avg_cpa, 2), "roas": round(avg_roas, 2)},
        },
        "recommendations": recommendations,
    }


# ---------------------------------------------------------------------------
# Device Performance Analysis
# ---------------------------------------------------------------------------

def _recommend_device_modifier(device_roas, avg_roas, efficiency_index):
    """Calculate recommended bid modifier for a device."""
    if avg_roas <= 0:
        return 1.0
    if device_roas >= avg_roas * 1.25 and efficiency_index > 1.0:
        return 1.15
    elif device_roas >= avg_roas * 1.10:
        return 1.10
    elif device_roas < avg_roas * 0.5:
        return 0.70
    elif device_roas < avg_roas * 0.75:
        return 0.85
    return 1.00


def analyze_device_performance(
    country_code: str,
    campaign_ids: Optional[List[str]] = None,
    campaign_types: Optional[List[str]] = None,
    start_date: str = "",
    end_date: str = "",
    include_ad_group_breakdown: bool = False,
    include_device_hour_cross: bool = False,
) -> Dict[str, Any]:
    """Analyze campaign performance by device type (MOBILE, DESKTOP, TABLET)."""
    from google_ads_mcp import _execute_gaql
    cc = country_code.upper()
    if not campaign_types:
        campaign_types = ["SEARCH", "PERFORMANCE_MAX", "SHOPPING"]

    if not campaign_ids:
        campaign_ids, campaign_meta = _discover_campaigns(cc, campaign_types)
    else:
        campaign_meta = {cid: {"name": cid, "type": "UNKNOWN"} for cid in campaign_ids}
    if not campaign_ids:
        return {"status": "error", "message": f"No ENABLED campaigns found for {cc}"}

    all_devices = {}
    per_campaign = {}
    device_hour_data = {}
    queries_used = 0

    for cid in campaign_ids:
        camp_devices = {}
        dev_query = f"""
            SELECT campaign.id, campaign.name,
                   campaign.advertising_channel_type,
                   segments.device,
                   metrics.impressions, metrics.clicks,
                   metrics.conversions, metrics.conversions_value,
                   metrics.cost_micros
            FROM campaign
            WHERE campaign.id = {cid}
              AND segments.date BETWEEN '{start_date}' AND '{end_date}'
              AND metrics.impressions > 0
        """
        try:
            dev_rows = _execute_gaql(_get_customer_id(), dev_query, page_size=100)
            queries_used += 1
            for row in dev_rows:
                device = _normalize_device_type(
                    str(_safe_val(row, "segments.device") or "OTHER"))
                impr = int(_safe_val(row, "metrics.impressions") or 0)
                clicks = int(_safe_val(row, "metrics.clicks") or 0)
                conv = float(_safe_val(row, "metrics.conversions") or 0)
                conv_val = float(_safe_val(row, "metrics.conversions_value") or 0)
                cost_m = int(_safe_val(row, "metrics.cost_micros") or 0)
                if device not in camp_devices:
                    camp_devices[device] = {"impressions": 0, "clicks": 0, "conversions": 0.0,
                                             "conversions_value": 0.0, "cost_micros": 0}
                camp_devices[device]["impressions"] += impr
                camp_devices[device]["clicks"] += clicks
                camp_devices[device]["conversions"] += conv
                camp_devices[device]["conversions_value"] += conv_val
                camp_devices[device]["cost_micros"] += cost_m
        except Exception:
            pass

        for device, metrics in camp_devices.items():
            if device not in all_devices:
                all_devices[device] = {"impressions": 0, "clicks": 0, "conversions": 0.0,
                                        "conversions_value": 0.0, "cost_micros": 0}
            for k in ("impressions", "clicks", "conversions", "conversions_value", "cost_micros"):
                all_devices[device][k] += metrics[k]
        per_campaign[cid] = {"meta": campaign_meta.get(cid, {}), "devices": camp_devices}

        if include_device_hour_cross:
            dh_query = f"""
                SELECT campaign.id, segments.device, segments.hour,
                       metrics.impressions, metrics.clicks,
                       metrics.conversions, metrics.conversions_value,
                       metrics.cost_micros
                FROM campaign
                WHERE campaign.id = {cid}
                  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
                  AND metrics.impressions > 0
            """
            try:
                dh_rows = _execute_gaql(_get_customer_id(), dh_query, page_size=1000)
                queries_used += 1
                for row in dh_rows:
                    device = _normalize_device_type(
                        str(_safe_val(row, "segments.device") or "OTHER"))
                    hour = int(_safe_val(row, "segments.hour") or 0)
                    clicks = int(_safe_val(row, "metrics.clicks") or 0)
                    conv = float(_safe_val(row, "metrics.conversions") or 0)
                    conv_val = float(_safe_val(row, "metrics.conversions_value") or 0)
                    cost_m = int(_safe_val(row, "metrics.cost_micros") or 0)
                    if device not in device_hour_data:
                        device_hour_data[device] = {}
                    if hour not in device_hour_data[device]:
                        device_hour_data[device][hour] = {"clicks": 0, "conversions": 0.0,
                                                           "conversions_value": 0.0, "cost_micros": 0}
                    device_hour_data[device][hour]["clicks"] += clicks
                    device_hour_data[device][hour]["conversions"] += conv
                    device_hour_data[device][hour]["conversions_value"] += conv_val
                    device_hour_data[device][hour]["cost_micros"] += cost_m
            except Exception:
                pass

    total_cost = sum(m["cost_micros"] for m in all_devices.values())
    total_conv = sum(m["conversions"] for m in all_devices.values())
    total_conv_val = sum(m["conversions_value"] for m in all_devices.values())
    total_cost_amt = total_cost / 1_000_000 if total_cost > 0 else 0
    avg_roas = (total_conv_val / total_cost_amt) if total_cost_amt > 0 else 0

    devices_result = {}
    for device, m in all_devices.items():
        cost = m["cost_micros"] / 1_000_000
        ctr = (m["clicks"] / m["impressions"] * 100) if m["impressions"] > 0 else 0
        cpc = (cost / m["clicks"]) if m["clicks"] > 0 else 0
        cpa = (cost / m["conversions"]) if m["conversions"] > 0 else 0
        roas = (m["conversions_value"] / cost) if cost > 0 else 0
        conv_rate = (m["conversions"] / m["clicks"] * 100) if m["clicks"] > 0 else 0
        spend_share = (m["cost_micros"] / total_cost * 100) if total_cost > 0 else 0
        conv_share = (m["conversions"] / total_conv * 100) if total_conv > 0 else 0
        efficiency_index = (conv_share / spend_share) if spend_share > 0 else 0
        devices_result[device] = {
            "impressions": m["impressions"], "clicks": m["clicks"],
            "conversions": round(m["conversions"], 2),
            "conversions_value": round(m["conversions_value"], 2),
            "cost": round(cost, 2),
            "ctr": round(ctr, 2), "cpc": round(cpc, 2),
            "cpa": round(cpa, 2), "roas": round(roas, 2),
            "conv_rate": round(conv_rate, 2),
            "spend_share_pct": round(spend_share, 1),
            "conv_share_pct": round(conv_share, 1),
            "efficiency_index": round(efficiency_index, 2),
        }

    per_campaign_summary = {}
    for cid, data in per_campaign.items():
        camp_dev = {}
        camp_total_cost = sum(dm["cost_micros"] for dm in data["devices"].values())
        for device, m in data["devices"].items():
            cost = m["cost_micros"] / 1_000_000
            roas = (m["conversions_value"] / cost) if cost > 0 else 0
            spend_share = (m["cost_micros"] / camp_total_cost * 100) if camp_total_cost > 0 else 0
            camp_dev[device] = {"spend_share_pct": round(spend_share, 1), "roas": round(roas, 2),
                                "conversions": round(m["conversions"], 2), "cost": round(cost, 2)}
        per_campaign_summary[cid] = {
            "name": data["meta"].get("name", cid),
            "type": data["meta"].get("type", "UNKNOWN"),
            "devices": camp_dev,
        }

    heatmap = {}
    if include_device_hour_cross and device_hour_data:
        for device, hours in device_hour_data.items():
            heatmap[device] = {}
            for h in range(24):
                hd = hours.get(h, {"clicks": 0, "conversions": 0.0, "conversions_value": 0.0, "cost_micros": 0})
                cost = hd["cost_micros"] / 1_000_000
                roas = (hd["conversions_value"] / cost) if cost > 0 else 0
                heatmap[device][str(h)] = {"clicks": hd["clicks"], "conversions": round(hd["conversions"], 2),
                                            "roas": round(roas, 2), "cost": round(cost, 2)}

    device_bid_modifiers = {}
    recommendations = []
    for device, dm in devices_result.items():
        modifier = _recommend_device_modifier(dm["roas"], avg_roas, dm["efficiency_index"])
        modifier_pct = f"{'+' if modifier > 1 else ''}{round((modifier - 1) * 100)}%"
        if modifier > 1:
            reason = f"ROAS {dm['roas']} above avg ({round(avg_roas, 2)}), efficiency {dm['efficiency_index']}"
        elif modifier < 1:
            reason = f"ROAS {dm['roas']} below avg ({round(avg_roas, 2)}), efficiency {dm['efficiency_index']}"
        else:
            reason = f"Inline with average ROAS ({round(avg_roas, 2)})"
        device_bid_modifiers[device] = {"modifier": modifier, "modifier_pct": modifier_pct, "reason": reason}
        if modifier != 1.0:
            recommendations.append({
                "priority": "HIGH" if abs(modifier - 1) >= 0.15 else "MEDIUM",
                "action": f"{'INCREASE' if modifier > 1 else 'REDUCE'}_{device}_BID",
                "reason": reason, "modifier": modifier_pct,
                "expected_impact": f"{'More' if modifier > 1 else 'Less'} budget for {device}: "
                                   f"{dm['spend_share_pct']}% spend, {dm['conv_share_pct']}% conv",
            })

    return {
        "status": "success", "country_code": cc,
        "period": {"start": start_date, "end": end_date},
        "campaigns_analyzed": len(campaign_ids), "campaign_types": campaign_types,
        "queries_used": queries_used,
        "devices": devices_result,
        "per_campaign": per_campaign_summary,
        "device_hour_heatmap": heatmap if heatmap else None,
        "device_bid_modifiers": device_bid_modifiers,
        "recommendations": recommendations,
        "avg_roas": round(avg_roas, 2),
    }


# ---------------------------------------------------------------------------
# Combined Location + Device + Time Orchestrator
# ---------------------------------------------------------------------------

def analyze_loc_device_time(
    country_code: str,
    campaign_ids: Optional[List[str]] = None,
    campaign_types: Optional[List[str]] = None,
    days: int = 90,
    min_impressions: int = 50,
    include_device_hour_cross: bool = True,
    include_regions: bool = True,
    top_n_locations: int = 20,
) -> Dict[str, Any]:
    """Orchestrate location + device + time analysis into one combined report."""
    from google_ads_mcp import _execute_gaql
    cc = country_code.upper()
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    if not campaign_types:
        campaign_types = ["SEARCH", "PERFORMANCE_MAX", "SHOPPING"]

    if not campaign_ids:
        campaign_ids, campaign_meta = _discover_campaigns(cc, campaign_types)
    else:
        campaign_meta = {cid: {"name": cid, "type": "UNKNOWN"} for cid in campaign_ids}
    if not campaign_ids:
        return {"status": "error", "message": f"No ENABLED campaigns found for {cc}"}

    search_ids = [c for c in campaign_ids if "SEARCH" in str(campaign_meta.get(c, {}).get("type", ""))]
    pmax_ids = [c for c in campaign_ids if "PERFORMANCE_MAX" in str(campaign_meta.get(c, {}).get("type", ""))]
    shopping_ids = [c for c in campaign_ids if "SHOPPING" in str(campaign_meta.get(c, {}).get("type", ""))]

    errors = []
    limitations = [
        "Postcode-level data is NOT available via Google Ads API",
        "Cross-device attribution changes are not exposed via API",
        "PMax campaigns have limited location granularity",
        "GAQL queries limited to 1000 rows per query — pagination applied",
        "user_location_view may not return data for all campaign types",
    ]

    # 1. Location Analysis
    t0 = time.time()
    try:
        location_result = analyze_location_performance(
            country_code=cc, campaign_ids=campaign_ids, campaign_types=campaign_types,
            start_date=start_date, end_date=end_date, min_impressions=min_impressions,
            top_n=top_n_locations, include_regions=include_regions, location_type="both",
        )
    except Exception as e:
        location_result = {"status": "error", "message": str(e)}
        errors.append(f"Location analysis failed: {e}")
    loc_time = round(time.time() - t0, 1)

    # 2. Device Analysis
    t0 = time.time()
    try:
        device_result = analyze_device_performance(
            country_code=cc, campaign_ids=campaign_ids, campaign_types=campaign_types,
            start_date=start_date, end_date=end_date,
            include_device_hour_cross=include_device_hour_cross,
        )
    except Exception as e:
        device_result = {"status": "error", "message": str(e)}
        errors.append(f"Device analysis failed: {e}")
    dev_time = round(time.time() - t0, 1)

    # 3. Time Analysis (Search only — PMax/Shopping don't support ad scheduling)
    time_result = None
    t0 = time.time()
    if search_ids:
        try:
            from batch_optimizer import _batch_time_analysis_core
            time_result = _batch_time_analysis_core(
                country_code=cc, campaign_ids=search_ids,
                start_date=start_date, end_date=end_date,
                min_impressions_hour=min_impressions,
                roas_threshold_low=1.0, roas_threshold_high=1.5,
                generate_schedules=True,
            )
        except ImportError:
            time_result = {"status": "info",
                           "message": "Time analysis available via separate /ads-time-optimize command"}
            limitations.append("Time analysis core not yet extracted — use /ads-time-optimize separately")
        except Exception as e:
            time_result = {"status": "error", "message": str(e)}
            errors.append(f"Time analysis failed: {e}")
    else:
        time_result = {"status": "skipped", "message": "No Search campaigns — time analysis skipped"}
    time_time = round(time.time() - t0, 1)

    # 4. Cross-dimensional insights
    cross_insights = []
    if device_result.get("status") == "success" and device_result.get("device_hour_heatmap"):
        hm = device_result["device_hour_heatmap"]
        combos = []
        for dev, hours in hm.items():
            for h, hd in hours.items():
                if hd.get("cost", 0) > 0:
                    combos.append({"device": dev, "hour": int(h), "roas": hd["roas"],
                                   "cost": hd["cost"], "conversions": hd["conversions"]})
        combos.sort(key=lambda x: x["roas"], reverse=True)
        if combos:
            best = combos[0]
            cross_insights.append({"type": "BEST_COMBO",
                "description": f"{best['device']} + {best['hour']}:00 = ROAS {best['roas']}", "data": best})
            worst = [c for c in combos if c["cost"] > 0]
            if worst:
                w = worst[-1]
                cross_insights.append({"type": "WORST_COMBO",
                    "description": f"{w['device']} + {w['hour']}:00 = ROAS {w['roas']}", "data": w})

    # 5. Merge recommendations
    all_recs = []
    if location_result.get("recommendations"):
        for r in location_result["recommendations"]:
            r["dimension"] = "LOCATION"
            all_recs.append(r)
    if device_result.get("recommendations"):
        for r in device_result["recommendations"]:
            r["dimension"] = "DEVICE"
            all_recs.append(r)
    if time_result and time_result.get("status") == "success":
        waste_h = [s for s in time_result.get("schedule_recommendations", [])
                   if s.get("category") in ("waste", "waste_severe", "dead")]
        peak_h = [s for s in time_result.get("schedule_recommendations", [])
                  if s.get("category") in ("peak", "peak_strong")]
        if waste_h:
            all_recs.append({"priority": "HIGH", "action": "AD_SCHEDULE_WASTE", "dimension": "TIME",
                "reason": f"{len(waste_h)} waste hours identified", "expected_impact": "Reduce low-ROAS spend"})
        if peak_h:
            all_recs.append({"priority": "MEDIUM", "action": "AD_SCHEDULE_PEAK", "dimension": "TIME",
                "reason": f"{len(peak_h)} peak hours identified", "expected_impact": "Increase high-ROAS spend"})
    all_recs.sort(key=lambda x: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(x.get("priority", "LOW"), 2))

    # 6. Impact projection
    impact = {}
    if location_result.get("status") == "success":
        s = location_result.get("summary", {})
        avg = s.get("avg_metrics", {})
        waste_locs = [l for l in location_result.get("locations", []) if l["classification"] == "WASTE"]
        waste_cost = sum(l["cost"] for l in waste_locs)
        total_c = sum(l["conversions"] for l in location_result.get("locations", []))
        impact = {
            "current_cpa": avg.get("cpa", 0), "projected_cpa": round(avg.get("cpa", 0) * 0.85, 2),
            "current_roas": avg.get("roas", 0), "projected_roas": round(avg.get("roas", 0) * 1.20, 2),
            "current_conversions": round(total_c, 1),
            "projected_conversions": round(total_c * 1.10, 1),
            "estimated_savings": round(waste_cost * 0.3, 2),
            "note": "Conservative: 30% waste reduction + 10% optimization lift",
        }

    return {
        "status": "success", "country_code": cc,
        "period": {"start": start_date, "end": end_date, "days": days},
        "campaigns": {"total": len(campaign_ids), "search": len(search_ids),
                       "pmax": len(pmax_ids), "shopping": len(shopping_ids),
                       "details": campaign_meta},
        "location": location_result, "device": device_result, "time": time_result,
        "cross_insights": cross_insights,
        "recommendations": all_recs,
        "impact_projection": impact,
        "limitations": limitations, "errors": errors,
        "timing": {"location_seconds": loc_time, "device_seconds": dev_time, "time_seconds": time_time},
    }


# ---------------------------------------------------------------------------
# MCP Tool Registration
# ---------------------------------------------------------------------------

class AnalyzeAssetsRequest(BaseModel):
    """Input for batch_analyze_assets tool."""
    country_code: str = Field(..., description="Country code (e.g., RO, TR)")
    campaign_id: str = Field(..., description="Google Ads campaign ID")

class WinningPatternsRequest(BaseModel):
    """Input for batch_winning_patterns tool."""
    country_code: str = Field(..., description="Country code")
    asset_type: str = Field("HEADLINE", description="HEADLINE or DESCRIPTION")
    min_occurrences: int = Field(2, description="Min occurrences to include")


def register_analytics_tools(mcp_app):
    """Register analytics MCP tools."""

    @mcp_app.tool()
    async def batch_analyze_assets(request: AnalyzeAssetsRequest) -> dict:
        """
        Analyze RSA headline and description performance in a campaign.

        Queries Google Ads for asset_performance_label (BEST, GOOD, LEARNING, LOW)
        for each headline and description. Generalizes patterns (replaces product names
        and prices with placeholders) and ranks them by performance score.

        Results are stored in DB for use by ad copy generation templates.
        """
        t0 = time.time()
        result = analyze_campaign_assets(
            request.country_code.upper(), request.campaign_id
        )
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return {"status": "success", **result}

    @mcp_app.tool()
    async def batch_winning_patterns(request: WinningPatternsRequest) -> dict:
        """
        Get top-performing headline/description patterns across all campaigns for a country.

        Shows which generic ad copy patterns consistently receive BEST/GOOD
        performance labels from Google. Use to optimize new ad copy templates.
        """
        db = _get_db()
        patterns = db.get_winning_patterns(
            request.country_code.upper(),
            request.asset_type,
            min_occurrences=request.min_occurrences,
        )
        return {
            "status": "success",
            "country_code": request.country_code.upper(),
            "asset_type": request.asset_type,
            "patterns": patterns,
            "count": len(patterns),
        }

    # ── Keyword Spend Analysis tools ────────────────────────────────

    class BatchKeywordSpendAnalysisRequest(BaseModel):
        """Analyze keyword spend trends with period-over-period comparison."""
        customer_id: str = Field(
            default_factory=_get_customer_id,
            description="Google Ads Customer ID. Default: configured account."
        )
        country_code: str = Field(
            ...,
            description="Country code (RO, TR, PL, HU, FR, DE, IT, ES, CZ, SK, BG, GR, etc.)"
        )
        campaign_ids: List[str] = Field(
            default_factory=list,
            description="Specific campaign IDs to analyze. Empty = auto-discover all ENABLED Search campaigns for this country."
        )
        current_period_days: int = Field(
            default=90,
            description="Number of days for the current (recent) period. Default: 90.",
            ge=7, le=365
        )
        previous_period_days: int = Field(
            default=90,
            description="Number of days for the previous (comparison) period. Default: 90.",
            ge=7, le=365
        )
        min_impressions: int = Field(
            default=10,
            description="Minimum impressions in EITHER period to include keyword. Default: 10."
        )
        min_cost_micros: int = Field(
            default=0,
            description="Minimum cost in micros in EITHER period. 0 = no filter. 1000000 = $1."
        )
        spike_threshold_pct: float = Field(
            default=50.0,
            description="Percent change threshold to flag as SPIKE or DROP. Default: 50%."
        )
        top_n: int = Field(
            default=50,
            description="Number of top keywords to include in final report per category. Default: 50."
        )
        include_monthly_breakdown: bool = Field(
            default=True,
            description="Include month-by-month data in results. Default: True."
        )
        include_quality_scores: bool = Field(
            default=False,
            description="Fetch quality scores (extra API call per campaign). Default: False."
        )
        include_product_mapping: bool = Field(
            default=True,
            description="Map keywords to product handles via ad group URLs. Default: True."
        )

    class BatchKeywordSpendAnalysisStatusRequest(BaseModel):
        """Poll status of keyword spend analysis job."""
        job_id: str = Field(
            ...,
            description="Job ID returned by batch_keyword_spend_analysis."
        )
        include_preview: bool = Field(
            default=False,
            description="Include preview of top 10 findings even if job not complete. Default: False."
        )

    @mcp_app.tool()
    async def batch_keyword_spend_analysis(request: BatchKeywordSpendAnalysisRequest) -> dict:
        """Analyze keyword spend trends with period-over-period comparison.

        Fetches keyword-level data for two periods, aggregates in SQLite,
        identifies spikes, drops, and anomalies. Returns structured report
        with comparison tables and prioritized recommendations.

        For small datasets (≤3 campaigns): returns results synchronously.
        For large datasets: spawns async worker, returns job_id for polling.
        """
        return await analyze_keyword_spend_trends(
            customer_id=request.customer_id,
            country_code=request.country_code,
            campaign_ids=request.campaign_ids,
            current_period_days=request.current_period_days,
            previous_period_days=request.previous_period_days,
            min_impressions=request.min_impressions,
            min_cost_micros=request.min_cost_micros,
            spike_threshold_pct=request.spike_threshold_pct,
            top_n=request.top_n,
            include_monthly_breakdown=request.include_monthly_breakdown,
            include_quality_scores=request.include_quality_scores,
            include_product_mapping=request.include_product_mapping,
        )

    @mcp_app.tool()
    async def batch_keyword_spend_analysis_status(request: BatchKeywordSpendAnalysisStatusRequest) -> dict:
        """Poll progress of keyword spend analysis job.

        Returns current phase, progress percentage, keywords fetched,
        and estimated time remaining. When complete, returns full report.
        """
        return get_keyword_spend_analysis_status(
            job_id=request.job_id,
            include_preview=request.include_preview,
        )

    # ── Location + Device + Time Analysis tools ─────────────────

    class LocationAnalysisRequest(BaseModel):
        """Analyze campaign performance by geographic location."""
        country_code: str = Field(..., description="Country code (e.g., TR, RO, PL)")
        campaign_ids: Optional[List[str]] = Field(
            None, description="Campaign IDs. None = auto-discover all ENABLED campaigns."
        )
        campaign_types: Optional[List[str]] = Field(
            None, description="Campaign types: SEARCH, PERFORMANCE_MAX, SHOPPING. None = all three."
        )
        start_date: str = Field(..., description="Start date YYYY-MM-DD")
        end_date: str = Field(..., description="End date YYYY-MM-DD")
        min_impressions: int = Field(50, description="Min impressions per location")
        top_n: int = Field(20, description="Top N locations to return")
        include_regions: bool = Field(True, description="Include state/region breakdown")
        location_type: str = Field("both", description="user_location | targeted | both")

    class DeviceAnalysisRequest(BaseModel):
        """Analyze campaign performance by device type."""
        country_code: str = Field(..., description="Country code (e.g., TR, RO, PL)")
        campaign_ids: Optional[List[str]] = Field(
            None, description="Campaign IDs. None = auto-discover all ENABLED campaigns."
        )
        campaign_types: Optional[List[str]] = Field(
            None, description="Campaign types: SEARCH, PERFORMANCE_MAX, SHOPPING. None = all three."
        )
        start_date: str = Field(..., description="Start date YYYY-MM-DD")
        end_date: str = Field(..., description="End date YYYY-MM-DD")
        include_ad_group_breakdown: bool = Field(False, description="Include per-ad-group device breakdown")
        include_device_hour_cross: bool = Field(False, description="Include device × hour heatmap")

    class LocDeviceTimeRequest(BaseModel):
        """Combined location + device + time analysis."""
        country_code: str = Field(..., description="Country code (e.g., TR, RO, PL)")
        campaign_ids: Optional[List[str]] = Field(
            None, description="Campaign IDs. None = auto-discover all types."
        )
        campaign_types: Optional[List[str]] = Field(
            None, description="Campaign types. None = SEARCH + PERFORMANCE_MAX + SHOPPING."
        )
        days: int = Field(90, description="Analysis period in days (default 90)")
        min_impressions: int = Field(50, description="Min impressions filter")
        include_device_hour_cross: bool = Field(True, description="Include device × hour heatmap")
        include_regions: bool = Field(True, description="Include region breakdown")
        top_n_locations: int = Field(20, description="Top N locations")

    @mcp_app.tool()
    async def batch_location_analysis(request: LocationAnalysisRequest) -> dict:
        """
        Analyze campaign performance by geographic location (country, state, region).

        Uses user_location_view and geographic_view to break down spend, CTR,
        conversions, CPA, and ROAS by location. Identifies best/worst locations
        and generates bid recommendations. Works with Search, PMax, and Shopping.
        """
        t0 = time.time()
        result = analyze_location_performance(
            country_code=request.country_code,
            campaign_ids=request.campaign_ids,
            campaign_types=request.campaign_types,
            start_date=request.start_date,
            end_date=request.end_date,
            min_impressions=request.min_impressions,
            top_n=request.top_n,
            include_regions=request.include_regions,
            location_type=request.location_type,
        )
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    @mcp_app.tool()
    async def batch_device_analysis(request: DeviceAnalysisRequest) -> dict:
        """
        Analyze campaign performance by device type (MOBILE, DESKTOP, TABLET).

        Shows where spend and results are concentrated per device. Calculates
        efficiency index (conv_share / spend_share) and generates bid modifier
        recommendations. Optionally includes device × hour heatmap.
        """
        t0 = time.time()
        result = analyze_device_performance(
            country_code=request.country_code,
            campaign_ids=request.campaign_ids,
            campaign_types=request.campaign_types,
            start_date=request.start_date,
            end_date=request.end_date,
            include_ad_group_breakdown=request.include_ad_group_breakdown,
            include_device_hour_cross=request.include_device_hour_cross,
        )
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    @mcp_app.tool()
    async def batch_loc_device_time_analysis(request: LocDeviceTimeRequest) -> dict:
        """
        Complete Location + Device + Time-of-Day analysis in one call.

        Orchestrates all three dimensions: geographic location breakdown,
        device type analysis, and time-of-day patterns. Generates cross-dimensional
        insights (e.g., best device+hour combo), merged priority recommendations,
        and 90-day impact projection. Analyzes Search, PMax, and Shopping campaigns
        separately with per-campaign breakdowns.
        """
        t0 = time.time()
        result = analyze_loc_device_time(
            country_code=request.country_code,
            campaign_ids=request.campaign_ids,
            campaign_types=request.campaign_types,
            days=request.days,
            min_impressions=request.min_impressions,
            include_device_hour_cross=request.include_device_hour_cross,
            include_regions=request.include_regions,
            top_n_locations=request.top_n_locations,
        )
        result["duration_ms"] = int((time.time() - t0) * 1000)
        return result

    return ["batch_analyze_assets", "batch_winning_patterns",
            "batch_keyword_spend_analysis", "batch_keyword_spend_analysis_status",
            "batch_location_analysis", "batch_device_analysis",
            "batch_loc_device_time_analysis"]
