"""
Shopping Coverage Monitor — Full-stack Shopping campaign health monitoring.

Tools:
- batch_shopping_coverage_audit: Feeds → MC products → listing groups → coverage → alerts
- batch_shopping_fix_bids: Batch-fix 0.01 GBP bids across regular Shopping campaigns

Stores results in local SQLite for trend tracking.

Usage: Import and call register_shopping_coverage_tools(mcp_instance) from google_ads_mcp.py
"""

import json
import os
import re
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from accounts_config import get_customer_id, load_config

import logging

_log = logging.getLogger("batch_shopping_coverage")
_log.setLevel(logging.DEBUG)
if not _log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"
    ))
    _log.addHandler(_handler)

# DB lives next to this module
_DB_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_PATH = os.path.join(_DB_DIR, "shopping_coverage.db")


# ============================================================================
# SQLite Persistence
# ============================================================================

class CoverageDB:
    """SQLite wrapper for shopping coverage tracking."""

    def __init__(self, db_path: str = _DB_PATH):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_tables()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                self._db_path, timeout=10, check_same_thread=False
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def _ensure_tables(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS feed_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL DEFAULT (datetime('now')),
                mc_id TEXT NOT NULL,
                country TEXT NOT NULL,
                feed_id TEXT,
                feed_name TEXT,
                feed_label TEXT,
                product_count INTEGER DEFAULT 0,
                fetch_url TEXT,
                fetch_status TEXT,
                last_fetch_time TEXT,
                cdn_source TEXT
            );

            CREATE TABLE IF NOT EXISTS product_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL DEFAULT (datetime('now')),
                mc_id TEXT NOT NULL,
                country TEXT NOT NULL,
                total_products INTEGER DEFAULT 0,
                regular_count INTEGER DEFAULT 0,
                np_count INTEGER DEFAULT 0,
                regular_label TEXT,
                np_label TEXT,
                has_more INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS campaign_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL DEFAULT (datetime('now')),
                campaign_id TEXT NOT NULL,
                campaign_name TEXT,
                country TEXT,
                mc_id TEXT,
                feed_label TEXT,
                campaign_type TEXT,
                subdivision_count INTEGER DEFAULT 0,
                unit_count INTEGER DEFAULT 0,
                named_products INTEGER DEFAULT 0,
                index_type TEXT,
                min_bid_gbp REAL DEFAULT 0,
                max_bid_gbp REAL DEFAULT 0,
                catch_all_bid_gbp REAL DEFAULT 0,
                impressions_7d INTEGER DEFAULT 0,
                clicks_7d INTEGER DEFAULT 0,
                conversions_7d REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS coverage_gaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL DEFAULT (datetime('now')),
                campaign_id TEXT NOT NULL,
                campaign_name TEXT,
                country TEXT,
                mc_id TEXT,
                mc_product_count INTEGER DEFAULT 0,
                lg_named_count INTEGER DEFAULT 0,
                coverage_pct REAL DEFAULT 0,
                stale_count INTEGER DEFAULT 0,
                missing_in_lg INTEGER DEFAULT 0,
                all_bids_001 INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL DEFAULT (datetime('now')),
                severity TEXT NOT NULL,
                country TEXT,
                campaign_id TEXT,
                campaign_name TEXT,
                alert_type TEXT NOT NULL,
                message TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_feed_snap_ts ON feed_snapshots(ts);
            CREATE INDEX IF NOT EXISTS idx_prod_snap_ts ON product_snapshots(ts);
            CREATE INDEX IF NOT EXISTS idx_camp_snap_ts ON campaign_snapshots(ts);
            CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts);
        """)
        conn.commit()

    def save_product_snapshot(self, mc_id, country, total, regular_count, np_count,
                              regular_label, np_label, has_more):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO product_snapshots
            (mc_id, country, total_products, regular_count, np_count, regular_label, np_label, has_more)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (mc_id, country, total, regular_count, np_count, regular_label, np_label, int(has_more)))
        conn.commit()

    def save_campaign_snapshot(self, data: dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO campaign_snapshots
            (campaign_id, campaign_name, country, mc_id, feed_label, campaign_type,
             subdivision_count, unit_count, named_products, index_type,
             min_bid_gbp, max_bid_gbp, catch_all_bid_gbp,
             impressions_7d, clicks_7d, conversions_7d)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            data.get('campaign_id'), data.get('campaign_name'), data.get('country'),
            data.get('mc_id'), data.get('feed_label'), data.get('campaign_type'),
            data.get('subdivision_count', 0), data.get('unit_count', 0),
            data.get('named_products', 0), data.get('index_type'),
            data.get('min_bid_gbp', 0), data.get('max_bid_gbp', 0),
            data.get('catch_all_bid_gbp', 0),
            data.get('impressions_7d', 0), data.get('clicks_7d', 0),
            data.get('conversions_7d', 0)
        ))
        conn.commit()

    def save_coverage_gap(self, data: dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO coverage_gaps
            (campaign_id, campaign_name, country, mc_id, mc_product_count,
             lg_named_count, coverage_pct, stale_count, missing_in_lg, all_bids_001)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            data.get('campaign_id'), data.get('campaign_name'), data.get('country'),
            data.get('mc_id'), data.get('mc_product_count', 0),
            data.get('lg_named_count', 0), data.get('coverage_pct', 0),
            data.get('stale_count', 0), data.get('missing_in_lg', 0),
            data.get('all_bids_001', 0)
        ))
        conn.commit()

    def save_alert(self, severity, country, campaign_id, campaign_name, alert_type, message):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO alerts (severity, country, campaign_id, campaign_name, alert_type, message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (severity, country, campaign_id, campaign_name, alert_type, message))
        conn.commit()

    def get_latest_alerts(self, limit=50):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_latest_coverage(self, limit=50):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM coverage_gaps ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_product_history(self, mc_id, limit=10):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM product_snapshots WHERE mc_id=? ORDER BY ts DESC LIMIT ?",
            (mc_id, limit)
        ).fetchall()
        return [dict(r) for r in rows]


# ============================================================================
# MC Account Map — config.json "shopping_coverage.stores" (see config.example.json):
#   {"<STORE_KEY>": {"mc_id": "...", "type": "<free label>", "regular_label": "<feed label or ->",
#                    "np_label": "<new-products feed label or ->"}}
# A store key starting with a two-letter country code (e.g. "PL" or "PL_B") is audited for that country.

def _mc_accounts() -> Dict[str, Dict[str, str]]:
    return (load_config().get("shopping_coverage", {}) or {}).get("stores", {}) or {}


# Minimum competitive bid (account currency)
MIN_COMPETITIVE_BID = 0.05


# ============================================================================
# Request Models
# ============================================================================

class ShoppingCoverageAuditRequest(BaseModel):
    """Request model for shopping coverage audit."""
    customer_id: str = Field(default_factory=get_customer_id, description="Google Ads Customer ID (defaults to the configured account)")
    country_code: Optional[str] = Field(default=None, description="Country code to audit (e.g. 'FR'). If empty, audits ALL countries.")
    include_mc_products: bool = Field(default=True, description="Also fetch MC product counts (slower but complete)")
    include_feed_status: bool = Field(default=False, description="Also check datafeed fetch status")


class ShoppingFixBidsRequest(BaseModel):
    """Request model for batch bid fixing."""
    customer_id: str = Field(default_factory=get_customer_id, description="Google Ads Customer ID (defaults to the configured account)")
    country_code: Optional[str] = Field(default=None, description="Country code (e.g. 'FR'). If empty, fixes ALL countries.")
    target_bid_gbp: float = Field(default=0.15, description="Target CPC bid in GBP for catch-all UNIT")
    dry_run: bool = Field(default=True, description="If true, only shows what would change without applying")
    campaign_ids: Optional[List[str]] = Field(default=None, description="Specific campaign IDs to fix. If empty, fixes all with bids <= 0.01 GBP")


# ============================================================================
# Tool Implementation
# ============================================================================

def _safe_attr(obj, path, default=None):
    """Safely get nested protobuf attribute via dot path."""
    try:
        current = obj
        for part in path.split("."):
            current = getattr(current, part)
        return current
    except (AttributeError, TypeError):
        return default


def _parse_listing_groups(gaql_results: list) -> dict:
    """Parse GAQL listing group results into per-campaign structure.

    Uses direct protobuf attribute access for reliability.
    """
    campaigns = {}
    for row in gaql_results:
        try:
            camp_id = str(_safe_attr(row, 'campaign.id', 0))
            camp_name = _safe_attr(row, 'campaign.name', '?')
            ag_id = str(_safe_attr(row, 'ad_group.id', 0))

            lg = _safe_attr(row, 'ad_group_criterion.listing_group')
            lg_type_raw = str(_safe_attr(lg, 'type_', 'UNIT'))
            lg_type = 'SUBDIVISION' if 'SUBDIVISION' in lg_type_raw else 'UNIT'

            # Extract custom attribute index and value
            case_value = _safe_attr(lg, 'case_value')
            pca = _safe_attr(case_value, 'product_custom_attribute') if case_value else None
            idx = str(_safe_attr(pca, 'index', '')) if pca else ''
            val = _safe_attr(pca, 'value', '') if pca else ''

            # Clean enum values like "INDEX4" from protobuf
            if idx and not idx.startswith('INDEX'):
                idx = str(idx)
            idx = idx.replace('ProductCustomAttributeIndex.', '')

            bid_micros = _safe_attr(row, 'ad_group_criterion.cpc_bid_micros', 0) or 0
            bid = int(bid_micros) / 1_000_000

            key = camp_id
            if key not in campaigns:
                campaigns[key] = {
                    'name': camp_name, 'subdivisions': 0, 'units': 0,
                    'named_products': [], 'bids': [], 'indices': set(),
                    'catch_all_bid': 0, 'ad_group_id': ag_id
                }

            if lg_type == 'SUBDIVISION':
                campaigns[key]['subdivisions'] += 1
            else:
                campaigns[key]['units'] += 1
                if val:
                    campaigns[key]['named_products'].append(val)
                else:
                    campaigns[key]['catch_all_bid'] = bid
                if bid > 0:
                    campaigns[key]['bids'].append(bid)
            if idx:
                campaigns[key]['indices'].add(idx)
        except Exception as e:
            _log.warning(f"Failed to parse LG row: {e}")
            continue

    return campaigns


def _parse_campaigns(gaql_results: list) -> dict:
    """Parse campaign GAQL results into structured dict.

    Uses direct protobuf attribute access.
    """
    campaigns = {}
    for row in gaql_results:
        try:
            cid = str(_safe_attr(row, 'campaign.id', 0))
            name = _safe_attr(row, 'campaign.name', '?')
            mc_id = str(_safe_attr(row, 'campaign.shopping_setting.merchant_id', 0))
            fl = _safe_attr(row, 'campaign.shopping_setting.feed_label', '')

            if cid and cid != '0':
                campaigns[cid] = {
                    'name': name,
                    'merchant_id': mc_id,
                    'feed_label': fl or '',
                }
        except Exception as e:
            _log.warning(f"Failed to parse campaign row: {e}")
            continue
    return campaigns


def _detect_country(campaign_name: str) -> str:
    """Extract country code from campaign name like 'HU | Hungary | ...'."""
    m = re.match(r'^([A-Z]{2})\s*\|', campaign_name)
    return m.group(1) if m else ''


def _is_np_campaign(campaign_name: str) -> bool:
    """Detect if this is an NP (New Products) campaign."""
    upper = campaign_name.upper()
    return ' NP' in upper or 'NEW PRODUCTS' in upper


def _count_products_by_label(products: list) -> dict:
    """Count products per feedLabel."""
    counts = {}
    for p in products:
        fl = p.get('feedLabel', '(none)')
        counts[fl] = counts.get(fl, 0) + 1
    return counts


# ============================================================================
# Register Tools
# ============================================================================

def register_shopping_coverage_tools(mcp_app):
    """Register shopping coverage audit and fix-bids tools."""

    import google_ads_mcp as _gam
    from google_ads_mcp import (
        _ensure_client,
        _format_customer_id,
        _execute_gaql,
    )

    # Import MC functions
    try:
        from mcp_merchant import (
            merchant_center_list_products,
            MerchantListProductsRequest,
        )
        _has_mc = True
    except ImportError:
        _has_mc = False

    db = CoverageDB()

    # ------------------------------------------------------------------
    # TOOL 1: batch_shopping_coverage_audit
    # ------------------------------------------------------------------
    @mcp_app.tool()
    async def batch_shopping_coverage_audit(request: ShoppingCoverageAuditRequest) -> dict:
        """
        Full-stack Shopping campaign health monitor.

        Audits: MC feeds → product counts → listing groups → coverage → alerts.
        Stores results in local SQLite for trend tracking.

        Returns: Dashboard with coverage matrix, alerts, and recommendations.
        Cost: ~3-5 GAQL queries + N MC API calls (where N = number of MC accounts).
        """
        try:
            _ensure_client()
            customer_id = _format_customer_id(request.customer_id)
            ts = datetime.utcnow().isoformat()
            alerts = []
            campaign_data = []
            product_data = []
            coverage_data = []

            # Determine which countries to audit
            mc_accounts = _mc_accounts()
            if not mc_accounts:
                return {"status": "error", "message": "No stores configured: add shopping_coverage.stores to config.json"}
            if request.country_code:
                cc = request.country_code.upper()
                accounts_to_audit = {k: v for k, v in mc_accounts.items()
                                     if k == cc or k.startswith(cc + "_")}
                if not accounts_to_audit:
                    return {"status": "error", "message": f"No MC account found for country {cc}"}
            else:
                accounts_to_audit = mc_accounts

            # ----- STEP 1: Get all Shopping campaigns -----
            _log.info("Step 1: Fetching all Shopping campaigns...")
            camp_query = (
                "SELECT campaign.id, campaign.name, campaign.status, "
                "campaign.shopping_setting.merchant_id, "
                "campaign.shopping_setting.feed_label "
                "FROM campaign "
                "WHERE campaign.advertising_channel_type = 'SHOPPING' "
                "AND campaign.status = 'ENABLED'"
            )
            try:
                camp_rows = _execute_gaql(customer_id, camp_query, page_size=100)
            except RuntimeError as e:
                return {"status": "error", "message": f"GAQL failed: {e}"}

            all_campaigns = _parse_campaigns(camp_rows)
            _log.info(f"Found {len(all_campaigns)} enabled Shopping campaigns")

            # Filter campaigns for target countries
            target_mc_ids = {v['mc_id'] for v in accounts_to_audit.values()}
            filtered_campaigns = {
                cid: cdata for cid, cdata in all_campaigns.items()
                if cdata['merchant_id'] in target_mc_ids
            }

            # ----- STEP 2: Get listing groups for all target campaigns -----
            _log.info("Step 2: Fetching listing groups...")
            all_listing_groups = {}
            campaign_ids_remaining = list(filtered_campaigns.keys())
            batch_size = 15  # Process in batches to avoid GAQL length limits

            while campaign_ids_remaining:
                batch = campaign_ids_remaining[:batch_size]
                campaign_ids_remaining = campaign_ids_remaining[batch_size:]

                id_filter = ", ".join(batch)
                lg_query = (
                    "SELECT campaign.id, campaign.name, ad_group.id, "
                    "ad_group_criterion.listing_group.type, "
                    "ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
                    "ad_group_criterion.listing_group.case_value.product_custom_attribute.value, "
                    "ad_group_criterion.cpc_bid_micros "
                    "FROM ad_group_criterion "
                    "WHERE ad_group_criterion.type = 'LISTING_GROUP' "
                    "AND campaign.advertising_channel_type = 'SHOPPING' "
                    "AND campaign.status = 'ENABLED' "
                    "AND ad_group_criterion.status != 'REMOVED' "
                    f"AND campaign.id IN ({id_filter})"
                )
                try:
                    lg_rows = _execute_gaql(customer_id, lg_query, page_size=1000)
                    batch_lgs = _parse_listing_groups(lg_rows)
                    all_listing_groups.update(batch_lgs)
                except RuntimeError as e:
                    _log.warning(f"LG batch query failed: {e}")

            _log.info(f"Parsed listing groups for {len(all_listing_groups)} campaigns")

            # ----- STEP 3: Get MC product counts -----
            mc_product_counts = {}  # mc_id -> {feed_label: count}
            if request.include_mc_products and _has_mc:
                _log.info("Step 3: Fetching MC product counts...")

                for label, acc in accounts_to_audit.items():
                    mc_id = acc['mc_id']
                    try:
                        req = MerchantListProductsRequest(merchant_id=mc_id, max_results=250)
                        result = await merchant_center_list_products(req)
                        if result.get('status') == 'success':
                            products = result.get('products', [])
                            has_more = result.get('next_page_token') is not None
                            by_label = _count_products_by_label(products)
                            mc_product_counts[mc_id] = {
                                'total': result.get('count', 0),
                                'by_label': by_label,
                                'has_more': has_more,
                            }

                            # Determine regular vs NP counts
                            regular_label = acc['regular_label']
                            np_label = acc['np_label']
                            regular_count = by_label.get(regular_label, 0)
                            np_count = by_label.get(np_label, 0)

                            # Save to DB
                            db.save_product_snapshot(
                                mc_id=mc_id, country=label,
                                total=result.get('count', 0),
                                regular_count=regular_count,
                                np_count=np_count,
                                regular_label=regular_label,
                                np_label=np_label,
                                has_more=has_more
                            )

                            product_data.append({
                                'country': label,
                                'mc_id': mc_id,
                                'total': result.get('count', 0),
                                'regular_label': regular_label,
                                'regular_count': regular_count,
                                'np_label': np_label,
                                'np_count': np_count,
                                'has_more': has_more,
                            })
                    except Exception as e:
                        _log.warning(f"MC {mc_id} ({label}): {e}")
                        mc_product_counts[mc_id] = {'total': 0, 'by_label': {}, 'has_more': False}
            else:
                _log.info("Step 3: Skipping MC product counts (disabled or MC not available)")

            # ----- STEP 4: Cross-reference & generate alerts -----
            _log.info("Step 4: Cross-referencing campaigns vs MC products...")

            # Get 7-day performance
            perf_query = (
                "SELECT campaign.id, campaign.name, "
                "metrics.impressions, metrics.clicks, metrics.conversions, "
                "metrics.cost_micros "
                "FROM campaign "
                "WHERE campaign.advertising_channel_type = 'SHOPPING' "
                "AND campaign.status = 'ENABLED' "
                "AND segments.date DURING LAST_7_DAYS"
            )
            perf_map = {}
            try:
                perf_rows = _execute_gaql(customer_id, perf_query, page_size=100)
                for row in perf_rows:
                    try:
                        cid = str(_safe_attr(row, 'campaign.id', 0))
                        imp = int(_safe_attr(row, 'metrics.impressions', 0) or 0)
                        clicks = int(_safe_attr(row, 'metrics.clicks', 0) or 0)
                        conv = float(_safe_attr(row, 'metrics.conversions', 0) or 0)
                        if cid and cid != '0':
                            if cid not in perf_map:
                                perf_map[cid] = {'imp': 0, 'clicks': 0, 'conv': 0.0}
                            perf_map[cid]['imp'] += imp
                            perf_map[cid]['clicks'] += clicks
                            perf_map[cid]['conv'] += conv
                    except Exception:
                        continue
            except RuntimeError as e:
                _log.warning(f"Performance query failed: {e}")

            for cid, cdata in filtered_campaigns.items():
                camp_name = cdata['name']
                mc_id = cdata['merchant_id']
                feed_label = cdata['feed_label']
                country = _detect_country(camp_name)
                is_np = _is_np_campaign(camp_name)
                camp_type = 'NP' if is_np else 'Regular'

                # Listing group data
                lg = all_listing_groups.get(cid, {})
                subdivisions = lg.get('subdivisions', 0)
                units = lg.get('units', 0)
                named = len(lg.get('named_products', []))
                bids = lg.get('bids', [])
                indices = lg.get('indices', set())
                catch_all_bid = lg.get('catch_all_bid', 0)
                min_bid = min(bids) if bids else 0
                max_bid = max(bids) if bids else 0

                # Performance
                perf = perf_map.get(cid, {'imp': 0, 'clicks': 0, 'conv': 0.0})

                # Save campaign snapshot
                snap = {
                    'campaign_id': cid, 'campaign_name': camp_name,
                    'country': country, 'mc_id': mc_id,
                    'feed_label': feed_label, 'campaign_type': camp_type,
                    'subdivision_count': subdivisions, 'unit_count': units,
                    'named_products': named,
                    'index_type': ','.join(sorted(indices)),
                    'min_bid_gbp': min_bid, 'max_bid_gbp': max_bid,
                    'catch_all_bid_gbp': catch_all_bid,
                    'impressions_7d': perf['imp'],
                    'clicks_7d': perf['clicks'],
                    'conversions_7d': perf['conv'],
                }
                db.save_campaign_snapshot(snap)
                campaign_data.append(snap)

                # ----- ALERTS -----

                # Alert: No listing groups at all
                if subdivisions == 0 and units == 0:
                    a = {
                        'severity': 'CRITICAL', 'country': country,
                        'campaign_id': cid, 'campaign_name': camp_name,
                        'alert_type': 'NO_LISTING_GROUPS',
                        'message': f'Campaign has ZERO listing groups — cannot serve ads'
                    }
                    alerts.append(a)
                    db.save_alert(**a)

                # Alert: All bids <= 0.01 GBP
                if bids and max_bid <= 0.01 and not is_np:
                    a = {
                        'severity': 'CRITICAL', 'country': country,
                        'campaign_id': cid, 'campaign_name': camp_name,
                        'alert_type': 'ALL_BIDS_001',
                        'message': f'All {len(bids)} bids are ≤ 0.01 GBP — non-competitive, winning ~0 auctions'
                    }
                    alerts.append(a)
                    db.save_alert(**a)

                # Alert: Zero impressions (NP campaigns that should be active)
                if is_np and perf['imp'] == 0 and subdivisions > 0:
                    a = {
                        'severity': 'HIGH', 'country': country,
                        'campaign_id': cid, 'campaign_name': camp_name,
                        'alert_type': 'NP_ZERO_IMPRESSIONS',
                        'message': f'NP campaign has listing groups but 0 impressions in 7 days'
                    }
                    alerts.append(a)
                    db.save_alert(**a)

                # Coverage analysis (if MC data available)
                mc_info = mc_product_counts.get(mc_id)
                if mc_info:
                    by_label = mc_info['by_label']
                    # Determine how many MC products this campaign targets
                    if feed_label and feed_label != '-':
                        mc_count = by_label.get(feed_label, 0)
                    else:
                        mc_count = mc_info['total']

                    coverage_pct = (named / mc_count * 100) if mc_count > 0 else 0
                    stale = max(0, named - mc_count) if mc_count > 0 else 0
                    missing = max(0, mc_count - named)

                    gap = {
                        'campaign_id': cid, 'campaign_name': camp_name,
                        'country': country, 'mc_id': mc_id,
                        'mc_product_count': mc_count,
                        'lg_named_count': named,
                        'coverage_pct': round(coverage_pct, 1),
                        'stale_count': stale,
                        'missing_in_lg': missing,
                        'all_bids_001': 1 if (bids and max_bid <= 0.01) else 0,
                    }
                    db.save_coverage_gap(gap)
                    coverage_data.append(gap)

                    # Alert: Low coverage
                    if coverage_pct < 50 and named > 0 and mc_count > 10:
                        a = {
                            'severity': 'HIGH', 'country': country,
                            'campaign_id': cid, 'campaign_name': camp_name,
                            'alert_type': 'LOW_COVERAGE',
                            'message': f'Only {named}/{mc_count} products ({coverage_pct:.0f}%) have individual listing groups'
                        }
                        alerts.append(a)
                        db.save_alert(**a)

                    # Alert: Many stale LGs
                    if stale > 0 and stale / max(named, 1) > 0.3:
                        a = {
                            'severity': 'HIGH', 'country': country,
                            'campaign_id': cid, 'campaign_name': camp_name,
                            'alert_type': 'STALE_LISTING_GROUPS',
                            'message': f'{stale} listing groups point to products no longer in MC (stale)'
                        }
                        alerts.append(a)
                        db.save_alert(**a)

            # ----- STEP 5: Build dashboard -----
            _log.info("Step 5: Building dashboard...")

            # Count alerts by severity
            alert_counts = {}
            for a in alerts:
                sev = a['severity']
                alert_counts[sev] = alert_counts.get(sev, 0) + 1

            # Summary
            total_campaigns = len(filtered_campaigns)
            np_camps = sum(1 for c in campaign_data if c['campaign_type'] == 'NP')
            reg_camps = total_campaigns - np_camps
            bids_001_count = sum(1 for c in campaign_data
                                if c['max_bid_gbp'] <= 0.01 and c['campaign_type'] != 'NP')

            return {
                "status": "success",
                "summary": {
                    "total_campaigns": total_campaigns,
                    "np_campaigns": np_camps,
                    "regular_campaigns": reg_camps,
                    "campaigns_all_bids_001": bids_001_count,
                    "mc_accounts_audited": len(product_data),
                    "total_alerts": len(alerts),
                    "alert_counts": alert_counts,
                },
                "alerts": alerts,
                "campaigns": campaign_data,
                "mc_products": product_data,
                "coverage": coverage_data,
                "db_path": _DB_PATH,
            }

        except Exception as e:
            _log.error(f"Coverage audit failed: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    # ------------------------------------------------------------------
    # TOOL 2: batch_shopping_fix_bids
    # ------------------------------------------------------------------
    @mcp_app.tool()
    async def batch_shopping_fix_bids(request: ShoppingFixBidsRequest) -> dict:
        """
        Batch-fix non-competitive bids (0.01 GBP) across regular Shopping campaigns.

        For each campaign with all bids ≤ 0.01 GBP:
        - Rebuilds listing group tree to simple: root SUBDIVISION → catch-all UNIT at target bid
        - Removes per-product micro-bid groups (they're ineffective at 0.01 GBP anyway)

        Use dry_run=True first to preview changes.
        Cost: 1 GAQL query + 1-2 mutate operations per campaign fixed.
        """
        try:
            _ensure_client()
            customer_id = _format_customer_id(request.customer_id)
            target_bid_micros = int(request.target_bid_gbp * 1_000_000)

            # Step 1: Find campaigns with bad bids
            _log.info("Finding campaigns with bids <= 0.01 GBP...")
            camp_query = (
                "SELECT campaign.id, campaign.name, "
                "campaign.shopping_setting.merchant_id, "
                "campaign.shopping_setting.feed_label "
                "FROM campaign "
                "WHERE campaign.advertising_channel_type = 'SHOPPING' "
                "AND campaign.status = 'ENABLED'"
            )
            try:
                camp_rows = _execute_gaql(customer_id, camp_query, page_size=100)
            except RuntimeError as e:
                return {"status": "error", "message": f"GAQL failed: {e}"}

            all_campaigns = _parse_campaigns(camp_rows)

            # Get listing groups
            lg_query = (
                "SELECT campaign.id, campaign.name, ad_group.id, "
                "ad_group_criterion.criterion_id, "
                "ad_group_criterion.listing_group.type, "
                "ad_group_criterion.listing_group.case_value.product_custom_attribute.index, "
                "ad_group_criterion.listing_group.case_value.product_custom_attribute.value, "
                "ad_group_criterion.cpc_bid_micros "
                "FROM ad_group_criterion "
                "WHERE ad_group_criterion.type = 'LISTING_GROUP' "
                "AND campaign.advertising_channel_type = 'SHOPPING' "
                "AND campaign.status = 'ENABLED' "
                "AND ad_group_criterion.status != 'REMOVED'"
            )
            try:
                lg_rows = _execute_gaql(customer_id, lg_query, page_size=1000)
            except RuntimeError as e:
                return {"status": "error", "message": f"LG query failed: {e}"}

            all_lgs = _parse_listing_groups(lg_rows)

            # Extract ad_group_ids per campaign (already stored by _parse_listing_groups)
            ag_map = {}  # campaign_id -> ad_group_id
            for cid, lg_data in all_lgs.items():
                if lg_data.get('ad_group_id'):
                    ag_map[cid] = lg_data['ad_group_id']

            # Filter: only campaigns with ALL bids <= 0.01 and not NP
            targets = []
            for cid, cdata in all_campaigns.items():
                if _is_np_campaign(cdata['name']):
                    continue  # Skip NP campaigns

                # Country filter
                country = _detect_country(cdata['name'])
                if request.country_code and country != request.country_code.upper():
                    continue

                # Campaign ID filter
                if request.campaign_ids and cid not in request.campaign_ids:
                    continue

                lg = all_lgs.get(cid, {})
                bids = lg.get('bids', [])
                max_bid = max(bids) if bids else 0

                if max_bid <= 0.01 and bids:
                    targets.append({
                        'campaign_id': cid,
                        'campaign_name': cdata['name'],
                        'country': country,
                        'merchant_id': cdata['merchant_id'],
                        'feed_label': cdata['feed_label'],
                        'ad_group_id': ag_map.get(cid),
                        'current_max_bid': max_bid,
                        'current_units': lg.get('units', 0),
                        'current_subdivisions': lg.get('subdivisions', 0),
                        'current_named': len(lg.get('named_products', [])),
                    })

            if not targets:
                return {
                    "status": "success",
                    "message": "No campaigns found with all bids <= 0.01 GBP",
                    "campaigns_checked": len(all_campaigns),
                    "changes": []
                }

            # Step 2: Preview or execute
            changes = []
            for t in targets:
                change = {
                    'campaign_id': t['campaign_id'],
                    'campaign_name': t['campaign_name'],
                    'country': t['country'],
                    'action': f"Rebuild listing groups: remove {t['current_units']} units + {t['current_subdivisions']} subdivisions at 0.01 GBP → 1 catch-all UNIT at {request.target_bid_gbp} GBP",
                    'current_max_bid': t['current_max_bid'],
                    'new_bid': request.target_bid_gbp,
                    'ad_group_id': t['ad_group_id'],
                    'applied': False,
                }

                if not request.dry_run and t['ad_group_id']:
                    # Actually rebuild the listing group tree
                    try:
                        from mcp_shopping import (
                            google_ads_rebuild_shopping_listing_group_tree,
                            RebuildShoppingListingGroupTreeRequest
                        )
                        rebuild_req = RebuildShoppingListingGroupTreeRequest(
                            customer_id=customer_id,
                            ad_group_id=t['ad_group_id'],
                            root_dimension="custom_label_4",
                            groups=[],  # Empty = just catch-all
                            everything_else_bid_micros=target_bid_micros
                        )
                        rebuild_result = await google_ads_rebuild_shopping_listing_group_tree(rebuild_req)
                        if rebuild_result.get('status') == 'success':
                            change['applied'] = True
                            change['result'] = 'SUCCESS'
                            _log.info(f"✅ Fixed {t['campaign_name']}: {request.target_bid_gbp} GBP")
                        else:
                            change['result'] = rebuild_result.get('message', 'Unknown error')
                            _log.warning(f"❌ Failed {t['campaign_name']}: {change['result']}")
                    except Exception as e:
                        change['result'] = str(e)
                        _log.error(f"❌ Error fixing {t['campaign_name']}: {e}")

                changes.append(change)

            return {
                "status": "success",
                "dry_run": request.dry_run,
                "campaigns_checked": len(all_campaigns),
                "campaigns_to_fix": len(targets),
                "target_bid_gbp": request.target_bid_gbp,
                "changes": changes,
                "message": (
                    f"DRY RUN: {len(targets)} campaigns would be fixed. "
                    f"Run with dry_run=false to apply."
                    if request.dry_run else
                    f"Applied fixes to {sum(1 for c in changes if c.get('applied'))} of {len(targets)} campaigns"
                )
            }

        except Exception as e:
            _log.error(f"Fix bids failed: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    return {
        "batch_shopping_coverage_audit": batch_shopping_coverage_audit,
        "batch_shopping_fix_bids": batch_shopping_fix_bids,
    }
