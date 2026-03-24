"""
BatchDB — SQLite persistence layer for Universal Google Ads Batch Optimizer.

Provides:
- Product setup tracking (which assets exist per product/campaign)
- Operation logging (audit trail of all API actions)
- GAQL query caching (avoid redundant API calls)
- XML feed caching (avoid re-downloading feeds)

DB file: batch_state.db (created in same directory as this module)
"""

import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


# DB lives next to this module
_DB_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB_PATH = os.path.join(_DB_DIR, "batch_state.db")


class BatchDB:
    """SQLite wrapper for batch optimizer state tracking."""

    def __init__(self, db_path: str = _DEFAULT_DB_PATH):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            # check_same_thread=False needed for async worker (daemon thread)
            self._conn = sqlite3.connect(
                self._db_path, timeout=10, check_same_thread=False
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def _ensure_tables(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS product_setup (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                product_handle TEXT NOT NULL,
                ad_group_id TEXT,
                ad_group_name TEXT,
                -- Status flags (0/1)
                has_rsa_a INTEGER DEFAULT 0,
                has_rsa_b INTEGER DEFAULT 0,
                has_rsa_c INTEGER DEFAULT 0,
                has_callouts INTEGER DEFAULT 0,
                has_sitelinks INTEGER DEFAULT 0,
                has_promotion INTEGER DEFAULT 0,
                has_snippets INTEGER DEFAULT 0,
                has_keywords INTEGER DEFAULT 0,
                has_images INTEGER DEFAULT 0,
                image_count INTEGER DEFAULT 0,
                -- Metadata
                product_name TEXT,
                product_price REAL,
                product_url TEXT,
                xml_feed_source TEXT,
                -- Tracking
                first_seen_at TEXT DEFAULT (datetime('now')),
                last_updated_at TEXT DEFAULT (datetime('now')),
                last_error TEXT,
                status TEXT DEFAULT 'pending',
                UNIQUE(country_code, campaign_id, product_handle)
            );

            CREATE TABLE IF NOT EXISTS operation_log (
                id INTEGER PRIMARY KEY,
                timestamp TEXT DEFAULT (datetime('now')),
                country_code TEXT,
                campaign_id TEXT,
                product_handle TEXT,
                operation TEXT NOT NULL,
                status TEXT NOT NULL,
                details TEXT,
                api_calls_count INTEGER DEFAULT 1,
                duration_ms INTEGER
            );

            CREATE TABLE IF NOT EXISTS gaql_cache (
                id INTEGER PRIMARY KEY,
                query_hash TEXT UNIQUE NOT NULL,
                customer_id TEXT NOT NULL,
                query_text TEXT NOT NULL,
                result_json TEXT NOT NULL,
                result_count INTEGER,
                cached_at TEXT DEFAULT (datetime('now')),
                ttl_seconds INTEGER DEFAULT 300
            );

            CREATE TABLE IF NOT EXISTS xml_feed_cache (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                feed_url TEXT NOT NULL,
                feed_hash TEXT,
                products_json TEXT NOT NULL,
                product_count INTEGER,
                cached_at TEXT DEFAULT (datetime('now')),
                ttl_seconds INTEGER DEFAULT 3600,
                UNIQUE(country_code, feed_url)
            );

            CREATE INDEX IF NOT EXISTS idx_product_setup_country_campaign
                ON product_setup(country_code, campaign_id);
            CREATE INDEX IF NOT EXISTS idx_product_setup_status
                ON product_setup(status);
            CREATE INDEX IF NOT EXISTS idx_operation_log_product
                ON operation_log(country_code, campaign_id, product_handle);
            CREATE INDEX IF NOT EXISTS idx_operation_log_timestamp
                ON operation_log(timestamp);
            CREATE INDEX IF NOT EXISTS idx_gaql_cache_hash
                ON gaql_cache(query_hash);

            -- Cross-campaign keyword intelligence
            CREATE TABLE IF NOT EXISTS keyword_intelligence (
                id INTEGER PRIMARY KEY,
                product_name TEXT NOT NULL,
                country_code TEXT,
                keyword_text TEXT NOT NULL,
                match_type TEXT,
                source_campaign_id TEXT,
                source_campaign_name TEXT,
                impressions INTEGER DEFAULT 0,
                clicks INTEGER DEFAULT 0,
                conversions REAL DEFAULT 0,
                cost_micros INTEGER DEFAULT 0,
                quality_score INTEGER,
                conversion_value REAL DEFAULT 0,
                ctr REAL,
                conv_rate REAL,
                cpc_micros REAL,
                roas REAL DEFAULT 0,
                roi_percent REAL,
                cached_at TEXT DEFAULT (datetime('now')),
                UNIQUE(product_name, country_code, keyword_text, match_type, source_campaign_id)
            );

            CREATE INDEX IF NOT EXISTS idx_keyword_intel_product
                ON keyword_intelligence(product_name, country_code);
            CREATE INDEX IF NOT EXISTS idx_keyword_intel_conv
                ON keyword_intelligence(conversions DESC);

            -- RSA headline/description performance tracking
            CREATE TABLE IF NOT EXISTS asset_performance (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                headline_pattern TEXT NOT NULL,
                performance_label TEXT,
                occurrences INTEGER DEFAULT 1,
                avg_impressions REAL DEFAULT 0,
                avg_clicks REAL DEFAULT 0,
                avg_conversions REAL DEFAULT 0,
                cached_at TEXT DEFAULT (datetime('now')),
                UNIQUE(country_code, campaign_id, asset_type, headline_pattern, performance_label)
            );

            CREATE INDEX IF NOT EXISTS idx_asset_perf_country
                ON asset_performance(country_code, performance_label);

            -- Category keyword mapping (cross-product keyword sharing)
            CREATE TABLE IF NOT EXISTS category_keywords (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                country_code TEXT NOT NULL,
                keyword_text TEXT NOT NULL,
                match_type TEXT DEFAULT 'EXACT',
                avg_conversions REAL DEFAULT 0,
                avg_ctr REAL DEFAULT 0,
                avg_roi REAL DEFAULT 0,
                source_products TEXT,
                cached_at TEXT DEFAULT (datetime('now')),
                UNIQUE(category, country_code, keyword_text, match_type)
            );

            CREATE INDEX IF NOT EXISTS idx_category_kw
                ON category_keywords(category, country_code);

            -- URL-based handle-to-ad-group mapping (persistent cache from RSA final_urls)
            CREATE TABLE IF NOT EXISTS handle_ag_map (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                product_handle TEXT NOT NULL,
                ad_group_id TEXT NOT NULL,
                ad_group_name TEXT,
                match_source TEXT DEFAULT 'url',
                sample_url TEXT,
                cached_at TEXT DEFAULT (datetime('now')),
                UNIQUE(country_code, campaign_id, product_handle)
            );

            CREATE INDEX IF NOT EXISTS idx_handle_ag_map_lookup
                ON handle_ag_map(country_code, campaign_id, product_handle);
            CREATE INDEX IF NOT EXISTS idx_handle_ag_map_ag
                ON handle_ag_map(ad_group_id);

            -- Image dedup tracking (MD5 hashes of uploaded images)
            CREATE TABLE IF NOT EXISTS image_uploads (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                ad_group_id TEXT NOT NULL,
                product_handle TEXT,
                image_md5 TEXT NOT NULL,
                asset_resource_name TEXT,
                image_url TEXT,
                filename TEXT,
                width INTEGER,
                height INTEGER,
                file_size INTEGER,
                uploaded_at TEXT DEFAULT (datetime('now')),
                UNIQUE(country_code, campaign_id, ad_group_id, image_md5)
            );

            CREATE INDEX IF NOT EXISTS idx_image_uploads_md5
                ON image_uploads(image_md5);
            CREATE INDEX IF NOT EXISTS idx_image_uploads_ag
                ON image_uploads(ad_group_id);

            -- Rate limit tracking
            CREATE TABLE IF NOT EXISTS rate_limit_log (
                id INTEGER PRIMARY KEY,
                hit_at TEXT DEFAULT (datetime('now')),
                retry_after_seconds INTEGER,
                operation TEXT,
                details TEXT
            );

            -- API call counting
            CREATE TABLE IF NOT EXISTS api_calls (
                id INTEGER PRIMARY KEY,
                date TEXT NOT NULL,
                hour INTEGER NOT NULL,
                operation_type TEXT NOT NULL,
                count INTEGER DEFAULT 1,
                timestamp TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_api_calls_date ON api_calls(date);

            -- Guardrail validation results
            CREATE TABLE IF NOT EXISTS guardrail_results (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                product_handle TEXT NOT NULL,
                ad_group_id TEXT,
                checks_json TEXT NOT NULL DEFAULT '{}',
                passed_count INTEGER DEFAULT 0,
                failed_count INTEGER DEFAULT 0,
                skipped_count INTEGER DEFAULT 0,
                overall_status TEXT DEFAULT 'PENDING',
                validated_at TEXT DEFAULT (datetime('now')),
                source TEXT DEFAULT 'setup',
                UNIQUE(country_code, campaign_id, product_handle)
            );

            CREATE INDEX IF NOT EXISTS idx_guardrail_status
                ON guardrail_results(country_code, campaign_id, overall_status);

            -- Ad/asset eligibility tracking
            CREATE TABLE IF NOT EXISTS ad_eligibility (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                ad_group_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                approval_status TEXT,
                review_status TEXT,
                disapproval_reasons TEXT,
                action_taken TEXT,
                action_at TEXT,
                product_handle TEXT,
                checked_at TEXT DEFAULT (datetime('now')),
                UNIQUE(country_code, campaign_id, entity_type, entity_id)
            );

            CREATE INDEX IF NOT EXISTS idx_eligibility_status
                ON ad_eligibility(approval_status);
            CREATE INDEX IF NOT EXISTS idx_eligibility_campaign
                ON ad_eligibility(country_code, campaign_id);

            -- ═══════════════════════════════════════════════════════════
            -- Performance Analytics Enhancement Tables (v2)
            -- ═══════════════════════════════════════════════════════════

            -- 1. Asset performance HISTORY (trend tracking over time)
            --    Stores snapshots of asset labels on each scan so we can
            --    track LEARNING→GOOD→BEST transitions and detect regressions.
            CREATE TABLE IF NOT EXISTS asset_performance_history (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                headline_pattern TEXT NOT NULL,
                performance_label TEXT NOT NULL,
                occurrences INTEGER DEFAULT 1,
                avg_impressions REAL DEFAULT 0,
                avg_clicks REAL DEFAULT 0,
                avg_conversions REAL DEFAULT 0,
                avg_conversion_value REAL DEFAULT 0,
                scanned_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_asset_history_lookup
                ON asset_performance_history(country_code, campaign_id, asset_type, headline_pattern);
            CREATE INDEX IF NOT EXISTS idx_asset_history_time
                ON asset_performance_history(scanned_at);

            -- 2. Product funnel diagnostics (per-product impression→CTR→conv analysis)
            --    Populated by funnel diagnostic scan, used to identify bottlenecks.
            CREATE TABLE IF NOT EXISTS product_funnel_diagnostics (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                product_handle TEXT NOT NULL,
                ad_group_id TEXT,
                -- Funnel metrics
                impressions INTEGER DEFAULT 0,
                clicks INTEGER DEFAULT 0,
                conversions REAL DEFAULT 0,
                conversion_value REAL DEFAULT 0,
                cost_micros INTEGER DEFAULT 0,
                -- Derived rates
                ctr REAL DEFAULT 0,
                conv_rate REAL DEFAULT 0,
                roas REAL DEFAULT 0,
                cpa_micros REAL DEFAULT 0,
                -- Diagnosis
                funnel_bottleneck TEXT,
                diagnosis_details TEXT,
                -- Comparison to campaign averages
                ctr_vs_avg REAL DEFAULT 0,
                conv_rate_vs_avg REAL DEFAULT 0,
                roas_vs_avg REAL DEFAULT 0,
                -- Tracking
                period_start TEXT,
                period_end TEXT,
                scanned_at TEXT DEFAULT (datetime('now')),
                UNIQUE(country_code, campaign_id, product_handle, period_start)
            );

            CREATE INDEX IF NOT EXISTS idx_funnel_diag_product
                ON product_funnel_diagnostics(country_code, campaign_id, product_handle);
            CREATE INDEX IF NOT EXISTS idx_funnel_diag_bottleneck
                ON product_funnel_diagnostics(funnel_bottleneck);

            -- 3. Seasonality patterns (monthly performance by product category)
            CREATE TABLE IF NOT EXISTS seasonality_patterns (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                category TEXT NOT NULL,
                month INTEGER NOT NULL,
                avg_ctr REAL DEFAULT 0,
                avg_conv_rate REAL DEFAULT 0,
                avg_roas REAL DEFAULT 0,
                avg_cpc_micros REAL DEFAULT 0,
                sample_count INTEGER DEFAULT 0,
                scanned_at TEXT DEFAULT (datetime('now')),
                UNIQUE(country_code, category, month)
            );

            CREATE INDEX IF NOT EXISTS idx_seasonality_lookup
                ON seasonality_patterns(country_code, category);

            -- 4. Cross-campaign product overlap (frequency/fatigue tracking)
            CREATE TABLE IF NOT EXISTS product_campaign_overlap (
                id INTEGER PRIMARY KEY,
                country_code TEXT NOT NULL,
                product_handle TEXT NOT NULL,
                campaign_count INTEGER DEFAULT 0,
                active_ad_group_count INTEGER DEFAULT 0,
                total_impressions INTEGER DEFAULT 0,
                total_cost_micros INTEGER DEFAULT 0,
                campaign_ids_json TEXT,
                overlap_risk TEXT DEFAULT 'LOW',
                scanned_at TEXT DEFAULT (datetime('now')),
                UNIQUE(country_code, product_handle)
            );

            CREATE INDEX IF NOT EXISTS idx_overlap_risk
                ON product_campaign_overlap(overlap_risk);

            CREATE TABLE IF NOT EXISTS changelog (
                id INTEGER PRIMARY KEY,
                created_at TEXT,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                details TEXT,
                country_code TEXT,
                campaign_id TEXT,
                severity TEXT DEFAULT 'info',
                auto_logged INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_changelog_created
                ON changelog(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_changelog_category
                ON changelog(category);

            -- MCP-side response cache: AI ad copy (avoids HTTP roundtrip on re-runs)
            CREATE TABLE IF NOT EXISTS ai_copy_cache (
                id INTEGER PRIMARY KEY,
                handle TEXT NOT NULL,
                country_code TEXT NOT NULL,
                campaign_id TEXT,
                payload_hash TEXT NOT NULL,
                response_json TEXT NOT NULL,
                cached_at TEXT DEFAULT (datetime('now')),
                ttl_seconds INTEGER DEFAULT 604800,
                UNIQUE(handle, country_code, payload_hash)
            );

            CREATE INDEX IF NOT EXISTS idx_ai_copy_cache_lookup
                ON ai_copy_cache(handle, country_code, payload_hash);

            -- MCP-side response cache: image candidates (avoids HTTP roundtrip on re-runs)
            CREATE TABLE IF NOT EXISTS image_candidates_cache (
                id INTEGER PRIMARY KEY,
                handle TEXT NOT NULL,
                candidates_json TEXT NOT NULL,
                candidate_count INTEGER DEFAULT 0,
                cached_at TEXT DEFAULT (datetime('now')),
                ttl_seconds INTEGER DEFAULT 86400,
                UNIQUE(handle)
            );

            CREATE INDEX IF NOT EXISTS idx_image_candidates_cache_handle
                ON image_candidates_cache(handle);

            -- ═══════════════════════════════════════════════════════════════
            -- Keyword Spend Analysis tables (batch_keyword_spend_analysis)
            -- ═══════════════════════════════════════════════════════════════

            -- Raw keyword data per period (data lake)
            CREATE TABLE IF NOT EXISTS keyword_spend_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                campaign_name TEXT,
                ad_group_id TEXT,
                ad_group_name TEXT,
                criterion_id TEXT NOT NULL,
                keyword_text TEXT NOT NULL,
                match_type TEXT,
                keyword_status TEXT,
                quality_score INTEGER,
                period_label TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                segment_month TEXT,
                impressions INTEGER DEFAULT 0,
                clicks INTEGER DEFAULT 0,
                conversions REAL DEFAULT 0,
                conversions_value REAL DEFAULT 0,
                cost_micros INTEGER DEFAULT 0,
                ctr REAL DEFAULT 0,
                cpc_micros REAL DEFAULT 0,
                conv_rate REAL DEFAULT 0,
                cpa_micros REAL DEFAULT 0,
                roas REAL DEFAULT 0,
                fetched_at TEXT DEFAULT (datetime('now')),
                UNIQUE(job_id, criterion_id, campaign_id, period_label, segment_month)
            );

            CREATE INDEX IF NOT EXISTS idx_ksp_job
                ON keyword_spend_periods(job_id);
            CREATE INDEX IF NOT EXISTS idx_ksp_campaign
                ON keyword_spend_periods(job_id, campaign_id, period_label);
            CREATE INDEX IF NOT EXISTS idx_ksp_keyword
                ON keyword_spend_periods(job_id, keyword_text, period_label);
            CREATE INDEX IF NOT EXISTS idx_ksp_cost
                ON keyword_spend_periods(job_id, period_label, cost_micros DESC);

            -- Aggregated comparison results (one row per keyword, both periods)
            CREATE TABLE IF NOT EXISTS keyword_spend_comparisons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                country_code TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                campaign_name TEXT,
                ad_group_id TEXT,
                ad_group_name TEXT,
                criterion_id TEXT NOT NULL,
                keyword_text TEXT NOT NULL,
                match_type TEXT,
                keyword_status TEXT,
                quality_score INTEGER,
                curr_impressions INTEGER DEFAULT 0,
                curr_clicks INTEGER DEFAULT 0,
                curr_conversions REAL DEFAULT 0,
                curr_conversions_value REAL DEFAULT 0,
                curr_cost_micros INTEGER DEFAULT 0,
                curr_ctr REAL DEFAULT 0,
                curr_cpc_micros REAL DEFAULT 0,
                curr_conv_rate REAL DEFAULT 0,
                curr_cpa_micros REAL DEFAULT 0,
                curr_roas REAL DEFAULT 0,
                prev_impressions INTEGER DEFAULT 0,
                prev_clicks INTEGER DEFAULT 0,
                prev_conversions REAL DEFAULT 0,
                prev_conversions_value REAL DEFAULT 0,
                prev_cost_micros INTEGER DEFAULT 0,
                prev_ctr REAL DEFAULT 0,
                prev_cpc_micros REAL DEFAULT 0,
                prev_conv_rate REAL DEFAULT 0,
                prev_cpa_micros REAL DEFAULT 0,
                prev_roas REAL DEFAULT 0,
                delta_impressions INTEGER DEFAULT 0,
                delta_clicks INTEGER DEFAULT 0,
                delta_conversions REAL DEFAULT 0,
                delta_conversions_value REAL DEFAULT 0,
                delta_cost_micros INTEGER DEFAULT 0,
                pct_impressions REAL,
                pct_clicks REAL,
                pct_conversions REAL,
                pct_conversions_value REAL,
                pct_cost_micros REAL,
                pct_ctr REAL,
                pct_cpc REAL,
                pct_conv_rate REAL,
                pct_cpa REAL,
                pct_roas REAL,
                trend_flag TEXT,
                severity TEXT,
                category TEXT,
                computed_at TEXT DEFAULT (datetime('now')),
                UNIQUE(job_id, criterion_id, campaign_id)
            );

            CREATE INDEX IF NOT EXISTS idx_ksc_job
                ON keyword_spend_comparisons(job_id);
            CREATE INDEX IF NOT EXISTS idx_ksc_trend
                ON keyword_spend_comparisons(job_id, trend_flag);
            CREATE INDEX IF NOT EXISTS idx_ksc_severity
                ON keyword_spend_comparisons(job_id, severity);
            CREATE INDEX IF NOT EXISTS idx_ksc_cost_delta
                ON keyword_spend_comparisons(job_id, delta_cost_micros DESC);
            CREATE INDEX IF NOT EXISTS idx_ksc_pct_cost
                ON keyword_spend_comparisons(job_id, pct_cost_micros DESC);

            -- Job metadata & final results
            CREATE TABLE IF NOT EXISTS keyword_spend_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL UNIQUE,
                country_code TEXT NOT NULL,
                campaign_ids TEXT NOT NULL,
                current_period_start TEXT NOT NULL,
                current_period_end TEXT NOT NULL,
                previous_period_start TEXT NOT NULL,
                previous_period_end TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                phase TEXT DEFAULT 'init',
                started_at TEXT,
                completed_at TEXT,
                total_campaigns INTEGER DEFAULT 0,
                processed_campaigns INTEGER DEFAULT 0,
                total_keywords_fetched INTEGER DEFAULT 0,
                total_gaql_calls INTEGER DEFAULT 0,
                resume_checkpoint TEXT,
                summary_json TEXT,
                error_message TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_ksj_status
                ON keyword_spend_jobs(status);
            CREATE INDEX IF NOT EXISTS idx_ksj_country
                ON keyword_spend_jobs(country_code);

            -- Geo target constant name cache (for location analysis)
            CREATE TABLE IF NOT EXISTS geo_target_names (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                canonical_name TEXT,
                country_code TEXT,
                target_type TEXT,
                parent_id INTEGER,
                cached_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_geo_target_country
                ON geo_target_names(country_code);

            -- ═══════════════════════════════════════════════════════════════
            -- Negative Keywords Cache (shared lists, account-level, campaign)
            -- Stores ALL negative keywords for offline analysis and conflict detection
            -- ═══════════════════════════════════════════════════════════════

            CREATE TABLE IF NOT EXISTS negative_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                criterion_id TEXT NOT NULL,
                keyword_text TEXT NOT NULL,
                match_type TEXT NOT NULL,          -- EXACT, PHRASE, BROAD (or numeric 2,3,4)
                source_type TEXT NOT NULL,          -- 'shared', 'account', 'campaign'
                source_name TEXT,                   -- e.g. 'TrueClicks Negatives', 'account-level negative keywords list'
                shared_set_id TEXT,                 -- shared set ID (NULL for campaign-level)
                campaign_id TEXT,                   -- campaign ID (NULL for shared/account)
                is_active INTEGER DEFAULT 1,        -- 1 = ENABLED shared set, 0 = REMOVED
                synced_at TEXT DEFAULT (datetime('now')),
                UNIQUE(criterion_id, shared_set_id, campaign_id)
            );

            CREATE INDEX IF NOT EXISTS idx_neg_kw_text ON negative_keywords(keyword_text);
            CREATE INDEX IF NOT EXISTS idx_neg_kw_source ON negative_keywords(source_type);
            CREATE INDEX IF NOT EXISTS idx_neg_kw_shared_set ON negative_keywords(shared_set_id);
            CREATE INDEX IF NOT EXISTS idx_neg_kw_active ON negative_keywords(is_active);

            -- PageSpeed Insights scan results
            CREATE TABLE IF NOT EXISTS pagespeed_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id TEXT NOT NULL,
                url TEXT NOT NULL,
                domain TEXT NOT NULL,
                country_code TEXT,
                strategy TEXT NOT NULL DEFAULT 'mobile',
                score_performance REAL,
                score_accessibility REAL,
                score_seo REAL,
                score_best_practices REAL,
                fcp_ms REAL,
                lcp_ms REAL,
                tbt_ms REAL,
                cls REAL,
                si_ms REAL,
                tti_ms REAL,
                crux_lcp_ms REAL,
                crux_inp_ms REAL,
                crux_cls REAL,
                crux_fcp_ms REAL,
                crux_ttfb_ms REAL,
                crux_category TEXT,
                audits_json TEXT,
                opportunities_json TEXT,
                diagnostics_json TEXT,
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                api_response_time_ms INTEGER,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_ps_scan_id ON pagespeed_scans(scan_id);
            CREATE INDEX IF NOT EXISTS idx_ps_domain ON pagespeed_scans(domain);
            CREATE INDEX IF NOT EXISTS idx_ps_scanned_at ON pagespeed_scans(scanned_at);

            -- PageSpeed detected issues mapped to fixes
            CREATE TABLE IF NOT EXISTS pagespeed_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                audit_id TEXT NOT NULL,
                audit_title TEXT,
                score REAL,
                savings_ms REAL,
                savings_bytes INTEGER,
                severity TEXT,
                fix_id TEXT,
                fix_applicable BOOLEAN,
                fix_description TEXT,
                items_json TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_pi_scan_id ON pagespeed_issues(scan_id);
            CREATE INDEX IF NOT EXISTS idx_pi_fix_id ON pagespeed_issues(fix_id);

            -- PageSpeed fix application log
            CREATE TABLE IF NOT EXISTS pagespeed_fixes_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fix_id TEXT NOT NULL,
                store_domain TEXT NOT NULL,
                template_type TEXT,
                file_path TEXT NOT NULL,
                action TEXT,
                diff_text TEXT,
                before_score REAL,
                after_score REAL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_pfl_fix_id ON pagespeed_fixes_log(fix_id);
            CREATE INDEX IF NOT EXISTS idx_pfl_store ON pagespeed_fixes_log(store_domain);
        """)
        conn.commit()

        # --- Schema migrations (add missing columns to existing tables) ---
        self._migrate_tables(conn)

    def _migrate_tables(self, conn):
        """Add columns that may be missing from older DB versions."""
        migrations = [
            ("keyword_intelligence", "conversion_value", "REAL DEFAULT 0"),
            ("keyword_intelligence", "source_campaign_name", "TEXT"),
            ("keyword_intelligence", "ctr", "REAL"),
            ("keyword_intelligence", "conv_rate", "REAL"),
            ("keyword_intelligence", "cpc_micros", "REAL"),
            ("keyword_intelligence", "roas", "REAL DEFAULT 0"),
            ("keyword_intelligence", "roi_percent", "REAL"),
        ]
        for table, column, col_type in migrations:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                conn.commit()
            except Exception:
                pass  # Column already exists

    # ------------------------------------------------------------------
    # Changelog — persistent change tracking across sessions
    # ------------------------------------------------------------------

    def add_changelog_entry(
        self,
        category: str,
        title: str,
        description: str = "",
        details: str = "",
        country_code: str = None,
        campaign_id: str = None,
        severity: str = "info",
        auto_logged: bool = False,
    ) -> int:
        """
        Add a changelog entry.
        Categories: code_fix, feature, config, rollout, bugfix, optimization, manual
        Severity: info, warning, critical
        Returns the new entry ID.
        """
        from datetime import datetime
        conn = self._get_conn()
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.execute("""
            INSERT INTO changelog (created_at, category, title, description, details,
                                   country_code, campaign_id, severity, auto_logged)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (now, category, title, description, details,
              country_code, campaign_id, severity, int(auto_logged)))
        conn.commit()
        return cur.lastrowid

    def get_changelog(
        self,
        limit: int = 50,
        category: str = None,
        country_code: str = None,
        since: str = None,
    ) -> List[Dict]:
        """
        Get changelog entries, newest first.
        Optional filters: category, country_code, since (ISO date string).
        """
        conn = self._get_conn()
        query = "SELECT * FROM changelog WHERE 1=1"
        params = []
        if category:
            query += " AND category = ?"
            params.append(category)
        if country_code:
            query += " AND (country_code = ? OR country_code IS NULL)"
            params.append(country_code)
        if since:
            query += " AND created_at >= ?"
            params.append(since)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_changelog_summary(self) -> Dict:
        """Quick summary: total entries, by category, last entry date."""
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM changelog").fetchone()[0]
        cats = conn.execute(
            "SELECT category, COUNT(*) as cnt FROM changelog GROUP BY category ORDER BY cnt DESC"
        ).fetchall()
        last = conn.execute(
            "SELECT created_at FROM changelog ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        return {
            "total_entries": total,
            "by_category": {r["category"]: r["cnt"] for r in cats},
            "last_entry": last["created_at"] if last else None,
        }

    # ------------------------------------------------------------------
    # MCP-side response caches — avoid HTTP roundtrips on re-runs
    # ------------------------------------------------------------------

    def get_ai_copy_cache(self, handle: str, country_code: str, payload_hash: str) -> Optional[Dict]:
        """
        Get cached AI copy response. Returns parsed JSON dict or None if miss/expired.
        TTL: 7 days (604800s) — matches PHP-side GPT cache TTL.
        """
        conn = self._get_conn()
        row = conn.execute("""
            SELECT response_json, cached_at, ttl_seconds FROM ai_copy_cache
            WHERE handle = ? AND country_code = ? AND payload_hash = ?
              AND datetime(cached_at, '+' || ttl_seconds || ' seconds') > datetime('now')
            LIMIT 1
        """, (handle, country_code, payload_hash)).fetchone()
        if row:
            try:
                return json.loads(row["response_json"])
            except Exception:
                return None
        return None

    def set_ai_copy_cache(self, handle: str, country_code: str, payload_hash: str,
                          response_data: Dict, campaign_id: str = None, ttl: int = 604800):
        """Store AI copy response in cache. TTL default = 7 days."""
        conn = self._get_conn()
        from datetime import datetime
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""
            INSERT OR REPLACE INTO ai_copy_cache
                (handle, country_code, campaign_id, payload_hash, response_json, cached_at, ttl_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (handle, country_code, campaign_id, payload_hash,
              json.dumps(response_data, ensure_ascii=False), now, ttl))
        conn.commit()

    def get_image_candidates_cache(self, handle: str) -> Optional[list]:
        """
        Get cached image candidates. Returns list of image dicts or None if miss/expired.
        TTL: 24h (86400s) — images change rarely but faster than AI copy.
        """
        conn = self._get_conn()
        row = conn.execute("""
            SELECT candidates_json, cached_at, ttl_seconds FROM image_candidates_cache
            WHERE handle = ?
              AND datetime(cached_at, '+' || ttl_seconds || ' seconds') > datetime('now')
            LIMIT 1
        """, (handle,)).fetchone()
        if row:
            try:
                return json.loads(row["candidates_json"])
            except Exception:
                return None
        return None

    def set_image_candidates_cache(self, handle: str, candidates: list, ttl: int = 86400):
        """Store image candidates in cache. TTL default = 24h."""
        conn = self._get_conn()
        from datetime import datetime
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""
            INSERT OR REPLACE INTO image_candidates_cache
                (handle, candidates_json, candidate_count, cached_at, ttl_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, (handle, json.dumps(candidates, ensure_ascii=False), len(candidates), now, ttl))
        conn.commit()

    def clear_response_caches(self, cache_type: str = "all", country_code: str = None):
        """
        Clear MCP-side response caches.
        cache_type: 'ai_copy', 'images', or 'all'
        country_code: optional filter for ai_copy cache
        """
        conn = self._get_conn()
        if cache_type in ("ai_copy", "all"):
            if country_code:
                conn.execute("DELETE FROM ai_copy_cache WHERE country_code = ?", (country_code,))
            else:
                conn.execute("DELETE FROM ai_copy_cache")
        if cache_type in ("images", "all"):
            conn.execute("DELETE FROM image_candidates_cache")
        conn.commit()

    def get_response_cache_stats(self) -> Dict:
        """Stats on MCP-side response caches."""
        conn = self._get_conn()
        ai_total = conn.execute("SELECT COUNT(*) FROM ai_copy_cache").fetchone()[0]
        ai_valid = conn.execute(
            "SELECT COUNT(*) FROM ai_copy_cache WHERE datetime(cached_at, '+' || ttl_seconds || ' seconds') > datetime('now')"
        ).fetchone()[0]
        img_total = conn.execute("SELECT COUNT(*) FROM image_candidates_cache").fetchone()[0]
        img_valid = conn.execute(
            "SELECT COUNT(*) FROM image_candidates_cache WHERE datetime(cached_at, '+' || ttl_seconds || ' seconds') > datetime('now')"
        ).fetchone()[0]
        return {
            "ai_copy_cache": {"total": ai_total, "valid": ai_valid, "expired": ai_total - ai_valid},
            "image_candidates_cache": {"total": img_total, "valid": img_valid, "expired": img_total - img_valid},
        }

    # ------------------------------------------------------------------
    # Queue — smart product prioritization for agent processing
    # ------------------------------------------------------------------

    def queue_next(
        self,
        country_code: str,
        campaign_id: str,
        count: int = 5,
        status_filter: str = None,
        include_errors: bool = False,
    ) -> List[Dict]:
        """
        Get next N products to process, prioritized by:
        1. 'partial' (have ad group, missing assets — fast, fits in 60s)
        2. 'error' (if include_errors=True — retry failed products)
        3. 'pending' (new products — slow, may timeout)

        Returns list of {handle, status, missing, ad_group_id, priority}.
        """
        conn = self._get_conn()

        if status_filter:
            statuses = [status_filter]
        elif include_errors:
            statuses = ["partial", "error", "pending"]
        else:
            statuses = ["partial", "pending"]

        placeholders = ",".join("?" for _ in statuses)
        rows = conn.execute(f"""
            SELECT product_handle, status, ad_group_id, ad_group_name,
                   has_rsa_a, has_rsa_b, has_rsa_c,
                   has_callouts, has_sitelinks, has_promotion,
                   has_snippets, has_keywords, has_images, image_count,
                   last_error, last_updated_at
            FROM product_setup
            WHERE country_code = ? AND campaign_id = ?
              AND status IN ({placeholders})
            ORDER BY
                CASE status
                    WHEN 'partial' THEN 1
                    WHEN 'error' THEN 2
                    WHEN 'pending' THEN 3
                    ELSE 4
                END,
                last_updated_at ASC
            LIMIT ?
        """, [country_code, campaign_id] + statuses + [count]).fetchall()

        results = []
        for r in rows:
            row = dict(r)
            missing = []
            if not row.get("has_rsa_a"):
                missing.append("rsa")
            if not row.get("has_callouts"):
                missing.append("callouts")
            if not row.get("has_sitelinks"):
                missing.append("sitelinks")
            if not row.get("has_promotion"):
                missing.append("promotion")
            if not row.get("has_snippets"):
                missing.append("snippets")
            if not row.get("has_keywords"):
                missing.append("keywords")
            if not row.get("has_images"):
                missing.append("images")
            results.append({
                "handle": row["product_handle"],
                "status": row["status"],
                "ad_group_id": row.get("ad_group_id"),
                "missing": missing,
                "last_error": row.get("last_error"),
                "priority": "fast" if row["status"] == "partial" else "slow",
            })
        return results

    def queue_stats(self, country_code: str, campaign_id: str) -> Dict:
        """
        Quick queue summary: how many products in each status.
        """
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT status, COUNT(*) as cnt
            FROM product_setup
            WHERE country_code = ? AND campaign_id = ?
            GROUP BY status
            ORDER BY cnt DESC
        """, (country_code, campaign_id)).fetchall()
        by_status = {r["status"]: r["cnt"] for r in rows}
        total = sum(by_status.values())
        actionable = by_status.get("partial", 0) + by_status.get("pending", 0) + by_status.get("error", 0)
        return {
            "total": total,
            "by_status": by_status,
            "actionable": actionable,
            "complete": by_status.get("complete", 0) + by_status.get("complete_no_images", 0),
            "next_action": (
                f"{by_status.get('partial', 0)} partial (fast)" if by_status.get("partial", 0)
                else f"{by_status.get('pending', 0)} pending (slow)" if by_status.get("pending", 0)
                else "All done!"
            ),
        }

    # ------------------------------------------------------------------
    # Product Dashboard — cached summary per product
    # ------------------------------------------------------------------

    def product_dashboard(
        self,
        country_code: str,
        campaign_id: str,
        status_filter: str = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict:
        """
        Cached product dashboard. Returns compact table of all products
        with asset completeness and status.
        """
        conn = self._get_conn()

        query = """
            SELECT product_handle, status, ad_group_id, ad_group_name,
                   has_rsa_a, has_rsa_b, has_rsa_c,
                   has_callouts, has_sitelinks, has_promotion,
                   has_snippets, has_keywords, has_images, image_count,
                   last_error, last_updated_at
            FROM product_setup
            WHERE country_code = ? AND campaign_id = ?
        """
        params = [country_code, campaign_id]

        if status_filter:
            query += " AND status = ?"
            params.append(status_filter)

        query += """
            ORDER BY
                CASE status
                    WHEN 'error' THEN 1
                    WHEN 'partial' THEN 2
                    WHEN 'pending' THEN 3
                    WHEN 'complete_no_images' THEN 4
                    WHEN 'complete' THEN 5
                    ELSE 6
                END,
                product_handle ASC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()

        # Count totals
        total_row = conn.execute("""
            SELECT COUNT(*) as total FROM product_setup
            WHERE country_code = ? AND campaign_id = ?
        """ + (" AND status = ?" if status_filter else ""),
            [country_code, campaign_id] + ([status_filter] if status_filter else [])
        ).fetchone()

        products = []
        for r in rows:
            row = dict(r)
            rsa_count = (row.get("has_rsa_a") or 0) + (row.get("has_rsa_b") or 0) + (row.get("has_rsa_c") or 0)
            products.append({
                "handle": row["product_handle"],
                "status": row["status"],
                "rsa": f"{rsa_count}/3",
                "callouts": "Y" if row.get("has_callouts") else "-",
                "sitelinks": "Y" if row.get("has_sitelinks") else "-",
                "promo": "Y" if row.get("has_promotion") else "-",
                "snippets": "Y" if row.get("has_snippets") else "-",
                "keywords": "Y" if row.get("has_keywords") else "-",
                "images": str(row.get("image_count") or 0),
                "error": (row.get("last_error") or "")[:80],
            })

        return {
            "total": total_row["total"],
            "showing": len(products),
            "offset": offset,
            "products": products,
        }

    # ------------------------------------------------------------------
    # Handle-to-ad-group URL map (persistent cache)
    # ------------------------------------------------------------------

    def save_url_map(
        self,
        country_code: str,
        campaign_id: str,
        url_map: Dict[str, Dict],
    ) -> int:
        """
        Persist URL-based handle→ad_group mapping to DB.
        url_map: {handle: {id, name, status, url}} from _get_ad_group_url_map()
        Returns number of rows upserted.
        """
        conn = self._get_conn()
        count = 0
        for handle, ag_info in url_map.items():
            conn.execute("""
                INSERT INTO handle_ag_map (country_code, campaign_id, product_handle,
                                           ad_group_id, ad_group_name, match_source, sample_url, cached_at)
                VALUES (?, ?, ?, ?, ?, 'url', ?, datetime('now'))
                ON CONFLICT(country_code, campaign_id, product_handle)
                DO UPDATE SET
                    ad_group_id = excluded.ad_group_id,
                    ad_group_name = excluded.ad_group_name,
                    sample_url = excluded.sample_url,
                    cached_at = excluded.cached_at
            """, (
                country_code, campaign_id, handle,
                ag_info["id"], ag_info.get("name", ""),
                ag_info.get("url", ""),
            ))
            count += 1
        conn.commit()
        return count

    def get_cached_url_map(
        self,
        country_code: str,
        campaign_id: str,
    ) -> Dict[str, Dict]:
        """
        Get cached URL-based handle→ad_group mapping from DB.
        Returns {handle: {id, name, match_source, url}} or empty dict.
        """
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT product_handle, ad_group_id, ad_group_name, match_source, sample_url
            FROM handle_ag_map
            WHERE country_code = ? AND campaign_id = ?
        """, (country_code, campaign_id)).fetchall()

        result = {}
        for row in rows:
            result[row[0]] = {
                "id": row[1],
                "name": row[2],
                "match_source": row[3],
                "url": row[4],
            }
        return result

    def get_ag_id_for_handle(
        self,
        country_code: str,
        campaign_id: str,
        handle: str,
    ) -> Optional[Dict]:
        """
        Quick lookup: get ad_group info for a specific handle from cached URL map.
        Returns {id, name, match_source} or None.
        """
        conn = self._get_conn()
        row = conn.execute("""
            SELECT ad_group_id, ad_group_name, match_source
            FROM handle_ag_map
            WHERE country_code = ? AND campaign_id = ? AND product_handle = ?
        """, (country_code, campaign_id, handle)).fetchone()

        if row:
            return {"id": row[0], "name": row[1], "match_source": row[2]}
        return None

    # ------------------------------------------------------------------
    # Product setup tracking
    # ------------------------------------------------------------------

    def upsert_product(self, country_code: str, campaign_id: str, handle: str, **kwargs) -> int:
        """
        Insert or update a product setup record.
        kwargs can include: ad_group_id, ad_group_name, has_rsa_a, has_rsa_b, has_rsa_c,
        has_callouts, has_sitelinks, has_promotion, has_snippets, has_keywords,
        has_images, image_count, product_name, product_price, product_url,
        xml_feed_source, last_error, status
        """
        conn = self._get_conn()

        # Check if exists
        existing = conn.execute(
            "SELECT id FROM product_setup WHERE country_code=? AND campaign_id=? AND product_handle=?",
            (country_code, campaign_id, handle)
        ).fetchone()

        if existing:
            # Update only provided fields
            if kwargs:
                sets = []
                vals = []
                for k, v in kwargs.items():
                    sets.append(f"{k}=?")
                    vals.append(v)
                sets.append("last_updated_at=datetime('now')")
                vals.extend([country_code, campaign_id, handle])
                conn.execute(
                    f"UPDATE product_setup SET {', '.join(sets)} "
                    f"WHERE country_code=? AND campaign_id=? AND product_handle=?",
                    vals
                )
                conn.commit()
            return existing["id"]
        else:
            # Insert new
            cols = ["country_code", "campaign_id", "product_handle"]
            vals = [country_code, campaign_id, handle]
            for k, v in kwargs.items():
                cols.append(k)
                vals.append(v)
            placeholders = ", ".join(["?"] * len(vals))
            col_names = ", ".join(cols)
            cursor = conn.execute(
                f"INSERT INTO product_setup ({col_names}) VALUES ({placeholders})",
                vals
            )
            conn.commit()
            return cursor.lastrowid

    def get_product(self, country_code: str, campaign_id: str, handle: str) -> Optional[Dict[str, Any]]:
        """Get a single product record."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM product_setup WHERE country_code=? AND campaign_id=? AND product_handle=?",
            (country_code, campaign_id, handle)
        ).fetchone()
        return dict(row) if row else None

    def get_all_products(self, country_code: str, campaign_id: str) -> List[Dict[str, Any]]:
        """Get all product records for a campaign."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM product_setup WHERE country_code=? AND campaign_id=? ORDER BY product_handle",
            (country_code, campaign_id)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_incomplete_products(self, country_code: str, campaign_id: str, include_no_images: bool = False) -> List[Dict[str, Any]]:
        """Get products that don't have complete status.
        If include_no_images=False (default), also excludes 'complete_no_images'.
        """
        conn = self._get_conn()
        if include_no_images:
            rows = conn.execute(
                "SELECT * FROM product_setup WHERE country_code=? AND campaign_id=? AND status NOT IN ('complete', 'complete_no_images') ORDER BY product_handle",
                (country_code, campaign_id)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM product_setup WHERE country_code=? AND campaign_id=? AND status != 'complete' ORDER BY product_handle",
                (country_code, campaign_id)
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_complete(self, country_code: str, campaign_id: str, handle: str):
        """Mark a product as fully complete."""
        self.upsert_product(country_code, campaign_id, handle, status="complete")

    def update_product_flags(self, country_code: str, campaign_id: str, handle: str, **flags):
        """
        Update specific has_* flags and auto-calculate status.
        Example: update_product_flags('RO', '123', 'abc-ro', has_rsa_a=1, has_callouts=1)
        """
        self.upsert_product(country_code, campaign_id, handle, **flags)

        # Auto-determine status based on flags — SOFT COMPLETENESS (v3.2.0)
        # Core flags: RSAs + sitelinks + promotion + snippets (NOT callouts — they hit limits)
        product = self.get_product(country_code, campaign_id, handle)
        if product:
            core_flags = [
                product.get("has_rsa_a", 0),
                product.get("has_rsa_b", 0),
                product.get("has_rsa_c", 0),
                product.get("has_sitelinks", 0),
                product.get("has_promotion", 0),
                product.get("has_snippets", 0),
            ]
            has_images = product.get("has_images", 0)
            if all(f == 1 for f in core_flags) and has_images:
                new_status = "complete"
            elif all(f == 1 for f in core_flags) and not has_images:
                new_status = "complete_no_images"
            elif any(f == 1 for f in core_flags):
                new_status = "partial"
            else:
                new_status = "pending"

            if new_status != product.get("status"):
                self.upsert_product(country_code, campaign_id, handle, status=new_status)

    def delete_campaign_products(self, country_code: str, campaign_id: str) -> int:
        """Delete ALL products for a country/campaign. Use before clean re-sync."""
        conn = self._get_conn()
        cursor = conn.execute(
            "DELETE FROM product_setup WHERE country_code = ? AND campaign_id = ?",
            (country_code.upper(), campaign_id),
        )
        conn.commit()
        return cursor.rowcount

    # ------------------------------------------------------------------
    # Operation logging
    # ------------------------------------------------------------------

    def log_operation(
        self,
        country_code: str,
        campaign_id: str,
        handle: str,
        operation: str,
        status: str,
        details: Optional[Any] = None,
        api_calls_count: int = 1,
        duration_ms: Optional[int] = None,
    ):
        """Log an operation (create_rsa, add_callout, add_image, etc.)."""
        conn = self._get_conn()
        details_str = json.dumps(details, default=str) if details is not None else None
        conn.execute(
            """INSERT INTO operation_log
               (country_code, campaign_id, product_handle, operation, status, details, api_calls_count, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (country_code, campaign_id, handle, operation, status, details_str, api_calls_count, duration_ms)
        )
        conn.commit()

    def get_operations(
        self,
        country_code: Optional[str] = None,
        campaign_id: Optional[str] = None,
        handle: Optional[str] = None,
        operation: Optional[str] = None,
        status_filter: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get operation logs with optional filters."""
        conn = self._get_conn()
        conditions = []
        params = []

        if country_code:
            conditions.append("country_code=?")
            params.append(country_code)
        if campaign_id:
            conditions.append("campaign_id=?")
            params.append(campaign_id)
        if handle:
            conditions.append("product_handle=?")
            params.append(handle)
        if operation:
            conditions.append("operation=?")
            params.append(operation)
        if status_filter:
            conditions.append("status=?")
            params.append(status_filter)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        rows = conn.execute(
            f"SELECT * FROM operation_log {where} ORDER BY timestamp DESC LIMIT ?",
            params
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # GAQL Cache
    # ------------------------------------------------------------------

    @staticmethod
    def _query_hash(customer_id: str, query: str) -> str:
        """Create a deterministic hash for a GAQL query."""
        normalized = " ".join(query.split())  # normalize whitespace
        key = f"{customer_id}:{normalized}"
        return hashlib.sha256(key.encode()).hexdigest()[:32]

    def get_cached_gaql(self, customer_id: str, query: str) -> Optional[List[Any]]:
        """
        Get cached GAQL results if available and not expired.
        Returns None if not cached or expired.
        """
        conn = self._get_conn()
        qhash = self._query_hash(customer_id, query)

        row = conn.execute(
            "SELECT result_json, cached_at, ttl_seconds FROM gaql_cache WHERE query_hash=?",
            (qhash,)
        ).fetchone()

        if not row:
            return None

        # Check TTL
        cached_at = datetime.fromisoformat(row["cached_at"])
        ttl = row["ttl_seconds"]
        if datetime.utcnow() - cached_at > timedelta(seconds=ttl):
            # Expired — delete and return None
            conn.execute("DELETE FROM gaql_cache WHERE query_hash=?", (qhash,))
            conn.commit()
            return None

        return json.loads(row["result_json"])

    def set_cached_gaql(self, customer_id: str, query: str, results: List[Any], ttl: int = 300):
        """Cache GAQL query results with TTL."""
        conn = self._get_conn()
        qhash = self._query_hash(customer_id, query)
        normalized = " ".join(query.split())
        result_json = json.dumps(results, default=str)

        conn.execute(
            """INSERT OR REPLACE INTO gaql_cache
               (query_hash, customer_id, query_text, result_json, result_count, cached_at, ttl_seconds)
               VALUES (?, ?, ?, ?, ?, datetime('now'), ?)""",
            (qhash, customer_id, normalized, result_json, len(results), ttl)
        )
        conn.commit()

    def clear_gaql_cache(self, older_than_seconds: Optional[int] = None):
        """Clear GAQL cache. If older_than_seconds provided, only clear expired entries."""
        conn = self._get_conn()
        if older_than_seconds:
            conn.execute(
                "DELETE FROM gaql_cache WHERE datetime(cached_at, '+' || ttl_seconds || ' seconds') < datetime('now')"
            )
        else:
            conn.execute("DELETE FROM gaql_cache")
        conn.commit()

    # ------------------------------------------------------------------
    # XML Feed Cache
    # ------------------------------------------------------------------

    def get_cached_feed(self, country_code: str, url: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached XML feed products if available and not expired."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT products_json, cached_at, ttl_seconds FROM xml_feed_cache WHERE country_code=? AND feed_url=?",
            (country_code, url)
        ).fetchone()

        if not row:
            return None

        cached_at = datetime.fromisoformat(row["cached_at"])
        ttl = row["ttl_seconds"]
        if datetime.utcnow() - cached_at > timedelta(seconds=ttl):
            conn.execute(
                "DELETE FROM xml_feed_cache WHERE country_code=? AND feed_url=?",
                (country_code, url)
            )
            conn.commit()
            return None

        return json.loads(row["products_json"])

    def set_cached_feed(self, country_code: str, url: str, products: List[Dict[str, Any]]):
        """Cache parsed XML feed products (1h TTL)."""
        conn = self._get_conn()
        products_json = json.dumps(products, default=str)
        feed_hash = hashlib.md5(products_json.encode()).hexdigest()

        conn.execute(
            """INSERT OR REPLACE INTO xml_feed_cache
               (country_code, feed_url, feed_hash, products_json, product_count, cached_at, ttl_seconds)
               VALUES (?, ?, ?, ?, ?, datetime('now'), 3600)""",
            (country_code, url, feed_hash, products_json, len(products))
        )
        conn.commit()

    def clear_feed_cache(self, country_code: Optional[str] = None):
        """Clear XML feed cache, optionally for a specific country."""
        conn = self._get_conn()
        if country_code:
            conn.execute("DELETE FROM xml_feed_cache WHERE country_code=?", (country_code,))
        else:
            conn.execute("DELETE FROM xml_feed_cache")
        conn.commit()

    def clear_url_map(self, country_code: Optional[str] = None, campaign_id: Optional[str] = None):
        """Clear handle→ad_group URL map cache. Removes stale REMOVED ad group mappings."""
        conn = self._get_conn()
        if country_code and campaign_id:
            conn.execute("DELETE FROM handle_ag_map WHERE country_code=? AND campaign_id=?",
                         (country_code, campaign_id))
        elif country_code:
            conn.execute("DELETE FROM handle_ag_map WHERE country_code=?", (country_code,))
        else:
            conn.execute("DELETE FROM handle_ag_map")
        conn.commit()

    # ------------------------------------------------------------------
    # Stats & Reports
    # ------------------------------------------------------------------

    def get_setup_summary(self, country_code: str, campaign_id: str) -> Dict[str, Any]:
        """Get aggregated setup status for a campaign."""
        conn = self._get_conn()

        total = conn.execute(
            "SELECT COUNT(*) as c FROM product_setup WHERE country_code=? AND campaign_id=?",
            (country_code, campaign_id)
        ).fetchone()["c"]

        by_status = conn.execute(
            "SELECT status, COUNT(*) as c FROM product_setup WHERE country_code=? AND campaign_id=? GROUP BY status",
            (country_code, campaign_id)
        ).fetchall()

        asset_sums = conn.execute(
            """SELECT
                SUM(has_rsa_a) as rsa_a, SUM(has_rsa_b) as rsa_b, SUM(has_rsa_c) as rsa_c,
                SUM(has_callouts) as callouts, SUM(has_sitelinks) as sitelinks,
                SUM(has_promotion) as promotions, SUM(has_snippets) as snippets,
                SUM(has_keywords) as keywords, SUM(has_images) as images,
                SUM(image_count) as total_images
            FROM product_setup WHERE country_code=? AND campaign_id=?""",
            (country_code, campaign_id)
        ).fetchone()

        # Recent operations count
        ops_24h = conn.execute(
            """SELECT COUNT(*) as c FROM operation_log
               WHERE country_code=? AND campaign_id=?
               AND timestamp > datetime('now', '-1 day')""",
            (country_code, campaign_id)
        ).fetchone()["c"]

        errors_24h = conn.execute(
            """SELECT COUNT(*) as c FROM operation_log
               WHERE country_code=? AND campaign_id=? AND status='error'
               AND timestamp > datetime('now', '-1 day')""",
            (country_code, campaign_id)
        ).fetchone()["c"]

        return {
            "country_code": country_code,
            "campaign_id": campaign_id,
            "total_products": total,
            "by_status": {row["status"]: row["c"] for row in by_status},
            "asset_totals": dict(asset_sums) if asset_sums else {},
            "operations_24h": ops_24h,
            "errors_24h": errors_24h,
        }

    def get_missing_assets_report(self, country_code: str, campaign_id: str) -> List[Dict[str, Any]]:
        """Get products with specific missing assets."""
        conn = self._get_conn()
        rows = conn.execute(
            """SELECT product_handle, ad_group_id, ad_group_name, status,
                      has_rsa_a, has_rsa_b, has_rsa_c, has_callouts,
                      has_sitelinks, has_promotion, has_snippets, has_keywords,
                      has_images, image_count, last_error
               FROM product_setup
               WHERE country_code=? AND campaign_id=? AND status != 'complete'
               ORDER BY status, product_handle""",
            (country_code, campaign_id)
        ).fetchall()

        report = []
        for r in rows:
            row = dict(r)
            missing = []
            if not row["has_rsa_a"]:
                missing.append("RSA_A")
            if not row["has_rsa_b"]:
                missing.append("RSA_B")
            if not row["has_rsa_c"]:
                missing.append("RSA_C")
            if not row["has_callouts"]:
                missing.append("CALLOUTS")
            if not row["has_sitelinks"]:
                missing.append("SITELINKS")
            if not row["has_promotion"]:
                missing.append("PROMOTION")
            if not row["has_snippets"]:
                missing.append("SNIPPETS")
            if not row["has_keywords"]:
                missing.append("KEYWORDS")
            if not row["has_images"]:
                missing.append("IMAGES")
            row["missing"] = missing
            report.append(row)

        return report

    # ------------------------------------------------------------------
    # Keyword Intelligence
    # ------------------------------------------------------------------

    def upsert_keyword_intel(self, product_name: str, country_code: str,
                             keyword_text: str, match_type: str,
                             source_campaign_id: str, **kwargs):
        """Insert or update a keyword intelligence record."""
        conn = self._get_conn()
        existing = conn.execute(
            """SELECT id FROM keyword_intelligence
               WHERE product_name=? AND country_code=? AND keyword_text=?
               AND match_type=? AND source_campaign_id=?""",
            (product_name, country_code, keyword_text, match_type, source_campaign_id)
        ).fetchone()

        if existing:
            if kwargs:
                sets = [f"{k}=?" for k in kwargs]
                vals = list(kwargs.values())
                sets.append("cached_at=datetime('now')")
                vals.extend([product_name, country_code, keyword_text, match_type, source_campaign_id])
                conn.execute(
                    f"UPDATE keyword_intelligence SET {', '.join(sets)} "
                    f"WHERE product_name=? AND country_code=? AND keyword_text=? AND match_type=? AND source_campaign_id=?",
                    vals
                )
        else:
            cols = ["product_name", "country_code", "keyword_text", "match_type", "source_campaign_id"]
            vals = [product_name, country_code, keyword_text, match_type, source_campaign_id]
            for k, v in kwargs.items():
                cols.append(k)
                vals.append(v)
            placeholders = ", ".join(["?"] * len(vals))
            conn.execute(
                f"INSERT INTO keyword_intelligence ({', '.join(cols)}) VALUES ({placeholders})", vals
            )
        conn.commit()

    def bulk_upsert_keyword_intel(self, records: List[Dict[str, Any]]):
        """Bulk insert/update keyword intelligence records."""
        for rec in records:
            self.upsert_keyword_intel(
                product_name=rec["product_name"],
                country_code=rec.get("country_code", ""),
                keyword_text=rec["keyword_text"],
                match_type=rec.get("match_type", "EXACT"),
                source_campaign_id=rec.get("source_campaign_id", ""),
                **{k: v for k, v in rec.items()
                   if k not in ("product_name", "country_code", "keyword_text",
                                "match_type", "source_campaign_id")}
            )

    def get_keyword_intel(self, product_name: str, country_code: Optional[str] = None,
                          min_roi: float = 0.0, min_conversions: float = 1.0,
                          limit: int = 50, cross_country: bool = False) -> List[Dict[str, Any]]:
        """Get keyword intelligence for a product.

        Args:
            product_name: Normalized product name (e.g. 'premium-widget')
            country_code: Target country code. Ignored when cross_country=True.
            min_roi: Minimum ROI filter. Default 0 (disabled) because many
                     countries lack conversion_value data → negative ROI.
            min_conversions: Minimum conversions threshold.
            limit: Max results.
            cross_country: When True, skip country_code filter to aggregate
                          keyword data from ALL countries for this product.
        """
        conn = self._get_conn()
        conditions = ["product_name = ?"]
        params: list = [product_name]

        if country_code and not cross_country:
            conditions.append("country_code = ?")
            params.append(country_code)

        conditions.append("conversions >= ?")
        params.append(min_conversions)

        if min_roi > 0:
            conditions.append("(roi_percent IS NULL OR roi_percent >= ?)")
            params.append(min_roi)

        params.append(limit)
        where = " AND ".join(conditions)

        # Sort by ROAS when available (> 0), fallback to conversions
        rows = conn.execute(
            f"""SELECT * FROM keyword_intelligence
                WHERE {where}
                ORDER BY
                    CASE WHEN roas > 0 THEN roas ELSE 0 END DESC,
                    conversions DESC
                LIMIT ?""",
            params
        ).fetchall()
        return [dict(r) for r in rows]

    def get_category_keywords(self, category: str, country_code: str,
                              min_conversions: float = 3.0,
                              limit: int = 30) -> List[Dict[str, Any]]:
        """Get category-level keywords for cross-product sharing."""
        conn = self._get_conn()
        rows = conn.execute(
            """SELECT * FROM category_keywords
               WHERE category=? AND country_code=? AND avg_conversions >= ?
               ORDER BY avg_conversions DESC LIMIT ?""",
            (category, country_code, min_conversions, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def upsert_category_keyword(self, category: str, country_code: str,
                                keyword_text: str, match_type: str = "EXACT",
                                **kwargs):
        """Insert or update a category keyword record."""
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO category_keywords (category, country_code, keyword_text, match_type,
               avg_conversions, avg_ctr, avg_roi, source_products, cached_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(category, country_code, keyword_text, match_type)
               DO UPDATE SET avg_conversions=excluded.avg_conversions,
               avg_ctr=excluded.avg_ctr, avg_roi=excluded.avg_roi,
               source_products=excluded.source_products, cached_at=datetime('now')""",
            (category, country_code, keyword_text, match_type,
             kwargs.get("avg_conversions", 0), kwargs.get("avg_ctr", 0),
             kwargs.get("avg_roi", 0), kwargs.get("source_products", ""))
        )
        conn.commit()

    # ------------------------------------------------------------------
    # Asset Performance
    # ------------------------------------------------------------------

    def upsert_asset_performance(self, country_code: str, campaign_id: str,
                                 asset_type: str, headline_pattern: str,
                                 performance_label: str, **kwargs):
        """Insert or update asset performance record."""
        conn = self._get_conn()
        conn.execute(
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
            (country_code, campaign_id, asset_type, headline_pattern,
             performance_label, kwargs.get("occurrences", 1),
             kwargs.get("avg_impressions", 0), kwargs.get("avg_clicks", 0),
             kwargs.get("avg_conversions", 0))
        )
        conn.commit()

    def get_winning_patterns(self, country_code: str,
                             asset_type: str = "HEADLINE",
                             min_occurrences: int = 2,
                             limit: int = 20) -> List[Dict[str, Any]]:
        """Get top-performing headline/description patterns across campaigns."""
        conn = self._get_conn()
        # asset_type in DB may be raw enum int ("2"=HEADLINE, "3"=DESCRIPTION)
        # or string name. Match both. Same for performance_label: "5"=GOOD, "6"=BEST, "4"=LOW.
        ASSET_TYPE_ALT = {"HEADLINE": "2", "DESCRIPTION": "3"}
        alt = ASSET_TYPE_ALT.get(asset_type, "")
        rows = conn.execute(
            """SELECT headline_pattern,
                      SUM(CASE WHEN performance_label IN ('BEST','6') THEN occurrences ELSE 0 END) as best_count,
                      SUM(CASE WHEN performance_label IN ('GOOD','5') THEN occurrences ELSE 0 END) as good_count,
                      SUM(CASE WHEN performance_label IN ('LOW','4') THEN occurrences ELSE 0 END) as low_count,
                      SUM(occurrences) as total_count,
                      AVG(avg_impressions) as avg_imp,
                      AVG(avg_clicks) as avg_clk,
                      AVG(avg_conversions) as avg_conv
               FROM asset_performance
               WHERE country_code=? AND asset_type IN (?, ?)
               GROUP BY headline_pattern
               HAVING total_count >= ?
               ORDER BY (best_count * 3 + good_count * 2 - low_count) DESC
               LIMIT ?""",
            (country_code, asset_type, alt, min_occurrences, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Bulk sync helper
    # ------------------------------------------------------------------

    def sync_from_audit(
        self,
        country_code: str,
        campaign_id: str,
        ad_groups: Dict[str, Dict],
        assets: Dict[str, Dict],
        feed_handles: Optional[List[str]] = None,
        url_map: Optional[Dict[str, Dict]] = None,
    ) -> Dict[str, Any]:
        """
        Sync DB state from Google Ads API audit data.
        ad_groups: dict from _get_existing_ad_groups() — keyed by lowercase name
        assets: dict from _get_ad_group_assets() — keyed by ad_group_id
        feed_handles: optional list of handles from XML feed
        url_map: optional dict from _get_ad_group_url_map() — keyed by product handle

        Returns sync summary.
        """
        synced = 0
        created = 0
        skipped_paused = 0
        skipped_removed = 0

        # Build reverse map: ad_group_id -> handle (from URL-based matching)
        ag_id_to_handle = {}
        if url_map:
            for handle, ag_info in url_map.items():
                ag_id = ag_info["id"]
                # Multiple handles can map to same ag_id; store all
                if ag_id not in ag_id_to_handle:
                    ag_id_to_handle[ag_id] = []
                ag_id_to_handle[ag_id].append(handle)

        import logging
        _log = logging.getLogger("batch_db")
        _log.info(f"sync_from_audit: {len(ad_groups)} ad groups, {len(assets)} assets entries, "
                   f"{len(ag_id_to_handle)} URL-mapped ag_ids, {len(feed_handles or [])} feed handles")

        for name_lower, ag in ad_groups.items():
            # v3.2.0: Include PAUSED ad groups if they have URL-map match (= batch product)
            # Skip PAUSED without URL-map match to avoid spam from old/unrelated ad groups
            ag_id_tmp = ag["id"]
            status = ag.get("status", "")
            if "REMOVED" in status:
                skipped_removed += 1
                continue
            if "ENABLED" not in status and ag_id_tmp not in ag_id_to_handle:
                skipped_paused += 1
                continue

            ag_id = ag["id"]
            ag_name = ag["name"]

            # PRIMARY: Use URL-based handle if available (most precise)
            handles_for_ag = ag_id_to_handle.get(ag_id, [])

            if not handles_for_ag:
                # FALLBACK: Derive handle from ad group name (legacy method)
                handle = ag_name.lower().replace(" ", "-")
                cc_suffix = f"-{country_code.lower()}"
                if not handle.endswith(cc_suffix):
                    handle = handle + cc_suffix
                handles_for_ag = [handle]

            ag_assets = assets.get(ag_id, {})
            rsa_count = ag_assets.get("RSA", 0)

            flags = {
                "ad_group_id": ag_id,
                "ad_group_name": ag_name,
                "has_rsa_a": 1 if rsa_count >= 1 else 0,
                "has_rsa_b": 1 if rsa_count >= 2 else 0,
                "has_rsa_c": 1 if rsa_count >= 3 else 0,
                "has_callouts": 1 if ag_assets.get("CALLOUT", 0) > 0 else 0,
                "has_sitelinks": 1 if ag_assets.get("SITELINK", 0) > 0 else 0,
                "has_promotion": 1 if ag_assets.get("PROMOTION", 0) > 0 else 0,
                "has_snippets": 1 if ag_assets.get("STRUCTURED_SNIPPET", 0) > 0 else 0,
                "has_images": 1 if ag_assets.get("AD_IMAGE", 0) > 0 else 0,
                "image_count": ag_assets.get("AD_IMAGE", 0),
            }

            # Determine status — SOFT COMPLETENESS (v3.2.0)
            # Core: RSAs + sitelinks + promotion + snippets (NOT callouts)
            core_flags = [flags["has_rsa_a"], flags["has_rsa_b"], flags["has_rsa_c"],
                          flags["has_sitelinks"],
                          flags["has_promotion"], flags["has_snippets"]]
            has_images = flags.get("has_images", 0)
            if all(f == 1 for f in core_flags) and has_images:
                flags["status"] = "complete"
            elif all(f == 1 for f in core_flags) and not has_images:
                flags["status"] = "complete_no_images"
            elif any(f == 1 for f in core_flags):
                flags["status"] = "partial"
            else:
                flags["status"] = "pending"

            # Upsert for each handle mapped to this ad group
            for handle in handles_for_ag:
                existing = self.get_product(country_code, campaign_id, handle)
                if existing:
                    synced += 1
                else:
                    created += 1
                self.upsert_product(country_code, campaign_id, handle, **flags)

        # Also add feed handles that don't have ad groups yet
        if feed_handles:
            for fh in feed_handles:
                existing = self.get_product(country_code, campaign_id, fh)
                if not existing:
                    self.upsert_product(country_code, campaign_id, fh, status="pending")
                    created += 1

        _log.info(f"sync_from_audit result: synced={synced}, created={created}, "
                   f"skipped_removed={skipped_removed}, skipped_paused_no_url={skipped_paused}")

        return {
            "synced_from_api": synced,
            "created_new": created,
            "total": synced + created,
            "_debug": {
                "skipped_removed": skipped_removed,
                "skipped_paused_no_url_map": skipped_paused,
                "url_mapped_ag_ids": len(ag_id_to_handle),
            },
        }

    # ------------------------------------------------------------------
    # Image dedup tracking
    # ------------------------------------------------------------------

    def check_image_uploaded(self, country_code: str, campaign_id: str, ad_group_id: str, image_md5: str) -> bool:
        """Check if an image with this MD5 was already uploaded to this ad group."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT id FROM image_uploads WHERE country_code=? AND campaign_id=? AND ad_group_id=? AND image_md5=?",
            (country_code, campaign_id, ad_group_id, image_md5)
        ).fetchone()
        return row is not None

    def check_image_uploaded_anywhere(self, country_code: str, campaign_id: str, image_md5: str) -> Optional[str]:
        """Check if image MD5 exists in any ad group. Returns ad_group_id or None."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT ad_group_id FROM image_uploads WHERE country_code=? AND campaign_id=? AND image_md5=? LIMIT 1",
            (country_code, campaign_id, image_md5)
        ).fetchone()
        return row["ad_group_id"] if row else None

    def record_image_upload(self, country_code: str, campaign_id: str, ad_group_id: str,
                            product_handle: str, image_md5: str, asset_resource_name: str = None,
                            image_url: str = None, filename: str = None,
                            width: int = None, height: int = None, file_size: int = None):
        """Record a successful image upload for dedup tracking."""
        conn = self._get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO image_uploads
               (country_code, campaign_id, ad_group_id, product_handle, image_md5,
                asset_resource_name, image_url, filename, width, height, file_size)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (country_code, campaign_id, ad_group_id, product_handle, image_md5,
             asset_resource_name, image_url, filename, width, height, file_size)
        )
        conn.commit()

    def get_image_upload_count(self, country_code: str, campaign_id: str) -> int:
        """Get total number of uploaded images for a campaign."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT COUNT(*) as c FROM image_uploads WHERE country_code=? AND campaign_id=?",
            (country_code, campaign_id)
        ).fetchone()
        return row["c"]

    # ------------------------------------------------------------------
    # Rate limit tracking
    # ------------------------------------------------------------------

    def log_rate_limit(self, retry_after: int = 0, operation: str = "", details: str = ""):
        """Log a rate limit hit."""
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO rate_limit_log (retry_after_seconds, operation, details) VALUES (?, ?, ?)",
            (retry_after, operation, details)
        )
        conn.commit()

    def get_recent_rate_limits(self, hours: int = 24) -> List[Dict]:
        """Get rate limit hits in the last N hours."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM rate_limit_log WHERE hit_at > datetime('now', ? || ' hours') ORDER BY hit_at DESC",
            (f"-{hours}",)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_rate_limit_count(self, hours: int = 1) -> int:
        """Count rate limit hits in the last N hours."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT COUNT(*) as c FROM rate_limit_log WHERE hit_at > datetime('now', ? || ' hours')",
            (f"-{hours}",)
        ).fetchone()
        return row["c"]

    # ------------------------------------------------------------------
    # Stale products detection
    # ------------------------------------------------------------------

    def get_stale_products(self, country_code: str, campaign_id: str, active_handles: List[str]) -> List[Dict]:
        """
        Find products in DB that are NOT in the active feed handles list.
        These are candidates for pausing/cleanup.
        """
        conn = self._get_conn()
        all_products = conn.execute(
            "SELECT product_handle, ad_group_id, ad_group_name, status FROM product_setup WHERE country_code=? AND campaign_id=?",
            (country_code, campaign_id)
        ).fetchall()

        active_set = set(active_handles)
        stale = []
        for row in all_products:
            if row["product_handle"] not in active_set and row["ad_group_id"]:
                stale.append(dict(row))
        return stale

    # ------------------------------------------------------------------
    # Dashboard stats
    # ------------------------------------------------------------------

    def get_dashboard_stats(self, country_code: str, campaign_id: str) -> Dict[str, Any]:
        """Get comprehensive dashboard stats for a campaign."""
        conn = self._get_conn()

        summary = self.get_setup_summary(country_code, campaign_id)

        # Top errors in last 24h
        top_errors = conn.execute(
            """SELECT last_error, COUNT(*) as cnt
               FROM product_setup
               WHERE country_code=? AND campaign_id=? AND last_error IS NOT NULL AND last_error != ''
               GROUP BY last_error ORDER BY cnt DESC LIMIT 10""",
            (country_code, campaign_id)
        ).fetchall()

        # Image stats
        image_stats = conn.execute(
            """SELECT
                SUM(CASE WHEN has_images = 1 THEN 1 ELSE 0 END) as with_images,
                SUM(CASE WHEN has_images = 0 THEN 1 ELSE 0 END) as without_images,
                SUM(image_count) as total_images
               FROM product_setup WHERE country_code=? AND campaign_id=?""",
            (country_code, campaign_id)
        ).fetchone()

        # Rate limits in last 24h
        rate_limits = self.get_rate_limit_count(24)

        # Recent operations by type
        ops_by_type = conn.execute(
            """SELECT operation, status, COUNT(*) as cnt
               FROM operation_log
               WHERE country_code=? AND campaign_id=? AND timestamp > datetime('now', '-1 day')
               GROUP BY operation, status""",
            (country_code, campaign_id)
        ).fetchall()

        # Last sync time
        last_sync = conn.execute(
            """SELECT timestamp FROM operation_log
               WHERE country_code=? AND campaign_id=? AND operation='sync_from_api'
               ORDER BY timestamp DESC LIMIT 1""",
            (country_code, campaign_id)
        ).fetchone()

        return {
            **summary,
            "top_errors": [{"error": r["last_error"][:100], "count": r["cnt"]} for r in top_errors],
            "image_stats": dict(image_stats) if image_stats else {},
            "rate_limits_24h": rate_limits,
            "operations_by_type": [dict(r) for r in ops_by_type],
            "last_sync": last_sync["timestamp"] if last_sync else None,
            "api_quota": self.get_api_usage_today(),
        }

    def increment_api_calls(self, operation_type: str, count: int = 1) -> None:
        """Atomically increment API call counter for today."""
        conn = self._get_conn()
        from datetime import datetime
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        hour = now.hour
        conn.execute(
            "INSERT INTO api_calls (date, hour, operation_type, count) VALUES (?, ?, ?, ?)",
            (date_str, hour, operation_type, count)
        )
        conn.commit()

    def get_api_usage_today(self) -> dict:
        """Get API usage stats for today."""
        conn = self._get_conn()
        from datetime import datetime
        date_str = datetime.now().strftime("%Y-%m-%d")

        # Total
        total = conn.execute(
            "SELECT COALESCE(SUM(count), 0) FROM api_calls WHERE date = ?",
            (date_str,)
        ).fetchone()[0]

        # By type
        by_type_rows = conn.execute(
            "SELECT operation_type, SUM(count) as cnt FROM api_calls WHERE date = ? GROUP BY operation_type ORDER BY cnt DESC",
            (date_str,)
        ).fetchall()
        by_type = {r["operation_type"]: r["cnt"] for r in by_type_rows}

        # By hour
        by_hour_rows = conn.execute(
            "SELECT hour, SUM(count) as cnt FROM api_calls WHERE date = ? GROUP BY hour ORDER BY hour",
            (date_str,)
        ).fetchall()
        by_hour = {r["hour"]: r["cnt"] for r in by_hour_rows}

        return {"date": date_str, "total": total, "by_type": by_type, "by_hour": by_hour}

    def get_remaining_quota(self, daily_limit: int = 15000) -> int:
        """Get remaining API quota for today."""
        usage = self.get_api_usage_today()
        return max(0, daily_limit - usage["total"])

    # ------------------------------------------------------------------
    # Guardrail methods
    # ------------------------------------------------------------------

    def upsert_guardrail(self, country_code: str, campaign_id: str, handle: str,
                         checks: Dict[str, Any], ad_group_id: Optional[str] = None,
                         source: str = 'setup') -> int:
        """Insert or update guardrail validation results for a product."""
        import json
        conn = self._get_conn()

        passed = sum(1 for c in checks.values() if c.get("status") == "PASS")
        warned = sum(1 for c in checks.values() if c.get("status") == "WARN")
        failed = sum(1 for c in checks.values() if c.get("status") == "FAIL")
        skipped = sum(1 for c in checks.values() if c.get("status") == "SKIP")

        if failed > 0:
            overall = "FAILED"
        elif warned > 0:
            overall = "WARN"
        elif skipped > 0:
            overall = "PARTIAL"
        elif passed > 0:
            overall = "COMPLETE"
        else:
            overall = "PENDING"

        checks_json = json.dumps(checks, ensure_ascii=False)

        existing = conn.execute(
            "SELECT id FROM guardrail_results WHERE country_code=? AND campaign_id=? AND product_handle=?",
            (country_code, campaign_id, handle)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE guardrail_results SET
                    ad_group_id=COALESCE(?, ad_group_id), checks_json=?, passed_count=?,
                    failed_count=?, skipped_count=?, overall_status=?,
                    validated_at=datetime('now'), source=?
                WHERE country_code=? AND campaign_id=? AND product_handle=?""",
                (ad_group_id, checks_json, passed, failed, skipped, overall, source,
                 country_code, campaign_id, handle)
            )
            conn.commit()
            return existing["id"]
        else:
            cursor = conn.execute(
                """INSERT INTO guardrail_results
                    (country_code, campaign_id, product_handle, ad_group_id, checks_json,
                     passed_count, failed_count, skipped_count, overall_status, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (country_code, campaign_id, handle, ad_group_id, checks_json,
                 passed, failed, skipped, overall, source)
            )
            conn.commit()
            return cursor.lastrowid

    def get_guardrail_summary(self, country_code: str, campaign_id: str) -> Dict[str, Any]:
        """Get aggregated guardrail stats for a campaign."""
        import json
        conn = self._get_conn()

        # Overall counts
        status_counts = conn.execute(
            """SELECT overall_status, COUNT(*) as cnt
               FROM guardrail_results
               WHERE country_code=? AND campaign_id=?
               GROUP BY overall_status""",
            (country_code, campaign_id)
        ).fetchall()

        summary = {"total": 0, "COMPLETE": 0, "PARTIAL": 0, "FAILED": 0, "PENDING": 0}
        for r in status_counts:
            summary[r["overall_status"]] = r["cnt"]
            summary["total"] += r["cnt"]

        # Per-check breakdown + reason aggregation
        all_rows = conn.execute(
            "SELECT checks_json FROM guardrail_results WHERE country_code=? AND campaign_id=?",
            (country_code, campaign_id)
        ).fetchall()

        check_names = ["rsa_a", "rsa_b", "rsa_c", "callouts", "sitelinks",
                       "promotion", "snippets", "keywords", "images", "ad_group"]
        by_check = {name: {"PASS": 0, "FAIL": 0, "SKIP": 0} for name in check_names}
        skip_reasons: Dict[str, int] = {}
        fail_reasons: Dict[str, int] = {}

        for row in all_rows:
            try:
                checks = json.loads(row["checks_json"])
            except (json.JSONDecodeError, TypeError):
                continue
            for name in check_names:
                check = checks.get(name, {})
                status = check.get("status", "SKIP")
                if status in by_check.get(name, {}):
                    by_check[name][status] += 1
                reason = check.get("reason", "")
                if reason:
                    if status == "SKIP":
                        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                    elif status == "FAIL":
                        fail_reasons[reason] = fail_reasons.get(reason, 0) + 1

        return {
            **summary,
            "by_check": by_check,
            "top_skip_reasons": dict(sorted(skip_reasons.items(), key=lambda x: -x[1])[:10]),
            "top_fail_reasons": dict(sorted(fail_reasons.items(), key=lambda x: -x[1])[:10]),
        }

    def get_guardrail_failures(self, country_code: str, campaign_id: str,
                               limit: int = 50) -> List[Dict[str, Any]]:
        """Get products with guardrail failures."""
        import json
        conn = self._get_conn()
        rows = conn.execute(
            """SELECT product_handle, ad_group_id, checks_json, overall_status,
                      passed_count, failed_count, skipped_count, validated_at, source
               FROM guardrail_results
               WHERE country_code=? AND campaign_id=? AND overall_status IN ('FAILED', 'PARTIAL')
               ORDER BY failed_count DESC, skipped_count DESC
               LIMIT ?""",
            (country_code, campaign_id, limit)
        ).fetchall()

        results = []
        for r in rows:
            entry = dict(r)
            try:
                entry["checks"] = json.loads(r["checks_json"])
            except (json.JSONDecodeError, TypeError):
                entry["checks"] = {}
            del entry["checks_json"]
            # Extract failed/skipped check names
            entry["failed_checks"] = [
                name for name, c in entry["checks"].items() if c.get("status") == "FAIL"
            ]
            entry["skipped_checks"] = [
                name for name, c in entry["checks"].items() if c.get("status") == "SKIP"
            ]
            results.append(entry)

        return results

    # ------------------------------------------------------------------
    # Eligibility methods
    # ------------------------------------------------------------------

    def upsert_eligibility(self, country_code: str, campaign_id: str,
                           ad_group_id: str, entity_type: str, entity_id: str,
                           approval_status: str, review_status: Optional[str] = None,
                           disapproval_reasons: Optional[List[str]] = None,
                           action_taken: Optional[str] = None,
                           product_handle: Optional[str] = None) -> int:
        """Record ad/asset/keyword eligibility status."""
        import json
        conn = self._get_conn()
        reasons_json = json.dumps(disapproval_reasons or [], ensure_ascii=False)

        existing = conn.execute(
            "SELECT id FROM ad_eligibility WHERE country_code=? AND campaign_id=? AND entity_type=? AND entity_id=?",
            (country_code, campaign_id, entity_type, entity_id)
        ).fetchone()

        if existing:
            sets = ["approval_status=?", "review_status=?", "disapproval_reasons=?",
                    "checked_at=datetime('now')"]
            vals = [approval_status, review_status, reasons_json]
            if action_taken:
                sets.append("action_taken=?")
                sets.append("action_at=datetime('now')")
                vals.append(action_taken)
            if product_handle:
                sets.append("product_handle=?")
                vals.append(product_handle)
            vals.extend([country_code, campaign_id, entity_type, entity_id])
            conn.execute(
                f"UPDATE ad_eligibility SET {', '.join(sets)} "
                f"WHERE country_code=? AND campaign_id=? AND entity_type=? AND entity_id=?",
                vals
            )
            conn.commit()
            return existing["id"]
        else:
            cursor = conn.execute(
                """INSERT INTO ad_eligibility
                    (country_code, campaign_id, ad_group_id, entity_type, entity_id,
                     approval_status, review_status, disapproval_reasons, action_taken,
                     action_at, product_handle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? IS NOT NULL THEN datetime('now') ELSE NULL END, ?)""",
                (country_code, campaign_id, ad_group_id, entity_type, entity_id,
                 approval_status, review_status, reasons_json, action_taken, action_taken, product_handle)
            )
            conn.commit()
            return cursor.lastrowid

    def get_disapproved_entities(self, country_code: str, campaign_id: str,
                                  entity_type: Optional[str] = None,
                                  limit: int = 100) -> List[Dict[str, Any]]:
        """Get disapproved ads/assets/keywords."""
        import json
        conn = self._get_conn()
        query = """SELECT * FROM ad_eligibility
                   WHERE country_code=? AND campaign_id=? AND approval_status='DISAPPROVED'"""
        params = [country_code, campaign_id]
        if entity_type:
            query += " AND entity_type=?"
            params.append(entity_type)
        query += f" ORDER BY checked_at DESC LIMIT {limit}"
        rows = conn.execute(query, params).fetchall()
        results = []
        for r in rows:
            entry = dict(r)
            try:
                entry["disapproval_reasons"] = json.loads(entry["disapproval_reasons"])
            except (json.JSONDecodeError, TypeError):
                entry["disapproval_reasons"] = []
            results.append(entry)
        return results

    def get_disapproval_patterns(self, country_code: str, campaign_id: str,
                                  min_occurrences: int = 2) -> Dict[str, int]:
        """Get recurring disapproval reasons for pattern analysis."""
        import json
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT disapproval_reasons FROM ad_eligibility WHERE country_code=? AND campaign_id=? AND approval_status='DISAPPROVED'",
            (country_code, campaign_id)
        ).fetchall()

        reason_counts: Dict[str, int] = {}
        for r in rows:
            try:
                reasons = json.loads(r["disapproval_reasons"])
                for reason in reasons:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
            except (json.JSONDecodeError, TypeError):
                continue

        return {k: v for k, v in sorted(reason_counts.items(), key=lambda x: -x[1]) if v >= min_occurrences}

    # ------------------------------------------------------------------
    # Asset Performance History (trend tracking)
    # ------------------------------------------------------------------

    def snapshot_asset_performance(self, country_code: str, campaign_id: str,
                                    asset_type: str, headline_pattern: str,
                                    performance_label: str, **kwargs):
        """Record a historical snapshot of asset performance (append-only)."""
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO asset_performance_history
               (country_code, campaign_id, asset_type, headline_pattern, performance_label,
                occurrences, avg_impressions, avg_clicks, avg_conversions, avg_conversion_value)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (country_code, campaign_id, asset_type, headline_pattern, performance_label,
             kwargs.get("occurrences", 1),
             kwargs.get("avg_impressions", 0), kwargs.get("avg_clicks", 0),
             kwargs.get("avg_conversions", 0), kwargs.get("avg_conversion_value", 0))
        )
        conn.commit()

    def get_asset_trend(self, country_code: str, headline_pattern: str,
                        asset_type: str = "HEADLINE",
                        days_back: int = 90) -> List[Dict[str, Any]]:
        """
        Get performance trend for a specific asset pattern over time.
        Returns chronological snapshots showing label transitions.
        """
        conn = self._get_conn()
        rows = conn.execute(
            """SELECT performance_label, occurrences, avg_impressions, avg_clicks,
                      avg_conversions, avg_conversion_value, scanned_at
               FROM asset_performance_history
               WHERE country_code=? AND asset_type=? AND headline_pattern=?
                 AND scanned_at > datetime('now', ? || ' days')
               ORDER BY scanned_at ASC""",
            (country_code, asset_type, headline_pattern, f"-{days_back}")
        ).fetchall()
        return [dict(r) for r in rows]

    def get_regressing_assets(self, country_code: str,
                               days_back: int = 30,
                               limit: int = 20) -> List[Dict[str, Any]]:
        """
        Find assets that WERE performing well but have regressed.
        Detects BEST/GOOD → LOW transitions.
        """
        conn = self._get_conn()
        cutoff = f"-{days_back}"
        half = f"-{days_back // 2}"

        # Assets that had BEST/GOOD in first half but LOW in second half
        rows = conn.execute(
            """SELECT h1.headline_pattern, h1.asset_type,
                      h1.performance_label as old_label,
                      h2.performance_label as new_label,
                      h1.scanned_at as was_good_at,
                      h2.scanned_at as became_bad_at
               FROM asset_performance_history h1
               JOIN asset_performance_history h2
                 ON h1.country_code = h2.country_code
                AND h1.asset_type = h2.asset_type
                AND h1.headline_pattern = h2.headline_pattern
               WHERE h1.country_code = ?
                 AND h1.performance_label IN ('BEST', 'GOOD')
                 AND h2.performance_label = 'LOW'
                 AND h1.scanned_at > datetime('now', ? || ' days')
                 AND h1.scanned_at < datetime('now', ? || ' days')
                 AND h2.scanned_at > datetime('now', ? || ' days')
               GROUP BY h1.headline_pattern, h1.asset_type
               ORDER BY h2.scanned_at DESC
               LIMIT ?""",
            (country_code, cutoff, half, half, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Funnel Diagnostics
    # ------------------------------------------------------------------

    def upsert_funnel_diagnostic(self, country_code: str, campaign_id: str,
                                  product_handle: str, **kwargs):
        """Insert or update funnel diagnostic for a product."""
        conn = self._get_conn()
        period_start = kwargs.get("period_start", "")

        conn.execute(
            """INSERT INTO product_funnel_diagnostics
               (country_code, campaign_id, product_handle, ad_group_id,
                impressions, clicks, conversions, conversion_value, cost_micros,
                ctr, conv_rate, roas, cpa_micros,
                funnel_bottleneck, diagnosis_details,
                ctr_vs_avg, conv_rate_vs_avg, roas_vs_avg,
                period_start, period_end)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(country_code, campaign_id, product_handle, period_start)
               DO UPDATE SET
                ad_group_id=excluded.ad_group_id,
                impressions=excluded.impressions, clicks=excluded.clicks,
                conversions=excluded.conversions, conversion_value=excluded.conversion_value,
                cost_micros=excluded.cost_micros,
                ctr=excluded.ctr, conv_rate=excluded.conv_rate,
                roas=excluded.roas, cpa_micros=excluded.cpa_micros,
                funnel_bottleneck=excluded.funnel_bottleneck,
                diagnosis_details=excluded.diagnosis_details,
                ctr_vs_avg=excluded.ctr_vs_avg,
                conv_rate_vs_avg=excluded.conv_rate_vs_avg,
                roas_vs_avg=excluded.roas_vs_avg,
                period_end=excluded.period_end,
                scanned_at=datetime('now')""",
            (country_code, campaign_id, product_handle,
             kwargs.get("ad_group_id"),
             kwargs.get("impressions", 0), kwargs.get("clicks", 0),
             kwargs.get("conversions", 0), kwargs.get("conversion_value", 0),
             kwargs.get("cost_micros", 0),
             kwargs.get("ctr", 0), kwargs.get("conv_rate", 0),
             kwargs.get("roas", 0), kwargs.get("cpa_micros", 0),
             kwargs.get("funnel_bottleneck"), kwargs.get("diagnosis_details"),
             kwargs.get("ctr_vs_avg", 0), kwargs.get("conv_rate_vs_avg", 0),
             kwargs.get("roas_vs_avg", 0),
             period_start, kwargs.get("period_end", ""))
        )
        conn.commit()

    def get_funnel_bottlenecks(self, country_code: str, campaign_id: str,
                                bottleneck: Optional[str] = None,
                                limit: int = 50) -> List[Dict[str, Any]]:
        """Get products with funnel bottlenecks. Filter by bottleneck type if given."""
        conn = self._get_conn()
        if bottleneck:
            rows = conn.execute(
                """SELECT * FROM product_funnel_diagnostics
                   WHERE country_code=? AND campaign_id=? AND funnel_bottleneck=?
                   ORDER BY impressions DESC LIMIT ?""",
                (country_code, campaign_id, bottleneck, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM product_funnel_diagnostics
                   WHERE country_code=? AND campaign_id=? AND funnel_bottleneck IS NOT NULL
                   ORDER BY cost_micros DESC LIMIT ?""",
                (country_code, campaign_id, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Seasonality Patterns
    # ------------------------------------------------------------------

    def upsert_seasonality(self, country_code: str, category: str, month: int, **kwargs):
        """Insert or update seasonality pattern for a category/month."""
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO seasonality_patterns
               (country_code, category, month, avg_ctr, avg_conv_rate, avg_roas,
                avg_cpc_micros, sample_count)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(country_code, category, month)
               DO UPDATE SET
                avg_ctr=excluded.avg_ctr, avg_conv_rate=excluded.avg_conv_rate,
                avg_roas=excluded.avg_roas, avg_cpc_micros=excluded.avg_cpc_micros,
                sample_count=excluded.sample_count, scanned_at=datetime('now')""",
            (country_code, category, month,
             kwargs.get("avg_ctr", 0), kwargs.get("avg_conv_rate", 0),
             kwargs.get("avg_roas", 0), kwargs.get("avg_cpc_micros", 0),
             kwargs.get("sample_count", 0))
        )
        conn.commit()

    def get_seasonality(self, country_code: str, category: str) -> List[Dict[str, Any]]:
        """Get 12-month seasonality patterns for a category."""
        conn = self._get_conn()
        rows = conn.execute(
            """SELECT month, avg_ctr, avg_conv_rate, avg_roas, avg_cpc_micros, sample_count
               FROM seasonality_patterns
               WHERE country_code=? AND category=?
               ORDER BY month""",
            (country_code, category)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_current_season_performance(self, country_code: str, category: str) -> Optional[Dict[str, Any]]:
        """Get seasonality data for the current month."""
        from datetime import datetime
        current_month = datetime.now().month
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM seasonality_patterns WHERE country_code=? AND category=? AND month=?",
            (country_code, category, current_month)
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Product Campaign Overlap (frequency/fatigue)
    # ------------------------------------------------------------------

    def upsert_product_overlap(self, country_code: str, product_handle: str, **kwargs):
        """Insert or update product campaign overlap data."""
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO product_campaign_overlap
               (country_code, product_handle, campaign_count, active_ad_group_count,
                total_impressions, total_cost_micros, campaign_ids_json, overlap_risk)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(country_code, product_handle)
               DO UPDATE SET
                campaign_count=excluded.campaign_count,
                active_ad_group_count=excluded.active_ad_group_count,
                total_impressions=excluded.total_impressions,
                total_cost_micros=excluded.total_cost_micros,
                campaign_ids_json=excluded.campaign_ids_json,
                overlap_risk=excluded.overlap_risk,
                scanned_at=datetime('now')""",
            (country_code, product_handle,
             kwargs.get("campaign_count", 0), kwargs.get("active_ad_group_count", 0),
             kwargs.get("total_impressions", 0), kwargs.get("total_cost_micros", 0),
             kwargs.get("campaign_ids_json", "[]"), kwargs.get("overlap_risk", "LOW"))
        )
        conn.commit()

    def get_high_overlap_products(self, country_code: str,
                                   min_campaigns: int = 2,
                                   limit: int = 30) -> List[Dict[str, Any]]:
        """Get products running in multiple campaigns (fatigue risk)."""
        conn = self._get_conn()
        rows = conn.execute(
            """SELECT * FROM product_campaign_overlap
               WHERE country_code=? AND campaign_count >= ?
               ORDER BY campaign_count DESC, total_impressions DESC
               LIMIT ?""",
            (country_code, min_campaigns, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    # ═══════════════════════════════════════════════════════════════════
    # Keyword Spend Analysis — CRUD methods
    # ═══════════════════════════════════════════════════════════════════

    def insert_keyword_spend_job(self, job_id: str, country_code: str,
                                  campaign_ids: list, current_start: str,
                                  current_end: str, previous_start: str,
                                  previous_end: str) -> None:
        """Create a new keyword spend analysis job record."""
        import json
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO keyword_spend_jobs
               (job_id, country_code, campaign_ids,
                current_period_start, current_period_end,
                previous_period_start, previous_period_end,
                status, phase, started_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 'init', datetime('now'))""",
            (job_id, country_code, json.dumps(campaign_ids),
             current_start, current_end, previous_start, previous_end)
        )
        conn.commit()

    def update_keyword_spend_job(self, job_id: str, **kwargs) -> None:
        """Update job fields (status, phase, progress, error, etc.)."""
        import json
        conn = self._get_conn()
        allowed = {
            "status", "phase", "started_at", "completed_at",
            "total_campaigns", "processed_campaigns",
            "total_keywords_fetched", "total_gaql_calls",
            "resume_checkpoint", "summary_json", "error_message",
        }
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k not in allowed:
                continue
            sets.append(f"{k} = ?")
            if isinstance(v, (dict, list)):
                vals.append(json.dumps(v))
            else:
                vals.append(v)
        if not sets:
            return
        sets.append("updated_at = datetime('now')")
        vals.append(job_id)
        conn.execute(
            f"UPDATE keyword_spend_jobs SET {', '.join(sets)} WHERE job_id = ?",
            vals
        )
        conn.commit()

    def get_keyword_spend_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve job by job_id."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM keyword_spend_jobs WHERE job_id = ?",
            (job_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_latest_keyword_spend_job(self, country_code: str,
                                      campaign_id: str = None) -> Optional[Dict[str, Any]]:
        """Get most recent job for a country (for resume detection)."""
        import json
        conn = self._get_conn()
        if campaign_id:
            rows = conn.execute(
                """SELECT * FROM keyword_spend_jobs
                   WHERE country_code = ? AND status IN ('rate_limited', 'pending', 'fetching')
                   ORDER BY created_at DESC LIMIT 5""",
                (country_code,)
            ).fetchall()
            for row in rows:
                cids = json.loads(row["campaign_ids"]) if row["campaign_ids"] else []
                if campaign_id in cids or str(campaign_id) in [str(c) for c in cids]:
                    return dict(row)
            return None
        else:
            row = conn.execute(
                """SELECT * FROM keyword_spend_jobs
                   WHERE country_code = ? AND status IN ('rate_limited', 'pending', 'fetching')
                   ORDER BY created_at DESC LIMIT 1""",
                (country_code,)
            ).fetchone()
            return dict(row) if row else None

    def cleanup_old_keyword_spend_data(self, days_old: int = 7) -> Dict[str, int]:
        """Delete keyword_spend_periods/comparisons older than N days.
        Keep keyword_spend_jobs for audit trail."""
        conn = self._get_conn()
        cutoff = f"-{days_old} days"

        c1 = conn.execute(
            "DELETE FROM keyword_spend_periods WHERE fetched_at < datetime('now', ?)",
            (cutoff,)
        ).rowcount

        c2 = conn.execute(
            "DELETE FROM keyword_spend_comparisons WHERE computed_at < datetime('now', ?)",
            (cutoff,)
        ).rowcount

        conn.commit()
        return {"keyword_spend_periods_deleted": c1, "keyword_spend_comparisons_deleted": c2}

    # ------------------------------------------------------------------
    # Negative Keywords Cache
    # ------------------------------------------------------------------

    def upsert_negative_keyword(self, criterion_id: str, keyword_text: str, match_type: str,
                                 source_type: str, source_name: str = None,
                                 shared_set_id: str = None, campaign_id: str = None,
                                 is_active: int = 1) -> None:
        """Insert or update a negative keyword in the cache."""
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO negative_keywords
                (criterion_id, keyword_text, match_type, source_type, source_name,
                 shared_set_id, campaign_id, is_active, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(criterion_id, shared_set_id, campaign_id)
            DO UPDATE SET keyword_text=excluded.keyword_text, match_type=excluded.match_type,
                          source_name=excluded.source_name, is_active=excluded.is_active,
                          synced_at=datetime('now')
        """, (criterion_id, keyword_text.lower(), match_type, source_type, source_name,
              shared_set_id, campaign_id, is_active))
        conn.commit()

    def bulk_upsert_negative_keywords(self, records: List[Dict]) -> int:
        """Bulk insert/update negative keywords. Returns count of rows affected."""
        conn = self._get_conn()
        count = 0
        for r in records:
            conn.execute("""
                INSERT INTO negative_keywords
                    (criterion_id, keyword_text, match_type, source_type, source_name,
                     shared_set_id, campaign_id, is_active, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(criterion_id, shared_set_id, campaign_id)
                DO UPDATE SET keyword_text=excluded.keyword_text, match_type=excluded.match_type,
                              source_name=excluded.source_name, is_active=excluded.is_active,
                              synced_at=datetime('now')
            """, (r['criterion_id'], r['keyword_text'].lower(), r['match_type'],
                  r['source_type'], r.get('source_name', ''),
                  r.get('shared_set_id'), r.get('campaign_id'),
                  r.get('is_active', 1)))
            count += 1
        conn.commit()
        return count

    def get_active_negatives(self, source_type: str = None, shared_set_id: str = None) -> List[Dict]:
        """Get active negative keywords, optionally filtered by source."""
        conn = self._get_conn()
        where = ["is_active = 1"]
        params = []
        if source_type:
            where.append("source_type = ?")
            params.append(source_type)
        if shared_set_id:
            where.append("shared_set_id = ?")
            params.append(shared_set_id)
        rows = conn.execute(
            f"SELECT * FROM negative_keywords WHERE {' AND '.join(where)} ORDER BY keyword_text",
            params
        ).fetchall()
        return [dict(r) for r in rows]

    def search_negatives(self, search_text: str) -> List[Dict]:
        """Search negative keywords by text (LIKE match)."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM negative_keywords WHERE keyword_text LIKE ? AND is_active = 1 ORDER BY keyword_text",
            (f"%{search_text.lower()}%",)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_negative_stats(self) -> Dict:
        """Get statistics about cached negative keywords."""
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM negative_keywords").fetchone()[0]
        active = conn.execute("SELECT COUNT(*) FROM negative_keywords WHERE is_active = 1").fetchone()[0]
        by_source = conn.execute(
            "SELECT source_type, source_name, is_active, COUNT(*) as cnt FROM negative_keywords GROUP BY source_type, source_name, is_active"
        ).fetchall()
        return {
            "total": total,
            "active": active,
            "by_source": [dict(r) for r in by_source],
        }

    def clear_negatives(self, shared_set_id: str = None) -> int:
        """Clear negative keywords cache. If shared_set_id given, clear only that set."""
        conn = self._get_conn()
        if shared_set_id:
            count = conn.execute("DELETE FROM negative_keywords WHERE shared_set_id = ?", (shared_set_id,)).rowcount
        else:
            count = conn.execute("DELETE FROM negative_keywords").rowcount
        conn.commit()
        return count
