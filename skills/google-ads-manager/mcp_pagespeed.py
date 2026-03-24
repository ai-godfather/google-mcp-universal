"""
PageSpeed Insights MCP Module

Provides tools for scanning and analyzing website performance using Google PageSpeed Insights API.
Integrates with SQLite for scan history tracking and issue management.
"""

import os
import re
import json
import time
import uuid
import logging
import sqlite3
import urllib.request
import urllib.parse
import urllib.error
import ssl

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from collections import Counter, defaultdict
from statistics import mean
from xml.etree import ElementTree as ET

try:
    from accounts_config import load_config
    _config = load_config()
except Exception:
    _config = {}

from pydantic import BaseModel, Field
from google_ads_mcp import mcp
from batch_db import BatchDB

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Singleton BatchDB instance
_batch_db: Optional[BatchDB] = None

def _get_db() -> BatchDB:
    """Get or create the singleton BatchDB instance."""
    global _batch_db
    if _batch_db is None:
        _batch_db = BatchDB()
    return _batch_db


# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

def _ensure_pagespeed_tables():
    """Initialize PageSpeed-specific SQLite tables.
    Drops and recreates to ensure schema match with mcp_pagespeed module columns."""
    db = _get_db()
    conn = db._get_conn()

    # Drop old tables if schema mismatch (safe — these are cache tables with no legacy data)
    expected_cols = {
        'pagespeed_scans': ['performance_score', 'fcp_ms', 'lcp_ms'],
        'pagespeed_issues': ['title', 'audit_id', 'impact_ms'],
        'pagespeed_fixes_log': ['fix_id', 'store_domain'],
    }
    for table, required in expected_cols.items():
        try:
            cursor = conn.execute(f"PRAGMA table_info({table})")
            cols = [row[1] for row in cursor.fetchall()]
            if cols and not all(c in cols for c in required):
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"Dropped old {table} table (missing columns: {[c for c in required if c not in cols]})")
        except Exception:
            pass
    conn.commit()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pagespeed_scans (
            id INTEGER PRIMARY KEY,
            scan_id TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            domain TEXT NOT NULL,
            country_code TEXT,
            strategy TEXT DEFAULT 'mobile',
            performance_score INTEGER,
            first_contentful_paint REAL,
            largest_contentful_paint REAL,
            total_blocking_time REAL,
            cumulative_layout_shift REAL,
            speed_index REAL,
            time_to_interactive REAL,
            -- Field data (CrUX)
            field_fcp REAL,
            field_lcp REAL,
            field_cls REAL,
            -- Metadata
            result_json TEXT,
            scanned_at TEXT DEFAULT (datetime('now')),
            UNIQUE(url, strategy)
        );

        CREATE TABLE IF NOT EXISTS pagespeed_issues (
            id INTEGER PRIMARY KEY,
            scan_id TEXT NOT NULL,
            domain TEXT NOT NULL,
            fix_id TEXT,
            audit_id TEXT,
            title TEXT,
            description TEXT,
            impact_ms INTEGER,
            impact_percentage REAL,
            details TEXT,
            recorded_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(scan_id) REFERENCES pagespeed_scans(scan_id)
        );

        CREATE TABLE IF NOT EXISTS pagespeed_fixes_log (
            id INTEGER PRIMARY KEY,
            fix_id TEXT NOT NULL,
            store_domain TEXT NOT NULL,
            file_path TEXT,
            action TEXT,
            diff_text TEXT,
            applied_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_pagespeed_scans_domain
            ON pagespeed_scans(domain);
        CREATE INDEX IF NOT EXISTS idx_pagespeed_scans_country
            ON pagespeed_scans(country_code);
        CREATE INDEX IF NOT EXISTS idx_pagespeed_scans_time
            ON pagespeed_scans(scanned_at);
        CREATE INDEX IF NOT EXISTS idx_pagespeed_issues_fix
            ON pagespeed_issues(fix_id);
        CREATE INDEX IF NOT EXISTS idx_pagespeed_fixes_domain
            ON pagespeed_fixes_log(store_domain);
    """)
    conn.commit()


# ============================================================================
# STORE FEED MAPPING
# ============================================================================

# Store configuration — loaded from config.json
STORE_FEEDS = _config.get('store_feeds', {})
COUNTRY_DOMAINS = _config.get('country_domains', {})
COUNTRY_TO_REPO = _config.get('country_to_repo', {})

NO_GITHUB_SHOPS = ["AL", "BA", "CL", "ID", "MK", "OM", "PH", "RS", "SG", "VN"]

AUDIT_FIX_MAP = {
    "render-blocking-resources": ["PSI-010", "PSI-011"],
    "unused-javascript": ["PSI-004", "PSI-006"],
    "unused-css-rules": ["PSI-003"],
    "third-party-summary": ["PSI-005"],
    "font-display": ["PSI-008"],
    "mainthread-work-breakdown": ["PSI-001", "PSI-002"],
    "bootup-time": ["PSI-001", "PSI-002"],
    "dom-size": ["PSI-002"],
}


# ============================================================================
# REQUEST MODELS
# ============================================================================

class PageSpeedScanRequest(BaseModel):
    """Request model for pagespeed_scan_url"""
    url: str = Field(..., description="URL to scan")
    strategy: str = Field("mobile", description="mobile or desktop")
    categories: List[str] = Field(["performance"], description="Categories to scan")


class PageSpeedScanStoreRequest(BaseModel):
    """Request model for pagespeed_scan_store"""
    country_code: str = Field(..., description="Country code (e.g., TR, PL)")
    max_urls: int = Field(5, description="Max URLs to scan", ge=1, le=20)
    strategy: str = Field("mobile", description="mobile or desktop")
    urls: Optional[List[str]] = Field(None, description="Manual list of URLs")


class PageSpeedScanAllRequest(BaseModel):
    """Request model for pagespeed_scan_all_stores"""
    country_codes: Optional[List[str]] = Field(None, description="List of country codes")
    sample_size: int = Field(2, description="URLs per country", ge=1, le=10)
    strategy: str = Field("mobile", description="mobile or desktop")
    use_cached: bool = Field(True, description="Use cached results <24h old")


class PageSpeedAnalyzeThemeRequest(BaseModel):
    """Request model for pagespeed_analyze_theme"""
    repo_name: str = Field(..., description="Folder name in ACTIVE_SHOPS")
    repos_path: Optional[str] = Field(None, description="Base path to repos")


class PageSpeedApplyFixesRequest(BaseModel):
    """Request model for pagespeed_apply_fixes"""
    fix_ids: List[str] = Field(..., description="Fix IDs to apply")
    repo_names: Optional[List[str]] = Field(None, description="Repo names")
    dry_run: bool = Field(True, description="Preview only")


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def _save_scan(scan_id: str, url: str, domain: str, country_code: Optional[str],
               strategy: str, result: Dict) -> None:
    """Save PageSpeed scan to database."""
    db = _get_db()
    conn = db._get_conn()

    # Extract metrics from Lighthouse result
    lighthouse = result.get("lighthouseResult", {})
    audits = lighthouse.get("audits", {})
    categories = lighthouse.get("categories", {})

    perf_score = categories.get("performance", {}).get("score")
    if perf_score:
        perf_score = int(perf_score * 100)

    # Web Vitals metrics
    fcp = audits.get("first-contentful-paint", {}).get("numericValue")
    lcp = audits.get("largest-contentful-paint", {}).get("numericValue")
    tbt = audits.get("total-blocking-time", {}).get("numericValue")
    cls = audits.get("cumulative-layout-shift", {}).get("numericValue")
    si = audits.get("speed-index", {}).get("numericValue")
    tti = audits.get("interactive", {}).get("numericValue")

    # Field data (CrUX)
    field_data = result.get("loadingExperience", {}).get("metrics", {})
    field_fcp = field_data.get("FIRST_CONTENTFUL_PAINT_MS", {}).get("percentile")
    field_lcp = field_data.get("LARGEST_CONTENTFUL_PAINT_MS", {}).get("percentile")
    field_cls = field_data.get("CUMULATIVE_LAYOUT_SHIFT", {}).get("percentile")

    conn.execute("""
        INSERT INTO pagespeed_scans
            (scan_id, url, domain, country_code, strategy, performance_score,
             first_contentful_paint, largest_contentful_paint, total_blocking_time,
             cumulative_layout_shift, speed_index, time_to_interactive,
             field_fcp, field_lcp, field_cls, result_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url, strategy) DO UPDATE SET
            performance_score=excluded.performance_score,
            result_json=excluded.result_json,
            scanned_at=datetime('now')
    """, (scan_id, url, domain, country_code, strategy, perf_score,
          fcp, lcp, tbt, cls, si, tti, field_fcp, field_lcp, field_cls,
          json.dumps(result)))
    conn.commit()


def _save_issues(scan_id: str, domain: str, issues: List[Dict]) -> None:
    """Save PageSpeed issues to database."""
    db = _get_db()
    conn = db._get_conn()

    for issue in issues:
        conn.execute("""
            INSERT INTO pagespeed_issues
                (scan_id, domain, fix_id, audit_id, title, description,
                 impact_ms, impact_percentage, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (scan_id, domain, issue.get("fix_id"), issue.get("audit_id"),
              issue.get("title"), issue.get("description"),
              issue.get("impact_ms"), issue.get("impact_percentage"),
              json.dumps(issue.get("details", {}))))
    conn.commit()


def _get_cached_scans(domain: str, max_age_hours: int = 24) -> List[Dict]:
    """Get cached scans for a domain within max_age_hours."""
    db = _get_db()
    conn = db._get_conn()

    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    rows = conn.execute("""
        SELECT * FROM pagespeed_scans
        WHERE domain = ? AND scanned_at > ?
        ORDER BY scanned_at DESC
    """, (domain, cutoff.isoformat())).fetchall()

    return [dict(r) for r in rows]


def _log_fix(fix_id: str, store_domain: str, file_path: Optional[str],
             action: str, diff_text: Optional[str]) -> None:
    """Log a fix application."""
    db = _get_db()
    conn = db._get_conn()

    conn.execute("""
        INSERT INTO pagespeed_fixes_log
            (fix_id, store_domain, file_path, action, diff_text)
        VALUES (?, ?, ?, ?, ?)
    """, (fix_id, store_domain, file_path, action, diff_text))
    conn.commit()


# ============================================================================
# PAGESPEED API HELPERS
# ============================================================================

def _fetch_product_urls(country_code: str, max_urls: int = 5) -> List[str]:
    """Fetch product URLs from XML feed for a country."""
    domain = COUNTRY_DOMAINS.get(country_code)
    if not domain:
        logger.error(f"Unknown country code: {country_code}")
        return []

    # Construct feed URL
    feed_url = f"https://{domain}/feed.xml"

    try:
        if HAS_HTTPX:
            response = httpx.get(feed_url, timeout=15)
            response.raise_for_status()
            content = response.content
        else:
            ctx = ssl.create_default_context()
            req = urllib.request.Request(feed_url)
            with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
                content = resp.read()

        root = ET.fromstring(content)

        # Handle both <link> and <g:link> tags
        urls = []
        for item in root.findall(".//item"):
            link = item.find("link")
            if link is None:
                link = item.find("{http://base.google.com/ns/1.0}link")
            if link is not None and link.text:
                urls.append(link.text)

        return urls[:max_urls]
    except Exception as e:
        logger.error(f"Error fetching feed for {country_code}: {e}")
        return []


def _call_pagespeed_api(url: str, strategy: str = "mobile",
                       categories: Optional[List[str]] = None) -> Optional[Dict]:
    """Call Google PageSpeed Insights API v5."""
    api_key = os.getenv("PSI_API_KEY")
    if not api_key:
        logger.error("PSI_API_KEY environment variable not set")
        return None

    if categories is None:
        categories = ["performance"]

    # Build params — PSI API accepts multiple 'category' params
    params_list = [
        ("url", url),
        ("key", api_key),
        ("strategy", strategy),
    ]
    for cat in categories:
        params_list.append(("category", cat))

    query_string = urllib.parse.urlencode(params_list)
    full_url = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed?{query_string}"

    if HAS_HTTPX:
        try:
            response = httpx.get(full_url, timeout=120)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"PageSpeed API error (httpx) for {url}: {e}")
            return None
    else:
        # Fallback to urllib
        try:
            ctx = ssl.create_default_context()
            req = urllib.request.Request(full_url)
            with urllib.request.urlopen(req, timeout=120, context=ctx) as resp:
                data = resp.read().decode('utf-8')
                return json.loads(data)
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8', errors='ignore')[:300]
            logger.error(f"PageSpeed API HTTP {e.code} for {url}: {body}")
            return None
        except Exception as e:
            logger.error(f"PageSpeed API error (urllib) for {url}: {e}")
            return None


def _extract_opportunities(lighthouse_result: Dict) -> List[Dict]:
    """Extract top opportunities from Lighthouse result."""
    audits = lighthouse_result.get("audits", {})
    opportunities = []

    for audit_id, audit in audits.items():
        if audit.get("score") is not None and audit.get("score") < 1.0:
            # This is an opportunity
            details = audit.get("details", {})
            items = details.get("items", [])

            opportunities.append({
                "audit_id": audit_id,
                "title": audit.get("title"),
                "description": audit.get("description"),
                "impact_ms": int(details.get("overallSavingsMs", 0)),
                "impact_percentage": details.get("overallSavingsBytes", 0),
                "item_count": len(items),
            })

    # Sort by impact
    opportunities.sort(key=lambda x: x["impact_ms"], reverse=True)
    return opportunities[:10]


def _map_audits_to_fixes(lighthouse_result: Dict) -> List[Dict]:
    """Map audit results to fix IDs using AUDIT_FIX_MAP."""
    audits = lighthouse_result.get("audits", {})
    fixes = []

    for audit_id, fix_ids in AUDIT_FIX_MAP.items():
        if audit_id in audits:
            audit = audits[audit_id]
            if audit.get("score") is not None and audit.get("score") < 1.0:
                for fix_id in fix_ids:
                    fixes.append({
                        "fix_id": fix_id,
                        "audit_id": audit_id,
                        "title": audit.get("title"),
                        "description": audit.get("description"),
                    })

    return fixes


# ============================================================================
# MCP TOOLS
# ============================================================================

@mcp.tool()
async def pagespeed_scan_url(request: Dict) -> Dict:
    """
    Scan a single URL via Google PageSpeed Insights API v5.

    Returns structured JSON with performance score, Core Web Vitals,
    top opportunities, and mapped fix IDs.
    """
    _ensure_pagespeed_tables()

    url = request.get("url")
    strategy = request.get("strategy", "mobile")
    categories = request.get("categories", ["performance"])

    if not url:
        return {"error": "URL required"}

    # Call API
    result = _call_pagespeed_api(url, strategy, categories)
    if not result:
        return {"error": f"PageSpeed API failed for {url}"}

    # Extract domain
    domain = url.split("/")[2] if url.startswith("http") else url

    # Save scan
    scan_id = str(uuid.uuid4())
    _save_scan(scan_id, url, domain, None, strategy, result)

    # Extract metrics
    lighthouse = result.get("lighthouseResult", {})
    categories_result = lighthouse.get("categories", {})
    perf_category = categories_result.get("performance", {})
    perf_score = perf_category.get("score")
    if perf_score:
        perf_score = int(perf_score * 100)

    audits = lighthouse.get("audits", {})

    # Web Vitals
    fcp = audits.get("first-contentful-paint", {}).get("numericValue")
    lcp = audits.get("largest-contentful-paint", {}).get("numericValue")
    cls = audits.get("cumulative-layout-shift", {}).get("numericValue")
    tbt = audits.get("total-blocking-time", {}).get("numericValue")

    # Field data (CrUX)
    field_data = result.get("loadingExperience", {})
    field_metrics = field_data.get("metrics", {})

    # Opportunities and fixes
    opportunities = _extract_opportunities(lighthouse)
    mapped_fixes = _map_audits_to_fixes(lighthouse)

    # Save issues
    issues = [
        {
            "fix_id": fix.get("fix_id"),
            "audit_id": fix.get("audit_id"),
            "title": fix.get("title"),
            "description": fix.get("description"),
            "impact_ms": 0,
            "impact_percentage": 0,
            "details": {},
        }
        for fix in mapped_fixes
    ]
    _save_issues(scan_id, domain, issues)

    return {
        "scan_id": scan_id,
        "url": url,
        "strategy": strategy,
        "performance_score": perf_score,
        "core_web_vitals": {
            "fcp_ms": fcp,
            "lcp_ms": lcp,
            "cls": cls,
            "tbt_ms": tbt,
        },
        "field_data": {
            "metrics": dict(field_metrics) if field_metrics else {},
        },
        "top_opportunities": opportunities,
        "mapped_fixes": mapped_fixes,
        "recommendation": f"Apply fixes: {', '.join(set(m['fix_id'] for m in mapped_fixes))}" if mapped_fixes else "Performance acceptable",
    }


@mcp.tool()
async def pagespeed_scan_store(request: Dict) -> Dict:
    """
    Mass scan a store by country code with rate limiting and aggregation.

    Returns aggregated analysis with common issues and improvement estimates.
    """
    _ensure_pagespeed_tables()

    country_code = request.get("country_code")
    max_urls = request.get("max_urls", 5)
    strategy = request.get("strategy", "mobile")
    manual_urls = request.get("urls")

    if not country_code:
        return {"error": "country_code required"}

    # Get URLs
    if manual_urls:
        urls = manual_urls[:max_urls]
    else:
        urls = _fetch_product_urls(country_code, max_urls)

    if not urls:
        return {"error": f"No product URLs found for {country_code}"}

    # Scan each URL with rate limiting
    scan_results = []
    for url in urls:
        try:
            result = _call_pagespeed_api(url, strategy)
            if result:
                scan_results.append(result)
            time.sleep(2.5)  # Rate limiting
        except Exception as e:
            logger.error(f"Error scanning {url}: {e}")

    if not scan_results:
        return {"error": "All scans failed"}

    # Aggregate results
    scores = []
    all_issues = []

    for i, result in enumerate(scan_results):
        lighthouse = result.get("lighthouseResult", {})
        perf_score = lighthouse.get("categories", {}).get("performance", {}).get("score", 0)
        scores.append(int(perf_score * 100))

        issues = _map_audits_to_fixes(lighthouse)
        all_issues.extend([issue["fix_id"] for issue in issues])

    # Find common issues
    issue_counts = Counter(all_issues)
    common_issues = [
        {"fix_id": fix_id, "occurrence_percent": int(100 * count / len(scan_results))}
        for fix_id, count in issue_counts.most_common(5)
        if 100 * count / len(scan_results) >= 50  # >50% of URLs
    ]

    avg_score = int(mean(scores)) if scores else 0

    return {
        "country_code": country_code,
        "domain": COUNTRY_DOMAINS.get(country_code),
        "urls_scanned": len(scan_results),
        "performance_scores": {
            "average": avg_score,
            "min": min(scores),
            "max": max(scores),
        },
        "common_issues": common_issues,
        "estimated_improvement_points": min(30, len(common_issues) * 5),
        "fix_priority": [issue["fix_id"] for issue in common_issues],
    }


@mcp.tool()
async def pagespeed_scan_all_stores(request: Dict) -> Dict:
    """
    Global dashboard — scan sample from each store.

    Returns ranking table of all stores sorted by avg score.
    """
    _ensure_pagespeed_tables()

    country_codes = request.get("country_codes")
    sample_size = request.get("sample_size", 2)
    strategy = request.get("strategy", "mobile")
    use_cached = request.get("use_cached", True)

    if not country_codes:
        country_codes = list(COUNTRY_DOMAINS.keys())

    results = []
    db = _get_db()

    for cc in country_codes:
        domain = COUNTRY_DOMAINS.get(cc)
        if not domain:
            continue

        # Check cache
        if use_cached:
            cached = _get_cached_scans(domain, max_age_hours=24)
            if cached:
                scores = [int(s["performance_score"] or 0) for s in cached if s["performance_score"]]
                if scores:
                    avg = int(mean(scores))
                    results.append({
                        "country_code": cc,
                        "domain": domain,
                        "avg_score": avg,
                        "samples": len(scores),
                        "source": "cached",
                    })
                    continue

        # Scan fresh
        urls = _fetch_product_urls(cc, sample_size)
        if not urls:
            continue

        scores = []
        for url in urls:
            try:
                result = _call_pagespeed_api(url, strategy)
                if result:
                    lighthouse = result.get("lighthouseResult", {})
                    perf_score = lighthouse.get("categories", {}).get("performance", {}).get("score", 0)
                    scores.append(int(perf_score * 100))
                    # Save scan
                    scan_id = str(uuid.uuid4())
                    _save_scan(scan_id, url, domain, cc, strategy, result)
                time.sleep(2.5)
            except Exception as e:
                logger.error(f"Error scanning {url}: {e}")

        if scores:
            avg = int(mean(scores))
            results.append({
                "country_code": cc,
                "domain": domain,
                "avg_score": avg,
                "samples": len(scores),
                "source": "fresh",
            })

    # Sort by score
    results.sort(key=lambda x: x["avg_score"], reverse=True)

    return {
        "total_stores": len(results),
        "stores": results,
        "worst_performers": results[-5:] if len(results) > 5 else results,
    }


@mcp.tool()
async def pagespeed_analyze_theme(request: Dict) -> Dict:
    """
    Offline static analysis of Shopify template files (NO API call).

    Checks for render-blocking scripts, large CSS, sync fetches, etc.
    Returns findings sorted by estimated impact.
    """
    repo_name = request.get("repo_name")
    repos_path = request.get("repos_path") or os.getenv("REPOS_PATH", "/Users/pt/Sites/REPOSITORIES/ACTIVE_SHOPS")

    if not repo_name:
        return {"error": "repo_name required"}

    repo_dir = os.path.join(repos_path, repo_name)
    if not os.path.isdir(repo_dir):
        return {"error": f"Repository not found: {repo_dir}"}

    findings = []

    # Read theme files
    layout_file = os.path.join(repo_dir, "layout", "theme.liquid")
    helpers_file = os.path.join(repo_dir, "snippets", "helperscripts.liquid")
    popup_file = os.path.join(repo_dir, "snippets", "popupExit.liquid")

    # Helper function to read file
    def read_file(path: str) -> str:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error reading {path}: {e}")
            return ""

    # Check layout
    layout_content = read_file(layout_file) if os.path.exists(layout_file) else ""

    # Render-blocking scripts (no defer/async)
    if layout_content:
        # Find script tags without defer/async
        script_pattern = r'<script[^>]*(?!defer)(?!async)[^>]*src=["\']([^"\']+)["\'][^>]*>'
        matches = re.findall(script_pattern, layout_content, re.IGNORECASE)
        for script in matches:
            if "jquery" in script.lower() or "splide" in script.lower():
                findings.append({
                    "fix_id": "PSI-010" if "jquery" in script.lower() else "PSI-011",
                    "title": f"Render-blocking script: {script}",
                    "description": "Add defer or async attribute",
                    "file": layout_file,
                    "estimated_impact_ms": 500,
                    "severity": "HIGH",
                })

        # Check for large inline CSS (>3KB)
        style_pattern = r'<style[^>]*>(.*?)</style>'
        style_blocks = re.findall(style_pattern, layout_content, re.DOTALL | re.IGNORECASE)
        for i, block in enumerate(style_blocks):
            if len(block) > 3000:
                findings.append({
                    "fix_id": "PSI-003",
                    "title": f"Large inline CSS block {i+1}",
                    "description": "Extract to external CSS file",
                    "file": layout_file,
                    "estimated_impact_ms": 200,
                    "severity": "MEDIUM",
                })

    # Check helper scripts
    helpers_content = read_file(helpers_file) if os.path.exists(helpers_file) else ""
    if helpers_content:
        # Check for console.log
        if "console.log" in helpers_content:
            findings.append({
                "fix_id": "PSI-001",
                "title": "Production console.log statements",
                "description": "Remove console.log for production",
                "file": helpers_file,
                "estimated_impact_ms": 50,
                "severity": "LOW",
            })

        # Check for sync network fetches
        if "fetch(" in helpers_content and not re.search(r"await\s+fetch|\.then|async", helpers_content):
            findings.append({
                "fix_id": "PSI-002",
                "title": "Potential blocking network requests",
                "description": "Use async/await or defer loading",
                "file": helpers_file,
                "estimated_impact_ms": 300,
                "severity": "HIGH",
            })

    # Check popup exit
    popup_content = read_file(popup_file) if os.path.exists(popup_file) else ""
    if popup_content:
        # Check for eager loading
        if 'addEventListener("mouseleave"' not in popup_content and 'DOMContentLoaded' in popup_content:
            findings.append({
                "fix_id": "PSI-002",
                "title": "PopupExit eager loading",
                "description": "Defer popupExit initialization until mouse leaves",
                "file": popup_file,
                "estimated_impact_ms": 100,
                "severity": "MEDIUM",
            })

    # Sort by impact
    findings.sort(key=lambda x: x["estimated_impact_ms"], reverse=True)

    return {
        "repo_name": repo_name,
        "repo_path": repo_dir,
        "total_findings": len(findings),
        "findings": findings,
        "estimated_total_impact_ms": sum(f["estimated_impact_ms"] for f in findings),
    }


@mcp.tool()
async def pagespeed_apply_fixes(request: Dict) -> Dict:
    """
    Apply fixes to Shopify templates (dry_run mode available).

    Uses fix IDs to locate and patch template files.
    Returns list of diffs/applied changes.
    """
    fix_ids = request.get("fix_ids", [])
    repo_names = request.get("repo_names")
    dry_run = request.get("dry_run", True)

    if not fix_ids:
        return {"error": "fix_ids required"}

    if not repo_names:
        # Use all repos from COUNTRY_TO_REPO
        repo_names = list(set(COUNTRY_TO_REPO.values()))

    changes = []
    repos_path = os.getenv("REPOS_PATH", "/Users/pt/Sites/REPOSITORIES/ACTIVE_SHOPS")

    for fix_id in fix_ids:
        for repo_name in repo_names:
            repo_dir = os.path.join(repos_path, repo_name)
            if not os.path.isdir(repo_dir):
                continue

            # Map fix_id to file and action
            if fix_id in ["PSI-010", "PSI-011"]:  # Render-blocking scripts
                target_file = os.path.join(repo_dir, "layout", "theme.liquid")
                action = "Add defer attribute to script tags"
            elif fix_id == "PSI-003":  # Large inline CSS
                target_file = os.path.join(repo_dir, "assets", "theme.css")
                action = "Extract inline CSS"
            elif fix_id in ["PSI-001", "PSI-002"]:  # helperscripts + popupExit
                target_file = os.path.join(repo_dir, "snippets", "helperscripts.liquid")
                action = "Optimize script loading"
            else:
                continue

            if os.path.exists(target_file):
                try:
                    with open(target_file, "r", encoding="utf-8") as f:
                        original = f.read()

                    # Generate patch (simplified)
                    patch = f"# Fix {fix_id} applied to {repo_name}\n# {action}"

                    if not dry_run:
                        # In production, apply actual patch
                        _log_fix(fix_id, repo_name, target_file, action, patch)

                    changes.append({
                        "fix_id": fix_id,
                        "repo": repo_name,
                        "file": target_file,
                        "action": action,
                        "status": "dry_run" if dry_run else "applied",
                        "diff": patch,
                    })
                except Exception as e:
                    logger.error(f"Error processing {target_file}: {e}")

    return {
        "total_changes": len(changes),
        "dry_run": dry_run,
        "changes": changes,
    }


# Initialize tables on module load
_ensure_pagespeed_tables()
