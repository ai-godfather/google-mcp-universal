---
description: PageSpeed Insights scan, analysis and optimization for store landing pages
allowed-tools: ["mcp__google-ads__pagespeed_scan_url", "mcp__google-ads__pagespeed_scan_store", "mcp__google-ads__pagespeed_scan_all_stores", "mcp__google-ads__pagespeed_analyze_theme", "mcp__google-ads__pagespeed_apply_fixes"]
---

# /ads-pagespeed — PageSpeed Performance Scanner

Scan store landing pages via Google PageSpeed Insights API, collect data, analyze patterns, and recommend optimizations.

## Usage

Parse the user's input to determine the mode:

### Mode 1: Scan a single URL
If user provides a full URL (starts with http):
1. Call `pagespeed_scan_url` with the URL and strategy="mobile"
2. Present results as table:
   - Performance Score (with color indicator: <50 red, 50-89 orange, 90+ green)
   - Core Web Vitals (lab): FCP, LCP, TBT, CLS, SI
   - Core Web Vitals (field/CrUX) if available
   - Top 5 Opportunities sorted by savings_ms
   - Mapped fix IDs with descriptions
   - Estimated score after fixes

### Mode 2: Scan a store by country code
If user provides a 2-letter country code (e.g., TR, PL, DE):
1. Call `pagespeed_scan_store` with country_code and max_urls=5, strategy="mobile"
2. Present aggregated results:
   - Summary: avg/min/max scores
   - Per-URL score table
   - Common issues (appearing in >50% of URLs) — these are TEMPLATE issues
   - URL-specific issues — these are CONTENT issues
   - Prioritized fix plan with estimated improvement
   - Compare with previous scan if available (before/after)

### Mode 3: Scan all stores (global dashboard)
If user says "all", "global", "dashboard":
1. Call `pagespeed_scan_all_stores` with sample_size=2, strategy="mobile"
2. Present ranking table sorted by avg score (worst first):
   | Rank | CC | Domain | Avg Score | Worst URL | Template | Fix IDs | Est. After |
3. Highlight stores below score 50 as CRITICAL
4. Show cross-store pattern analysis

### Mode 4: Offline template analysis
If user says "analyze" + repo name or country code:
1. Call `pagespeed_analyze_theme` with the repo name
2. Present findings sorted by impact:
   - Each finding: severity, file, line, description, estimated savings
   - Total estimated savings in ms
   - Applicable fix IDs

## Output Format

Always present in Polish. Use tables for data. Include:
- Score interpretation (0-49: Wolny, 50-89: Wymaga poprawy, 90-100: Szybki)
- CWV status (Zdane/Niezdane based on thresholds: LCP<2.5s, INP<200ms, CLS<0.1)
- Actionable next steps (suggest /ads-pagespeed-fix for applying fixes)

## Important Notes
- PSI API requires API key (env PSI_API_KEY). Without it, quota = 0/day.
- Rate limit: max 1 request per 2.5 seconds
- 10 shops have NO GitHub theme (AL, BA, CL, ID, MK, OM, PH, RS, SG, VN) — scan only, cannot fix
- Multi-instance stores: MULTI2 (49 countries), MULTI1 (38), EU (8), WWW1 (7), EU2 (5)
