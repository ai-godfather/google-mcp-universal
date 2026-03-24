---
name: ads-keyword-trends
description: Keyword spend trend analysis — period-over-period comparison with anomaly detection
---

# /ads-keyword-trends {CC} [{campaign_id}] [{days}]

Comprehensive keyword-level spend and conversion trend analysis with period-over-period comparison.
Identifies drastic changes, validates all data directly from Google Ads API, and produces
a prioritized action plan.

## Parameters
- `CC` (required): Country code (RO, TR, PL, HU, FR, DE, IT, ES, CZ, SK, BG, GR, etc.)
- `campaign_id` (optional): Specific campaign ID. Default: all ENABLED Search campaigns for CC.
- `days` (optional): Period length in days. Default: 90. Compares last N days vs previous N days.

## Execution Steps

1. Parse parameters from user input

2. Call `batch_keyword_spend_analysis`:
   - country_code = CC
   - campaign_ids = [campaign_id] if provided, else []
   - current_period_days = days (default 90)
   - previous_period_days = days (default 90)
   - spike_threshold_pct = 50
   - top_n = 50
   - include_monthly_breakdown = True
   - include_product_mapping = True

3. If response.status == "submitted" (async for >3 campaigns):
   - Poll `batch_keyword_spend_analysis_status(job_id)` every 20 seconds
   - Show progress to user: "Analizuję keywordy: X/Y kampanii (Z%)..."
   - Continue until status == "completed" or "failed" or "rate_limited"

4. When data is ready, present FULL report in Polish with ALL of the following sections:

   ## SECTION A: PODSUMOWANIE WYKONAWCZE
   - Period definitions: "Bieżący okres: YYYY-MM-DD → YYYY-MM-DD" vs "Poprzedni okres: ..."
   - High-level totals table:
     | Metryka | Poprzedni okres | Bieżący okres | Zmiana | Zmiana % |
     | Łączne wydatki | ... | ... | ... | ... |
     | Łączne kliknięcia | ... | ... | ... | ... |
     | Łączne konwersje | ... | ... | ... | ... |
     | Średni CPC | ... | ... | ... | ... |
     | Średni CPL (CPA) | ... | ... | ... | ... |
     | Łączny ROAS | ... | ... | ... | ... |
   - Trend distribution: ile keywordów SPIKE / DROP / STABLE / NEW / STOPPED
   - Severity distribution: CRITICAL / HIGH / MEDIUM / LOW
   - Category distribution: scaling_winner / spend_waste / efficiency_up / declining / new / dead / stable

   ## SECTION B: TOP WZROSTY WYDATKÓW (SPEND SPIKES)
   Table with ALL metrics for keywords where spend increased >50%:
   | Keyword | Kampania | Ad Group | Wydatki prev | Wydatki curr | Δ% | Imp prev | Imp curr | Clicks prev | Clicks curr | Conv prev | Conv curr | CPC prev | CPC curr | CPA prev | CPA curr | ROAS prev | ROAS curr | Trend | Severity |
   - Highlight rows where spend went up BUT conversions went down (spend_waste = RED FLAG)
   - Show up to top_n rows sorted by pct_cost_micros DESC

   ## SECTION C: TOP SPADKI WYDATKÓW (SPEND DROPS)
   Same full-metric table for keywords where spend decreased >50%
   - Highlight rows where conversions also dropped (declining = investigate)
   - Highlight rows where conversions stayed stable despite spend drop (efficiency_up = good)

   ## SECTION D: DRASTYCZNE ZMIANY KONWERSJI
   Keywords with >50% change in conversions (regardless of spend direction):
   - Conversion spikes: conversions went up >50% — are we scaling winners or is it noise?
   - Conversion drops: conversions went down >50% — even if spend is stable, this needs attention
   Table: Keyword | Conv prev | Conv curr | Δ conv % | Cost prev | Cost curr | Δ cost % | CPA prev | CPA curr | Campaign | AdGroup

   ## SECTION E: MARNOWANIE BUDŻETU (SPEND WASTE ALERTS)
   Keywords where spend increased but conversions decreased or stayed flat:
   Table + for EACH keyword, concrete recommendation:
   - "Pause keyword X — spend +350%, conversions -80%, CPA wzrósł 5x"
   - "Obniż bid na keyword Y — CPC wzrósł +120% bez wzrostu konwersji"
   Total wasted spend estimate: SUM(delta_cost) for spend_waste keywords

   ## SECTION F: TOP PERFORMERS — NAJLEPSZY ROAS (bieżący okres)
   Table: Keyword | ROAS | Conversions | Conv Value | Cost | CPC | CTR | Campaign
   - These are keywords worth SCALING (increase bid, expand match type)

   ## SECTION G: BOTTOM PERFORMERS — NAJGORSZY CPA (bieżący okres)
   Table: Keyword | CPA | Cost | Conversions | ROAS | Clicks | CTR | Campaign
   - These are keywords to PAUSE or drastically reduce bids

   ## SECTION H: BOTTOM PERFORMERS — NAJGORSZY CPA (poprzedni okres)
   Same structure but for previous period
   - Compare: which bottom performers from last period improved? Which got worse?
   - Cross-reference with current period to show trajectory

   ## SECTION I: TOP KEYWORDS OSTATNI MIESIĄC vs TOP KEYWORDS TEN MIESIĄC
   Side-by-side ranking comparison:
   - "Top 20 keywords by spend — last full month of previous period"
   - "Top 20 keywords by spend — last full month of current period"
   Show: rank changes, new entrants, dropouts
   Table: Rank prev | Rank curr | Keyword | Spend prev month | Spend curr month | Conv prev | Conv curr | Δ spend % | Δ conv %

   ## SECTION J: NOWE KEYWORDY (pojawiły się w bieżącym okresie)
   Table: Keyword | Spend | Clicks | Conversions | CPC | CPA | ROAS | Campaign | AdGroup
   - Flag high-spend-no-conversion new keywords as risk

   ## SECTION K: ZNIKNĘŁE KEYWORDY (były w poprzednim, nie ma w bieżącym)
   Table: Keyword | Prev Spend | Prev Conversions | Prev ROAS | Campaign
   - Flag keywords that were converting well but stopped (lost opportunity)

   ## SECTION L: BREAKDOWN MIESIĘCZNY (month-by-month)
   Table: Miesiąc | Wydatki | Impressions | Kliknięcia | CTR | Konwersje | Conv Value | CPC | CPA | ROAS | Active Keywords
   - One row per month, chronological
   - Visual trend indicators (↑ ↓ →)

   ## SECTION M: ROLLUP PER KAMPANIA
   Table: Kampania | Keywords | Wydatki prev | Wydatki curr | Δ% | Conv prev | Conv curr | Δ% | Spikes | Drops | Waste Alerts
   - For TOP 3 campaigns by spend: also show AdGroup-level breakdown with conversion changes

   ## SECTION N: ROLLUP PER AD GROUP (top campaigns only)
   For each of the top 3 campaigns by current spend, show:
   Table: Ad Group | Keywords | Spend prev | Spend curr | Δ% | Conv prev | Conv curr | Δ% | CPA prev | CPA curr | ROAS prev | ROAS curr
   - Highlight ad groups with drastic conversion changes

   ## SECTION O: REKOMENDACJE PRIORYTETYZOWANE
   Ordered by priority (KRYTYCZNE first):
   1. [KRYTYCZNE] Specific keyword/action — with data backing
   2. [WYSOKIE] Specific keyword/action — with data backing
   3. [ŚREDNIE] Specific keyword/action — with data backing
   Each recommendation MUST include:
   - The specific keyword text
   - Campaign and ad group name
   - The data that supports the recommendation (e.g., "spend +200%, conversions -50%")
   - The concrete action (pause, reduce bid by X%, increase bid, change match type, add negative)

   ## SECTION P: OGRANICZENIA I UWAGI (Data Completeness Disclosure)
   MANDATORY — always include this section:
   - PMax campaigns excluded (no keyword-level data in PMax)
   - Shopping campaigns excluded (keyword data not available)
   - Keywords with <{min_impressions} impressions filtered out
   - Conversion attribution: Google's default attribution model (data-driven or last-click)
   - conversion_value may be 0 for countries without value tracking → ROAS = 0 does NOT mean no conversions
   - Quality scores NOT included unless include_quality_scores=True (extra API cost)
   - GAQL 1000-row pagination: data is auto-paginated, but if a single campaign has >20,000 keywords per period, the tail may be truncated
   - If any campaigns returned errors or empty data, list them explicitly
   - If job was rate_limited and data is partial, disclose which campaigns are missing

5. FORMATTING RULES:
   - All monetary values in local currency with proper formatting (e.g., "PLN 1,234.56" not "1234560000 micros")
   - All percentages with sign ("+18.7%" or "-5.3%")
   - All tables include BOTH absolute values AND % changes
   - Use "N/A" for unavailable metrics, never leave blank
   - Every finding and hypothesis MUST be supported by direct data from the report
   - Do NOT speculate or hallucinate data — if a metric is not in the report, say so explicitly

6. If the dataset is too large (>5000 keywords), the tool automatically chunks analysis via async worker.
   The final report aggregates all chunks. If chunking occurred, mention it in Section P.

7. CONCLUDE with Section O (Rekomendacje) as the final actionable takeaway — this should be the
   part the user reads first after the executive summary.
