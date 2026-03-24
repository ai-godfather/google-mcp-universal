---
name: ads-optimize-shopping-pmax
description: Comprehensive Shopping + Performance Max optimization — product audit, device, time, location, assets, search terms, impression share, spend anomalies, spiral-of-death detection, asset group segmentation
---

# Comprehensive Shopping + PMax Optimization Audit

## Command Syntax
```
/ads-optimize-shopping-pmax {CC} [{days}]
```

### Parameters
- **CC** (required): Country code (e.g., RO, PL, TR, HU, GR, DE, IT, FR, ES, CZ, SK, BG)
- **days** (optional): Analysis period in days. Default: 365

### Example Usage
```
/ads-optimize-shopping-pmax RO
/ads-optimize-shopping-pmax PL 90
/ads-optimize-shopping-pmax TR 30
```

---

## Pre-Audit: Multi-Campaign Discovery & Health Check

**CRITICAL — Before running any phase, perform these checks:**

### 1. Discover ALL campaigns for the country (including PAUSED)
```sql
SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type,
       campaign.bidding_strategy_type, campaign_budget.amount_micros,
       campaign.maximize_conversion_value.target_roas,
       campaign.shopping_setting.merchant_id,
       metrics.cost_micros, metrics.conversions, metrics.conversions_value, metrics.impressions
FROM campaign
WHERE campaign.advertising_channel_type IN ('SHOPPING', 'PERFORMANCE_MAX')
  AND campaign.status != 'REMOVED'
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
ORDER BY metrics.cost_micros DESC
```

Filter by CC in campaign name. **Include PAUSED campaigns** — they may represent lost reach.

### 2. Spiral-of-Death Detection (CRITICAL)
For each ENABLED campaign, check:
- **Target ROAS** (from `campaign.maximize_conversion_value.target_roas`)
- **Actual ROAS** (from `metrics.conversions_value / metrics.cost_micros * 1e6`)
- **Gap**: If target ROAS > actual ROAS by >15% → **SPIRAL ALERT**
  - System throttles spend because it can't hit target
  - Less spend → less data → worse optimization → worse ROAS → more throttling
  - **Action**: Lower target ROAS to (actual ROAS - 10%) to break the spiral

### 3. PAUSED Sibling Campaign Check
Look for PAUSED campaigns with same CC that were recently active:
- If PAUSED campaign has conversions in the analysis period → it was contributing
- Calculate combined country spend (ENABLED + PAUSED)
- **Alert if PAUSED campaigns represent >30% of historical country spend**
- **Recommend**: Consider reactivating PAUSED campaigns to restore reach

### 4. Merchant Center Feed Diagnostics
Extract `campaign.shopping_setting.merchant_id` for each campaign:
- Try accessing MC via `merchant_center_list_products(merchant_id)`
- If accessible: count active products, check for disapprovals
- If NOT accessible: **Flag as potential feed issue** — user must check MC manually
- Compare current product count with historical (via `shopping_performance_view` unique item_ids)
- **Alert if product count dropped >30%** vs 6 months ago

### 5. Budget Adequacy Check
For each ENABLED campaign:
- Current daily budget (from `campaign_budget.amount_micros`)
- Actual daily spend (from monthly data)
- Peak daily spend (from historical data)
- **Alert if current budget < 50% of peak** → campaign may be artificially constrained

Present ALL findings before proceeding to phases.

---

## Workflow — 8 Phases

### Phase 1: Campaign Discovery

**Objective:** Identify all ENABLED Shopping and PMax campaigns for the specified country.

**Process:**
1. Execute GAQL query to fetch all ENABLED Shopping and PMax campaigns:
```sql
SELECT campaign.id, campaign.name, campaign.advertising_channel_type,
       campaign.bidding_strategy_type, metrics.cost_micros, metrics.conversions,
       metrics.conversions_value, metrics.impressions, metrics.clicks
FROM campaign
WHERE campaign.status = 'ENABLED'
  AND campaign.advertising_channel_type IN ('SHOPPING', 'PERFORMANCE_MAX')
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
```

2. Filter by country code in campaign name (e.g., campaign name contains "RO", "Romania", "TR", "Türkiye")

3. **CRITICAL VERIFICATION:** campaign.name LIKE '%RO%' can match false positives such as "ECOM_PROFIT".
   - Manually verify campaign names
   - Discard matches that don't contain the target country code as a distinct token
   - Use specific campaign IDs for subsequent queries

4. **Output:** Campaign overview table with:
   - Campaign ID, Name, Type (SHOPPING/PERFORMANCE_MAX)
   - Bidding Strategy, Days Active, Status
   - Total Spend, Total Conversions, Conversion Value, ROAS
   - Impressions, Clicks, CTR, CPA

5. **Summary Metrics:**
   - Total campaigns by type (Shopping vs PMax)
   - Total spend across all campaigns
   - Blended ROAS, CPA, CTR

---

### Phase 2: Product Performance Audit (HIGHEST VALUE)

**Objective:** Identify waste products, top performers, feed quality issues, and spend concentration patterns.

**Process:**

1. Query `shopping_performance_view` per campaign:
```sql
SELECT segments.product_item_id, segments.product_title,
       metrics.clicks, metrics.impressions, metrics.cost_micros,
       metrics.conversions, metrics.conversions_value
FROM shopping_performance_view
WHERE campaign.id IN ({campaign_ids})
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
ORDER BY metrics.cost_micros DESC
```

2. **Product Variant Analysis:**
   - Aggregate by base product (strip variant suffix _N from item_id)
   - Count variants per base product
   - **Variant Strategy Insight**: More variants = more auction entries = more impressions
   - But: Low-CTR variants waste budget → keep only best-converting images
   - Ideal: 1-3 high-quality variants per product

3. **Historical Product Count Comparison:**
   - Compare unique item_ids in current period vs 6 months ago
   - If count dropped >30%: flag as potential feed issue or intentional cleanup
   - Present: "Lipiec 2025: X produktów → Marzec 2026: Y produktów (Z%)"

4. **Classification & Analysis:**

   **Waste Products** (Exclusion Candidates):
   - Spend > $10 AND Conversions = 0
   - Action: Recommend exclusion via `google_ads_add_product_exclusion`

   **Low ROAS Products** (Exclusion Candidates):
   - Spend > $20 AND ROAS < 0.5
   - Action: Recommend exclusion

   **Zombie Products** (Feed Quality Issue):
   - Impressions > 1000 AND Clicks = 0
   - Action: Flag for feed quality audit, do NOT exclude (likely approval issue)

   **Top Performers** (Protect & Scale):
   - Highest ROAS with significant volume (clicks > 100)
   - Action: Recommend dedicated asset group (see Phase 6)

   **Spend Concentration Analysis:**
   - Calculate cumulative % of spend and conversions for top products
   - Flag if top 20% products > 80% of budget → single-product risk
   - Flag if top 20% products < 20% of conversions → unprofitable concentration

5. **Output Tables:**
   - Waste Products Table (top 20 by spend)
   - Low ROAS Products Table
   - Zombie Products Table (if any)
   - Top Performers by ROAS (top 10)
   - Product Count Historical Comparison
   - Spend Distribution Chart (% of budget)
   - ROAS Distribution Buckets (>3, 2-3, 1.3-2, 1-1.3, 0.5-1, <0.5, 0)
   - Product Count Summary (total, active, waste, zombie)

---

### Phase 3: Device Optimization

**Objective:** Analyze device-level performance and recommend bid modifiers (Shopping only).

**Process:**

1. Query device breakdown per campaign:
```sql
SELECT campaign.id, campaign.name, segments.device,
       metrics.impressions, metrics.clicks, metrics.cost_micros,
       metrics.conversions, metrics.conversions_value
FROM campaign
WHERE campaign.id IN ({campaign_ids})
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
  AND segments.device IN ('MOBILE', 'TABLET', 'DESKTOP', 'CONNECTED_TV')
```

2. **Device Rules:**

   **Shopping Campaigns:**
   - Device bid modifiers WORK and are applied directly
   - Used as signals by Smart Bidding algorithms
   - Formula: `recommended_modifier = actual_roas / campaign_avg_roas`

   **PMax Campaigns:**
   - Device bid modifiers DO NOT WORK (returns OPERATION_NOT_PERMITTED_FOR_CAMPAIGN_TYPE)
   - Device-level optimization limited to analysis/reporting only
   - Note in report as informational only

3. **Optimization Rules:**
   - Tablet: If ROAS < 70% of avg → -30%; If < 50% → -50%
   - Desktop: If ROAS < 70% of avg → -20%; If < 50% → -30%
   - Mobile: NEVER reduce (typically highest volume); If ROAS > 120% of avg → monitor
   - Connected TV: Recommend -100% for e-commerce

4. **Application (Shopping Only):**
   - Apply via `google_ads_update_device_bid_modifier`

---

### Phase 4: Time-of-Day Scheduling

**Objective:** Identify waste hours and create ad schedules to exclude them.

**Process:**

1. Query hourly performance per campaign (use `campaign` resource, NOT `ad_group`):
```sql
SELECT campaign.id, segments.hour,
       metrics.impressions, metrics.clicks, metrics.cost_micros,
       metrics.conversions, metrics.conversions_value
FROM campaign
WHERE campaign.id = {id}
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
ORDER BY segments.hour ASC
```

**NOTE:** ad_group hourly query can exceed 1000-row limit. Use campaign resource (returns max 24 rows per campaign).

2. **Schedule Rules — Shopping Campaigns:**
   - Ad schedules work
   - Bid modifiers on schedules are IGNORED by Smart Bidding — do not apply
   - Use schedules ONLY for hour exclusion

3. **Schedule Rules — PMax Campaigns:**
   - Ad schedules work WITHOUT bid_modifier field
   - Setting bid_modifier causes OPERATION_NOT_PERMITTED_FOR_CONTEXT / UBERVERSAL error
   - Create schedules covering ONLY active hours (not-covered hours won't serve)

4. **Important: Low-volume campaigns**
   - If campaign spends < €500/month, ad schedule restrictions can further reduce volume
   - **Recommend AGAINST schedules for low-volume campaigns** — better to let algorithm learn
   - Only apply schedules for campaigns with clear waste hours AND sufficient volume

---

### Phase 5: Location Analysis & Exclusions

**Objective:** Identify waste locations and optimize location targeting strategy.

**Process:**

1. Query city/county level location data:
```sql
SELECT campaign.id, segments.geo_target_most_specific_location,
       geographic_view.country_criterion_id,
       metrics.impressions, metrics.clicks, metrics.cost_micros,
       metrics.conversions, metrics.conversions_value
FROM geographic_view
WHERE campaign.id IN ({campaign_ids})
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
  AND metrics.impressions > 50
ORDER BY metrics.cost_micros DESC
```

2. **Location ID Resolution:**
   - Batch resolve geo_target_constant IDs to human-readable names:
```sql
SELECT geo_target_constant.id, geo_target_constant.name,
       geo_target_constant.canonical_name, geo_target_constant.target_type
FROM geo_target_constant
WHERE geo_target_constant.id IN (...)
```
   - Batch in groups of ~40 IDs

3. **Foreign Traffic Check:**
   - Filter locations where `country_criterion_id` != target country
   - If foreign traffic > 5% of spend → **set PRESENCE targeting** immediately

4. **Location Targeting Options:**
   - Set PRESENCE only to eliminate foreign "interest" traffic
   - Apply via `google_ads_set_campaign_location_options`

---

### Phase 6: Asset Performance & Group Segmentation (PMax — CRITICAL)

**Objective:** Analyze assets AND recommend asset group segmentation for top products.

**Process:**

1. Query assets:
```sql
SELECT campaign.id, asset_group.id, asset_group.name,
       asset_group_asset.field_type, asset_group_asset.status,
       asset.id, asset.type, asset.text_asset.text,
       asset.image_asset.full_size.url
FROM asset_group_asset
WHERE campaign.id IN ({pmax_campaign_ids})
```

2. **Asset Group Health Checks:**
   - Headlines: min 3, max 15 (recommend 10+) ✅/❌
   - Long Headlines: min 1, max 5 (recommend 3+) ✅/❌
   - Descriptions: min 2, max 5 (recommend 4+) ✅/❌
   - Images: min 3, max 20 (recommend 10+) ✅/❌
   - Logos: min 1 ✅/❌

3. **CRITICAL — Asset Group Segmentation Best Practice:**

   **Problem**: One "All Products" asset group with generic headlines = poor relevance.

   **Solution**: Create dedicated asset groups for TOP CONVERTING products:

   a. Identify top 5-10 products by conversion value from Phase 2
   b. For each top product, recommend a NEW asset group with:
      - **Listing group filter**: `custom_attribute_0 = "{product-slug}"` (or by item_id)
      - **Product-specific headlines** in local language (e.g., "Depanten — Fájdalomcsillapító Krém")
      - **Product-specific descriptions** highlighting USP, ingredients, benefits
      - **Product-specific images** (from feed or creatives)
   c. Keep "All Products" as catch-all for remaining products
   d. **Expected impact**: Higher CTR → lower CPC → better ROAS → more impressions

4. **Listing Group Structure Analysis:**
   - Query current listing group filters:
```sql
SELECT asset_group.id, asset_group.name,
       asset_group_listing_group_filter.type,
       asset_group_listing_group_filter.case_value.product_custom_attribute.value,
       asset_group_listing_group_filter.case_value.product_custom_attribute.index,
       asset_group_listing_group_filter.case_value.product_item_id.value
FROM asset_group_listing_group_filter
WHERE campaign.id = {campaign_id}
```
   - Check if catch-all exists (products without matching filters should still be included)
   - Check for Channable-style custom_attribute segmentation (custom_attribute_0 = product slug, custom_attribute_3 = converting/non-converting)
   - **Alert if no catch-all** → products not matching any filter won't serve!

5. **Copy Quality Check:**
   - Flag headlines with typos or spacing issues (e.g., "Különleges ajánlatmost" → missing space)
   - Flag headlines that are too generic ("megvesz" = "buy" — too short, low quality)
   - Flag headlines missing product names (for single-product asset groups)
   - Recommend: Include product name + key benefit + CTA in headlines

---

### Phase 7: Search Term Mining & Negative Keywords (PMax)

**Objective:** Identify irrelevant search categories and add negative keywords.

**Process:**

1. Query search terms from PMax campaigns:
```sql
SELECT campaign.id, campaign_search_term_insight.category_label
FROM campaign_search_term_insight
WHERE campaign.id IN ({pmax_campaign_ids})
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
  AND campaign_search_term_insight.category_label IS NOT NULL
ORDER BY metrics.cost_micros DESC
```

2. **Irrelevant Category Classification:**
   - Flag competitor + offline retail terms: "rossmann", "dm", "lidl", "farmacia tei"
   - Flag unrelated product terms: "we vibe", "removio" (not our products)
   - Flag generic offline intent: "{product} rossmann preis"

3. **Application:**
   - Apply via `google_ads_pmax_negative_keywords`
   - PMax negative keywords are campaign-level shared lists

---

### Phase 8: Impression Share & Spend Anomaly Detection

**Objective:** Diagnose budget constraints, competitive position, and spend anomalies.

**Process:**

1. **Impression Share:**
```sql
SELECT campaign.id, campaign.name,
       metrics.search_impression_share,
       metrics.search_budget_lost_impression_share,
       metrics.search_rank_lost_impression_share
FROM campaign
WHERE campaign.id IN ({campaign_ids})
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
```

2. **Monthly Spend Trend:**
```sql
SELECT campaign.id, segments.month,
       metrics.impressions, metrics.clicks, metrics.cost_micros,
       metrics.conversions, metrics.conversions_value
FROM campaign
WHERE campaign.id IN ({campaign_ids})
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
ORDER BY segments.month DESC
```

3. **Anomaly Detection:**
   - Month-over-month comparison: flag drops >40% in cost or impressions
   - CPC trend: flag increases >50% in CPC (suggests competition or quality issues)
   - ROAS trend: flag sustained decline over 3+ months
   - **Cliff drop detection**: if single-day drop >50% in impressions → investigate:
     - Feed issue? Budget change? Campaign pause? Disapprovals?
     - Use daily data around the cliff date to pinpoint

4. **Daily Drill-Down for Anomalies:**
   If monthly data shows anomaly, drill into daily data:
```sql
SELECT campaign.id, segments.date,
       metrics.impressions, metrics.clicks, metrics.cost_micros
FROM campaign
WHERE campaign.id = {campaign_id}
  AND segments.date BETWEEN '{anomaly_start}' AND '{anomaly_end}'
ORDER BY segments.date ASC
```

---

## Presentation & Application Rules

### Output Language
- **ALL output in Polish** (campaign names, table headers, explanations, recommendations)

### Presentation Structure
1. **Pre-Audit**: Multi-campaign discovery, spiral detection, feed diagnostics
2. **Phase 1-8**: One section per phase with summary table + key findings + recommendations
3. **Summary Section**: Consolidated list of ALL proposed actions with estimated impact
4. **Confirmation Step**: ASK FOR USER CONFIRMATION before applying ANY changes
5. **Application**: Apply all approved changes and report results

### Confirmation Format
```
Proponowane zmiany do wprowadzenia:
[ List all changes ]

Które działania chcesz wdrożyć? (Wszystkie / Wybrane numery / Żadne)
```

### Application Order
1. Bidding strategy adjustments (tROAS changes — break spiral first!)
2. Campaign status changes (unpause sibling campaigns)
3. Budget adjustments
4. Product exclusions
5. Device modifiers (Shopping only)
6. Location exclusions & PRESENCE targeting
7. Ad schedules
8. Negative keywords

---

## Important Technical Notes (Production Experience)

1. **Campaign Name Matching:**
   - campaign.name LIKE '%RO%' matches false positives (e.g., "ECOM_PROFIT")
   - ALWAYS verify with specific campaign IDs

2. **Hourly Query Limits:**
   - ad_group resource hourly query can exceed 1000-row limit
   - Use campaign resource instead (max 24 rows per campaign)

3. **PMax Limitations:**
   - Does NOT support device bid modifiers (OPERATION_NOT_PERMITTED_FOR_CAMPAIGN_TYPE)
   - Does NOT support bid_modifier on ad_schedule criteria (UBERVERSAL error)
   - Does NOT return Impression Share data reliably
   - Use ad schedules for hour coverage ONLY (omit bid_modifier field)

4. **Location Query:**
   - geographic_view works for location data (user_location_view also works)
   - Batch geo_target_constant resolution in groups of ~40 IDs
   - Filter by country_criterion_id to detect foreign traffic

5. **Merchant Center Access:**
   - Extract merchant_id from `campaign.shopping_setting.merchant_id`
   - MC API access varies — if 'NoneType' errors, flag for manual check

6. **Change Event Limitation:**
   - change_event API only goes back 30 days
   - For older history, rely on monthly/daily metrics to detect changes

7. **Asset Group Listing Filters:**
   - Channable uses custom_attribute_0 (product slug) and custom_attribute_3 (converting/non-converting)
   - Always verify catch-all exists to avoid blocking products
   - `asset_group_listing_group_filter.type = UNIT_INCLUDED` with empty case_value = catch-all

8. **Low-Volume Campaign Warning:**
   - Campaigns spending < €500/month have insufficient data for reliable optimization
   - Focus on fixing structural issues (spiral, feed, budget) before tactical optimization
   - Ad schedule restrictions on low-volume campaigns can make things worse

---

## Date Range Calculation

- **end_date:** Today's date (automatic)
- **start_date:** Today's date minus {days} parameter
- Default {days}: 365 (12 months)
- Format: YYYY-MM-DD

---

## Error Handling

- **Campaign not found:** "Nie znaleziono kampanii dla {CC} w podanym okresie"
- **Insufficient data:** "Niewystarczająco danych dla fazy {N}"
- **API error:** Display error message with phase name and recovery action
- **Spiral detected:** Prioritize tROAS fix BEFORE other optimizations

---

## Success Criteria

Command succeeds when:
1. All campaigns for {CC} identified (including PAUSED)
2. Spiral-of-death check completed
3. Phases 1-8 complete with data
4. User confirms changes
5. All approved changes applied successfully
6. Final summary report generated with monitoring recommendations
