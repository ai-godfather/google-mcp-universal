---
description: Compare two campaigns performance side-by-side
allowed-tools: ["mcp__google-ads__google_ads_get_performance_report"]
argument-hint: <campaign_id_1> <campaign_id_2> <days>
---

Compare two campaigns side-by-side: campaign `$1` vs campaign `$2`, over last `$3` days. Present in Polish.

**Steps:**
1. Calculate date range: end_date = today, start_date = today minus $3 days
2. Call `google_ads_get_performance_report` for campaign "$1" with customer_id="YOUR_CUSTOMER_ID"
3. Call `google_ads_get_performance_report` for campaign "$2" with customer_id="YOUR_CUSTOMER_ID"
4. Calculate for both: impressions, clicks, cost, CTR, CPC, conversions, ROAS
5. Compare: delta %, highlight winner/loser per metric
6. Identify setup gaps (which campaign has more products, assets, keywords)
7. Return side-by-side comparison table with deltas and recommendations
