---
description: Performance report with trends and recommendations
allowed-tools: ["mcp__google-ads__google_ads_get_performance_report"]
argument-hint: <CC> <campaign_id> <days>
---

Generate a performance report for country `$1`, campaign `$2`, over the last `$3` days. Present in Polish.

**Steps:**
1. Calculate date range: end_date = today, start_date = today minus $3 days
2. Call `google_ads_get_performance_report` with customer_id="YOUR_CUSTOMER_ID", campaign_id="$2", start_date, end_date
3. Calculate: CTR, CPC, CPA, conversion_rate, ROAS (convert micros to readable values)
4. Compare vs previous period (same number of days immediately before)
5. Flag anomalies: CTR drop > 20%, CPC spike > 30%
6. Return formatted report in Polish with metrics table, trend analysis, and recommendations
