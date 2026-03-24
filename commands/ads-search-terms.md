---
description: Search terms analysis with negative keyword suggestions
allowed-tools: ["mcp__google-ads__google_ads_list_search_terms", "mcp__google-ads__google_ads_add_negative_keyword"]
argument-hint: <CC> <campaign_id> <days>
---

Analyze search terms for country `$1`, campaign `$2`, over last `$3` days. Present in Polish.

**Steps:**
1. Calculate date range: end_date = today, start_date = today minus $3 days
2. Call `google_ads_list_search_terms` with customer_id="YOUR_CUSTOMER_ID", campaign_id="$2", start_date, end_date
3. Filter wasteful queries: CTR < average, CPC > average
4. Group by intent: brand, competitor, irrelevant
5. Suggest negative keywords to add (do NOT add them without user confirmation)
6. Return analysis with recommendations table

Wait for user confirmation before adding any negative keywords.
