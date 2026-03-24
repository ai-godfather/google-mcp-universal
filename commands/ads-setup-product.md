---
description: Setup specific products in a campaign
allowed-tools: ["mcp__google-ads__batch_setup_products"]
argument-hint: <CC> <campaign_id> <handle1,handle2,...>
---

Setup specific products for country `$1`, campaign `$2`. Product handles: `$3` (comma-separated). Present in Polish.

**Steps:**
1. Parse comma-separated handles from "$3" into a list
2. Call `batch_setup_products` with country_code="$1", campaign_id="$2", product_handles=[parsed list]
3. Show progress: created ad groups, RSAs, callouts, sitelinks, promotions, snippets, keywords, images
4. Return per-product summary

Always use customer_id `YOUR_CUSTOMER_ID`.
