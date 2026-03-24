---
description: Check and auto-fix disapproved ads/assets
allowed-tools: ["mcp__google-ads__batch_check_eligibility"]
argument-hint: <CC> <campaign_id>
---

Check ad/asset eligibility for country `$1`, campaign `$2`. Present in Polish.

**Steps:**
1. Call `batch_check_eligibility` with country_code="$1", campaign_id="$2" — query API for disapproved ads
2. For each disapproved ad (status=NOT_ELIGIBLE): auto-pause
3. For each NOT_ELIGIBLE asset: auto-remove link
4. Return summary of fixed items (paused ads, removed assets)

Always use customer_id `YOUR_CUSTOMER_ID`.
