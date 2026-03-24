---
description: Quick health check — quota, guardrails, eligibility, empty ad groups
allowed-tools: ["mcp__google-ads__batch_api_quota", "mcp__google-ads__batch_dashboard", "mcp__google-ads__batch_check_eligibility", "mcp__google-ads__batch_fix_empty_groups"]
---

Perform a quick health check of the Google Ads account. Execute all steps and present results in Polish.

**Steps:**
1. Call `batch_api_quota` — show status (healthy/warning/critical) with exact numbers
2. Call `batch_dashboard` for main campaigns (RO 22734106761, TR 22744242209) — show progress %, top errors
3. Call `batch_check_eligibility` for the same campaigns — show disapproved ads count
4. Call `batch_fix_empty_groups` with `dry_run=true` — detect ENABLED ad groups without active ads
   - Show count of empty groups per category (ENABLE_AD, CREATE_RSA, PAUSE_GROUP, REVIEW)
   - Flag if any high-ROI groups are missing ads (potential revenue loss)
5. Return a concise 1-page summary in Polish with tables, including:
   - Quota status
   - Campaign progress
   - Disapproved ads/assets
   - Empty ad groups alert (if any found)

Always use customer_id `YOUR_CUSTOMER_ID`.
