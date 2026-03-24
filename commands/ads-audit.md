---
description: Full campaign audit — sync, missing, guardrails, eligibility
allowed-tools: ["mcp__google-ads__batch_sync_from_api", "mcp__google-ads__batch_missing", "mcp__google-ads__batch_validate_guardrails", "mcp__google-ads__batch_check_eligibility", "mcp__google-ads__batch_dashboard"]
argument-hint: <CC> <campaign_id>
---

Full campaign audit for country code `$1` and campaign `$2`. Execute all steps and present results in Polish.

**Steps:**
1. Call `batch_sync_from_api` with country_code="$1", campaign_id="$2"
2. Call `batch_missing` with country_code="$1", campaign_id="$2" — list incomplete products
3. Call `batch_validate_guardrails` with country_code="$1", campaign_id="$2" — guardrails report
4. Call `batch_check_eligibility` with country_code="$1", campaign_id="$2" — ad eligibility issues
5. Call `batch_dashboard` with country_code="$1", campaign_id="$2" — comprehensive summary

Always use customer_id `YOUR_CUSTOMER_ID`. Return full audit report (3-5 pages) with issues flagged and recommendations.
