---
description: Stale product cleanup — dry run first, then confirm
allowed-tools: ["mcp__google-ads__batch_cleanup_stale"]
argument-hint: <CC> <campaign_id>
---

Clean up stale products for country `$1`, campaign `$2`. Present in Polish.

**Steps:**
1. Call `batch_cleanup_stale` with country_code="$1", campaign_id="$2", dry_run=true — show what would be paused
2. Present the list and wait for explicit user confirmation
3. Only after confirmation: call `batch_cleanup_stale` with dry_run=false — pause stale ad groups
4. Return summary of paused ad groups

Always use customer_id `YOUR_CUSTOMER_ID`. NEVER auto-execute without user confirmation.
