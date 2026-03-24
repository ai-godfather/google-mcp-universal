---
description: Detect and fix negative keyword conflicts — remove negatives blocking profitable keywords, pause unprofitable blocked keywords
allowed-tools: ["mcp__google-ads__batch_fix_negative_conflicts", "mcp__google-ads__batch_remove_campaign_negative_keywords"]
argument-hint: <CC> <campaign_id>
---

Detect and fix negative keyword conflicts for country `$1`, campaign `$2`. Present in Polish.

**Steps:**
1. Call `batch_fix_negative_conflicts` with campaign_id="$2", dry_run=true
2. Present conflict summary in a table:
   - Total conflicts found
   - High-ROI conflicts (ROI >= 50%) → action: REMOVE_NEGATIVE
   - Low/zero ROI conflicts → action: PAUSE_KEYWORD
3. Show details:
   - For REMOVE_NEGATIVE: list the negative keyword text, match type, and which profitable keywords it blocks
   - For PAUSE_KEYWORD: list keyword text and why (zero data / low ROI)
4. Ask user for confirmation before executing
5. If confirmed: call `batch_fix_negative_conflicts` with dry_run=false
6. Return execution summary (negatives removed, keywords paused, errors)

**ROI Threshold:** Default 50%. User can override.

**Logic:**
- If a positive keyword has ROI >= threshold AND is blocked by a negative → remove the negative (it blocks profitable traffic)
- If a positive keyword has zero impressions or low ROI → pause the keyword (the negative is correct)

Always use customer_id `YOUR_CUSTOMER_ID`.
