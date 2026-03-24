---
description: Detailed API quota breakdown
allowed-tools: ["mcp__google-ads__batch_api_quota"]
---

Show detailed API quota report. Present in Polish.

**Steps:**
1. Call `batch_api_quota` — get detailed breakdown
2. Parse: total used, remaining, by-hour timeline, by-tool breakdown
3. Project: quota remaining vs typical daily burn rate
4. Alert if critical (>90%)
5. Return formatted quota report with table

Always use customer_id `YOUR_CUSTOMER_ID`.
