---
description: Deep guardrails report with fix recommendations
allowed-tools: ["mcp__google-ads__batch_validate_guardrails"]
argument-hint: <CC> <campaign_id>
---

Run deep guardrails validation for country `$1`, campaign `$2`. Present in Polish.

**Steps:**
1. Call `batch_validate_guardrails` with country_code="$1", campaign_id="$2" — full validation
2. For each FAIL: suggest specific fix (create RSA A/B/C, add keywords, upload images, etc.)
3. For each WARN: suggest improvement
4. Prioritize: FAIL > WARN
5. Return detailed report with step-by-step fix recommendations per product

Always use customer_id `YOUR_CUSTOMER_ID`.
