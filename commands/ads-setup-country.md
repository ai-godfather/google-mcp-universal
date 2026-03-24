---
description: Full country setup — sync, warmup, reset errors, async setup all, guardrails
allowed-tools: ["mcp__google-ads__batch_sync_from_api", "mcp__google-ads__batch_reset_errors", "mcp__google-ads__batch_warmup_cache", "mcp__google-ads__batch_warmup_status", "mcp__google-ads__batch_setup_all", "mcp__google-ads__batch_setup_progress", "mcp__google-ads__batch_dashboard", "mcp__google-ads__batch_validate_guardrails"]
argument-hint: <CC> <campaign_id>
---

Full country setup for country code `$1` and campaign `$2`. Execute all steps sequentially and present results in Polish.

**Steps (v3.2.0 async):**
1. Call `batch_sync_from_api` with country_code="$1", campaign_id="$2", include_feed=true
2. Call `batch_reset_errors` with country_code="$1", campaign_id="$2" — reset any previous errors to pending
3. Call `batch_warmup_cache` with country_code="$1", campaign_id="$2" — pre-warm AI copy cache (auto-chunks ≤95 handles)
4. Poll `batch_warmup_status` with job_id every 30s until completed
5. Call `batch_setup_all` with country_code="$1", campaign_id="$2", dry_run=false — returns {status: "submitted", job_id: "..."} immediately (async)
6. Poll `batch_setup_progress` with job_id every 60s until status="completed"
7. Call `batch_dashboard` with country_code="$1", campaign_id="$2" — show setup report
8. Call `batch_validate_guardrails` with country_code="$1", campaign_id="$2" — show quality report

Always use customer_id `YOUR_CUSTOMER_ID`. Present multi-page report with product counts, assets created, errors, guardrails status.
