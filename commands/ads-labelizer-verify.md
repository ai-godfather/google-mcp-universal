---
description: Verify the Producthero Labelizer pipeline for a country — labels in feed, listing groups, traffic, Merchant Center — with SQLite tracking
---

# /ads-labelizer-verify — Verify Labelizer Pipeline Health

## What it does

Comprehensive health check of the entire Producthero Labelizer pipeline for a country. Checks ALL 4 steps and stores results in local SQLite DB (`labelizer_state.db`) for tracking across sessions. Present results in the user's language.

## 4 Checks

| # | Check | What it verifies | PASS criteria |
|---|-------|-----------------|---------------|
| 1 | **LABELS_IN_FEED** | Labels reported by your label-push endpoint (`endpoints.labelizer_push`, `GET ?action=status`) | > 0 labeled products, all feed backends in sync |
| 2 | **LISTING_GROUPS_DEPLOYED** | Shopping campaign has `custom_label_4` listing groups | 4 segments (HERO/SIDEKICK/VILLAIN/ZOMBIE) + Everything Else on correct INDEX |
| 3 | **CAMPAIGN_TRAFFIC** | Campaign is serving impressions | > 0 impressions in last 7 days |
| 4 | **MERCHANT_CENTER** | Products are active in MC | > 0 active products, 0 disapproved |

**Supplemental-feed setups**: without `endpoints.labelizer_push`, check 1 always FAILs ("not configured"), so the overall status is BROKEN even when `/ads-supplemental-feed` works. In that case judge checks 2-4 and confirm the labels on a few products in Merchant Center (e.g. `merchant_center_get_product` → `customLabel4`).

## Pipeline Status

Based on checks, the tool assigns an overall status:
- **HEALTHY** — all 4 checks PASS
- **DEGRADED** — no FAILs but some WARNs (e.g., MC disapprovals, sync lag)
- **BROKEN** — at least one FAIL (labels missing, no deployment, no traffic)

## SQLite Tracking

Results are persisted in `labelizer_state.db` (same directory as the MCP server):

**`labelizer_country_state`** — one row per country with full pipeline state:
- Labels: total, per-segment counts, push timestamp, feed backend sync status (`backends_synced` / `backends_total`)
- Deployment: approach, campaign/ad_group IDs, per-segment bids, node count
- Traffic: impressions/clicks/conversions/cost (7d)
- MC: active/disapproved/pending product counts
- Overall: pipeline_status, last_verified_at, notes

**`labelizer_verify_log`** — append-only log of every check run (for trend analysis)

## Pipeline

1. Call `ecom_labelizer_verify` with the country code
2. Review the 4 checks — each shows PASS/WARN/FAIL with detail
3. If issues found:
   - LABELS_IN_FEED FAIL → run `/ads-labelizer-apply {CC}` (or check `endpoints.labelizer_push`)
   - LISTING_GROUPS_DEPLOYED FAIL → run `/ads-labelizer-deploy {CC}`
   - CAMPAIGN_TRAFFIC FAIL → check campaign status, budget, MC feed
   - MERCHANT_CENTER WARN → run `/ads-merchant-gc {CC}`
4. Review `all_countries` summary to see pipeline health across all verified countries
5. Re-run verify after fixes to confirm resolution

## When to use

- **After any labelizer operation** — verify the pipeline is complete end-to-end
- **Before reporting "done"** — NEVER mark deployment as complete without running verify
- **Periodic health check** — run weekly to catch drift (labels expired, campaign paused, MC disapprovals)
- **When debugging** — if a country's Shopping campaign isn't performing, start here

## Parameters

- `country_code` — **required** (e.g., DE, PL, US)
- `customer_id` — defaults to `account.customer_id` from config.json
- `custom_label_slot` — default 4 (Producthero slot)
- `fix` — default false (future: auto-fix issues)

## Example

```
/ads-labelizer-verify DE
```

## Full Labelizer Pipeline Reminder

The complete pipeline is 4 steps. ALL must pass verify:

```
/ads-labelizer {CC}          → Step 0: Classify products (analysis only)
/ads-labelizer-apply {CC}    → Step 1: Push labels to Feed API (or /ads-supplemental-feed {CC})
/ads-labelizer-deploy {CC}   → Step 2: Deploy bid differentiation
/ads-labelizer-verify {CC}   → Step 3: Verify everything works
```

If verify shows BROKEN or DEGRADED, fix the issue and re-verify until HEALTHY.
