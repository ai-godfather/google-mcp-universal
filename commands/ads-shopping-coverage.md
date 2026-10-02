---
description: Full Shopping campaign coverage audit — MC products vs listing groups, feed health, gap detection
allowed-tools: ["mcp__google-ads__batch_shopping_coverage_audit"]
---

# /ads-shopping-coverage — Shopping Coverage Audit

Full-stack audit of Shopping campaign coverage. Cross-references Merchant Center products against campaign listing groups to find coverage gaps, stale entries, and feed health issues.

## Arguments
- `{CC}` (optional) — country code to audit (e.g., "DE", "PL"). If not set, audits ALL configured stores.

## Setup (config.json)

The audit only covers stores listed in `shopping_coverage.stores`:

```json
"shopping_coverage": {
  "stores": {
    "DE":   {"mc_id": "333333333", "type": "main",   "regular_label": "DE", "np_label": "DE_NEW"},
    "DE_B": {"mc_id": "444444444", "type": "outlet", "regular_label": "DE", "np_label": "-"}
  }
}
```

- Key = country code, optionally with a suffix for a second store in the same country (`DE_B`); `{CC}` audits `DE` and every `DE_*` store
- `mc_id` — Merchant Center account of the store; `type` — free label shown in the report
- `regular_label` / `np_label` — feed labels of the main and new-products feed (`-` if none)

Without this section the tool returns "No stores configured".

## What It Does

1. **Fetches all Shopping campaigns** — via GAQL, extracts merchant_id and feed_label
2. **Fetches listing groups** — maps which products are named in each campaign's product group tree
3. **Fetches MC products** — per merchant account, counts products by feed label
4. **Cross-references coverage** — products in MC but NOT in listing groups = gap
5. **Detects stale listings** — products in listing groups but NOT in MC = stale
6. **Generates alerts** — CRITICAL / WARNING / INFO severity
7. **Stores in SQLite** — `shopping_coverage.db` for trend tracking

## Workflow

### Step 1: Run the audit
```
batch_shopping_coverage_audit(
    country_code="{CC or empty for all}"
)
```
Optional: `include_mc_products` (default true), `include_feed_status` (default false).

### Step 2: Present results in the user's language

Summary table:
| Country | Campaign | MC Products | Named LGs | Coverage % | Stale | Alerts |

Then per-country details:
- Feed health (active/disapproved counts)
- Missing products (top 20 by name)
- Stale listing groups to remove
- Bid competitiveness check

### Step 3: Recommendations

Based on findings:
- If coverage < 50%: suggest rebuilding listing group tree with ALL products
- If stale > 30%: suggest cleanup via `/ads-shopping-groups {CC}`
- If bids ≤ 0.01: suggest `/ads-shopping-fix-bids {CC}`
- If MC products = 0: check feed URLs and Merchant Center status

## Examples
```
/ads-shopping-coverage PL        -- audit Poland only
/ads-shopping-coverage           -- audit all configured stores
```

## API Cost
~3-5 GAQL queries + 1 MC API call per merchant account. Moderate quota usage.

Account: the tool uses `account.customer_id` from config.json by default; pass `customer_id` to audit another account.
