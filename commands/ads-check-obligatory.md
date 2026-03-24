---
description: Cross-reference OBLIGATORY_GLOBAL whitelist against Merchant Center to find missing/blocked products
allowed-tools: ["mcp__google-ads__batch_check_obligatory"]
---

# /ads-check-obligatory — OBLIGATORY Whitelist Visibility Check

Cross-reference the OBLIGATORY_GLOBAL whitelist against Merchant Center `shopping_product` data to verify which must-have products are actually visible in Shopping campaigns.

## Arguments
- `{CC}` (required) — country code (e.g., "PL", "GR", "RO") or "ALL" to check all countries

## What It Checks

For each OBLIGATORY handle per country:
- **ELIGIBLE** — product is in MC feed and approved for Shopping
- **NOT_ELIGIBLE** — product is in MC feed but blocked/disapproved
- **MISSING** — product is NOT in MC feed at all (possibly blacklisted or removed from Shopify)

Also reads local blacklist files from `dat/BLACKLIST/gMerchant/{CC}.txt` to cross-reference.

## Workflow

### Step 1: Run the check
```
batch_check_obligatory(
    country_code="{CC}",
    include_eligible_details=false
)
```

### Step 2: Present results in Polish

Per-country table:
| Kraj | Obligatory | W MC | Eligible | Not Eligible | Brak w MC | Visibility% |

Then for each country with issues:
- **NOT_ELIGIBLE handles**: table with handle, status, variants count, feed_label
- **MISSING handles**: list of handles not found in MC at all
- **Blacklisted variants count**: how many variants are in local blacklist files

### Step 3: Recommendations

For MISSING products:
- Check if they're in `dat/BLACKLIST/gMerchant/{CC}.txt` — if yes, they were removed intentionally
- If NOT blacklisted — check Shopify admin if the product exists and is published
- If product exists in Shopify but not in MC — check feed label and XML feed

For NOT_ELIGIBLE:
- Run `/ads-merchant-gc {merchant_id} {CC}` to get specific disapproval reasons
- Check if product needs appeal or blacklist cleanup

## Examples
```
/ads-check-obligatory PL          -- check Polish obligatory products
/ads-check-obligatory ALL         -- full multi-country audit
/ads-check-obligatory GR          -- check Greek obligatory products
```

## Data Sources
- Whitelist: `dat/WHITELIST/HANDMADE/OBLIGATORY_GLOBAL.txt` (1681+ handles, format: `product-name-cc`)
- Blacklists: `dat/BLACKLIST/gMerchant/{CC}.txt`
- MC data: `shopping_product` resource via GAQL

## API Cost
1 GAQL query per country checked. `ALL` mode = 1 query per country with OBLIGATORY handles.

Always use customer_id `YOUR_CUSTOMER_ID`.
