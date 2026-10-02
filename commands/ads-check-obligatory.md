---
description: Cross-reference your must-have products whitelist against Merchant Center to find missing/blocked products
allowed-tools: ["mcp__google-ads__batch_check_obligatory"]
---

# /ads-check-obligatory — Must-Have Products Visibility Check

Cross-reference the must-have products whitelist (the "obligatory" list) against Merchant Center `shopping_product` data to verify which must-have products are actually visible in Shopping campaigns.

## Arguments
- `{CC}` (required) — country code (e.g., "DE", "PL") or "ALL" to check every country that has whitelist entries

## Setup (config.json)

```json
"whitelist": {
  "obligatory_file": "data/whitelist/obligatory.txt",
  "blacklist_dir": "data/blacklist/merchant"
}
```

- **`obligatory_file`** — the must-have products whitelist: one product handle per line, suffixed with the lowercase country code (`vitaboost-pl`, `vitaboost-de`). The suffix decides the country.
- Handles are matched against `custom_label_0` of the products in Merchant Center, so your feed must put the product handle (e.g. `vitaboost-pl`) into `custom_label_0`.
- **`blacklist_dir`** — your Merchant Center blacklist files: one `<shop domain>.txt` per shop (one item ID per line). Country files named `<CC>.txt` (e.g. `PL.txt`) in this directory are counted as "blacklisted variants" for that country.
- Paths are absolute or relative to the plugin root. Countries without a merchant ID in `merchant_center.merchant_ids` are skipped.

## What It Checks

For each must-have handle per country:
- **ELIGIBLE** — product is in MC feed and approved for Shopping
- **NOT_ELIGIBLE** — product is in MC feed but blocked/disapproved
- **MISSING** — product is NOT in MC feed at all (possibly blacklisted or removed from the shop)

Also reads local blacklist files from `blacklist_dir` to cross-reference.

## Workflow

### Step 1: Run the check
```
batch_check_obligatory(
    country_code="{CC}",
    include_eligible_details=false
)
```

### Step 2: Present results in the user's language

Per-country table:
| Country | Must-have | In MC | Eligible | Not eligible | Missing from MC | Visibility % |

Then for each country with issues:
- **NOT_ELIGIBLE handles**: table with handle, status, variants count, feed_label
- **MISSING handles**: list of handles not found in MC at all
- **Blacklisted variants count**: how many variants are in local blacklist files

### Step 3: Recommendations

For MISSING products:
- Check if they're in your blacklist files (`blacklist_dir`) — if yes, they were removed intentionally
- If NOT blacklisted — check in your shop admin (e.g. Shopify) that the product exists and is published
- If the product exists in the shop but not in MC — check feed label, `custom_label_0` and the XML feed

For NOT_ELIGIBLE:
- Run `/ads-merchant-gc {merchant_id} {CC}` to get specific disapproval reasons
- Check if product needs appeal or blacklist cleanup

## Examples
```
/ads-check-obligatory PL          -- check must-have products for Poland
/ads-check-obligatory ALL         -- full multi-country audit
/ads-check-obligatory DE          -- check must-have products for Germany
```

## Data Sources
- Whitelist: `whitelist.obligatory_file` (default `data/whitelist/obligatory.txt`, format: `product-handle-cc`)
- Blacklists: `whitelist.blacklist_dir` (default `data/blacklist/merchant`)
- MC data: `shopping_product` resource via GAQL

## API Cost
1 GAQL query per country checked. `ALL` mode = 1 query per country with whitelist entries.

Account: the check runs on the account configured in config.json (`account.customer_id`).
