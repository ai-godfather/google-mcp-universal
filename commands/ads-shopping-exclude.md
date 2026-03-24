---
description: Exclude specific products from Shopping campaign by rebuilding listing group tree with minimum bids
allowed-tools: ["mcp__google-ads__batch_shopping_exclude_products", "mcp__google-ads__batch_shopping_listing_groups"]
---

# /ads-shopping-exclude — Exclude Products from Shopping

Exclude specific product handles from a Shopping campaign by rebuilding the listing group tree. Excluded products get bid=1 micro (effectively invisible), active products keep the specified bid.

## Arguments
- `{CC}` (required) — country code
- `{campaign_id}` (required) — Shopping campaign ID
- `{ad_group_id}` (required) — ad group ID containing the listing group tree
- `{handles}` (required) — comma-separated product handles to exclude

## How It Works

1. Reads current listing group tree (existing handles and bids)
2. Rebuilds the tree atomically:
   - Excluded handles → bid = 1 micro (won't win any auction)
   - Active handles → keep specified bid (default 0.10)
   - "Everything else" node → keeps active bid
3. Uses atomic tree rebuild to avoid broken intermediate states

## Workflow

### Step 1: View current tree (optional but recommended)
```
batch_shopping_listing_groups(
    country_code="{CC}",
    campaign_id="{campaign_id}"
)
```

### Step 2: Preview exclusion (DRY RUN)
```
batch_shopping_exclude_products(
    country_code="{CC}",
    campaign_id="{campaign_id}",
    ad_group_id="{ad_group_id}",
    exclude_handles=["{handle1}", "{handle2}", ...],
    dry_run=true
)
```

### Step 3: Present results in Polish
| Metryka | Wartość |
|---------|---------|
| Existing handles in tree | {count} |
| To exclude | {excluded_count} |
| To keep active | {active_count} |
| Not in tree (new) | {not_found list} |

### Step 4: Confirm & Execute
```
batch_shopping_exclude_products(
    ...,
    dry_run=false
)
```

## Important Notes
- This rebuilds the ENTIRE listing group tree — all bids are reset
- `active_bid_micros` default is 100000 (0.10) — confirm with user
- Products not already in the tree are added with bid=1 (excluded)
- Use `dimension=custom_label_0` for product handle partitioning (default)

## Examples
```
/ads-shopping-exclude GR 12345678 98765432 abslim-gr,prostonix-gr
/ads-shopping-exclude PL 12345678 98765432 detoxin-pl
```

## API Cost
1 GAQL query + 1 atomic mutate (tree rebuild).

Always use customer_id `YOUR_CUSTOMER_ID`.
