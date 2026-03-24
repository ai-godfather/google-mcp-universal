---
description: Auto-optimize Shopping campaign CPC bids per product group based on ROAS target
allowed-tools: ["mcp__google-ads__batch_shopping_bid_optimizer", "mcp__google-ads__batch_shopping_listing_groups", "mcp__google-ads__batch_api_quota"]
---

# /ads-shopping-optimize — Shopping Bid Optimization

Auto-optimize CPC bids on Shopping campaign product groups based on ROAS performance. Increases bids for good performers, decreases for poor performers.

## Arguments
- `{CC}` (required) — country code (e.g., "GR", "PL")
- `{campaign_id}` (required) — Shopping campaign ID
- `{days}` (optional) — lookback period: 7, 14, 30, 90. Default: 30

## How It Works

1. Queries `product_group_view` for all UNIT nodes (leaf product groups)
2. Calculates ROAS per node: `conv_value / cost`
3. Proposes bid changes:
   - **ROAS >= target** → INCREASE bid by `bid_increase_pct` (default +15%)
   - **ROAS < target** → DECREASE bid by `bid_decrease_pct` (default -20%)
   - **Low data** (< min_impressions) → SKIP (not enough data to decide)
4. Respects max/min bid limits

## Workflow

### Step 1: Check quota
```
batch_api_quota()
```

### Step 2: Preview (always DRY RUN first)
```
batch_shopping_bid_optimizer(
    country_code="{CC}",
    campaign_id="{campaign_id}",
    days={days},
    target_roas=1.6,
    dry_run=true
)
```

### Step 3: Present results in Polish

Summary:
| Metryka | Wartość |
|---------|---------|
| Total product groups | {total} |
| Skipped (low data) | {skipped} |
| INCREASE proposals | {inc_count} |
| DECREASE proposals | {dec_count} |

Proposals table:
| Ad Group | Handle | ROAS | Current Bid | New Bid | Action | Impr | Clicks | Cost | Conv Value |

### Step 4: Confirm & Execute
Ask user for confirmation. If confirmed:
```
batch_shopping_bid_optimizer(
    ...,
    dry_run=false
)
```

Present: applied count, errors.

## Parameters (Advanced)
- `target_roas` — Target ROAS (default 1.6 = 160%). Configurable.
- `min_impressions` — Min impressions to consider (default 100)
- `max_bid_micros` — Max CPC cap (default 5.00)
- `min_bid_micros` — Min CPC floor (default 0.01)
- `bid_increase_pct` — Increase % for good ROAS (default 0.15 = +15%)
- `bid_decrease_pct` — Decrease % for poor ROAS (default 0.20 = -20%)

## Examples
```
/ads-shopping-optimize GR 12345678         -- 30-day analysis
/ads-shopping-optimize PL 12345678 90      -- 90-day analysis
```

## API Cost
1 GAQL query + N mutate calls (if not dry_run).

Always use customer_id `YOUR_CUSTOMER_ID`.
