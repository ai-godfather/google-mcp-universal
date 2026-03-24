---
description: List product groups (listing groups) in Shopping campaigns with performance metrics
allowed-tools: ["mcp__google-ads__batch_shopping_listing_groups"]
---

# /ads-shopping-groups — Shopping Campaign Product Groups

List all product groups (listing groups) in Shopping campaigns for a country. Shows the full product group tree with bids and performance metrics.

## Arguments
- `{CC}` (required) — country code (e.g., "GR", "PL")
- `{campaign_id}` (optional) — specific Shopping campaign ID. If not set, finds all Shopping campaigns for the country.

## What It Shows

For each Shopping campaign:
- Campaign name, status, merchant_id, feed_label
- Full listing group tree:
  - Root subdivisions
  - Custom label subdivisions (custom_label_0 = product handles)
  - Individual product groups with CPC bids
  - Performance metrics: impressions, clicks, cost, conversions, conv_value

## Workflow

### Step 1: Fetch listing groups
```
batch_shopping_listing_groups(
    country_code="{CC}",
    campaign_id="{campaign_id or None}"
)
```

### Step 2: Present results in Polish

Per-campaign summary:
| Kampania | Status | Merchant ID | Product Groups |

Then for each campaign, product groups table:
| Ad Group | Handle/Value | Typ | CPC Bid | Impressions | Clicks | Cost | Conv | ROAS |

Sort by cost descending. Highlight:
- 🔴 Groups with high cost + zero conversions
- 🟢 Groups with ROAS > 2.0
- ⚠️ Groups with very low bids (< 0.05)

### Step 3: Recommendations

Based on data:
- Suggest `/ads-shopping-optimize {CC} {campaign_id}` for bid optimization
- Suggest `/ads-shopping-exclude {CC} {campaign_id} {ag_id} {handles}` for excluding wasteful products
- Identify products not in tree that should be added

## Examples
```
/ads-shopping-groups GR             -- all Shopping campaigns for Greece
/ads-shopping-groups PL 12345678    -- specific campaign
```

## API Cost
2-3 GAQL queries (campaign list + product_group_view per campaign).

Always use customer_id `YOUR_CUSTOMER_ID`.
