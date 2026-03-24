---
description: Clone a Shopping campaign structure to another country with adjusted merchant_id, bids, and feed label
allowed-tools: ["mcp__google-ads__batch_shopping_clone_campaign", "mcp__google-ads__batch_shopping_listing_groups"]
---

# /ads-shopping-clone — Clone Shopping Campaign Cross-Country

Clone a Shopping campaign structure to another country. Reads source campaign (ad groups, listing groups, bids) and creates a complete copy for the target country with adjusted merchant_id and optional bid multiplier. Created as PAUSED.

## Arguments
- `{source_campaign_id}` (required) — source Shopping campaign ID to clone from
- `{target_CC}` (required) — target country code (e.g., "IT", "FR")
- `{bid_multiplier}` (optional) — multiply all bids by this factor (default 1.0). Use 0.8 for conservative start.

## What It Creates

1. **Campaign Budget** — new daily budget (default 5.00)
2. **Shopping Campaign** — named `{CC} | {Country} | Shopping | NEW_PRODUCTS | CLONE | CLAUDE_MCP`, status PAUSED
3. **Ad Groups** — cloned from source with adjusted bids
4. **Shopping Ads** — one per ad group
5. **Listing Group Trees** — full product group structure with adjusted bids

## Workflow

### Step 1: Preview source (optional)
```
batch_shopping_listing_groups(
    country_code="{source_CC}",
    campaign_id="{source_campaign_id}"
)
```

### Step 2: Preview clone plan (DRY RUN)
```
batch_shopping_clone_campaign(
    source_campaign_id="{source_campaign_id}",
    target_country_code="{target_CC}",
    bid_multiplier={bid_multiplier},
    dry_run=true
)
```

### Step 3: Present results in Polish

Source campaign details:
| Pole | Wartość |
|------|---------|
| Source | {name} (ID: {id}) |
| Merchant ID | {source_merchant_id} |
| Feed Label | {source_feed_label} |

Clone plan:
| Pole | Wartość |
|------|---------|
| Target Country | {CC} ({country_name}) |
| Target Merchant ID | {target_merchant_id} |
| Target Feed Label | {feed_label} |
| Ad Groups to Clone | {count} |
| Budget | {daily_budget} |
| Bid Multiplier | {multiplier}x |

Ad groups detail:
| Ad Group | Listing Groups | Default Bid | Adjusted Bid |

### Step 4: Confirm & Execute
Ask user to confirm (especially budget, feed_label, bid_multiplier). If confirmed:
```
batch_shopping_clone_campaign(
    ...,
    dry_run=false
)
```

Present: new campaign ID, cloned ad groups, listing groups cloned per group, errors.

### Step 5: Post-Creation
- Campaign is created as PAUSED — user must enable manually when ready
- Remind to verify feed label matches actual MC feed
- Suggest: "Po aktywacji monitoruj przez 3-7 dni, potem użyj /ads-shopping-optimize {CC} {new_campaign_id}"

## Parameters (Advanced)
- `target_campaign_name` — custom campaign name (auto-generated if not set)
- `daily_budget_micros` — budget in micros (default 5000000 = 5.00)
- `feed_label` — MC feed label (auto-generated as `{CC}FEEDNEW` if not set)

## Examples
```
/ads-shopping-clone 12345678 IT                    -- clone to Italy, same bids
/ads-shopping-clone 12345678 FR 0.8                -- clone to France, 80% bids
/ads-shopping-clone 12345678 HR 1.2                -- clone to Croatia, 120% bids
```

## API Cost
3-5 GAQL queries + N mutate calls (campaign + budget + ad groups + ads + listing groups).

Always use customer_id `YOUR_CUSTOMER_ID`.
