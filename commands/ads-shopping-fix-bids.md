---
description: Fix Shopping campaigns with non-competitive bids (≤0.01) — rebuild listing groups into one catch-all unit with a proper CPC
allowed-tools: ["mcp__google-ads__batch_shopping_fix_bids", "mcp__google-ads__batch_shopping_coverage_audit"]
---

# /ads-shopping-fix-bids — Fix Non-Competitive Shopping Bids

Detects Shopping campaigns where ALL listing group bids are ≤ 0.01 (account currency — non-competitive) and rebuilds their listing group tree into a single catch-all unit with a competitive CPC.

## Arguments
- `{CC}` (optional) — country code (e.g., "DE", "PL"). If not set, scans ALL countries.
- `dry_run` — default true. Set to false to actually apply changes.
- `target_bid_gbp` — default 0.15. CPC for the catch-all unit, in **account currency** (the parameter name says GBP, but the value is used as-is in your account currency).
- `campaign_ids` — optional list of specific campaign IDs to fix.

## What It Does

1. **Scans all ENABLED Shopping campaigns** — finds those whose listing group bids are ALL ≤ 0.01
2. **Skips NP campaigns** (name contains ` NP` or `NEW PRODUCTS`) and filters by country via the campaign name prefix (`DE | ...`)
3. **Rebuilds the tree**: root SUBDIVISION → one catch-all UNIT at `target_bid_gbp`; the per-product micro-bid groups are removed (they're ineffective at 0.01 anyway)
4. **Reports changes** — before/after bid comparison per campaign

Need different bids per performance segment instead of one flat bid? Use the Labelizer (`/ads-labelizer-deploy`, HERO/SIDEKICK/VILLAIN/ZOMBIE groups on `custom_label_4`).

## Workflow

### Step 1: Dry run first (ALWAYS)
```
batch_shopping_fix_bids(
    country_code="{CC or empty}",
    dry_run=true
)
```

### Step 2: Review and confirm with user

Show table (in the user's language):
| Campaign | Country | Current Max Bid | New Catch-all Bid | Units / Subdivisions Removed | Ad Group |

⚠️ ALWAYS show dry run results and get user confirmation before applying!

### Step 3: Apply (after confirmation)
```
batch_shopping_fix_bids(
    country_code="{CC or empty}",
    dry_run=false,
    target_bid_gbp=0.15
)
```

### Step 4: Verify
Run `/ads-shopping-groups {CC}` to confirm new bids are live.

## Bid Choice

Pick `target_bid_gbp` close to your typical Shopping CPC (default 0.15). In low-value currencies (e.g. HUF) 0.15 units is far below a competitive bid — scale it up.

## Examples
```
/ads-shopping-fix-bids PL              -- dry run for Poland
/ads-shopping-fix-bids                 -- dry run all countries
/ads-shopping-fix-bids PL --apply      -- apply fixes for Poland
```

## API Cost
1-2 GAQL queries per run + 1 atomic mutate per campaign rebuilt. Moderate quota.

Account: the tool uses `account.customer_id` from config.json by default; pass `customer_id` to work on another account.
