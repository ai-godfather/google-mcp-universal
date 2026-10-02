---
description: Deploy Producthero bid differentiation for a country — Shopping listing groups by custom_label_4 (recommended) or 4 segment PMax campaigns
---

# /ads-labelizer-deploy — Deploy Producthero Bid Differentiation

## What it does

Deploys Producthero Labelizer bid differentiation for a country. Two approaches available:

**Account**: `<CUSTOMER_ID>` = `account.customer_id` from config.json unless the user names another account (`google_ads_*` tools require it; `ecom_labelizer_deploy` defaults to it).

### Approach A: Shopping Listing Groups (RECOMMENDED)

Rebuilds the listing group tree in an existing Shopping campaign with `custom_label_4` subdivision and different CPC bids per segment. This is what Producthero and Channable actually do.

**Advantages**: No PMax campaign limit issue, uses existing campaign history, standard industry method.

**How to use**:
1. Run `/ads-labelizer-apply {CC}` (or `/ads-supplemental-feed {CC}`) first to push labels
2. Identify the target Shopping campaign and ad group ID
3. Use `google_ads_rebuild_shopping_listing_group_tree` with **EXACT** parameters:
   ```
   google_ads_rebuild_shopping_listing_group_tree(
       customer_id="<CUSTOMER_ID>",
       ad_group_id="<AD_GROUP_ID>",
       root_dimension="custom_label_4",
       everything_else_bid_micros=10000,
       groups=[
           {"value": "HERO", "cpc_bid_micros": 600000},
           {"value": "SIDEKICK", "cpc_bid_micros": 600000},
           {"value": "VILLAIN", "cpc_bid_micros": 150000},
           {"value": "ZOMBIE", "cpc_bid_micros": 450000}
       ]
   )
   ```

**CRITICAL API parameter notes**:
- `root_dimension` MUST be `"custom_label_4"` (NOT `"product_custom_attribute"` or `"custom_attribute_4"`)
- Groups use keys `"value"` and `"cpc_bid_micros"` (NOT `"dimension_value"` or `"bid_micros"`)
- Google API lowercases values automatically ("HERO" → "hero" in listing groups). Matching is case-insensitive.

**Bid multiplier reference (base = 300,000 micros = 0.30 in account currency)**:
| Segment | Multiplier | Bid (micros) | Bid (account currency) |
|---------|-----------|-------------|-----------|
| HERO | 2.0× | 600,000 | 0.60 |
| SIDEKICK | 2.0× | 600,000 | 0.60 |
| VILLAIN | 0.5× | 150,000 | 0.15 |
| ZOMBIE | 1.5× | 450,000 | 0.45 |
| Everything Else | min | 10,000 | 0.01 |

**Currency**: micros are in the account currency (1,000,000 micros = 1 unit). Scale the base bid to your currency — for low-value currencies (e.g. HUF) the same micros are far below a competitive CPC.

### Approach B: 4 PMax Campaigns

Creates the exact Producthero 4-campaign architecture:

| Campaign | Segment | Budget Multiplier | Purpose |
|----------|---------|-------------------|---------|
| {CC} \| PMax \| Labelizer \| Heros | HERO | 2.0× | Scale proven winners |
| {CC} \| PMax \| Labelizer \| Sidekicks | SIDEKICK | 2.0× | Grow hidden potential |
| {CC} \| PMax \| Labelizer \| Villains | VILLAIN | 0.5× | Reduce waste spending |
| {CC} \| PMax \| Labelizer \| Zombies | ZOMBIE | 1.5× | Activate dormant products |

**Key principle**: ALL 4 campaigns use the SAME tROAS. Budget multipliers control allocation.
**Budget currency**: Auto-detected per country (e.g. HUF for HU, PLN for PL, EUR for DE)
**Needs** config.json `merchant_center.merchant_ids["{CC}"]` and `domains["{CC}"]` (final URL).

**Warning**: Google Ads limits enabled PMax campaigns per account (100, error `ENABLED_UBERVERSAL_CAMPAIGNS_PER_CUSTOMER`). Check available slots before deploying.

## Prerequisites
Run `/ads-labelizer-apply` (or `/ads-supplemental-feed`) first so the labels are in your feed. Labels must be in `custom_label_4` (or the slot you pass as `custom_label_slot`).

## Pipeline (Approach A — Shopping Listing Groups)
1. Run `/ads-labelizer-apply {CC}` with `dry_run=true` → verify labels
2. Run `/ads-labelizer-apply {CC}` with `dry_run=false` → push labels to feed
3. Identify target Shopping campaign + ad group via `google_ads_execute_gaql`: `SELECT campaign.id, campaign.name, ad_group.id FROM ad_group WHERE campaign.advertising_channel_type = 'SHOPPING' AND campaign.name LIKE '{CC} |%'`
4. Call `google_ads_rebuild_shopping_listing_group_tree` with correct params (see above)
5. **VERIFY DEPLOYMENT** (see checklist below)

## Pipeline (Approach B — PMax)
1. Run `ecom_labelizer_deploy` with `dry_run=true` — preview plan
2. Show budget calculations (currency-aware) and campaign specs
3. User confirms → run with `dry_run=false`
4. All campaigns created as **PAUSED**
5. If a created campaign has no `asset_group_id` in the result, create one per campaign with `google_ads_create_asset_group` and add `google_ads_create_listing_group_filter(filter_type="CUSTOM", custom_label_4="<SEGMENT>")`
6. Add text + image assets to each asset group
7. Set geo/language targeting
8. Enable campaigns when ready
9. Schedule periodic label refresh via `ecom_labelizer_apply` (or the supplemental feed)

## ⚠️ MANDATORY Post-Deployment Verification Checklist

After ANY deployment, run these checks. DO NOT mark deployment as "done" until ALL pass:

1. **Query listing groups**: `SELECT ad_group_criterion.listing_group.case_value.product_custom_attribute.index, ad_group_criterion.listing_group.case_value.product_custom_attribute.value, ad_group_criterion.cpc_bid_micros, ad_group_criterion.status FROM ad_group_criterion WHERE campaign.id = {CAMPAIGN_ID} AND ad_group_criterion.type = 'LISTING_GROUP' AND ad_group_criterion.status != 'REMOVED'`
2. **Verify 6 nodes exist**: ROOT (SUBDIVISION) + HERO + SIDEKICK + VILLAIN + ZOMBIE + Everything Else
3. **Verify INDEX4** on all nodes (custom_label_4 slot)
4. **Verify bid micros**: HERO=600000, SIDEKICK=600000, VILLAIN=150000, ZOMBIE=450000, else=10000
5. **Verify all UNIT nodes ENABLED**
6. **Check impressions after 24h**: `SELECT campaign.name, metrics.impressions, metrics.clicks FROM campaign WHERE campaign.id = {CAMPAIGN_ID} AND segments.date DURING LAST_7_DAYS`

If ANY check fails → deployment is NOT complete. Fix before reporting success.

## Parameters
- `country_code` — **required** (e.g., DE)
- `target_roas` — default 2.0 (same for all 4 campaigns)
- `custom_label_slot` — default **4** (Producthero slot)
- `days_back` — default 180 (lookback window)
- `dry_run` — default true

## Example
```
/ads-labelizer-deploy DE
```

## Deployment Status

Run `/ads-labelizer-verify {CC}` after each deployment — it records the campaign, ad group, node count, bids and pipeline status per country in the local `labelizer_state.db` and lists every verified country in `all_countries`.
