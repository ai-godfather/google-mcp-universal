---
name: ads-setup-shopping-np
description: Create Shopping NP + PMax NP campaigns for a country — full pipeline with feed label separation
---

# /ads-setup-shopping-np {CC}

Create Shopping NP + PMax NP campaigns for New Products, with proper feed label separation from existing (old) campaigns. Present results in the user's language.

## Parameters

- `{CC}` — Country code (e.g., DE, FR, ES). Must exist in config.json `merchant_center.merchant_ids` and `domains`.
- Optional: `ALL` — create for all countries that still need NP campaigns (from the NP roadmap, see `/ads-np-roadmap`)

**Account**: `<CUSTOMER_ID>` below = `account.customer_id` from config.json, unless the user names another account. `google_ads_*` tools require `customer_id`; batch tools default to the configured account.

## CRITICAL: Feed Label Separation Logic

The whole point of NP campaigns is to **separate NEW products from OLD products** using feed labels in Merchant Center:

| Campaign type | Feed Label | What it targets |
|--------------|------------|-----------------|
| **Existing (old) Shopping/PMax** | `{CC}` (e.g. `DE`, `FR`) | Old/proven products from main feed |
| **NEW NP Shopping** | `{CC}_NEW` (e.g. `DE_NEW`, `FR_NEW`) | New products from NP feed only |
| **NEW NP PMax** | `{CC}_NEW` | New products from NP feed only |

### STEP 0 (BEFORE creating NP campaigns): Fix feed labels on OLD campaigns!

**If old campaigns don't have a feed_label set** (shows as `-` or `?` in config dashboard), they target ALL products including new ones. You MUST:

1. Check existing campaign feed_label via `batch_campaign_config_dashboard(country_code="{CC}")`
2. If feed_label is empty/`-`/`?` → set it to `{CC}` using `batch_update_campaign_settings(campaign_id=X, feed_label="{CC}")`
3. This ensures old campaigns ONLY show old products and don't overlap with NP campaigns

If the NP feed itself still carries the label `{CC}` in Merchant Center, run `/ads-fix-feed-labels {CC}` first.

## IMPORTANT: Check the NP Roadmap First

Before creating any campaign, check the roadmap tracker and the synced campaign config:
- `batch_np_roadmap_dashboard(country_code="{CC}")` — tier, priority, Merchant Center status and eligible products, Shopping NP / PMax NP status
- `batch_campaign_config_dashboard(country_code="{CC}")` — campaigns already running for this country (run `/ads-sync-config {CC}` first for fresh data)

Skip the country if Shopping NP is already CREATED/ENABLED. Do NOT set up a country whose Merchant Center account is NOT_ELIGIBLE or has 0 eligible products — repair Merchant Center first (e.g. `/ads-merchant-gc`).

## Merchant Center & Domain Lookup

- Merchant ID: config.json `merchant_center.merchant_ids["{CC}"]` (if a country has several stores, ask which one — extra stores may be listed in `shopping_coverage.stores`)
- Shop domain for PMax `final_url`: config.json `domains["{CC}"]`
- Eligible NP products: Merchant Center, or the GAQL fallback in Step 5

## Campaign Settings

### Shopping NP
| Setting | Value |
|---------|-------|
| Name | `{CC} \| {Country} \| Shopping \| NEW PRODUCTS \| NP` |
| Feed Label | **`{CC}_NEW`** |
| Priority | HIGH |
| Bidding | TARGET_ROAS 100% (target_roas=1.0) |
| Budget | **3.00/day in account currency (3,000,000 micros)** |
| Search Network | DISABLED |
| Conversion Goals | the goals your business optimizes for (e.g. Purchase; or Submit lead form + Converted lead for lead-gen) |
| Product Groups | custom_label_0 subdivision |
| Status | PAUSED |

### PMax NP
| Setting | Value |
|---------|-------|
| Name | `{CC} \| {Country} \| PMax \| NEW PRODUCTS \| NP` |
| Feed Label | **`{CC}_NEW`** (set at campaign creation) |
| Bidding | MAXIMIZE_CONVERSION_VALUE (target_roas=1.0) |
| Budget | **3.00/day in account currency (3,000,000 micros)** |
| Conversion Goals | same as Shopping NP |
| Asset Groups | feed-only (no text/image assets — products only) |
| Status | PAUSED |

**Naming matters**: the tracker tools read the country from the `{CC} |` name prefix and recognise NP campaigns by `NEW PRODUCTS` / ` NP` in the name and the `_NEW` feed label — keep that pattern.

## Full Pipeline (10 steps per country)

### PRE-CHECK: Verify MC readiness
```
1. batch_np_roadmap_dashboard(country_code="{CC}") + batch_campaign_config_dashboard(country_code="{CC}")
2. Verify the merchant has eligible products (> 0) with feed label {CC}_NEW
3. If 0 or all NOT_ELIGIBLE → STOP. MC needs repair first.
4. Check if NP Shopping/PMax campaign already exists (name contains "NEW PRODUCTS", or feed_label = {CC}_NEW)
5. If exists and ENABLED → skip (already running)
```

### STEP 0: Fix feed labels on OLD campaigns
```
# Check existing campaigns for this country (Shopping + PMax rows of the result)
batch_campaign_config_dashboard(country_code="{CC}")

# For each ENABLED old Shopping campaign with empty/missing feed_label:
batch_update_campaign_settings(
  campaign_id="<OLD_CAMPAIGN_ID>",
  feed_label="{CC}"           ← OLD feed label, NOT {CC}_NEW!
)

# Same for ENABLED old PMax campaigns:
batch_update_campaign_settings(
  campaign_id="<OLD_PMAX_ID>",
  feed_label="{CC}"
)
```
**This is CRITICAL** — without this, old campaigns will also show new products and NP separation won't work.

```
# Log feed label fix to NP roadmap tracker:
batch_np_roadmap_update(
  country_code="{CC}",
  action="feed_label_fixed",
  notes="<old campaign IDs> set to {CC}, ready for NP separation"
)
```

### STEP 1: Create Shopping NP Campaign
```
google_ads_create_shopping_campaign(
  customer_id="<CUSTOMER_ID>",
  campaign_name="{CC} | {Country} | Shopping | NEW PRODUCTS | NP",
  daily_budget_micros=3000000,
  merchant_id="<MERCHANT_ID>",
  sales_country="{CC}",
  feed_label="{CC}_NEW",
  campaign_priority="HIGH",
  status="PAUSED"
)
```
→ Save shopping_campaign_id. The campaign is created with Manual CPC — switch it to tROAS:
```
google_ads_update_campaign_bidding_strategy(
  customer_id="<CUSTOMER_ID>",
  campaign_id="<SHOPPING_CAMPAIGN_ID>",
  bidding_strategy="TARGET_ROAS",
  target_roas=1.0
)
```

```
# Log Shopping NP creation to roadmap tracker (records feed label {CC}_NEW and the 3,000,000-micros budget):
batch_np_roadmap_update(
  country_code="{CC}",
  action="shopping_created",
  campaign_id="<SHOPPING_CAMPAIGN_ID>",
  notes="Shopping NP created, PAUSED"
)
```

### STEP 2: Create Ad Group + Shopping Ad
```
google_ads_create_shopping_ad_group(
  customer_id="<CUSTOMER_ID>",
  campaign_id="<SHOPPING_CAMPAIGN_ID>",
  ad_group_name="All NP Products",
  cpc_bid_micros=10000
)
→ Save ad_group_id

google_ads_create_shopping_ad(
  customer_id="<CUSTOMER_ID>",
  ad_group_id="<AD_GROUP_ID>"
)
```

### STEP 3: Disable Search Network on Shopping
```
batch_update_campaign_settings(
  campaign_id="<SHOPPING_CAMPAIGN_ID>",
  target_search_network=false
)
```

### STEP 4: Set Conversion Goals on Shopping
Use the goals your other Shopping campaigns optimize for. E-commerce example:
```
google_ads_set_campaign_conversion_goals(
  customer_id="<CUSTOMER_ID>",
  campaign_id="<SHOPPING_CAMPAIGN_ID>",
  purchase_goal=true,
  lead_form_goal=false,        # lead-gen: true
  converted_lead_goal=false,   # lead-gen: true (and purchase_goal=false)
  add_to_cart_goal=false,
  begin_checkout_goal=false,
  contact_goal=false
)
```

### STEP 5: Build Product Group Tree (Shopping)
```
# 5a: Get products — try MC first (paginate with page_token), fall back to GAQL
merchant_center_list_products(merchant_id="<MERCHANT_ID>", max_results=250)
# If the MC call fails (e.g. expired token) → google_ads_execute_gaql:
#   SELECT shopping_product.item_id, shopping_product.custom_attribute0,
#          shopping_product.feed_label, shopping_product.status
#   FROM shopping_product
#   WHERE shopping_product.merchant_center_id = <MERCHANT_ID>

# 5b: Keep products with feed_label {CC}_NEW, extract unique custom_label_0 values

# 5c: Rebuild listing group tree
google_ads_rebuild_shopping_listing_group_tree(
  customer_id="<CUSTOMER_ID>",
  ad_group_id="<AD_GROUP_ID>",
  root_dimension="custom_label_0",
  groups=[{"value": label, "cpc_bid_micros": 10000} for label in custom_labels],
  everything_else_bid_micros=10000
)
```

### STEP 6: Create PMax NP Campaign
```
google_ads_create_pmax_campaign(
  customer_id="<CUSTOMER_ID>",
  campaign_name="{CC} | {Country} | PMax | NEW PRODUCTS | NP",
  daily_budget_micros=3000000,
  merchant_id="<MERCHANT_ID>",
  sales_country="{CC}",
  feed_label="{CC}_NEW",
  final_url="https://<domains[CC]>/",
  status="PAUSED"
)
```
→ Save pmax_campaign_id. The campaign is created with Maximize conversion value without a target — set tROAS 1.0:
```
google_ads_update_campaign_bidding_strategy(
  customer_id="<CUSTOMER_ID>",
  campaign_id="<PMAX_CAMPAIGN_ID>",
  bidding_strategy="MAXIMIZE_CONVERSION_VALUE",
  target_roas=1.0
)
```

```
# Log PMax NP creation to roadmap tracker:
batch_np_roadmap_update(
  country_code="{CC}",
  action="pmax_created",
  campaign_id="<PMAX_CAMPAIGN_ID>",
  notes="PMax NP created, PAUSED"
)
```

### STEP 7: Create Asset Group for PMax (feed-only)
```
google_ads_create_asset_group(
  customer_id="<CUSTOMER_ID>",
  campaign_id="<PMAX_CAMPAIGN_ID>",
  asset_group_name="NP Products — Feed Only",
  final_url="https://<domains[CC]>/"
)
→ Save asset_group_id

google_ads_create_listing_group_filter(
  customer_id="<CUSTOMER_ID>",
  asset_group_id="<ASSET_GROUP_ID>",
  filter_type="ALL_PRODUCTS"
)
```
→ This creates a feed-only PMax with no text/image assets, just the product feed (the campaign's feed label limits it to `{CC}_NEW` products).

### STEP 8: Set Conversion Goals on PMax
```
google_ads_set_campaign_conversion_goals(
  customer_id="<CUSTOMER_ID>",
  campaign_id="<PMAX_CAMPAIGN_ID>",
  ...same goal flags as Step 4...
)
```

### STEP 9: Sync Both Campaigns to Tracker
```
batch_sync_campaign_config(campaign_ids=["<SHOPPING_CAMPAIGN_ID>", "<PMAX_CAMPAIGN_ID>"])
```

### STEP 9b: Update NP Roadmap Tracker
```
# Final roadmap update — both campaigns synced and ready
batch_np_roadmap_update(
  country_code="{CC}",
  action="mc_verified",
  notes="Full NP pipeline complete: Shopping NP + PMax NP created, synced, PAUSED; <N> eligible products"
)

# Check overall progress:
batch_np_roadmap_dashboard()
```
When the user later enables the campaigns, log it with `action="shopping_enabled"` / `action="pmax_enabled"`.

### STEP 10: Verify & Report
Present a summary in the user's language:

| Parameter | Shopping NP | PMax NP |
|----------|------------|---------|
| Campaign ID | {id} | {id} |
| Name | {name} | {name} |
| Feed Label | {CC}_NEW | {CC}_NEW |
| Budget | 3.00/day | 3.00/day |
| Bidding | TARGET_ROAS 1.0 | MAX_CONV_VALUE (tROAS 1.0) |
| Conv Goals | {goals} | {goals} |
| Product Groups | {count} groups | feed-only asset group |
| MC Eligible | {count} products | {count} products |
| Status | PAUSED | PAUSED |

Also report:
- Old campaigns feed_label fix status (Step 0)
- Roadmap progress: X of N roadmap countries now have NP Shopping + PMax

## Multi-Country Mode (`ALL`)

When `ALL` is specified:
1. `batch_np_roadmap_dashboard()` → country list ordered by `priority_rank`
2. Process countries in priority order, skipping those where Shopping NP is already CREATED/ENABLED
3. **PRE-CHECK each country** — skip if eligible products = 0 or Merchant Center is NOT_ELIGIBLE
4. **Few eligible products** (e.g. < 50): warn that the NP feed probably needs loading first via `/ads-add-np-feed`
5. **For each country: Steps 0-10** (fix old feed labels → create Shopping NP → create PMax NP → sync)
6. Report per-country summary at the end with roadmap progress update

## Pre-requisites (before running this command)

1. **NP feed must exist in Merchant Center** — use `/ads-add-np-feed {CC}` first
2. **MC sub-account must have eligible products** — check via the roadmap or config dashboard
3. **Feed label {CC}_NEW must be set on the NP feed** — use `/ads-fix-feed-labels {CC}` if the NP feed doesn't have the proper label
4. **Old campaigns must have their feed_label set to {CC}** — Step 0 handles this
5. **config.json** has `merchant_center.merchant_ids["{CC}"]` and `domains["{CC}"]`

## API Gotchas

- Use `feed_label`, NOT `sales_country`, to select products (`sales_country` is deprecated in API v15+; the create tools still require it and use it as feed label when none is given)
- Standard Shopping doesn't support `MAXIMIZE_CONVERSION_VALUE` — use `TARGET_ROAS`
- PMax DOES support `MAXIMIZE_CONVERSION_VALUE` with optional target_roas
- `google_ads_create_shopping_campaign` starts with Manual CPC and `google_ads_create_pmax_campaign` with Maximize conversion value without a target — set tROAS afterwards with `google_ads_update_campaign_bidding_strategy`
- `contains_eu_political_advertising` is auto-set in code (enum 3)
- SUBDIVISION nodes cannot have bids — only UNIT nodes
- "Everything else" node needs case_value dimension initialized (without value)
- Remove listing groups: UNIT first, then SUBDIVISION
- Listing group `groups` entries use the keys `value` and `cpc_bid_micros`
- Use `ProductCustomAttributeIndexEnum` (not `ListingCustomAttributeIndexEnum`)
- If MC token expired → use GAQL `shopping_product` query as fallback for product data
- PMax feed-only = asset group with listing_group_filter but NO text/image assets
