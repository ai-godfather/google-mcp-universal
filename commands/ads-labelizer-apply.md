---
description: Apply Producthero Labelizer segments (HERO/SIDEKICK/VILLAIN/ZOMBIE) to your product feed through the optional label-push endpoint
---

# /ads-labelizer-apply — Apply Producthero Labels via Feed API

## What it does
Runs the Producthero Labelizer to classify products into HERO / SIDEKICK / VILLAIN / ZOMBIE segments, then pushes the labels to your feed backend(s) through the label-push endpoint configured in config.json (`endpoints.labelizer_push`).

This is **Step 1** of the Producthero strategy — labels must be set before deploying bid differentiation.

## Architecture
The MCP does NOT write to Merchant Center directly. Instead it calls your label-push endpoint, which stores the labels in your feed backend(s):

```
MCP → POST {endpoints.labelizer_push}?action=update                → your feed backend(s)
MCP → GET  {endpoints.labelizer_push}?action=status&country_code=XX  (verification)
```

- POST body: `{"country_code", "custom_label_slot", "classification_params", "products": [{"product_handle", "segment", "metrics"}]}`
- GET status answer: `{"status": "success", "in_sync": true|false, "servers": {"<backend name>": {"response": {"data": {"total_labeled_products", "segment_counts", "last_update"}}}}}`

Your feed generator writes the stored segment into `custom_label_4` (or the chosen slot) of the primary feed. Google Merchant Center picks up labels on the next feed fetch.

**No endpoint?** Use `/ads-supplemental-feed {CC}` instead — it delivers the same labels as a CSV supplemental source that Merchant Center fetches daily. Without `endpoints.labelizer_push`, `dry_run=false` returns an error in `feed_api`.

## Handle Mapping Pipeline

The critical step is mapping Google Ads `item_id` → product handle:

1. GAQL `shopping_performance_view` returns `segments.product_item_id` (e.g., `12345678_1`)
2. MC `products.list()` returns `offerId` (same format) + `customLabel0` — **your feed must put the product handle into `custom_label_0`**
3. The tool paginates through ALL MC products of the country's merchant (config.json `merchant_center.merchant_ids`) and builds an `offerId → customLabel0` map
4. Items not found in MC are **unmapped** — these are historical/removed products (safely skipped)
5. Multiple variants (same handle, different `_N` suffix) are aggregated into one handle entry (the best segment wins: HERO > SIDEKICK > VILLAIN > ZOMBIE)

## Behaviour Notes
- **Merchant filter**: GAQL filters by `segments.product_merchant_id` for country isolation
- **Unmapped items diagnostic**: response includes `unmapped_item_ids` and `unmapped_note` for transparency
- **Auto-verification**: after a push the tool GETs the status endpoint and returns it as `verification`
- **Backend cross-validation** (optional): with `backend_conversions` configured, the response includes `backend_cross_validation_summary`

## Pipeline
1. Run `ecom_labelizer_apply` with `dry_run=true` for the requested country
2. Show the segment breakdown and sample handles
3. Check unmapped items count — if high, investigate MC product coverage
4. Ask user to confirm
5. Run with `dry_run=false` — single POST to the label-push endpoint, which updates your feed backend(s)
6. The endpoint returns its aggregated response (per backend)
7. Auto-verify via the GET status endpoint — confirm all backends are in sync

## Parameters
- `country_code` — **required** (e.g., DE, PL, US)
- `customer_id` — defaults to `account.customer_id` from config.json
- `target_roas` — default 2.0
- `custom_label_slot` — default 4 (`custom_label_4`)
- `days_back` — default 180
- `campaign_ids` — optional, defaults to all Shopping + PMax campaigns
- `dry_run` — default true (ALWAYS dry_run first!)

## Example
```
/ads-labelizer-apply DE
```

## Next Steps After Apply
- **Option A (PREFERRED)**: Rebuild Shopping campaign listing groups by `custom_label_4`:
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
  `<CUSTOMER_ID>` = `account.customer_id` from config.json (required by `google_ads_*` tools).
  **CRITICAL**: Parameter keys are `"value"` and `"cpc_bid_micros"` (NOT `dimension_value`/`bid_micros`).
  After rebuild, **ALWAYS verify** with GAQL that 6 listing group nodes exist with correct bids. See `/ads-labelizer-deploy` checklist.
- **Option B**: Create 4 PMax campaigns: `/ads-labelizer-deploy {CC}`
- **Periodic refresh**: Re-run `/ads-labelizer-apply {CC}` weekly/bi-weekly to reclassify

## Troubleshooting

### Most handles mapped = 0
- **Cause**: Merchant Center client not initialized (missing/expired Merchant Center credentials) or your feed does not fill `custom_label_0` with the product handle
- **Fix**: Check Merchant Center access (e.g. `merchant_center_list_products`) and the `custom_label_0` values of a few products

### High unmapped count
- **Cause**: Products exist in GAQL history (180 days) but were removed from store/MC
- **Not a bug**: These products can't be labeled because they're no longer in the feed
- **Action**: Review if unmapped products are important — if so, re-add to store

### Labels not appearing in MC
- **Cause**: Feed hasn't been regenerated yet
- **Action**: Wait for next feed generation cycle (~daily) or trigger manual feed fetch
