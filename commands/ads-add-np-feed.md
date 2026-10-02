---
description: Add Google NEW Products feed to Merchant Center for a country and trigger immediate fetch
allowed-tools: ["mcp__google-ads__merchant_center_insert_datafeed", "mcp__google-ads__merchant_center_fetch_datafeed_now", "mcp__google-ads__merchant_center_get_datafeed_status", "mcp__google-ads__merchant_center_get_datafeed", "mcp__google-ads__merchant_center_list_datafeeds", "mcp__google-ads__merchant_center_list_datafeed_statuses"]
argument-hint: <CC> [merchant_id]
---

Add a Google NEW Products XML feed to a Merchant Center sub-account for country `$1`, optionally targeting merchant_id `$2`. Present all results in the user's language.

## Context

New-products (NP) feeds are configured in config.json `np_feeds`: per country and channel (`google`, …) a list of shops, each with `domain`, `feed_url` and `brand_code` (e.g. `MAIN`, `OUTLET`). A country may have several shops/domains, each with its own feed URL and Merchant Center sub-account.

**Feed URL**: whatever URL your feed backend publishes for the new-products feed (e.g. `https://feeds.yourshop.com/de/google-new-products.xml`).

**Merchant Center accounts**: the sub-account per country comes from config.json `merchant_center.merchant_ids`; your MCA ID(s) are in `merchant_center.mca_ids`.

## Steps

### 1. Identify the correct feed and merchant
- Look up country `$1` in config.json `np_feeds`
- Find ALL Google NEW Products feed entries for the country (may be multiple shops)
- Resolve the correct Merchant Center sub-account ID:
  - If `$2` is provided, use it directly
  - Otherwise use `merchant_center.merchant_ids["$1"]`; for additional shops, match the shop domain to a sub-account of your MCA (ask the user if unsure)
- List existing datafeeds on that merchant: `merchant_center_list_datafeeds(merchant_id)`
- Check if a NEW Products feed already exists (avoid duplicates)

### 2. Verify feed accessibility
- Confirm the feed URL is reachable and contains valid product data
- Note: feed language and country MUST match the merchant sub-account's target market

### 3. Create the datafeed
- Call `merchant_center_insert_datafeed` with:
  - `merchant_id`: the sub-account ID
  - `name`: `"{CC} - Google NEW Products"` (e.g., "DE - Google NEW Products")
  - `feed_url`: from config.json `np_feeds`
  - `target_country`: country code (uppercase)
  - `content_language`: language ISO code matching the country (use LANG_ISO mapping below)
  - `feed_format`: "xml"
  - `fetch_hour`: 0
  - `fetch_timezone`: "America/Los_Angeles"
  - optional, recommended: `feed_label`: `"{CC}_NEW"` + `target_countries`: `["{CC}"]` — keeps NP products separate from the main feed (otherwise fix it later with `/ads-fix-feed-labels {CC}`)
- Destinations are set automatically: Shopping + SurfacesAcrossGoogle + DisplayAds (all marketing methods)

### 4. Trigger immediate fetch
- Call `merchant_center_fetch_datafeed_now(merchant_id, datafeed_id)` to force Google to fetch immediately
- If the fetchnow tool is not available, instruct user to click "Fetch now" in Merchant Center GUI

### 5. Verify processing
- Wait ~60 seconds
- Call `merchant_center_get_datafeed_status(merchant_id, datafeed_id)` to check:
  - `processingStatus`: should be "success" (not "none" or "failure")
  - `itemsTotal`: number of products in feed
  - `itemsValid`: number of valid products
  - `warnings` / `errors`: any issues
- If still "none", wait another 60s and retry (up to 3 times)

### 6. Report
Present a summary table in the user's language:

| Parameter | Value |
|----------|---------|
| Datafeed ID | {id} |
| Name | {name} |
| URL | {feed_url} |
| Country | {CC} ({country_name}) |
| Language | {lang} |
| Destinations | Shopping + SurfacesAcrossGoogle + DisplayAds |
| Merchant ID | {merchant_id} |
| Feed label | {feed_label} |
| Status | {processingStatus} |
| Products | {itemsValid} / {itemsTotal} |
| Errors | {errors count or "none"} |

## Multi-shop countries

If the country has multiple shops (e.g., a country with 3 shops: MAIN, OUTLET, PREMIUM), ask the user which shop/domain to set up, or offer to set up all of them sequentially.

## Language mapping reference

| CC | Language code | CC | Language code |
|----|-------------|-----|-------------|
| PL | pl | NL | nl |
| RO | ro | SE | sv |
| TR | tr | PT | pt |
| DE | de | HR | hr |
| FR | fr | BG | bg |
| IT | it | SI | sl |
| ES | es | LT | lt |
| CZ | cs | LV | lv |
| SK | sk | EE | et |
| HU | hu | UA | uk |
| GR | el | EN markets | en |

This command only uses Merchant Center tools — no Google Ads customer ID is needed. Merchant IDs vary per sub-account.
