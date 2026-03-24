---
description: Add Google NEW Products feed to Merchant Center for a country and trigger immediate fetch
allowed-tools: ["mcp__google-ads__merchant_center_insert_datafeed", "mcp__google-ads__merchant_center_fetch_datafeed_now", "mcp__google-ads__merchant_center_get_datafeed_status", "mcp__google-ads__merchant_center_get_datafeed", "mcp__google-ads__merchant_center_list_datafeeds", "mcp__google-ads__merchant_center_list_datafeed_statuses"]
argument-hint: <CC> [merchant_id]
---

Add a Google NEW Products XML feed to a Merchant Center sub-account for country `$1`, optionally targeting merchant_id `$2`. Present all results in Polish.

## Context

The NP_FEEDS registry in batch_optimizer.py contains Google NEW Products feed URLs for 93 countries. Each country may have multiple shops/domains, each with its own feed URL and Merchant Center sub-account.

**Feed URL pattern**: `https://feedhlm.cdn-shopifycloud.com/h/{hash}` or `https://slm.cdn-shopifycloud.com/h/{hash}`

**Two MCA accounts**:
- **YOUR_MCA_ID** (Your Brand) — PL, RO, CZ, DE, ES, FR, GR, HU, IT, SK, AT, TR
- **YOUR_MCA_ID** (Your Brand) — 37 international markets

## Steps

### 1. Identify the correct feed and merchant
- Look up country `$1` in the NP_FEEDS registry (from SKILL.md or batch_optimizer.py)
- Find ALL Google NEW Products feed entries for the country (may be multiple shops)
- Resolve the correct Merchant Center sub-account ID:
  - If `$2` is provided, use it directly
  - Otherwise, match domain to sub-account from the MCA structure in SKILL.md
- List existing datafeeds on that merchant: `merchant_center_list_datafeeds(merchant_id)`
- Check if a NEW Products feed already exists (avoid duplicates)

### 2. Verify feed accessibility
- Confirm the feed URL is reachable and contains valid product data
- Note: feed language and country MUST match the merchant sub-account's target market

### 3. Create the datafeed
- Call `merchant_center_insert_datafeed` with:
  - `merchant_id`: the sub-account ID
  - `name`: `"{CC} - Google NEW Products"` (e.g., "PL - Google NEW Products")
  - `feed_url`: from NP_FEEDS registry
  - `target_country`: country code (uppercase)
  - `content_language`: language ISO code matching the country (use LANG_ISO mapping below)
  - `feed_format`: "xml"
  - `fetch_hour`: 0
  - `fetch_timezone`: "America/Los_Angeles"
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
Present a summary table in Polish:

| Parametr | Wartość |
|----------|---------|
| Datafeed ID | {id} |
| Nazwa | {name} |
| URL | {feed_url} |
| Kraj | {CC} ({country_name}) |
| Język | {lang} |
| Destynacje | Shopping + SurfacesAcrossGoogle + DisplayAds |
| Merchant ID | {merchant_id} |
| Status | {processingStatus} |
| Produkty | {itemsValid} / {itemsTotal} |
| Błędy | {errors count or "brak"} |

## Multi-shop countries

If the country has multiple shops (e.g., PL has 5: HLP, DO, HLE, VGL, HBS), ask the user which shop/domain to set up, or offer to set up all of them sequentially.

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

Always use customer_id `YOUR_CUSTOMER_ID` for Google Ads operations. Merchant IDs vary per sub-account.
