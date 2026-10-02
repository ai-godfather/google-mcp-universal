---
description: Fix feed labels on Merchant Center data sources — change NP feeds from CC to CC_NEW so they can be separated in Google Ads campaigns
allowed-tools: ["mcp__google-ads__merchant_center_list_datafeeds", "mcp__google-ads__merchant_center_get_datafeed", "mcp__google-ads__merchant_center_update_datafeed", "mcp__google-ads__merchant_center_insert_datafeed", "mcp__google-ads__merchant_center_delete_datafeed", "mcp__google-ads__merchant_center_fetch_datafeed_now", "mcp__google-ads__merchant_center_get_datafeed_status", "mcp__google-ads__merchant_center_list_datafeed_statuses"]
argument-hint: <CC|ALL> [merchant_id]
---

Fix feed labels on Merchant Center data sources for country `$1`, optionally targeting merchant_id `$2`. Present all results in the user's language.

## Problem

When a NEW Products (NP) feed is added with only the legacy `targetCountry` field (for example via `/ads-add-np-feed` without `feed_label`), Merchant Center auto-generates `feedLabel = CC` (e.g. "DE"). The NP feed and the main feed then have **identical feed labels**, making it impossible to target them separately in Google Ads Shopping campaigns.

**Fix**: Change the NP feed's feed label from `CC` to `CC_NEW` (e.g. "DE" → "DE_NEW").

## Strategy: Option A first, then Option B fallback

**Option A — Update existing datafeed** (preferred, no data loss):
- Use `merchant_center_update_datafeed` with `feed_label` parameter
- If the API accepts it, the feed label changes instantly
- Products get re-labeled after the next fetch

**Option B — Delete + Create** (fallback if Option A fails):
- Delete the old NP datafeed
- Create a new one with the correct `feed_label` via `merchant_center_insert_datafeed`
- Trigger immediate fetch
- Products need time to reprocess (~30-60 min)

## Merchant Center accounts

Read the accounts from config.json — never guess IDs:
- `merchant_center.merchant_ids` — Merchant Center sub-account per country
- `domains` — shop domain per country (helps to recognise which datafeed belongs to which shop)
- `merchant_center.mca_ids` — your MCA(s); `merchant_center_list_accounts(mca_id=...)` lists further sub-accounts, e.g. a second store selling in the same country

If `$2` is given, use that merchant ID directly.

Example (fake values):

| CC | Merchant ID | Domain |
|----|-------------|--------|
| DE | 333333333 | de.yourshop.com |

## Language mapping

| CC | Lang | CC | Lang |
|----|------|----|------|
| PL | pl | NL | nl |
| RO | ro | SE | sv |
| TR | tr | HR | hr |
| DE | de | BG | bg |
| FR | fr | SI | sl |
| IT | it | LT | lt |
| ES | es | LV | lv |
| CZ | cs | EE | et |
| SK | sk | UA | uk |
| HU | hu | EN | en |
| GR | el | | |

## Steps

### 1. List existing datafeeds
```
merchant_center_list_datafeeds(merchant_id=MERCHANT_ID)
```
Identify which datafeeds are "NEW Products" feeds. Look for:
- Name containing "NEW Products" or "Google NEW" or "NP"
- Multiple feeds targeting the same country

### 2. Check current feed labels
For each NP datafeed found:
```
merchant_center_get_datafeed(merchant_id=MERCHANT_ID, datafeed_id=DATAFEED_ID)
```
Examine `targets[].feedLabel` or `targetCountry`. If feedLabel == CC (same as main feed), it needs fixing.

### 3. Try Option A — Update feed label
```
merchant_center_update_datafeed(
    merchant_id=MERCHANT_ID,
    datafeed_id=NP_DATAFEED_ID,
    feed_label="{CC}_NEW",
    target_countries=["{CC}"]
)
```
- If `status: "success"` and `feed_label_changed: true` → done!
- If error with hint about Option B → proceed to Step 4

### 4. Fallback: Option B — Delete + Create

**4a. Save current datafeed config** (name, feed_url, content_language, fetch settings)

**4b. Delete old datafeed**:
```
merchant_center_delete_datafeed(merchant_id=MERCHANT_ID, datafeed_id=OLD_NP_DATAFEED_ID)
```

**4c. Create new datafeed with correct label**:
```
merchant_center_insert_datafeed(
    merchant_id=MERCHANT_ID,
    name="{CC} Feed (New)",
    feed_url=SAVED_FEED_URL,
    content_language=LANG_ISO[CC],
    target_country="{CC}",
    feed_label="{CC}_NEW",
    target_countries=["{CC}"],
    feed_format="xml",
    fetch_hour=0,
    fetch_timezone="America/Los_Angeles"
)
```

**4d. Trigger immediate fetch**:
```
merchant_center_fetch_datafeed_now(merchant_id=MERCHANT_ID, datafeed_id=NEW_DATAFEED_ID)
```

### 5. Verify
Wait 60s, then:
```
merchant_center_get_datafeed_status(merchant_id=MERCHANT_ID, datafeed_id=DATAFEED_ID)
```
Confirm `processingStatus: "success"` and `itemsValid > 0`.

### 6. Report

Present a summary in the user's language:

| Account | Merchant ID | Old label | New label | Method | Status |
|---------|-------------|-----------|-----------|--------|--------|
| DE | 333333333 | DE | DE_NEW | Option A/B | ✅/❌ |

**Next steps**: after the feed label change, the Shopping campaign settings in Google Ads should show two separate feed labels:
- `{CC}` — main feed
- `{CC}_NEW` — NEW Products feed

## Batch mode (CC=ALL)

If `$1` is `ALL`, iterate over all merchant accounts from config.json (`merchant_center.merchant_ids`, plus any extra sub-accounts the user confirms):
1. List datafeeds on each account
2. Identify NP feeds with duplicate labels
3. Fix each one (Option A → Option B fallback)
4. Present final summary table

**IMPORTANT**: Process accounts sequentially, not in parallel. Each account fix is independent.

## Safety

- ALWAYS show the user what will change BEFORE executing
- ALWAYS try Option A first
- NEVER delete the MAIN feed — only the NP (NEW Products) feed
- If unsure which feed is "new" vs "main", check product counts and feed URL patterns
- After Option B delete+create, the new datafeed ID will be different — update any references
