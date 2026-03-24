---
description: "Find and fix keywords with wrong-domain final URLs (destination mismatch)"
---

# Fix Keyword URLs Command

Finds keywords with express-domain final URLs that mismatch with ad URLs (pharm/shop domain),
and clears their custom URLs so they inherit from the ad.

## Required Arguments
- **campaign_id** (optional — scan specific campaign or all)

## Workflow

### Step 1: Find express-domain keywords
```
google_ads_find_express_keywords(customer_id="YOUR_CUSTOMER_ID", campaign_id=CAMPAIGN_ID)
```

### Step 2: Review results
Show the user:
- How many keywords have express URLs
- Which campaigns/ad groups they're in
- Current approval status

### Step 3: Fix by clearing custom URLs
For each express keyword:
```
google_ads_update_keyword_url(
    customer_id="YOUR_CUSTOMER_ID",
    ad_group_id=AG_ID,
    criterion_id=CRITERION_ID,
    final_urls=null
)
```
This clears the keyword's custom final_url so it inherits from the ad (which has the correct domain).

### Alternative: Re-add keyword trick
If `update_keyword_url` doesn't work, use `add_keyword` with the same text and match type
but WITHOUT a custom URL. This overwrites the existing keyword and clears the URL:
```
google_ads_add_keyword(
    customer_id="YOUR_CUSTOMER_ID",
    ad_group_id=AG_ID,
    keyword_text="the keyword",
    match_type="EXACT"
)
```
Returns the same criterion_id = confirms overwrite worked.

## Notes
- Keywords without custom final_urls inherit from the ad's final_url
- "Destination mismatch" = keyword URL domain ≠ ad URL domain
- GAQL max 1000 results — if hit, re-run with campaign_id filter
- Some keywords may have "Destination not working" (broken landing page) — different issue, needs page fix not URL fix
