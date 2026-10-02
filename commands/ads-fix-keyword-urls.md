---
description: "Find and fix keywords with wrong-domain final URLs (destination mismatch)"
---

# Fix Keyword URLs Command

Finds keywords whose own final URL points to a different domain than the ads (e.g. an old domain or a second store's domain),
and clears their custom URLs so they inherit from the ad.

## Required Arguments
- **campaign_id** (optional — scan specific campaign or all)
- Expected domain: config.json `domains[CC]` for the campaign's country (or the domain of the campaign's ads)

Account: `customer_id` = `account.customer_id` from config.json unless the user names another account (required by `google_ads_*` tools).

## Workflow

### Step 1: Find keywords with their own final URL
```
google_ads_execute_gaql(
    customer_id="YOUR_CUSTOMER_ID",
    query="SELECT campaign.id, ad_group.id, ad_group.name,
           ad_group_criterion.criterion_id, ad_group_criterion.keyword.text,
           ad_group_criterion.keyword.match_type, ad_group_criterion.final_urls,
           ad_group_criterion.approval_status
    FROM ad_group_criterion
    WHERE ad_group_criterion.type = 'KEYWORD'
      AND ad_group_criterion.status != 'REMOVED'
      AND campaign.id = CAMPAIGN_ID",
    page_size=1000
)
```
Keep the rows whose `final_urls` domain differs from the expected domain (drop the `campaign.id` filter to scan all campaigns).

### Step 2: Review results
Show the user:
- How many keywords have wrong-domain URLs
- Which campaigns/ad groups they're in
- Current approval status

### Step 3: Fix by clearing custom URLs (re-add keyword trick)
There is no tool that edits a keyword's final URL directly. Use `google_ads_add_keyword` with the same text and match type
but WITHOUT a custom URL. This overwrites the existing keyword and clears the URL:
```
google_ads_add_keyword(
    customer_id="YOUR_CUSTOMER_ID",
    ad_group_id=AG_ID,
    keyword_text="the keyword",
    match_type="EXACT"
)
```
Returns the same criterion_id = confirms overwrite worked. Re-run the Step 1 query to verify; if a URL is still set, clear it in the Google Ads UI or Google Ads Editor.

## Notes
- Keywords without custom final_urls inherit from the ad's final_url
- "Destination mismatch" = keyword URL domain ≠ ad URL domain
- GAQL max 1000 results per page — if hit, re-run with campaign_id filter
- Some keywords may have "Destination not working" (broken landing page) — different issue, needs page fix not URL fix
