---
description: "Migrate campaign ads from one domain to another (e.g. express→pharm) — full pipeline with verification"
---

# Domain Migration Command

Migrates all RSAs in a campaign from source domain to target domain. Handles the full pipeline:
create new RSAs → remove disapproved → verify clean.

## Required Arguments
- **country_code** (e.g. HR, BG, CH, HU)
- **campaign_id** (Google Ads campaign ID)
- **target_domain** (e.g. `bg.yourstore.com` or `ch.yourstore.com`)

## Workflow

### Step 1: Discover existing RSAs
```
google_ads_find_mixed_domain_groups(customer_id="YOUR_CUSTOMER_ID", campaign_id=CAMPAIGN_ID)
```
This auto-paginates and finds all ad groups with mixed domains.

### Step 2: Build replacement RSAs
For each mixed-domain group:
1. Take the best existing RSA (most headlines) from the WRONG domain
2. Copy headlines and descriptions
3. Replace the domain in final_urls with target_domain
4. Keep variant IDs in URLs (just swap domain)

### Step 3: Create new RSAs
Split into two batches:
- **Direct** (ad groups with <3 ENABLED RSAs): Create immediately
- **Needs-pause** (ad groups with 3 ENABLED RSAs): Pause 1 wrong-domain RSA first, then create

Use `google_ads_create_responsive_search_ad` for each.

### Step 4: Remove disapproved ads
```
batch_remove_not_eligible_ads(campaign_id=CAMPAIGN_ID, dry_run=true)
```
Then if results look good:
```
batch_remove_not_eligible_ads(campaign_id=CAMPAIGN_ID, dry_run=false)
```

### Step 5: Verify
```
google_ads_find_mixed_domain_groups(customer_id="YOUR_CUSTOMER_ID", campaign_id=CAMPAIGN_ID)
```
Must return `mixed_domain_groups: 0`. If not, repeat steps 4-5.

### Step 6: Fix keyword URLs
```
google_ads_find_express_keywords(customer_id="YOUR_CUSTOMER_ID", campaign_id=CAMPAIGN_ID)
```
For each express keyword found:
```
google_ads_update_keyword_url(customer_id="YOUR_CUSTOMER_ID", ad_group_id=AG_ID, criterion_id=CRITERION_ID, final_urls=null)
```

## Critical Notes
- GAQL returns max 1000 results — `find_mixed_domain_groups` auto-paginates
- Max 3 ENABLED RSAs per ad group — RESOURCE_LIMIT error if exceeded
- REMOVED ads are PERMANENT — data readable but ad cannot be restored
- Rate limit: ~1250 mutations/hour. NEVER run parallel batch jobs
- After creating RSAs with new domain, old wrong-domain RSAs get auto-disapproved (ONE_WEBSITE_PER_AD_GROUP)
- PAUSING wrong-domain RSAs is NOT enough — must REMOVE them for disapproval to clear

## Domain Mapping Reference
```
HR → hr.yourstore.com
BG → bg.yourstore.com
HU → hu.yourstore.com
CH → ch.yourstore.com
IT → eu.yourstore.com
IN → in.yourstore.com
MY → my.yourstore.com (still on express)
```
