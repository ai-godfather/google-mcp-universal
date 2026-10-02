---
description: "Migrate campaign ads from one domain to another (e.g. from a second store's domain to your main store) — full pipeline with verification"
---

# Domain Migration Command

Migrates all RSAs in a campaign from source domain to target domain. Handles the full pipeline:
create new RSAs → remove disapproved → verify clean.

## Required Arguments
- **country_code** (e.g. HR, BG, CH, HU)
- **campaign_id** (Google Ads campaign ID)
- **target_domain** (e.g. `bg.yourstore.com` or `ch.yourstore.com`; default: config.json `domains[country_code]`)

Account: `customer_id` = `account.customer_id` from config.json unless the user names another account (`google_ads_*` tools require it; batch tools use the configured account).

## Workflow

### Step 1: Discover existing RSAs
```
google_ads_execute_gaql(
    customer_id="YOUR_CUSTOMER_ID",
    query="SELECT ad_group.id, ad_group.name, ad_group_ad.ad.id, ad_group_ad.status,
           ad_group_ad.ad.final_urls,
           ad_group_ad.ad.responsive_search_ad.headlines,
           ad_group_ad.ad.responsive_search_ad.descriptions
    FROM ad_group_ad
    WHERE campaign.id = CAMPAIGN_ID
      AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
      AND ad_group_ad.status != 'REMOVED'",
    page_size=1000
)
```
Group the RSAs by ad group and final-URL domain: ad groups that still contain the source domain (alone or mixed with the target domain) need migration. Paginate for large campaigns.

**Shortcut**: `batch_fix_domains(country_code, campaign_id, dry_run=true, target_domain=...)` lists the RSAs whose domain does not match the target; with `dry_run=false` it pauses them and creates copies with the target domain as a background job (poll `batch_fix_domains_progress(job_id)`). It can replace Steps 2-3; Steps 4-6 are still needed.

### Step 2: Build replacement RSAs
For each ad group to migrate:
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
Re-run the Step 1 query. Every ENABLED RSA must use the target domain (no ad group with mixed domains). If not, repeat steps 4-5.

### Step 6: Fix keyword URLs
Keywords with their own final URL on the old domain cause "destination mismatch" — run `/ads-fix-keyword-urls CAMPAIGN_ID` (GAQL on `ad_group_criterion.final_urls`, then the re-add keyword trick with `google_ads_add_keyword`).

## Critical Notes
- GAQL returns max 1000 results per page — paginate (or filter by ad group)
- Max 3 ENABLED RSAs per ad group — RESOURCE_LIMIT error if exceeded
- REMOVED ads are PERMANENT — data readable but ad cannot be restored
- Rate limit: ~1250 mutations/hour. NEVER run parallel batch jobs
- After creating RSAs with new domain, old wrong-domain RSAs get auto-disapproved (ONE_WEBSITE_PER_AD_GROUP)
- PAUSING wrong-domain RSAs is NOT enough — must REMOVE them for disapproval to clear (`batch_remove_wrong_domain_ads(dry_run=true)` previews this account-wide against config.json `domains`)

## Domain Mapping Reference
Target domains come from config.json `domains` (country → shop domain), e.g.:
```
HR → hr.yourstore.com
BG → bg.yourstore.com
HU → hu.yourstore.com
CH → ch.yourstore.com
IT → eu.yourstore.com
```
