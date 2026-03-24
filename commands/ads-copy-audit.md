---
description: "Audit RSA copy for country/language mismatches and template errors"
---

# Copy Audit Command

Scans RSA headlines and descriptions for common template errors:
wrong country names, wrong languages, wrong currencies, wrong domains.

## Required Arguments
- **campaign_id** (Google Ads campaign ID)
- **expected_country** (e.g. "India", "Malaysia", "Hungary")

## Workflow

### Step 1: Query all ENABLED RSAs with full copy
```
google_ads_execute_gaql(
    customer_id="YOUR_CUSTOMER_ID",
    query="SELECT ad_group.id, ad_group.name, ad_group_ad.ad.id,
           ad_group_ad.ad.responsive_search_ad.headlines,
           ad_group_ad.ad.responsive_search_ad.descriptions,
           ad_group_ad.ad.final_urls
    FROM ad_group_ad
    WHERE campaign.id = CAMPAIGN_ID
      AND ad_group_ad.status = 'ENABLED'
      AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'"
)
```

### Step 2: Parse and check for mismatches
Using Python, check each RSA for:

**Country name mismatches:**
- "Belgium" in a Malaysia campaign
- "Vietnam"/"Wietnam"/"वियतनाम" in an India campaign
- "Romania" in a Bulgaria campaign
- Any country name that doesn't match expected_country

**Language mismatches:**
- Romanian text ("Cumpărați", "Comandă") in a Bulgarian campaign
- Hindi text in an English-language campaign
- French text in a German campaign

**Currency mismatches:**
- € in CHF markets
- PLN in RON markets

**Domain mismatches:**
- express domain in pharm/shop campaigns
- Wrong country subdomain (e.g. hu. in bg. campaign)

### Step 3: Report findings
For each issue found, report:
- Ad group name and ID
- Ad ID
- The problematic text (headline or description)
- What's wrong and what it should be
- Severity (HIGH = wrong country/language, MEDIUM = wrong currency, LOW = minor)

### Step 4: Generate fix payloads
For HIGH severity issues, prepare:
- New RSA creation payload with corrected copy
- Old RSA to pause/remove

## Common Template Errors Found
From session 2026-03-18:
- **IN Alpha Power**: "वियतनाम" (Vietnam) instead of India in descriptions
- **MY Dazibet**: "Belgium" instead of "Malaysia" in descriptions
- **BG campaign**: "Site-ul oficial" (Romanian) appeared in Bulgarian ads

## Notes
- GAQL max 1000 results — use pagination for large campaigns
- Headlines max 30 chars — be careful when replacing country names
- Descriptions max 90 chars — same constraint
- Some "wrong language" may be intentional (e.g. French in Swiss campaign for FR-speaking regions)
