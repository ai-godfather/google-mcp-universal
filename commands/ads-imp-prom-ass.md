---
description: Implement percent-off Promotion Assets (default 50%) across all campaigns missing them
allowed-tools: ["mcp__google-ads__batch_implement_promotion_assets"]
---

Implement percent-off Promotion Assets for all campaigns that are missing them. Execute all steps and present results in the user's language.

Ask the user for the discount they really offer and pass it as `percent_off` (tool default 50) — promotions must match a real offer on the landing page.

**Steps:**

1. Call `batch_implement_promotion_assets` with **dry_run=true** first — preview all campaigns:
   - Use campaign IDs/names from your Google Ads account
   - Verify country detection, language codes, promotional texts, and final_urls
   - Show summary table: campaign | country | language | promo texts | occasion | final_urls

2. Present dry-run results to user and ask for confirmation before proceeding.

3. On confirmation, call `batch_implement_promotion_assets` with **dry_run=false**:
   - `percent_off` discount for all campaigns (default 50)
   - 3 localized promotion texts per campaign (auto-detected by country)
   - Rotating occasions: SPRING_SALE, SUMMER_SALE, END_OF_SEASON, WOMENS_DAY
   - Final URLs auto-fetched from existing ads

4. Return a summary with:
   - Total assets created & linked
   - Per-campaign status table
   - Any errors encountered

Account: the tools use `account.customer_id` from config.json.

**Example campaign format:**
```json
[
  {"campaign_id": "CAMPAIGN_ID_1", "campaign_name": "PL | Poland | Search | Product"},
  {"campaign_id": "CAMPAIGN_ID_2", "campaign_name": "TR | Turkey | Search | Product"},
  {"campaign_id": "CAMPAIGN_ID_3", "campaign_name": "DE | Germany | Performance Max"},
  {"campaign_id": "CAMPAIGN_ID_4", "campaign_name": "RO | Romania | Shopping"},
  {"campaign_id": "CAMPAIGN_ID_5", "campaign_name": "FR | France | Search | All Products"}
]
```
