---
name: ads-pmax-strategy
description: "Generate PMax/Shopping campaign architecture from Labelizer segmentation — Engine, Optimizer, Zombie Awakener"
---

# PMax Strategy Generator

Generate a complete campaign architecture based on product performance segmentation.

## Default Parameters
- **customer_id**: `account.customer_id` from config.json (required by `ecom_pmax_strategy`; use another account only if the user names it)
- **target_roas**: Ask user or default to `2.0`
- **daily_budget_gbp**: Ask user or default to `50.0` — total daily budget in **account currency** (the parameter name says GBP, the value is used as-is)

## Procedure

1. Ask user for target ROAS and total daily budget
2. Call `ecom_pmax_strategy` — this internally runs the Labelizer first
3. Present the 3-campaign blueprint (in the user's language, amounts in account currency):

```
=== CAMPAIGN ARCHITECTURE BLUEPRINT ===

💰 Total Daily Budget: X.XX

1️⃣ PMax "The Engine" (Heroes + Sidekicks)
   Budget: X.XX/day (60%)
   Bidding: tROAS X.X
   Products: X items
   Purpose: Scale proven winners

2️⃣ Shopping "The Optimizer" (Villains)
   Budget: X.XX/day (25%)
   Bidding: Manual CPC 0.05
   Products: X items
   Purpose: Controlled testing with bid caps

3️⃣ PMax "Zombie Awakener"
   Budget: X.XX/day (15%)
   Bidding: tROAS X.X (50% of normal)
   Products: X items
   Purpose: Force algorithm to test dormant products

🚫 Exclusion List:
   X products to remove immediately (X.XX wasted)
```

4. Offer to implement:
   - Create campaigns using `google_ads_create_pmax_campaign` / `google_ads_create_shopping_campaign`
   - Set custom labels in Merchant Center
   - Create product exclusions

## Implementation Steps

After user approves the blueprint:
1. Create exclusion list in existing PMax campaigns
2. Create "The Engine" PMax campaign with Hero+Sidekick product IDs
3. Create "The Optimizer" Shopping campaign for Villains
4. Create "Zombie Awakener" PMax with low tROAS
5. Set Custom Label 0 in Merchant Center for each segment
