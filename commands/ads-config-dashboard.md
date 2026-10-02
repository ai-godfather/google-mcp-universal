---
name: ads-config-dashboard
description: View campaign configurations from local SQLite tracker (no API calls)
---

# /ads-config-dashboard [CC|TYPE|ID]

Read campaign configuration from the local SQLite database. No API calls — instant results. Present results in the user's language.

## Usage

- `/ads-config-dashboard` → Full cross-country rollout dashboard
- `/ads-config-dashboard DE` → All campaigns for Germany
- `/ads-config-dashboard SHOPPING_NP` → All Shopping NP campaigns across countries
- `/ads-config-dashboard 1234567890` → Deep detail for specific campaign

The tool reads the account in config.json (`account.customer_id`) unless the user passes another `customer_id`.

## Modes

### 1. Full Dashboard (no args)

Call `batch_campaign_config_dashboard()` — returns:
- **Per-country status**: has_pmax, has_shopping, has_shopping_np, pipeline_complete
- **Countries complete** vs **countries incomplete**
- **Total metrics**: campaigns, enabled/paused, 30d impressions, clicks, cost, conversions
- **Summary table**: one row per campaign with key config + metrics

Present as (costs in account currency):
```
Cross-Country Rollout Dashboard
================================

| CC | PMax | Shopping | Shop NP | Pipeline | 30d Cost | 30d ROAS |
|----|------|----------|---------|----------|----------|----------|
| DE | ✅   | ✅        | ✅       | ✅ DONE  | 40.00    | 1.80     |
| FR | ✅   | ✅        | ❌       | ⏳ 2/3   | 12.30    | 1.20     |
| US | ✅   | ❌        | ❌       | ⏳ 1/3   | 85.00    | 2.10     |

Complete: DE (1/3)
Incomplete: FR, US (2/3)
```

### 2. Country View (CC arg)

Call `batch_campaign_config_dashboard(country_code="DE")` — returns full config for each campaign in that country. Present all settings, bidding, budget, network, pipeline flags, metrics.

### 3. Type View (SHOPPING/SHOPPING_NP/PMAX)

Call `batch_campaign_config_dashboard(campaign_type="SHOPPING_NP")` — returns all campaigns of that type across all countries. Useful for checking which countries have Shopping NP set up.

### 4. Campaign Detail (campaign_id)

Call `batch_campaign_config_dashboard(campaign_id="1234567890")` — returns:
- Full campaign_config row (60+ fields)
- Conversion goals (with biddable flags)
- Targeting criteria (locations, languages)
- Last 7 days of daily metrics

## Prerequisites

Run `/ads-sync-config` first to populate the database. Data shows the state as of last sync (check `synced_at` timestamp).

Country codes come from the campaign name prefix (`DE | ...`); a Shopping campaign whose feed label contains `_NEW` is classified as SHOPPING_NP.

## Example

```
/ads-config-dashboard DE
```

Expected output (illustrative values):
```
Germany — 3 campaigns

1. DE | Performance Max (<PMAX_CAMPAIGN_ID>)
   Status: ENABLED | Channel: PERFORMANCE_MAX
   Shopping: merchant=333333333, feed_label=DE, priority=0, local=true
   Bidding: MAXIMIZE_CONVERSION_VALUE, target_roas=1.7
   Budget: 5.00/day (STANDARD, not shared)
   Network: search=true, content=true, partner=false
   Geo: positive=PRESENCE, negative=PRESENCE
   Pipeline: ✅ feed_label | ✅ conv_goals | - product_groups | ✅ enabled
   30d: 5,000 impr | 100 clicks | 30.00 cost | 2 conv | 50.00 value | ROAS 1.67

2. DE | Shopping (<SHOPPING_CAMPAIGN_ID>)
   Status: ENABLED | Channel: SHOPPING
   Shopping: merchant=333333333, feed_label=DE, priority=HIGH(2), local=false
   Bidding: TARGET_ROAS, target_roas=1.8
   ...

3. DE | Germany | Shopping | NEW PRODUCTS | NP (1234567890)
   Status: ENABLED | Channel: SHOPPING
   Shopping: merchant=333333333, feed_label=DE_NEW, priority=HIGH(2), local=false
   Bidding: TARGET_ROAS, target_roas=1.0
   Budget: 5.00/day (STANDARD, shared)
   Listing Groups: 100 units (custom_label_0)
   Pipeline: ✅ feed_label | ✅ conv_goals | ✅ product_groups | ✅ enabled | ✅ old_feed_labels
   30d: 0 impr (just enabled)
```
