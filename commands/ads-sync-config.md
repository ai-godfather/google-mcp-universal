---
name: ads-sync-config
description: Sync Shopping + PMax campaign configs from Google Ads API into local SQLite tracker
---

# /ads-sync-config [CC|ALL]

Sync ALL Shopping + PMax campaign configurations from Google Ads API into the local campaign_config database. Present results in the user's language.

## Usage

- `/ads-sync-config` → Sync ALL Shopping + PMax campaigns across all countries
- `/ads-sync-config DE` → Sync only Germany campaigns
- `/ads-sync-config DE,FR,US` → Sync specific countries (one call per country)

The tool uses the account in config.json (`account.customer_id`) unless the user passes another `customer_id`.

## Steps

1. Call `batch_sync_campaign_config(country_code=CC)` (or no filter for ALL; `campaign_ids=[...]` syncs specific campaigns)
2. Present summary table:
   - Per campaign: name, type, status, feed_label, bidding, budget, listing_groups, 30d metrics
   - Pipeline flags: feed_label_set, conversion_goals_set, product_groups_built, campaign_enabled
3. Show errors if any (API failures, missing data)
4. If syncing ALL: show cross-country rollout overview

## Naming Conventions Used by the Tracker

- **Country**: taken from the campaign name prefix before the first `|` (e.g. `DE | Germany | Shopping`)
- **campaign_type**: PMAX for Performance Max; SHOPPING_NP for Shopping campaigns whose feed label contains `_NEW`; otherwise SHOPPING

## What Gets Synced

Per campaign (into `campaign_config` table):
- **Identity**: campaign_id, name, country_code, campaign_type (SHOPPING/SHOPPING_NP/PMAX)
- **Shopping Settings**: merchant_id, feed_label, priority, enable_local
- **Bidding**: strategy_type, target_roas, target_cpa, enhanced_cpc
- **Budget**: daily_budget_micros, delivery_method, shared
- **Network**: search, content, partner networks
- **Geo Targeting**: positive/negative_geo_target_type
- **Structure**: ad_group_count, listing_group_count, listing_group_dimension, ad_count, asset counts
- **Pipeline Flags**: feed_label_set, conversion_goals_set, product_groups_built, campaign_enabled
- **Metrics**: 30d/7d/1d impressions, clicks, cost, conversions, ROAS

Into related tables:
- `campaign_conversion_goals` — all goals with biddable flag
- `campaign_targeting` — location + language criteria
- `campaign_ad_groups` — all ad groups with structure info
- `campaign_metrics_history` — daily metrics snapshots (last 30 days)

## Example

```
/ads-sync-config DE
```

Expected output (illustrative values, budgets in account currency):
```
Synced 3 DE campaigns:

| Campaign | Type | Status | feed_label | Bidding | Budget | LG | 30d Impr | 30d ROAS | Pipeline |
|----------|------|--------|------------|---------|--------|----|----------|----------|----------|
| DE | PMax | ENABLED | DE | MCV 1.7 | 5.00/d | - | 5,000 | 1.67 | ✅✅-✅ |
| DE | Shopping | ENABLED | DE | TROAS 1.8 | 7.50/d | 1 | 900 | 1.20 | ✅✅-✅ |
| DE | Shopping NP | ENABLED | DE_NEW | TROAS 1.0 | 5.00/d | 100 | 0 | - | ✅✅✅✅ |

Pipeline: feed_label / conv_goals / product_groups / enabled
Daily metrics history: 90 rows synced
```

## After Sync

Use `/ads-config-dashboard` to view the data without API calls.
