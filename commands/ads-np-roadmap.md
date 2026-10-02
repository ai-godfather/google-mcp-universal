---
description: "NP Shopping roadmap — per-country rollout status of new-products Shopping + PMax campaigns, ranked by priority, Merchant Center eligible products and conversion value"
---

# /ads-np-roadmap — NP Shopping Deployment Roadmap

## What this command does

Shows the NP (New Products) Shopping + PMax rollout per country: which countries already run NP campaigns, which still need an NP feed or campaign, and which are blocked in Merchant Center. Data comes from the local roadmap tracker (`batch_np_roadmap_dashboard`), cross-checked with the synced campaign config. Present results in the user's language.

## One-time setup: seed the roadmap

Add your countries to config.json `np_roadmap` (all numbers are your own baseline, e.g. last 180 days):

```json
"np_roadmap": {
  "seed": [
    {"country_code": "DE", "country_name": "Germany", "tier": "T1", "priority_rank": 1,
     "merchant_id": "333333333", "eligible_products": 0, "total_products": 0,
     "conversion_value": 0, "leads": 0, "roas": 0, "xml_products": 0, "mc_status": "ACTIVE"}
  ],
  "np_existing": ["US"]
}
```

- `tier`: T1 = scale (NP already running), T2 = roll out (needs NP feed + campaign), T3 = reactivate (Merchant Center needs repair)
- `np_existing`: countries that already run NP campaigns (seeded as ENABLED)

Then run `batch_np_roadmap_update(action="seed")`. Re-seeding upserts the rows and resets their Shopping/PMax NP status (ENABLED for `np_existing`, otherwise NOT_STARTED) — keep `np_existing` up to date before re-seeding.

## Steps

1. `batch_np_roadmap_dashboard()` — progress (total countries, Shopping NP / PMax NP deployed, old feed labels fixed, completion %) plus one row per country. If it returns `empty`, do the one-time setup above.
2. `batch_campaign_config_dashboard(campaign_type="SHOPPING_NP")` — cross-check against the campaigns actually synced (run `/ads-sync-config` first for fresh data)
3. For each roadmap country, check:
   - Does it have an ENABLED Shopping campaign with NP feed label (`{CC}_NEW`)?
   - What is the current `mc_eligible_products` count?
   - Any changes since the last roadmap update?
4. Present as a table sorted by `priority_rank`, with traffic light status:
   - GREEN: NP Shopping active + products flowing
   - YELLOW: MC active but no NP campaign yet
   - RED: MC NOT_ELIGIBLE or EMPTY — blocked
5. Suggest the next action for each country:
   - YELLOW → `/ads-add-np-feed {CC}` (if there is no NP feed yet), then `/ads-setup-shopping-np {CC}`
   - RED → repair Merchant Center first (`/ads-merchant-gc`, account/feed issues)
   - GREEN → scale: `/ads-shopping-coverage {CC}`, `/ads-labelizer {CC}`
6. Progress tracker: how many roadmap countries have NP Shopping active

## Updating the roadmap

- `/ads-setup-shopping-np` logs each step with `batch_np_roadmap_update` (`feed_label_fixed`, `shopping_created`, `pmax_created`, `mc_verified`)
- When the user enables NP campaigns: `batch_np_roadmap_update(country_code="{CC}", action="shopping_enabled")` / `action="pmax_enabled"`
- To refresh baseline numbers (eligible products, conversion value, leads): re-sync with `/ads-sync-config`, update `np_roadmap.seed` in config.json, then re-seed (see the warning above)

The roadmap is a local SQLite tracker (no API calls); numbers are as fresh as the last seed/update. The config dashboard uses `account.customer_id` from config.json by default.
