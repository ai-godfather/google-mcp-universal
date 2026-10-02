---
name: ads-labelizer
description: "Producthero Labelizer — classify products into Hero/Villain/Sidekick/Zombie segments based on ROAS performance"
---

# Producthero Labelizer 2.0

Run the `ecom_labelizer_run` tool to classify all shopping products into performance segments.

## Default Parameters
- **customer_id**: `account.customer_id` from config.json (required by `ecom_labelizer_run`; use another account only if the user names it)
- **target_roas**: Ask user or default to `2.0`
- **days_back**: `180` (last 180 days of data for reliable classification)
- **min_clicks_zombie**: `5`

## Procedure

1. Ask user which country to analyze (`country_code` — limits the data to that country's merchant from config.json `merchant_center.merchant_ids`)
2. Ask user for their target ROAS (default: 2.0)
3. Call `ecom_labelizer_run` with the parameters
4. Present results in a clear table format (in the user's language):

### Output Format

```
=== LABELIZER REPORT ===

Segment Summary:
| Segment    | Products | Cost     | Revenue  | Avg ROAS | Cost % | Revenue % |
|------------|----------|----------|----------|----------|--------|-----------|
| HERO       | X        | X.XX     | X.XX     | X.XX     | X%     | X%        |
| VILLAIN    | X        | X.XX     | X.XX     | X.XX     | X%     | X%        |
| SIDEKICK   | X        | X.XX     | X.XX     | X.XX     | X%     | X%        |
| ZOMBIE     | X        | X.XX     | X.XX     | X.XX     | X%     | X%        |

Top Heroes (scale these):
[list top 10 with ROAS, revenue]

Top Villains (reduce/exclude):
[list top 10 with cost, low ROAS]

Top Sidekicks (increase visibility):
[list top 10 with ROAS, low clicks]

Zombies needing reactivation:
[list top 10 with impressions]
```

Amounts are in the account currency.

**Optional backend cross-validation**: if config.json has `backend_conversions` (a CSV URL of your own orders with `{date_start}`, `{date_end}`, `{country}` placeholders and a `columns` map), the result also contains `backend_cross_validation` — Google Ads attribution compared with your real orders per product. Mention large discrepancies. Without that config the section only reports that it is not configured.

5. Offer next steps:
   - `/ads-labelizer-apply {CC}` — Push labels to your feed (custom_label_4) via the label-push endpoint
   - `/ads-supplemental-feed {CC}` — Same labels as a Merchant Center supplemental feed (no endpoint needed)
   - `/ads-labelizer-deploy {CC}` — Deploy bid differentiation (Shopping listing groups RECOMMENDED, or 4 PMax)
   - `/ads-pmax-strategy` — Generate campaign architecture based on segments
   - Exclude Villains from PMax using `google_ads_add_product_exclusion`

**IMPORTANT**: The full pipeline is: `/ads-labelizer` (classify) → `/ads-labelizer-apply` or `/ads-supplemental-feed` (push labels) → `/ads-labelizer-deploy` (bid differentiation). ALL 3 steps required. After deploy, ALWAYS run the verification checklist from `/ads-labelizer-deploy`.

## Segment Classification Matrix

| | High Clicks (≥ median) | Low Clicks (< median) |
|---|---|---|
| **High ROAS (≥ target)** | HERO (2.0× bid) | SIDEKICK (2.0× bid) |
| **Low ROAS (< target)** | VILLAIN (0.5× bid) | ZOMBIE (1.5× bid) |
| **< 5 clicks total** | — | ZOMBIE (1.5× bid) |
