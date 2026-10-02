---
description: Generate a Producthero supplemental feed CSV (product ID + Labelizer segment) to host as a Merchant Center supplemental source
---

# /ads-supplemental-feed — Generate Producthero Supplemental Feed CSV

## What it does
Generates a CSV supplemental feed containing product IDs and their Labelizer segment labels (`custom_label_4` by default, or the slot you choose). This is the Producthero-style approach for automatic daily label refresh, and the alternative to `/ads-labelizer-apply` when you have no label-push endpoint.

## How it works
1. Runs the Labelizer (180 days; with backend cross-validation if `backend_conversions` is configured in config.json)
2. Generates CSV: `id,custom_label_4` with HERO/SIDEKICK/VILLAIN/ZOMBIE per product
3. Saves to file (`output_path`) or returns inline
4. User hosts the CSV at a public URL they control (e.g. `https://feeds.yourshop.com/de_labels.csv`) and adds it as MC Supplemental Source

## Why use this instead of Content API?
- **Automatic daily refresh** — MC fetches the CSV on schedule
- **Products automatically move between campaigns** as labels change
- **No API quota usage** — MC handles the fetching
- **Scalable** — works across all countries with one setup per feed

## MC Setup (one-time)
1. Go to Google Merchant Center → Data Sources → Supplemental Sources
2. Add supplemental product data → paste the hosted URL
3. Set fetch schedule to DAILY (recommend 10 AM)
4. Match feed label and language to your primary feed

Regenerate and re-upload the CSV regularly (e.g. weekly) so the labels follow performance.

## Parameters
- `country_code` — **required** (its merchant comes from config.json `merchant_center.merchant_ids`)
- `customer_id` — defaults to `account.customer_id` from config.json
- `output_path` — file path to save CSV (e.g., `/path/to/de_labels.csv`); empty = return inline
- `custom_label_slot` — default 4 (`custom_label_4`)
- `target_roas` — default 2.0
- `days_back` — default 180

## Example
```
/ads-supplemental-feed DE
```
