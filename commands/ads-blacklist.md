---
description: Image variant A/B test and blacklist losers
allowed-tools: ["mcp__google-ads__google_ads_execute_gaql", "mcp__google-ads__google_ads_list_campaigns", "Bash"]
argument-hint: <CC>
---

Run image variant A/B testing for country `$1` (uppercase). Present in Polish.

**Steps:**
1. Find ALL Shopping + PMax campaigns for country "$1":
   ```sql
   SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type
   FROM campaign
   WHERE campaign.advertising_channel_type IN ('SHOPPING', 'PERFORMANCE_MAX')
     AND campaign.status != 'REMOVED'
   ```
2. For EACH campaign, pull shopping_performance_view:
   ```sql
   SELECT campaign.id, campaign.name, segments.product_item_id,
          metrics.impressions, metrics.clicks, metrics.cost_micros,
          metrics.conversions, metrics.conversions_value
   FROM shopping_performance_view
   WHERE campaign.id = {ID} AND metrics.impressions > 0
   ORDER BY metrics.impressions DESC
   ```
3. Aggregate across all campaigns by item_id. Parse variants (last `_N` suffix). Group by base product. Calculate CTR per variant. Winner = highest CTR, rest = losers (min 2 variants, 10+ impressions each).
4. Write to `dat/BLACKLIST/gMerchant/{CC}.txt` (ALWAYS overwrite entire file):
   ```
   ============== UPDATE: {YYYY-MM-DD_HH-MM-SS} NON_PERFORMING_VARIANTS (Shopping+PMax COMBINED) ==============
   {loser_item_id_1}
   ...
   ```
5. Git commit + push:
   ```bash
   cd /path/to/your-project
   git add dat/BLACKLIST/gMerchant/{CC}.txt
   git commit -m "Update {CC}.txt blacklist: {N} losers (Shopping+PMax combined)"
   git push
   ```

Always use customer_id `YOUR_CUSTOMER_ID`. ALWAYS include Shopping + PMax data together. ALWAYS overwrite (not append).
