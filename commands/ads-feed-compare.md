---
name: ads-feed-compare
description: "Compare product catalogs between two Merchant Center accounts — find products that exist in one store but not in the other"
---

# Feed Compare (Store A vs Store B)

Compare products between two merchant accounts to find unique items — e.g. your main store vs a second (outlet/express) store selling in the same country.

## Default Parameters
- **customer_id**: `account.customer_id` from config.json (required by `ecom_feed_compare`; use another account only if the user names it)
- **only_eligible**: `false` (set `true` to compare ELIGIBLE products only)

## Choosing the Merchant Pairs

Take the merchant IDs from config.json — `merchant_center.merchant_ids` (one per country) and, for extra stores in the same country, `shopping_coverage.stores` — or ask the user. Example pair (fake IDs):

| Country | Store A (main) | Store B (second store) |
|---------|----------------|------------------------|
| DE | 333333333 | 444444444 |

## Procedure

1. Ask user which countries/merchants to compare
2. Call `ecom_feed_compare` for each pair:
   ```
   ecom_feed_compare(
       customer_id="<CUSTOMER_ID>",
       merchant_a_id="<STORE_A_MERCHANT_ID>",
       merchant_b_id="<STORE_B_MERCHANT_ID>",
       merchant_a_name="Store A",
       merchant_b_name="Store B",
       only_eligible=false
   )
   ```
3. Present results in the user's language:

```
=== FEED COMPARISON: Store A vs Store B (Country) ===

| Metric            | Store A | Store B |
|-------------------|---------|---------|
| Total products    | X       | X       |
| Shared            | X       |         |
| Unique to Store A | X       |         |
| Unique to Store B | X       |         |

🆕 Products ONLY in Store B:
[list with title, status, feed_label]

📋 Products ONLY in Store A:
[list with title, status, feed_label]
```

4. For unique products that are ELIGIBLE → recommend creating dedicated campaigns (or adding them to the other store's catalog)
5. For NOT_ELIGIBLE products → recommend Merchant Center audit (`/ads-merchant-gc`)
