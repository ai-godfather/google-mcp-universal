---
name: ads-channable
description: "Channable Rules Engine — apply IF-THEN transformation rules to product feed data"
---

# Channable Rules Engine

Apply Channable-style IF-THEN rules to product feed data via `ecom_channable_transform`.

## Default Parameters
- **customer_id**: `account.customer_id` from config.json (required by `ecom_channable_transform`; use another account only if the user names it)
- **merchant_id**: from config.json `merchant_center.merchant_ids` (ask which country/store)
- **feed_label**: optional filter (e.g. `DE`)
- **dry_run**: `true` (always start with dry run!)

## Procedure

1. Ask user which merchant to process and what rules to apply
2. If user doesn't specify rules, suggest common ones:

### Common Rules

```
Rule 1: NORMALIZE titles (clean HTML entities, fix casing)
Rule 2: IF brand IS EMPTY THEN extract_from_title
Rule 3: IF title CONTAINS "low price" THEN custom_label_1 = "discount"
Rule 4: IF title CONTAINS "off" THEN custom_label_1 = "promo"
Rule 5: IF status = NOT_ELIGIBLE THEN flag for review
Rule 6: IF price < 20 THEN exclude (too low margin)
```

3. Run `ecom_channable_transform(customer_id, merchant_id, rules=[...], feed_label, dry_run=true)` FIRST
4. Present changes summary (in the user's language):

```
=== CHANNABLE TRANSFORM REPORT (DRY RUN) ===

📋 Rules Applied: X
📦 Products Scanned: X

Changes:
| Type              | Count |
|-------------------|-------|
| Field changes     | X     |
| Exclusions        | X     |
| Custom label updates | X  |
| Warnings          | X     |

[Show detailed changes list]
```

5. Ask user to confirm before running with dry_run=false
6. To apply custom labels, use `merchant_center_update_custom_labels`
