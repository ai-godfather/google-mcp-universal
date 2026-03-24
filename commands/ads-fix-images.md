---
description: Find and fix missing/failed product images
allowed-tools: ["mcp__google-ads__batch_missing", "mcp__google-ads__batch_enhance_images", "mcp__google-ads__batch_add_images_only"]
argument-hint: <CC> <campaign_id>
---

Find and fix missing or failed images for country `$1`, campaign `$2`. Present in Polish.

**Steps:**
1. Call `batch_missing` with country_code="$1", campaign_id="$2" — filter products missing images
2. For products with missing images: attempt `batch_add_images_only` first
3. For failed uploads: call `batch_enhance_images` to enhance/recreate
4. Return summary of fixed products with counts

Always use customer_id `YOUR_CUSTOMER_ID`.
