---
description: Keyword research for a product — historical + category
allowed-tools: ["mcp__google-ads__batch_keyword_research", "mcp__google-ads__batch_category_keywords"]
argument-hint: <CC> <product_handle>
---

Perform keyword research for country `$1` and product `$2`. Present results in Polish.

**Steps:**
1. Call `batch_keyword_research` with country_code="$1", product_handle="$2", days_back=90
2. Call `batch_category_keywords` for the product's category in country "$1"
3. Merge results and rank by ROI
4. Return top 20 keywords with: keyword text, CTR, CPC, conversions, ROI %
