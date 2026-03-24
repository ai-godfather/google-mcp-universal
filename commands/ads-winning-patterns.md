---
description: Show best-performing headline/description patterns
allowed-tools: ["mcp__google-ads__batch_analyze_assets", "mcp__google-ads__batch_winning_patterns"]
argument-hint: <CC>
---

Show winning ad copy patterns for country `$1`. Present in Polish.

**Steps:**
1. Call `batch_analyze_assets` for main campaigns in country "$1" to refresh performance labels
2. Call `batch_winning_patterns` with country_code="$1", asset_type="HEADLINE"
3. Call `batch_winning_patterns` with country_code="$1", asset_type="DESCRIPTION"
4. Filter: performance_label = 'BEST' or 'GOOD'
5. Generalize patterns (replace product names with {Product}, prices with {Price})
6. Rank by frequency + performance
7. Return top 10 winning patterns per type (headline, description) in table format
