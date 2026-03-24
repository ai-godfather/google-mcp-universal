---
description: Global cross-country dashboard — priority ranking, setup progress, XML feed counts
allowed-tools: ["mcp__google-ads__batch_global_dashboard"]
---

Show global cross-country dashboard. Present results in Polish.

**Steps:**
1. Call `batch_global_dashboard` with include_xml=true
2. Format as priority-sorted table (SCALE UP countries first)
3. Show: rank, CC, country, recommendation, XML products, campaigns, batch %, status, next action
4. Highlight next country to work on (first non-DONE SCALE UP country)

Always use customer_id `YOUR_CUSTOMER_ID`.
