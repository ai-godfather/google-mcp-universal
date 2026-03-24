---
description: Detect and fix ad groups without active ads - enable paused ads, create new RSAs for high-ROI groups, pause dead groups
allowed-tools: ["mcp__google-ads__batch_fix_empty_groups", "mcp__google-ads__batch_api_quota"]
---

Detect and fix ad groups that have no active ads. Present results in Polish.

**Steps:**

1. Check API quota first with `batch_api_quota` - if critical (>90%), warn and stop
2. Run `batch_fix_empty_groups` with `dry_run=true` first to preview changes
3. Present the preview showing:
   - How many empty ad groups found
   - ENABLE_AD: groups with PAUSED+APPROVED ads (list top 10 with ROI)
   - CREATE_RSA: groups needing new ads (list all with ROI and URL)
   - PAUSE_GROUP: dead groups to deactivate (show count)
   - REVIEW: groups needing human review (list all)
4. Ask user for confirmation before executing
5. Run `batch_fix_empty_groups` with `dry_run=false`
6. Present final results with counts

**Parameters from user:**
- First argument: country_code (optional, e.g. "HU")
- Second argument: campaign_id (optional)
- If no arguments: scan ALL enabled Search campaigns

**Domain Rule:**
When creating new RSAs, the domain must match the campaign's existing ads.
Never mix yourstore.com with yourstore.com in the same campaign.
Each campaign belongs to one shop/domain - respect that boundary.

**Examples:**
```
/ads-fix-empty-groups          -- scan all campaigns
/ads-fix-empty-groups HU       -- scan only HU campaigns
/ads-fix-empty-groups HU YOUR_CAMPAIGN_ID  -- scan specific campaign
```

Always use customer_id `YOUR_CUSTOMER_ID`.
