---
description: Mine converting keywords from ALL campaigns for a country (Search + PMax)
allowed-tools: ["mcp__google-ads__batch_mine_country_keywords", "mcp__google-ads__batch_mine_keywords_progress"]
argument-hint: <CC>
---

Mine ALL converting keywords from ALL campaigns (Search + PMax) for country `$1`. Present results in Polish.

**Steps (async):**
1. Call `batch_mine_country_keywords` with country_code="$1", min_conversions=1, days_back=0 (all-time)
2. Returns {status: "submitted", job_id: "..."}
3. Poll `batch_mine_keywords_progress` with job_id every 30s until status="completed"
4. Show summary: total campaigns scanned, keywords found, top 20 keywords by conversions

This mines actual user search queries (search_term_view for Search, campaign_search_term_insight for PMax) and stores them permanently in keyword_intelligence DB. Results are used by get_keywords_for_setup() during ad generation.

Always use customer_id `YOUR_CUSTOMER_ID`.
