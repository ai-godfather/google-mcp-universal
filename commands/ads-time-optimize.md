---
name: ads-time-optimize
description: Time-of-day & day-of-week performance analysis with ad scheduling optimization
---

# /ads-time-optimize {CC} {days}

Analyze Google Ads account for time-of-day and day-of-week performance differences in clicks and conversions. Identify top-performing hours and days based on CTR and conversion rate, and highlight inefficient windows where spend is less effective. Recommend ad scheduling strategies and pinpoint campaigns that show clear engagement peaks for schedule-based optimization.

## Parameters
- `CC` (required): Country code — e.g., DE, FR, US
- `days` (optional): Analysis period in days — default 90

## Workflow

### Phase 1: Analysis
1. Calculate date range: `end_date = today`, `start_date = today - {days} days`
2. Call `batch_time_analysis` with:
   - `country_code = CC`
   - `start_date`, `end_date`
   - `generate_schedules = true`
3. Auto-discovers all ENABLED Search campaigns for the country

### Phase 2: Presentation (in the user's language, amounts in account currency)
Present the analysis results in these sections:

**A. Hourly Performance Table** (24 rows)
| Hour | Impressions | Clicks | CTR% | Conversions | Conv Rate% | CPA | ROAS | Cost | Status |
Sort by ROAS descending. Flag waste (🔴) and peak (🟢) hours.

**B. Day-of-Week Table** (7 rows)
| Day | Impressions | Clicks | CTR% | Conversions | Conv Rate% | CPA | ROAS | Cost |

**C. Time Blocks Summary** (5 rows)
| Block | Hours | ROAS | Cost | Conversions | Rating |
Night (0-5), Morning (6-9), Day (10-14), Afternoon (15-18), Evening (19-23)

**D. Waste Analysis**
- Total waste hours (ROAS < 1.0): count, total cost, % of budget
- Top 5 worst hours by ROAS
- Estimated savings if waste hours were reduced by 30%

**E. Peak Analysis**
- Total peak hours (ROAS > 1.5): count, total conversions
- Top 5 best hours by ROAS
- Opportunity: estimated additional conversions if peak hours got +15% more budget

**F. Per-Campaign Breakdown**
| Campaign | ROAS | Cost | Conversions | Conversion value |

### Phase 3: Schedule Recommendations
Show the generated schedule as:

| Hour | Category | Bid Modifier | ROAS | Cost | Conversions | Expected effect |
Grouped by category: peak_strong (+25%), peak (+15%), normal (0%), waste (-15%), waste_severe (-30%), dead (-30%)

Summary: "X hours with a bid increase, Y neutral hours, Z hours with a decrease"

### Phase 4: Implementation
Ask: "Apply this ad schedule to the {CC} campaigns? (Yes/No)"

If confirmed:
1. For EACH campaign from the analysis:
   a. `google_ads_list_ad_schedules(campaign_id)` — check existing
   b. If existing schedules found: `google_ads_remove_ad_schedule(campaign_id)` — clear them
   c. `google_ads_create_ad_schedule(campaign_id, schedules=ad_schedule_payload)` — apply new
2. Report: per-campaign creation summary
3. Recommend: "Monitor for 2 weeks, then re-run /ads-time-optimize to measure the impact"

If not confirmed:
- Save analysis for reference
- Suggest: "You can apply it later with google_ads_create_ad_schedule"

## Notes
- Smart Bidding (MAXIMIZE_CONVERSION_VALUE + target_roas): bid modifiers are SIGNALS, not hard rules
- For extreme waste hours (consistently ROAS=0), consider time-based campaign segmentation
- Ad schedules cover the full week (168 hours) — grouped into consecutive blocks with same modifier
- Always present data in the user's language
- Account: `batch_time_analysis` uses `account.customer_id` from config.json; the `google_ads_*` schedule tools need `customer_id` passed (same account unless the user names another one)
