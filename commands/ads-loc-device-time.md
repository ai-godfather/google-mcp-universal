---
name: ads-loc-device-time
description: Location + Device + Time-of-Day comprehensive analysis per campaign
---

# /ads-loc-device-time {CC} [{days}]

Complete Google Ads account analysis by Location, Device, and Time-of-Day dimensions.
Separately for each campaign type (Search, PMax, Shopping) on the account.

## Parameters
- `CC` (required): Country code — e.g., DE, FR, US
- `days` (optional): Analysis period in days — default 90

## Workflow

### Phase 0: Campaign Discovery & Data Collection
1. Calculate date range: `end_date = today`, `start_date = today - {days} days`
2. Call `batch_loc_device_time_analysis` with:
   - `country_code = CC`
   - `days = {days}`
   - `include_device_hour_cross = true`
   - `include_regions = true`
   - `top_n_locations = 20`
3. Auto-discovers ALL ENABLED campaigns (Search + PMax + Shopping) for the country
4. Present campaign overview:
   | Campaign | ID | Type | Status |

### Phase 1: Location Analysis (presented in the user's language)

**A. Top Locations** (top 20 by spend)
| # | Location | Type | Impressions | Clicks | CTR% | Conversions | CPA | ROAS | Cost | Share% | vs Average | Classification |

Color-code classifications:
- 🟢 TOP_PERFORMER — ROAS >30% above average
- 🟡 AVERAGE — within ±30% of average
- 🔴 UNDERPERFORMER — ROAS >30% below average
- ⚫ WASTE — spend with 0 conversions

**B. Worst locations** (bottom 10 + waste)
| # | Location | Cost | Conversions | CPA | ROAS | Problem |

**C. Geographic concentration**
- "Top 3 locations = X% of budget and Y% of conversions"

**D. Locations per campaign** (for each campaign type separately)
Flag exceptional performance: any campaign where a location has ROAS 2x+ above campaign average.

### Phase 2: Device Analysis

**E. Performance per device** (3-4 rows)
| Device | Impressions | Clicks | CTR% | Conversions | CPA | ROAS | Cost | Spend share% | Conversion share% | Efficiency Index |

**F. Devices per campaign**
| Campaign | Type | Mobile ROAS | Desktop ROAS | Tablet ROAS | Dominant device |

**G. Device bid modifier recommendations**
| Device | Proposed modifier | Change | ROAS | Efficiency | Rationale |

### Phase 3: Time-of-Day Analysis
Uses existing `batch_time_analysis` for Search campaigns only.

**H. Hourly performance table** (24 rows)
**I. Day-of-week table** (7 rows)
**J. Time blocks** (5 blocks)

### Phase 4: Cross-Dimensional Insights

**K. Device × Hour heatmap** (emoji matrix)
| Hour | Mobile | Desktop | Tablet |
ROAS color coding: 🟢 >1.5, 🟡 1.0-1.5, 🔴 <1.0, ⚫ no data/0

**L. Best and worst combinations**

### Phase 5: Prioritized Action Plan

**M. Prioritized recommendations**
🔴 CRITICAL (week 1) → 🟡 HIGH (weeks 2-3) → 🟢 MEDIUM (week 4+)

**N. 90-day impact forecast**
| Metric | Current value | Forecast | Change% |

### Phase 6: Data & Limitations Disclosure

**O. Data limitations** — ALWAYS disclose missing metrics, API limitations, GAQL row limits.

## Post-Analysis Options
1. "Apply an ad schedule?" → google_ads_create_ad_schedule (Search only)
2. "Exclude waste locations?" → google_ads_set_campaign_geo_target
3. "Save the report as a file (.md)?"
4. "Deep-dive into a specific campaign?"

## Notes
- All data validated directly from Google Ads API — no sample or hypothetical numbers
- Large datasets processed in chunks with GAQL pagination (1000 rows/query)
- Every recommendation is supported by specific performance data
- Smart Bidding: device/location bid modifiers serve as SIGNALS to the algorithm
- PMax: location data limited; device data available
- Always present in the user's language
- Account: the analysis tools use `account.customer_id` from config.json; pass `customer_id` (from config.json unless the user names another account) to the `google_ads_*` tools
