---
name: ads-loc-device-time
description: Location + Device + Time-of-Day comprehensive analysis per campaign
---

# /ads-loc-device-time {CC} [{days}]

Complete Google Ads account analysis by Location, Device, and Time-of-Day dimensions.
Separately for each campaign type (Search, PMax, Shopping) on the account.

## Parameters
- `CC` (required): Country code — e.g., TR, RO, PL
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
   | Kampania | ID | Typ | Status |

### Phase 1: Location Analysis (prezentacja po polsku)

**A. Top Lokalizacje** (top 20 by spend)
| # | Lokalizacja | Typ | Wyświetlenia | Kliknięcia | CTR% | Konwersje | CPA | ROAS | Koszt | Udział% | vs Średnia | Klasyfikacja |

Color-code classifications:
- 🟢 TOP_PERFORMER — ROAS >30% above average
- 🟡 AVERAGE — within ±30% of average
- 🔴 UNDERPERFORMER — ROAS >30% below average
- ⚫ WASTE — spend with 0 conversions

**B. Najgorsze lokalizacje** (bottom 10 + waste)
| # | Lokalizacja | Koszt | Konwersje | CPA | ROAS | Problem |

**C. Koncentracja geograficzna**
- "Top 3 lokalizacje = X% budżetu i Y% konwersji"

**D. Lokalizacje per kampania** (for each campaign type separately)
Flag exceptional performance: any campaign where a location has ROAS 2x+ above campaign average.

### Phase 2: Device Analysis

**E. Wydajność per urządzenie** (3-4 rows)
| Urządzenie | Wyświetlenia | Kliknięcia | CTR% | Konwersje | CPA | ROAS | Koszt | Udział wydatków% | Udział konwersji% | Efficiency Index |

**F. Urządzenia per kampania**
| Kampania | Typ | Mobile ROAS | Desktop ROAS | Tablet ROAS | Dominujące urządzenie |

**G. Rekomendacje bid modifiers per urządzenie**
| Urządzenie | Proponowany modifier | Zmiana | ROAS | Efficiency | Uzasadnienie |

### Phase 3: Time-of-Day Analysis
Uses existing `batch_time_analysis` for Search campaigns only.

**H. Tabela wydajności godzinowej** (24 rows)
**I. Tabela dni tygodnia** (7 rows)
**J. Bloki czasowe** (5 blocks)

### Phase 4: Cross-Dimensional Insights

**K. Heatmapa Device × Hour** (emoji matrix)
| Godzina | Mobile | Desktop | Tablet |
ROAS color coding: 🟢 >1.5, 🟡 1.0-1.5, 🔴 <1.0, ⚫ no data/0

**L. Najlepsze i najgorsze kombinacje**

### Phase 5: Prioritized Action Plan

**M. Priorytetyzowane rekomendacje**
🔴 KRYTYCZNE (tydzień 1) → 🟡 WYSOKIE (tydzień 2-3) → 🟢 ŚREDNIE (tydzień 4+)

**N. Prognoza wpływu na 90 dni**
| Metryka | Obecna wartość | Prognoza | Zmiana% |

### Phase 6: Data & Limitations Disclosure

**O. Ograniczenia danych** — ALWAYS disclose missing metrics, API limitations, GAQL row limits.

## Post-Analysis Options
1. "Wdrożyć harmonogram reklam?" → google_ads_create_ad_schedule (Search only)
2. "Wykluczyć waste lokalizacje?" → google_ads_set_campaign_geo_target
3. "Zapisać raport jako plik (.md)?"
4. "Pogłębić analizę dla konkretnej kampanii?"

## Notes
- All data validated directly from Google Ads API — no sample or hypothetical numbers
- Large datasets processed in chunks with GAQL pagination (1000 rows/query)
- Every recommendation is supported by specific performance data
- Smart Bidding: device/location bid modifiers serve as SIGNALS to the algorithm
- PMax: location data limited; device data available
- Always present in Polish unless user switches to English
