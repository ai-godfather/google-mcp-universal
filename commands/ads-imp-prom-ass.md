---
description: Implement Promotion Assets (50% off) across all campaigns missing them
allowed-tools: ["mcp__google-ads__batch_implement_promotion_assets"]
---

Implement Promotion Assets (50% rabat) for all 69 campaigns that are missing them. Execute all steps and present results in Polish.

**Steps:**

1. Call `batch_implement_promotion_assets` with **dry_run=true** first — preview all 69 campaigns:
   - Use the full campaign list below
   - Verify country detection, language codes, promotional texts, and final_urls
   - Show summary table: campaign | country | language | promo texts | occasion | final_urls

2. Present dry-run results to user and ask for confirmation before proceeding.

3. On confirmation, call `batch_implement_promotion_assets` with **dry_run=false**:
   - 50% discount for all campaigns
   - 3 localized promotion texts per campaign (auto-detected by country)
   - Rotating occasions: SPRING_SALE, SUMMER_SALE, END_OF_SEASON, WOMENS_DAY
   - Final URLs auto-fetched from existing ads

4. Return a summary in Polish with:
   - Total assets created & linked
   - Per-campaign status table
   - Any errors encountered

Always use customer_id `YOUR_CUSTOMER_ID`.

**Campaign list (69 campaigns):**
```json
[
  {"campaign_id": "22825068889", "campaign_name": "PL | Poland | HL | Pmax feed-only | Non-converting | ECOM_PROFIT"},
  {"campaign_id": "21407380475", "campaign_name": "PH | Philipines | Performance Max | HLE"},
  {"campaign_id": "22825068886", "campaign_name": "PL | Poland | HL | Pmax feed-only | Converting | ECOM_PROFIT"},
  {"campaign_id": "20452025214", "campaign_name": "TR - Search | Product | BW (Turkey) | ECOM_PROFIT"},
  {"campaign_id": "20933327684", "campaign_name": "RO | Romania | Performance Max (New)"},
  {"campaign_id": "18289198059", "campaign_name": "HU | Hungary | HL | Pmax full-asset | Converting | ECOM_PROFIT"},
  {"campaign_id": "21799638222", "campaign_name": "PL | Poland | HLE | Pmax | ALL_NEW_WITHOUT_SEARCH_TERMS | ECOM_PROFIT"},
  {"campaign_id": "22744242209", "campaign_name": "TR - Search | Product | BW (Turkey) | ALL_PRODUCTS | HLP"},
  {"campaign_id": "18296767412", "campaign_name": "GR | Greece | Performance Max"},
  {"campaign_id": "18290264159", "campaign_name": "CZ | Czech Republic | Performance Max"},
  {"campaign_id": "20458058813", "campaign_name": "CZ - Search | Product | BW (Czech Republic)"},
  {"campaign_id": "20462097526", "campaign_name": "BG - Search | Product | BW (Bulgaria)"},
  {"campaign_id": "20457789533", "campaign_name": "HR - Search | Product | BW (Croatia)"},
  {"campaign_id": "20318325812", "campaign_name": "SK | Slovakia | HL | Pmax full-asset | Converting | ECOM_PROFIT"},
  {"campaign_id": "20457942602", "campaign_name": "SK - Search | Product | BW (Slovakia)"},
  {"campaign_id": "22425776810", "campaign_name": "DE | Germany | HL | Pmax feed-only | Converting | ECOM_PROFIT"},
  {"campaign_id": "20448904293", "campaign_name": "MX - Search | Product | BW (Mexico)"},
  {"campaign_id": "20458023995", "campaign_name": "HU - Search | Product | BW (Hungary) | ECOM_PROFIT"},
  {"campaign_id": "20197263324", "campaign_name": "HU - Search | Product | BW (Hungary) (NewFeed)"},
  {"campaign_id": "20458084493", "campaign_name": "RO - Search | Product | BW (Romania)"},
  {"campaign_id": "20454982508", "campaign_name": "ES - Search | Product | BW (Spain)"},
  {"campaign_id": "20462145490", "campaign_name": "FR - Search | Product | BW (France)"},
  {"campaign_id": "20197182939", "campaign_name": "FR - Search | Product | BW (France) (NewFeed)"},
  {"campaign_id": "20459074012", "campaign_name": "IT - Search | Product | BW (Italy)"},
  {"campaign_id": "20202121577", "campaign_name": "PT - Search | Product | BW (Portugal) (NewFeed)"},
  {"campaign_id": "21378160291", "campaign_name": "HU - Search | Product | BW (Hungary) | HLE"},
  {"campaign_id": "21799552266", "campaign_name": "TH | Thailand | HLE | Pmax feed-only | Converting | ECOM_PROFIT"},
  {"campaign_id": "23167608632", "campaign_name": "TH | Thailand | HLE | Pmax feed-only | Non-converting | ECOM_PROFIT"},
  {"campaign_id": "22813950662", "campaign_name": "PH - Search | Product | BW (Philipines) | HLE | Filipino | ALL_PRODUCTS"},
  {"campaign_id": "22813640113", "campaign_name": "SI - Search | Product | BW (Slowenia) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22813823987", "campaign_name": "UG - Uganda - BW (Product) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22814158034", "campaign_name": "BA - Search | Product | BW (Bosnia And Hercegowina) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22814152781", "campaign_name": "CO - Search | Product | BW (Colombia) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22814153897", "campaign_name": "NG - Search | Product | BW (Nigeria) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22808046921", "campaign_name": "AR - Search | Product | BW (Argentina) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22808070663", "campaign_name": "ID - Search | Product | BW (Indonesia) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22808305101", "campaign_name": "KE- Search | Product | BW (Kenya) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22808316912", "campaign_name": "RS - Search | Product | BW (Serbia) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22808362290", "campaign_name": "PE - Search | Product | BW (Peru) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22808387190", "campaign_name": "QA - Qatar - BW (Product) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22807984278", "campaign_name": "GT - Guatemala - BW (Product) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22807989009", "campaign_name": "EC - Ecuador - BW (Product) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22806434345", "campaign_name": "HU - Search | Product | BW (Hungary) | ALL_PRODUCTS | HLE | ECOM_PROFIT"},
  {"campaign_id": "22809450188", "campaign_name": "LT - Search | Product | BW (Lithuinia) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22809453272", "campaign_name": "LV - Search | Product | BW (Latvia) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22815776159", "campaign_name": "AE - Search | Product | BW (Unted Arab Emirates) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22815894149", "campaign_name": "BD - Search | Product | BW (Bangladesh) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22815912365", "campaign_name": "UA - Search | Product | BW (Ukraine) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22815981263", "campaign_name": "UZ - Uzbekistan - BW (Product) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22817575630", "campaign_name": "MD - Moldova - BW (Product) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22817984800", "campaign_name": "MY - Search | Product | BW (Malaysia) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22819489393", "campaign_name": "EG - Egypt - BW (Product) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22819492447", "campaign_name": "MA - Search | Product | BW (Morocco) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22826525184", "campaign_name": "BE - Search | Product | BW (Belgium) | HLP (FR) | ALL_PRODUCTS"},
  {"campaign_id": "22836186916", "campaign_name": "CH - Search | Product | BW (Switzerland) | HLP (FR) | ALL_PRODUCTS"},
  {"campaign_id": "22836245500", "campaign_name": "EE - Search | Product | BW (Estonia) | HLP | ALL_PRODUCTS"},
  {"campaign_id": "22836273745", "campaign_name": "LU - Search | Product | BW (Luxemburg) | HLP (FR) | ALL_PRODUCTS"},
  {"campaign_id": "22737957282", "campaign_name": "AT - Search | Product | BW (Austria) | HLP | ALL_PRODUCTS"},
  {"campaign_id": "22737985104", "campaign_name": "CY - Search | Product | BW (Cyprus) | HLP | ALL_PRODUCTS"},
  {"campaign_id": "22838289586", "campaign_name": "US - Search | Product | BW (United States) | ALL_FROM_NEW_NETWORKS"},
  {"campaign_id": "22838319991", "campaign_name": "NZ - Search | Product | BW (New Zealand) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "22839635654", "campaign_name": "CI - Search | Product | BW (Ivory Coast) | HLE | ALL_PRODUCTS"},
  {"campaign_id": "18856465883", "campaign_name": "BW (Product) - Romania"},
  {"campaign_id": "22734106761", "campaign_name": "RO - Search | Product | BW (Romania) | HLP | ALL_PRODUCTS_NEW | ECOM_PROFIT"},
  {"campaign_id": "23624226159", "campaign_name": "PL | Poland | HLP | Search | Product | BW"},
  {"campaign_id": "20453299122", "campaign_name": "PL - Search | Product | BW (Poland #2)"},
  {"campaign_id": "23645519742", "campaign_name": "AZ | Azerbaijan | Search | NEW_PRODUCTS | HLE | CLAUDE_MCP"},
  {"campaign_id": "23645632971", "campaign_name": "UZ | Uzbekistan | Search | NEW_PRODUCTS | HLE | CLAUDE_MCP"},
  {"campaign_id": "23649974048", "campaign_name": "PT | Portugal | Search | NEW_PRODUCTS | HLE | CLAUDE_MCP"}
]
```
