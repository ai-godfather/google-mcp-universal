---
name: google-mcp-universal
description: |
  Universal Google Ads campaign manager. Use this skill whenever the user mentions Google Ads, campaigns, keywords, ad groups, bidding, search terms, ad performance, ROAS, CPC, CTR, conversions, budgets, or anything related to paid search/display advertising management. Also trigger when the user asks about campaign optimization, negative keywords, quality scores, or ad assets. This skill provides account context and best practices so Claude can manage campaigns without asking for IDs or setup details. ALWAYS use this skill for any Google Ads related task — even simple ones like checking campaign status or pausing a keyword.
---

# Google Ads Manager — Universal Plugin

You are managing a Google Ads account configured through this universal plugin. The plugin supports campaigns across multiple markets and industries.

## Configuration & Setup

This plugin requires initial configuration before use. All account details (Customer IDs, Merchant Center accounts, domains, etc.) are loaded from a central configuration file.

### Configuration File Setup

Create a `config.json` file in the plugin directory with the following structure:

```json
{
  "account": {
    "customer_id": "YOUR_CUSTOMER_ID",
    "mcc_id": "YOUR_MCC_ID",
    "company_name": "Your Company",
    "industry": "your industry",
    "primary_markets": ["PL", "DE", "ES", "IT", "CZ"]
  },
  "merchant_center": {
    "accounts": [
      {
        "name": "Account 1",
        "mca_id": "MCA_ID_1",
        "primary_domain": "shop.example.com",
        "sub_accounts": [
          {
            "country_code": "PL",
            "merchant_id": "MERCHANT_ID_1",
            "domain": "pl.example.com"
          }
        ]
      }
    ]
  },
  "domains": {
    "PL": "pl.yourdomain.com",
    "DE": "de.yourdomain.com",
    "ES": "es.yourdomain.com"
  },
  "currencies": {
    "PL": "PLN",
    "DE": "EUR",
    "TR": "TRY"
  }
}
```

### Environment Variables

Set these environment variables before using the plugin:

```bash
GOOGLE_ADS_CUSTOMER_ID="your_customer_id"
GOOGLE_ADS_DEVELOPER_TOKEN="your_developer_token"
GOOGLE_ADS_REFRESH_TOKEN="your_refresh_token"
OPENAI_API_KEY="your_openai_key"  # For AI ad copy generation
```

### Setup Wizard

Run the setup script to initialize your configuration:

```bash
python setup_account.py
```

This script will:
1. Authenticate with Google Ads API
2. Load all campaigns and merchant centers
3. Create a populated config.json
4. Validate API access
5. Initialize the database

### Demo Mode

To test the plugin without a real Google Ads account:

```bash
python setup_account.py --demo
```

This creates sample data for testing workflows without making real API calls.

---

## Account Details (Loaded from config.json)

Your Google Ads account details are loaded from the configuration file set up during initialization. The plugin automatically retrieves:

- **Customer ID** — loaded from config.json
- **MCC ID** — loaded from config.json
- **Company Name** — loaded from config.json
- **Industry** — loaded from config.json
- **Primary Markets** — loaded from config.json

All campaigns should be organized according to your configured account structure.

---

## Available MCP Tools (64 total)

### Read / Analysis Tools (no confirmation needed)

| Tool | Purpose |
|------|---------|
| `google_ads_list_campaigns` | List all campaigns with status, budget, and metrics |
| `google_ads_get_campaign_details` | Get detailed info about a specific campaign |
| `google_ads_list_ad_groups` | List ad groups within a campaign |
| `google_ads_list_keywords` | List keywords with quality scores and bids |
| `google_ads_get_performance_report` | Performance report by campaign/ad_group/keyword for date range |
| `google_ads_list_assets` | List assets (headlines, descriptions, images) |
| `google_ads_list_search_terms` | Search terms report — actual queries triggering ads |
| `google_ads_get_account_summary` | Account-level summary metrics |
| `google_ads_list_ads` | List ads with performance metrics (ID, type, status, URLs) |
| `google_ads_list_campaign_criteria` | Campaign-level targeting (locations, languages) |
| `google_ads_list_conversion_actions` | Conversion actions configured in the account |
| `google_ads_list_recommendations` | Google's optimization recommendations |
| `google_ads_get_shopping_performance` | Shopping performance by product item ID |
| `google_ads_execute_gaql` | Execute raw GAQL queries for custom reporting |

### Campaign Management Tools

| Tool | Purpose |
|------|---------|
| `google_ads_update_campaign_status` | Enable or pause a campaign |
| `google_ads_update_campaign_budget` | Change daily budget (value in micros) |
| `google_ads_update_campaign_bidding_strategy` | Change bidding (MANUAL_CPC, TARGET_CPA, TARGET_ROAS, etc.) |
| `google_ads_set_campaign_geo_target` | Set geographic targets |
| `google_ads_set_campaign_language` | Set language targets |
| `google_ads_set_campaign_location_options` | Location targeting options (PRESENCE vs PRESENCE_OR_INTEREST) |
| `google_ads_remove_campaign` | Permanently remove a campaign |

### Ad Group & Ad Management

| Tool | Purpose |
|------|---------|
| `google_ads_update_ad_group_status` | Enable or pause an ad group |
| `google_ads_update_ad_group_bid` | Update ad group default CPC bid |
| `google_ads_update_ad_status` | Enable or pause an individual ad |

### Keyword Management

| Tool | Purpose |
|------|---------|
| `google_ads_update_keyword_bid` | Update max CPC bid for a keyword |
| `google_ads_pause_keyword` / `google_ads_enable_keyword` | Pause/enable keywords |
| `google_ads_add_negative_keyword` | Add negative keyword to a campaign |
| `google_ads_add_keyword` | Add positive keyword to an ad group (BROAD, PHRASE, EXACT) |

### Campaign & Ad Creation

| Tool | Purpose |
|------|---------|
| `google_ads_create_campaign_budget` | Create a shared campaign budget |
| `google_ads_create_search_campaign` | Create a Search campaign |
| `google_ads_create_search_ad_group` | Create a Search ad group |
| `google_ads_create_responsive_search_ad` | Create RSA with pinning support |
| `google_ads_create_shopping_campaign` | Create Standard Shopping campaign |
| `google_ads_create_shopping_ad_group` | Create Shopping ad group |
| `google_ads_create_shopping_ad` | Create Shopping ad |
| `google_ads_create_pmax_campaign` | Create Performance Max campaign |
| `google_ads_create_asset_group` | Create PMax asset group |
| `google_ads_create_asset_group_text_assets` | Create PMax text assets |
| `google_ads_create_listing_group_filter` | Set product listing filters for PMax |

### Extension / Asset Tools (support Account, Campaign, AND AdGroup level linking)

| Tool | Purpose |
|------|---------|
| `google_ads_create_callout_assets` | Callout extensions (max 25 chars each) |
| `google_ads_create_sitelink_assets` | Sitelink extensions (title + descriptions + URL) |
| `google_ads_create_structured_snippet_assets` | Structured snippets (header + values) |
| `google_ads_create_promotion_asset` | Promotion extensions (% off, money off, occasions) |
| `google_ads_create_price_assets` | Price extensions (product/price/URL) |
| `google_ads_create_campaign_image_asset` | Image extensions for Search (URL or local file path) |
| `google_ads_create_image_asset` | Image asset for PMax asset groups |
| `google_ads_create_business_identity_assets` | Business name + logo |
| `google_ads_update_asset` | Update text assets |

### Asset Link Management

| Tool | Purpose |
|------|---------|
| `google_ads_remove_campaign_asset_links` | Unlink assets from a campaign |
| `google_ads_remove_ad_group_asset_links` | Unlink assets from an ad group |

### Shopping / Merchant Center

| Tool | Purpose |
|------|---------|
| `google_ads_add_product_exclusion` | Exclude products from Shopping campaign |
| `merchant_center_list_products` | List products |
| `merchant_center_get_product` / `merchant_center_get_product_status` | Get product details/status |
| `merchant_center_list_product_statuses` | List product approval statuses |
| `merchant_center_list_accounts` / `merchant_center_get_account` | Account management |
| `merchant_center_update_product` / `merchant_center_delete_product` | Modify/delete products |

### Batch Operations (58 tools available)

The plugin includes comprehensive batch optimization tools:

- `batch_setup_all` — Process all products from XML feed (async)
- `batch_setup_products` — Setup specific products
- `batch_sync_from_api` — Sync DB state from Google Ads
- `batch_status` — Get setup status for all products
- `batch_missing` — Find incomplete product setups
- `batch_queue_next` — Get next products to process
- `batch_add_images_only` — Add images without full setup
- `batch_process_images` — Pre-process images for upload
- `batch_enhance_images` — Enhance images via GPT-4o
- `batch_warmup_cache` — Pre-warm AI ad copy cache
- `batch_warmup_market_intel` — Fetch market intelligence data
- `batch_mine_country_keywords` — Mine converting keywords
- `batch_mine_keywords_progress` — Check keyword mining progress
- `batch_keyword_research` — Research keywords for a product
- `batch_category_keywords` — Get category-level keywords
- `batch_analyze_assets` — Analyze RSA performance
- `batch_fix_domains` — Fix wrong-domain RSAs
- `batch_fix_sitelinks` — Fix bad sitelink URLs
- `batch_fix_empty_groups` — Find and fix empty ad groups
- `batch_fix_negative_conflicts` — Fix keyword conflicts
- `batch_remove_wrong_domain_ads` — Remove disapproved ads
- `batch_remove_not_eligible_ads` — Remove DISAPPROVED ads
- `batch_remove_not_eligible_sitelinks` — Remove bad sitelinks
- `batch_remove_not_eligible_images` — Remove bad images
- `batch_check_eligibility` — Check ad/asset approval status
- `batch_validate_guardrails` — Validate guardrails via API
- `batch_guardrails_report` — Guardrails compliance report
- `batch_enable_paused_keywords` — Mass enable keywords
- `batch_enable_paused_ad_groups` — Mass enable ad groups
- `batch_cleanup_stale` — Remove stale products
- `batch_shopping_listing_groups` — List product groups
- `batch_shopping_bid_optimizer` — Auto-optimize product bids
- `batch_shopping_exclude_products` — Exclude products
- `batch_shopping_clone_campaign` — Clone campaign cross-country
- `batch_loc_device_time_analysis` — Combined location/device/time analysis
- `batch_time_analysis` — Time-of-day performance analysis
- `batch_location_analysis` — Geographic performance analysis
- `batch_device_analysis` — Device-type performance analysis
- `batch_keyword_spend_analysis` — Keyword spend trends
- `batch_implement_promotion_assets` — Create promotion assets
- `batch_health_check` — System health check
- `batch_api_quota` — Check API quota usage
- `batch_account_performance_summary` — Comprehensive account summary
- `batch_conversion_diagnostics` — Diagnose conversion tracking
- `batch_remove_ad_groups` — Permanently remove ad groups
- `batch_sync_negatives` — Sync negative keywords
- `batch_remove_shared_criterion` — Remove shared negatives
- `batch_remove_campaign_negative_keywords` — Remove campaign negatives
- `batch_check_obligatory` — Check OBLIGATORY products visibility
- `batch_merchant_gc` — Merchant Center garbage collection
- `batch_reset_errors` — Reset error products
- `batch_clear_cache` — Clear cache
- `batch_changelog_read` / `batch_changelog_add` — Read/write changelog
- `batch_get_instructions` — Get operational instructions
- `batch_dashboard` — Campaign dashboard
- `batch_product_dashboard` — Product-level dashboard
- `batch_global_dashboard` — Cross-country dashboard
- `batch_verify_campaign_settings` — Verify campaign configuration
- `batch_update_campaign_settings` — Update campaign settings
- `batch_warmup_status` — Check warmup job progress
- `batch_setup_progress` — Check setup job progress
- `batch_images_progress` — Check image upload progress
- `batch_fix_domains_progress` — Check domain fix progress
- `batch_warmup_market_intel_status` — Check market intel progress
- `batch_mine_keywords_progress` — Check keyword mining progress
- `batch_keyword_spend_analysis_status` — Check spend analysis progress
- `batch_list_market_intel_jobs` — List recent market intel jobs
- `batch_stop_job` — Stop a running background job
- `batch_remove_stale` — Remove stale products
- `batch_setup_parallel` — Parallel product setup
- `pagespeed_scan_url` — Scan single URL for performance
- `pagespeed_scan_store` — Mass scan store for performance
- `pagespeed_scan_all_stores` — Global dashboard for all stores
- `pagespeed_analyze_theme` — Analyze Shopify theme
- `pagespeed_apply_fixes` — Apply PageSpeed fixes
- `batch_generate_ad_copy` — Generate localized ad copy
- `batch_feed_prices` — Parse feed for prices
- `batch_rsa_price_audit` — Audit RSA prices vs feed
- `merchant_center_report_search` — Run Merchant Center reports
- `merchant_center_list_datafeeds` — List configured datafeeds
- `merchant_center_get_datafeed` / `merchant_center_update_datafeed` — Manage datafeeds
- `merchant_center_insert_datafeed` — Create new datafeed
- `merchant_center_delete_datafeed` — Delete datafeed
- `merchant_center_fetch_datafeed_now` — Trigger immediate datafeed fetch
- `merchant_center_list_datafeed_statuses` — Check datafeed processing status
- `merchant_center_get_datafeed_status` — Get single datafeed status
- `merchant_center_update_custom_labels` — Update product custom labels
- `merchant_center_list_account_statuses` — Get account statuses

---

## Extension Assets — Linking Levels (CRITICAL)

Extensions can be linked at **three levels**. **Always prefer the lowest applicable level** for product-specific content:

1. **Account level** — omit both `campaign_id` and `ad_group_id` → applies to all campaigns
2. **Campaign level** — provide `campaign_id` → all ad groups in that campaign
3. **AdGroup level** — provide `ad_group_id` → only that ad group's ads (**PREFERRED for product-specific extensions**)

**Why AdGroup level matters**: When each ad group represents a different product, campaign-level extensions would show the same callouts/promotions/prices for ALL products. That's wrong when products have different prices, discounts, and features.

### Character Limits

| Asset Type | Field | Max Length |
|-----------|-------|-----------|
| Callout | callout_text | 25 chars |
| Sitelink | link_text | 25 chars |
| Sitelink | description1, description2 | 35 chars each |
| Structured Snippet | values | 25 chars each, min 3 values |
| Promotion | promotion_target | 20 chars |
| Price | header / description | 25 chars each |
| Business Name | business_name | 25 chars |

### Structured Snippet Headers (predefined)

Brands, Types, Models, Destinations, Styles, Courses, Amenities, Insurance coverage, Neighborhoods, Service catalog, Shows, Degree programs

### Promotion Occasions (predefined)

NEW_YEARS, VALENTINES_DAY, EASTER, MOTHERS_DAY, FATHERS_DAY, LABOR_DAY, BACK_TO_SCHOOL, HALLOWEEN, BLACK_FRIDAY, CYBER_MONDAY, CHRISTMAS, BOXING_DAY, INDEPENDENCE_DAY, NATIONAL_DAY, END_OF_SEASON, WINTER_SALE, SUMMER_SALE, FALL_SALE, SPRING_SALE, RAMADAN, EID_AL_FITR, EID_AL_ADHA, SINGLES_DAY, WOMENS_DAY, HOLI, PARENTS_DAY, ST_NICHOLAS_DAY, CARNIVAL, EPIPHANY, ROSH_HASHANAH, PASSOVER, HANUKKAH, DIWALI, NAVRATRI, SONGKRAN, YEAR_END_GIFT

---

## Market-Specific Limitations & Workarounds

### Turkish Market (TR) — Known Issues

| Feature | Works? | Notes |
|---------|--------|-------|
| **Promotions** | YES | `language_code: "tr"`, currency in local code. Multiply percentage × 1000 (50% = 50000) |
| **Price Extensions** | NO | Some markets don't support Price assets in all languages. Use callout workaround |
| **Callouts** | YES | Use as pricing workaround: "From [price]", "3-pack [price]" |
| **Sitelinks** | YES | Verify landing pages work in target market |
| **Business Name** | CAREFUL | Must match landing page exactly |
| **Images** | YES | But category-specific images may get "Prohibited content" — use clean product-only shots |

### Product Pricing Rules

- Always use `<g:sale_price>` from XML feed as the base price, NOT `<g:price>` (that's the crossed-out "before" price)
- Packages: multiply `sale_price × quantity` for multi-unit pricing
- The promotion discount (e.g., 50%) is from `<g:price>` to `<g:sale_price>`

### Product Naming Rules

- Use ONLY clean product names from your product database
- NEVER add form factors unless it's the actual feed name
- Filesystem names differ from Google Ads display names

---

## RSA Management Workflow

### Golden Rules for A/B Testing

1. **NEVER edit existing running ads** — create NEW ads and pause old ones. Editing resets learning data.
2. Run **2-3 ads per AdGroup** simultaneously
3. Let each ad accumulate at least 5,000 impressions before conclusions
4. Label consistently: Ad A (base), Ad B (price angle), Ad C (trust/pinned)

### RSA Pinning Support

Headlines and descriptions support pinning via dict format:

```python
headlines = [
    {"text": "Brand Product Name", "pinned_field": "HEADLINE_1"},  # always shows in H1
    "Benefit or Feature",  # not pinned
    "Special Offer or Discount"
]
# Valid: HEADLINE_1, HEADLINE_2, HEADLINE_3, DESCRIPTION_1, DESCRIPTION_2
```

**Recommended A/B/C structure per AdGroup:**
- **Ad A**: No pins, general messaging
- **Ad B**: No pins, price/value angle
- **Ad C**: H1 pinned (brand/trust headline), rest free

### Top-Performing Headline Patterns

| Pattern | Example | Why |
|---------|---------|-----|
| Authenticity | "Official [Product]", "Genuine Sale" | Trust, reduces objections |
| Dynamic Pricing | Dynamic price insertion | Personalizes to query |
| Discount | "50% Off", "Special Price" | Price-sensitive audience |
| Problem-Solution | "[Category] for [Need]" | Matches search intent |
| Urgency | "Limited Stock", "Order Now" | Drives action |

### Asset Performance Labels

`BEST` > `GOOD` > `LEARNING` > `LOW` > `PENDING` — replace LOW assets, scale BEST patterns.

### Useful GAQL Queries for RSA Analysis

```sql
-- All RSAs in a campaign with headlines
SELECT campaign.id, ad_group.id, ad_group.name,
       ad_group_ad.ad.id, ad_group_ad.ad.responsive_search_ad.headlines,
       ad_group_ad.ad.responsive_search_ad.descriptions,
       ad_group_ad.status, ad_group_ad.ad_strength
FROM ad_group_ad
WHERE campaign.id = {CAMPAIGN_ID}
  AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
  AND ad_group_ad.status != 'REMOVED'

-- Asset-level performance labels
SELECT campaign.id, ad_group.id,
       ad_group_ad_asset_view.field_type,
       ad_group_ad_asset_view.performance_label,
       asset.text_asset.text, asset.type
FROM ad_group_ad_asset_view
WHERE campaign.id = {CAMPAIGN_ID}
```

---

## Image Management Workflow

### Requirements for Search Ad Image Extensions

- **Square**: exactly 1:1 ratio, min 300×300px (recommended 1200×1200)
- **Landscape**: exactly 1.91:1 ratio, min 600×314px
- **CRITICAL**: Google enforces EXACT ratios. Even 571×602 (0.95:1) → `ASPECT_RATIO_NOT_ALLOWED`. Always crop first!
- No text/logo/promo overlays (badges cause disapproval)
- Category-specific images may trigger disapproval — use clean product shots

### Step-by-Step Workflow

#### 1. Check Existing Images

```sql
SELECT campaign.id, ad_group.id, ad_group.name,
       ad_group_asset.resource_name, asset.name,
       asset.image_asset.full_size.url,
       ad_group_asset.status, ad_group_asset.primary_status,
       ad_group_asset.primary_status_reasons
FROM ad_group_asset
WHERE ad_group.id IN ({AG_IDS}) AND asset.type = 'IMAGE'
```

Status meaning:
- `ELIGIBLE` → active, serving
- `NOT_ELIGIBLE` + `ASSET_DISAPPROVED` → rejected, must remove
- `PENDING` + `ASSET_UNDER_REVIEW` → new, wait
- `REMOVED` → already gone, ignore

#### 2. Remove Rejected Images

```python
google_ads_remove_ad_group_asset_links(
    customer_id="configured_in_config",
    ad_group_asset_resource_names=[
        "customers/ID/adGroupAssets/{AG_ID}~{ASSET_ID}~AD_IMAGE"
    ]
)
```

#### 3. Find Replacements from Product Images

Search your product image repository (configured in config.json) for product variants.

**Selection criteria:**
1. Highest resolution
2. Clean product shots (no promo text/badges)
3. Category-appropriate for your market

#### 4. Crop to 1:1 Square

```python
from PIL import Image
img = Image.open("product.jpg")
w, h = img.size
if h > w:
    top = (h - w) // 2
    img = img.crop((0, top, w, top + w))
else:
    left = (w - h) // 2
    img = img.crop((left, 0, left + h, h))
img.save("product-square.jpg", "JPEG", quality=95)
```

#### 5. Upload

The image tool accepts both URLs and local file paths:

```python
google_ads_create_campaign_image_asset(
    customer_id="configured_in_config",
    ad_group_id="184537980479",
    image_url="/path/to/product-square.jpg",  # local file or https:// URL
    asset_name="product-name-square"
)
```

---

## Important Conventions

### Budget & Bid Values (Micros)

Multiply by 1,000,000: $10 = `10000000`, PLN 50 = `50000000`

### Promotion percent_off (Micro-Percentages)

Multiply by 1,000: 50% = `50000`, 25% = `25000`

### Campaign Status Codes

`2` = ENABLED, `3` = PAUSED, `4` = REMOVED

### Campaign Type Codes

`2` = SEARCH, `3` = DISPLAY, `6` = SHOPPING, `9` = PERFORMANCE_MAX, `14` = DEMAND_GEN

---

## Account Structure

Campaigns are organized by:
- **Market prefix**: loaded from configured markets
- **Type suffix**: Search, Display, Remarketing, RSLA, DSA, Shopping, PMax
- **Product category**: Your product categories as defined

All campaigns should follow your configured naming convention.

---

## Workflow Guidelines

### Setting Up a New Product (Complete Checklist)

1. Create ad group (or campaign if new market)
2. Create 2-3 RSA ads per ad group (A: general, B: price, C: trust+pinned H1)
3. Add **AdGroup-level** extensions:
   - Callouts with pricing tiers
   - Promotions (percent off or discount amount)
   - Sitelinks to product/category/about pages
   - Structured snippets (product types/forms)
4. Add images: find highest-resolution variant → crop 1:1 → upload
5. Set targeting: geo + language based on market
6. Add keywords: branded + generic + competitor

### Performance Analysis

1. `google_ads_get_account_summary` → big picture
2. `google_ads_list_campaigns` → individual metrics
3. `google_ads_get_performance_report` → date range trends
4. `google_ads_list_search_terms` → wasteful queries / opportunities

### Before Making Changes

1. Show current values and proposed changes
2. Wait for explicit confirmation
3. For budgets: show current vs proposed in local currency
4. For pauses: explain why (low quality, high CPC, irrelevant)

### Presenting Data

- Tables for comparisons
- Calculate: CTR, CPC, CPA, conversion rate
- Convert micros to readable values
- Flag anomalies
- Use the user's preferred language for presentations

---

## Known Limitations & Gotchas

### API

- Developer Token access level affects available operations
- Some account-level metrics may have delays or inaccuracies
- PMax: no keyword data — focus on asset performance
- Merchant Center API: may timeout on large product feeds

### GAQL Quirks

- `campaign.id` MUST be in SELECT when in WHERE for `shopping_performance_view`
- Max 1000 rows per query — paginate with appropriate filters
- `ad_group_asset` filtering requires careful query structure

### Common Disapprovals

- **"Prohibited content"** for category-specific images — try different product-only shots
- **"Business Name Irrelevance"** — must match landing page name exactly
- **"Destination not working"** for sitelinks — verify URL accessibility
- **"Unsupported language"** for certain extensions in some markets — use workarounds

### Image Upload

- SSL errors may occur from certain CDN domains
- Aspect ratio must be EXACT 1:1 or 1.91:1 — crop before upload
- New images may not serve via CDN immediately — use local file path

---

## Image Performance A/B Testing

If using multiple image variants for products, the batch tools support analyzing which variants drive highest CTR and excluding poor performers.

### Process

**Step 1**: Identify all campaigns for a country (both Shopping and PMax campaigns produce performance data).

**Step 2**: Use batch tools to analyze image performance by item ID.

**Step 3**: Exclude low-CTR variants from campaigns.

**Step 4**: Reallocate budget to high-CTR variants.

---

## Batch Optimizer System

The plugin includes a comprehensive batch optimization system with 58+ specialized tools for managing campaigns at scale. All batch operations:

- **Auto-resume**: Progress saved to DB after each product
- **Rate-limit aware**: Respects Google Ads API quotas
- **Async-capable**: Long operations run in background with job tracking
- **Safety**: Guardrails prevent invalid operations, with dry-run preview mode

### Key Batch Tools

**Campaign Setup**
- `batch_setup_all` — Process all products from feed (async, auto-resume)
- `batch_setup_products` — Setup specific products
- `batch_setup_parallel` — Parallel processing (use with caution)

**Auditing & Sync**
- `batch_sync_from_api` — Sync local DB with actual Google Ads state
- `batch_audit_campaign` — Find missing assets in ad groups
- `batch_status` — Get setup progress for all products
- `batch_missing` — Find incomplete product setups
- `batch_queue_next` — Get next products to process (prioritized)

**Images**
- `batch_add_images_only` — Add images without full setup (light operation)
- `batch_process_images` — Pre-process images server-side
- `batch_enhance_images` — Enhance images via GPT-4o if needed

**AI Ad Copy**
- `batch_warmup_cache` — Pre-warm AI copy cache for instant generation
- `batch_warmup_market_intel` — Fetch market data (trends, competitor ads, keywords, CPC)
- `batch_generate_ad_copy` — Generate localized copy for a product

**Keywords & Negative Keywords**
- `batch_mine_country_keywords` — Mine converting keywords from all campaigns
- `batch_keyword_research` — Deep research for a specific product
- `batch_category_keywords` — Get category-level keyword patterns
- `batch_fix_negative_conflicts` — Fix keywords blocked by negatives
- `batch_enable_paused_keywords` — Mass-enable keywords by brand filter

**Ad Group Management**
- `batch_fix_empty_groups` — Find ad groups with no active ads, take action
- `batch_enable_paused_ad_groups` — Enable groups with active keywords/ads
- `batch_remove_ad_groups` — Permanently delete ad groups (irreversible)

**Domain & Policy Issues**
- `batch_fix_domains` — Fix RSAs with wrong-domain final_urls
- `batch_remove_wrong_domain_ads` — Remove ads with policy disapprovals
- `batch_check_eligibility` — Scan for approval status issues
- `batch_validate_guardrails` — Live API validation of guardrails

**Product Management**
- `batch_cleanup_stale` — Find and pause products no longer in feed
- `batch_shopping_exclude_products` — Set minimal bid for products

**Shopping Campaign Specific**
- `batch_shopping_listing_groups` — List all product groups in campaign
- `batch_shopping_bid_optimizer` — Auto-adjust bids by ROAS
- `batch_shopping_exclude_products` — Exclude products from Shopping
- `batch_shopping_clone_campaign` — Clone Shopping campaign to new country

**Analysis & Reporting**
- `batch_account_performance_summary` — Comprehensive account metrics
- `batch_conversion_diagnostics` — Check conversion tracking health
- `batch_time_analysis` — Time-of-day + day-of-week performance
- `batch_location_analysis` — Geographic performance breakdown
- `batch_device_analysis` — Device-type performance breakdown
- `batch_loc_device_time_analysis` — Combined analysis all dimensions
- `batch_keyword_spend_analysis` — Period-over-period keyword trends
- `batch_dashboard` — Campaign-level dashboard
- `batch_product_dashboard` — Product completeness dashboard
- `batch_global_dashboard` — Cross-country overview

**Asset Analysis**
- `batch_analyze_assets` — Score RSA headlines/descriptions (BEST/GOOD/LOW)
- `batch_winning_patterns` — Top-performing asset patterns by country
- `batch_feed_prices` — Parse feed for pricing
- `batch_rsa_price_audit` — Audit RSAs for wrong prices
- `batch_fix_sitelinks` — Fix sitelinks with bad URLs
- `batch_remove_not_eligible_*` — Remove disapproved assets

**Merchant Center**
- `batch_check_obligatory` — Check critical product visibility
- `batch_merchant_gc` — Garbage collection: blacklist/appeal disapproved products

**System**
- `batch_health_check` — Diagnose system health
- `batch_api_quota` — Check API quota usage
- `batch_clear_cache` — Flush cache (GAQL or feed)
- `batch_changelog_read` — Read operation history
- `batch_changelog_add` — Add changelog entry
- `batch_get_instructions` — Get operational guide

### Database Tables

The batch system uses SQLite with these tables:

- `products` — product handles, status (pending/partial/complete/error), last_updated
- `handle_ag_map` — handle → (campaign_id, ad_group_id) mapping
- `asset_performance` — asset performance labels by country/campaign
- `keyword_intelligence` — converting keywords by product handle
- `negative_keywords` — active + inactive shared/campaign negatives
- `rates` — API rate limit tracking
- `guardrails` — validation results per product
- `changelog` — timestamped operation log

---

## Guardrails System

Every product setup is validated against strict guardrails before creation. The guardrails system ensures:

1. **Ad Group exists** — required base structure
2. **RSA A/B/C created** — minimum 1 ad, recommended 3
3. **Callouts attached** — ad group level
4. **Sitelinks attached** — ad group level
5. **Promotion asset attached** — ad group level
6. **Structured snippets attached** — ad group level
7. **Keywords added** — at least 1 keyword per ad group
8. **Images attached** — at least 1 image per ad group

Products failing guardrails are flagged with status "incomplete" and added to the retry queue.

Run `batch_validate_guardrails` to check actual Google Ads state (uses API calls).
Run `batch_guardrails_report` to see cached compliance status (no API calls).

---

## Data-Driven Ad Copy

The batch system can generate AI-powered ad copy using:

1. **Asset Performance Analysis** — learn from top-performing headlines/descriptions in your account
2. **Keyword Intelligence** — incorporate converting keywords and search volume
3. **Market Intelligence** — trending topics, competitor ads, CPC/volume data
4. **Product Data** — feed prices, descriptions, categories

### Workflow

**Step 1**: Warm market intelligence (uses DataForSEO/Botster APIs via batch_warmup_market_intel)
```bash
batch_warmup_market_intel(country_code="TR", product_handles=["product1", "product2"])
```

**Step 2**: Warm AI cache (pulls analytics from Google Ads, saves to DB)
```bash
batch_warmup_cache(country_code="TR", campaign_id="123456789")
```

**Step 3**: Generate copy via batch_setup_all
```bash
batch_setup_all(country_code="TR", campaign_id="123456789")
```

The AI endpoint (your configured OpenAI key) receives context and generates 3 RSA variants (A/B/C) with headlines and descriptions.

---

## Language Safety Guardrails

Ad copy generation uses language-specific safety rules:

- **Turkish**: Remove diacritics for broader reach, use colloquial phrasing for engagement
- **Polish**: Formal/informal tone balance based on product category
- **German**: Compound word support, precision in claims
- **Multilingual**: Proper name handling, brand consistency

All generated copy is validated for:
- Brand consistency
- No prohibited claims
- Compliance with category regulations
- Proper character limits per field

---

## AI Ad Copy Pre-Warming Workflow

When you have many products to set up, pre-warming the AI copy cache saves significant time:

### Step 1: Market Intelligence (Optional but Recommended)

```bash
batch_warmup_market_intel(
  country_code="TR",
  product_handles=[],  # empty = discover all from feed
  timeout=180,
  batch_size=3
)
```

This fetches:
- Trending topics
- Competitor ads
- Keyword suggestions
- CPC and search volume

Then poll status with `batch_warmup_market_intel_status(job_id)`.

### Step 2: AI Cache Warmup

```bash
batch_warmup_cache(
  country_code="TR",
  campaign_id="22744242209",
  max_products=0  # 0 = all
)
```

This gathers performance data from Google Ads and sends to AI endpoint for instant copy generation.

Then poll status with `batch_warmup_status(job_id)`.

### Step 3: Setup Products

```bash
batch_setup_all(
  country_code="TR",
  campaign_id="22744242209",
  skip_existing=true
)
```

Now all calls to generate_ads.php use cached/warmed data and return instantly (1-3ms per product vs 35-50s cold).

---

## Supported Countries

| CC | Market | Language | Currency | Notes |
|----|--------|----------|----------|-------|
| PL | Poland | Polish | PLN | Primary market |
| DE | Germany | German | EUR | Strong market |
| ES | Spain | Spanish | EUR | |
| IT | Italy | Italian | EUR | |
| CZ | Czech Republic | Czech | CZK | |
| SK | Slovakia | Slovak | EUR | |
| BG | Bulgaria | Bulgarian | BGN | |
| AT | Austria | German | EUR | |
| HU | Hungary | Hungarian | HUF | |
| FR | France | French | EUR | |
| GR | Greece | Greek | EUR | Sensitive to image content |
| RO | Romania | Romanian | RON | |
| TR | Turkey | Turkish | TRY | Price extensions don't work; use callouts |
| UA | Ukraine | Ukrainian | UAH | |
| UK | United Kingdom | English | GBP | |
| US | United States | English | USD | |

Add more countries by updating config.json.

---

## Product Categories

Product categories are configured in config.json. Examples:

- Electronics
- Fashion
- Home & Garden
- Beauty
- Sports
- Food
- Toys
- Pet
- Automotive
- Other

---

## Safety Rules

### Rule 1: Always Use config.json

Never hardcode Customer IDs, Merchant IDs, or domains. Always load from config.json.

### Rule 2: Always Check Dry-Run First

Before any batch operation that modifies data, use dry_run=true to preview changes.

### Rule 3: Show & Confirm Before Changing

Before making changes (pause, bid update, budget change):
1. Display current values
2. Show proposed values
3. Explain why
4. Wait for user confirmation

### Rule 4: Respect API Quotas

- Check `batch_api_quota` before large operations
- Use `batch_size=1-3` for normal operation
- Slow down if seeing rate_limited responses

### Rule 5: Use Database for State

Don't rely on API calls for everything — use the local database for:
- Product status
- Ad group mapping
- Asset completeness
- Operation history

### Rule 6: Async for Long Operations

Operations handling 50+ products should use async mode:
- `batch_setup_all` (returns job_id, poll status)
- `batch_warmup_cache` (returns job_id, poll status)
- `batch_warmup_market_intel` (returns job_id, poll status)

### Rule 7: Restart After Long Pauses

After 1+ hour of inactivity, call `batch_sync_from_api` to re-sync database with actual Google Ads state.

### Rule 8: Never Delete Without Confirmation

Batch operations that delete/remove require explicit dry_run review + user confirmation.

### Rule 9: Preserve Image Assets

When removing disapproved images, always provide replacements before removal.

### Rule 10: Language Consistency

All ad copy for a product must use the same language as the target market (loaded from config.json).

### Rule 11: Validate Feed Before Batch

Before running batch operations on new feed:
1. `batch_sync_from_api` to load feed into DB
2. `batch_status` to see product count
3. Start with `max_products=5` to test

### Rule 12: Monitor for Policy Issues

Check `batch_check_eligibility` weekly to catch disapprovals early.

### Rule 13: Respect Timezones

When analyzing time-of-day performance, use the target market's timezone (loaded from config.json).

### Rule 14: Changelog Everything

Major operations should be logged:
```bash
batch_changelog_add(
  category="code_fix",
  title="Fixed domain issue in TR campaign",
  country_code="TR"
)
```

---

## Common Workflows

### Workflow 1: Full Product Setup from Scratch

1. Create campaign (or use existing)
2. Load feed: `batch_sync_from_api(CC, campaign_id)`
3. Warm market intel: `batch_warmup_market_intel(CC, product_handles=[...])`
4. Warm AI cache: `batch_warmup_cache(CC, campaign_id)`
5. Setup products: `batch_setup_all(CC, campaign_id)`
6. Check guardrails: `batch_guardrails_report(CC, campaign_id)`
7. Monitor: `batch_dashboard(CC, campaign_id)`

### Workflow 2: Add Images to Existing Campaign

1. Sync DB: `batch_sync_from_api(CC, campaign_id)`
2. Process images: `batch_process_images(product_handles=[...])`
3. Upload images: `batch_add_images_only(CC, campaign_id, product_handles=[...])`
4. Check status: `batch_images_progress(job_id)`

### Workflow 3: Pause Stale Products

1. Sync DB: `batch_sync_from_api(CC, campaign_id, include_feed=true)`
2. Check for stale: `batch_cleanup_stale(CC, campaign_id, dry_run=true)`
3. Approve: `batch_cleanup_stale(CC, campaign_id, dry_run=false)`

### Workflow 4: Fix Approval Issues

1. Check eligibility: `batch_check_eligibility(CC, campaign_id)`
2. Remove disapproved: `batch_remove_not_eligible_ads(campaign_id, dry_run=true)`
3. Approve: `batch_remove_not_eligible_ads(campaign_id, dry_run=false)`
4. Check domains: `batch_fix_domains(CC, campaign_id, dry_run=true)`

### Workflow 5: Analyze Keyword Performance

1. Mine keywords: `batch_mine_country_keywords(CC, days_back=90)`
2. Check progress: `batch_mine_keywords_progress(job_id)`
3. Research product: `batch_keyword_research(CC, "product_handle", days_back=180)`

---

## Troubleshooting

### Issue: "API rate limited" errors

**Cause**: Too many API calls in quick succession.

**Fix**:
1. Wait 5-10 minutes before retrying
2. Check `batch_api_quota` — if >90%, wait
3. Use smaller `batch_size` (e.g., batch_size=1)
4. Use `delay_between_batches=5` in batch_setup_all

### Issue: "Conversion not available" when setting TARGET_CPA

**Cause**: Campaign has no conversions yet, or conversion action is not properly configured.

**Fix**:
1. Run `google_ads_list_conversion_actions` to see active conversions
2. Verify conversion tracking is installed on your site
3. Wait 24-48 hours for initial data
4. Use MANUAL_CPC until conversions accumulate

### Issue: "Destination not working" for sitelinks

**Cause**: Final URL is invalid or inaccessible from Google bots.

**Fix**:
1. Verify URL is accessible in browser
2. Remove the sitelink: `batch_remove_not_eligible_sitelinks(campaign_id, dry_run=false)`
3. Create new sitelinks with verified URLs
4. Test URLs with `pagespeed_scan_url(url)`

### Issue: Images rejected with "Aspect ratio not allowed"

**Cause**: Image dimensions don't match exact 1:1 or 1.91:1 ratio.

**Fix**:
1. Calculate actual ratio: width / height
2. Crop to exact 1:1 using PIL
3. Verify with: `python -c "from PIL import Image; img=Image.open('x.jpg'); print(img.size[0]/img.size[1])"`
4. Re-upload

### Issue: "One website per ad group" disapproval

**Cause**: Multiple RSAs in same ad group have different domain final_urls.

**Fix**:
1. Run `batch_fix_domains(CC, campaign_id, dry_run=true)`
2. Review proposed changes
3. Approve: `batch_fix_domains(CC, campaign_id, dry_run=false)`
4. Verify all RSAs now use same domain

### Issue: Products not appearing after batch_setup_all

**Cause**: Products still in "pending" or "partial" status, or guardrails failed.

**Fix**:
1. Check: `batch_status(CC, campaign_id)`
2. Detailed: `batch_product_dashboard(CC, campaign_id)`
3. Find incomplete: `batch_missing(CC, campaign_id)`
4. Check logs: `batch_logs(campaign_id=campaign_id, limit=50)`

### Issue: "Cannot create RSA: Missing required fields"

**Cause**: Headlines or descriptions count is out of range.

**Fix**:
- Headlines: 3-15 required
- Descriptions: 2-4 required
- Pinning: only HEADLINE_1, HEADLINE_2, HEADLINE_3, DESCRIPTION_1, DESCRIPTION_2 valid

### Issue: "Database is locked" when running batch operations

**Cause**: Two operations writing to DB simultaneously.

**Fix**:
1. Stop current operation: `batch_stop_job(job_id)`
2. Wait 10 seconds
3. Retry operation

### Issue: Market intel warmup taking >30 minutes

**Cause**: Cold cache or network issues with DataForSEO.

**Fix**:
1. Check: `batch_warmup_market_intel_status(job_id)`
2. If stuck >30min: `batch_stop_job(job_id)`
3. Retry with smaller batch: `batch_warmup_market_intel(CC, batch_size=1, max_products=10)`

### Issue: AI copy generation returning generic placeholders

**Cause**: Market intel cache not warmed, or OpenAI API unavailable.

**Fix**:
1. Verify OpenAI key is set: `echo $OPENAI_API_KEY`
2. Pre-warm cache: `batch_warmup_cache(CC, campaign_id)`
3. Check logs for API errors

---

## Slash Commands

The plugin provides interactive slash commands for common tasks:

### `/status`
Show current account status and recent operations.

**Usage**: `/status` or `/status country_code=TR`

**Output**:
- Campaign count
- Total products
- Status breakdown (complete, partial, error)
- Recent operations

### `/setup-products`
Interactively setup specific products.

**Usage**: `/setup-products`

**Flow**:
1. Select country
2. Select campaign
3. Enter product handles (comma-separated)
4. Preview what will be created
5. Confirm to proceed

### `/audit`
Audit a campaign for missing assets and problems.

**Usage**: `/audit country_code=TR campaign_id=123456789`

**Output**:
- Ad group count
- Asset completeness per ad group
- Missing extensions
- Disapproved ads/assets

### `/fix-domains`
Fix wrong-domain RSAs in a campaign.

**Usage**: `/fix-domains country_code=TR campaign_id=123456789`

**Flow**:
1. Preview wrong-domain ads
2. Confirm fix
3. Process in background
4. Show results

### `/enable-keywords`
Mass-enable paused keywords matching a brand filter.

**Usage**: `/enable-keywords brand_filter="product1,product2" campaign_id=123456789`

**Flow**:
1. Preview paused keywords matching filter
2. Confirm
3. Enable in bulk
4. Show count enabled

### `/analyze`
Analyze campaign performance across dimensions.

**Usage**: `/analyze country_code=TR days=30`

**Outputs**:
- Time-of-day performance
- Location performance
- Device performance
- Keyword spend trends

### `/health-check`
Check system health and API status.

**Usage**: `/health-check`

**Outputs**:
- API connectivity: OK/ERROR
- Database: OK/ERROR
- Cache freshness
- Rate limit status
- Recent errors

### `/quota`
Check API quota usage.

**Usage**: `/quota`

**Output**:
- Quota used today
- Remaining quota
- Breakdown by operation type
- Status: healthy/warning/critical

### `/logs`
View operation logs.

**Usage**: `/logs campaign_id=123456789 limit=20`

**Output**:
- Timestamped log entries
- Operation type
- Status (success/error)
- Details

### `/sync`
Sync database with actual Google Ads state.

**Usage**: `/sync country_code=TR campaign_id=123456789`

**Flow**:
1. Show number of changes to sync
2. Confirm
3. Update DB from API
4. Show sync summary

### `/dashboard`
Show interactive dashboard.

**Usage**: `/dashboard country_code=TR`

**Displays**:
- Campaign list with metrics
- Product setup progress
- Top errors
- Recent operations

---

## Implementation Notes for Claude

When using this skill:

1. **Always check config.json first** — Don't assume hardcoded values
2. **Use batch tools for scale** — Single-product operations use MCP tools; multi-product use batch tools
3. **Respect async workflows** — Long operations return job_id; poll with status tools
4. **Dry-run everything destructive** — Show preview before confirming deletion/changes
5. **Convert micros** — Always convert between API micros and user-facing currency
6. **Use local timezone** — Convert UTC API times to market timezone
7. **Log major operations** — Use batch_changelog_add for tracking
8. **Check guardrails regularly** — Weekly batch_check_eligibility runs
9. **Leverage caching** — Pre-warm market intel and AI cache before large setups
10. **Prioritize by status** — Use batch_queue_next to find highest-priority products

---

## Version History

This is a universal plugin template. Version updates are managed through your plugin deployment system.

The core batch optimizer, guardrails system, and MCP tools are compatible with:
- Google Ads API v16+
- Merchant Center API v1.0+
- Python 3.8+

---

## Support & Customization

For custom configurations:

1. Edit `config.json` — Update account IDs, markets, domains, currencies
2. Add countries — Update `COUNTRY_CONFIG` dictionary with new market codes
3. Customize guardrails — Edit guardrail validation rules
4. Extend product categories — Add to `CATEGORIES` list
5. Modify templates — Update ad copy templates for your brand voice

For technical support, check logs with `batch_logs` or run `batch_health_check` to diagnose issues.

---

*This is a universal plugin for Google Ads campaign management. Adapt configurations and workflows to your specific business needs.*
