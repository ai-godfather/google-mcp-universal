# Plugin configuration and MCP tools

Rediscover the live tool list and input schemas at the start of each run. A tool
named here, a saved receipt or local source code does not prove the tool is
available in the current MCP session; a stale client can miss newer tools, so
reconnect or restart the client rather than guessing. Never print credentials or
private configuration values.

## Account and markets

All account details come from the plugin's `config.json` (discovery order: the
`GOOGLE_ADS_PLUGIN_CONFIG` env var, the plugin folder, `~/.google-ads-plugin/`):

| Key | Use in this skill |
|---|---|
| `account.customer_id` | Default account for tool calls. Still confirm customer ID, currency and time zone live; an MCC login is not the customer selection. `additional_accounts` may hold others. |
| `domains` | Country code → shop domain. A campaign targets exactly one of these domains. |
| `markets` | Countries in scope; a request for another country needs explicit confirmation. |
| `country_config` | Optional language/currency hints per country; the live account currency always wins for budgets and bids. |

Never fall back to an example or previously used account. If the config is
missing or incomplete, stop and ask (or run the plugin's `setup_account.py`).

## `search_launch` rules

Read by `mcp_search_delivery.py` at call time and by `scripts/preflight.py`
offline. Every key is optional.

```json
"search_launch": {
  "campaign_name_pattern": "^[A-Za-z0-9 _-]{1,40} \\| SEARCH \\| [A-Z]{2} \\| [a-z0-9.-]+ \\| \\d{4}-\\d{2}$",
  "product_path_prefix": "/products/",
  "blocked_product_prefixes": [],
  "allowed_product_prefixes": [],
  "business_name_must_match_domain": false
}
```

- **`campaign_name_pattern`**: Python regex that the full campaign name must match.
  The default means `<Prefix> | SEARCH | <CC> | <shop domain> | <YYYY-MM>`, e.g.
  `Brand | SEARCH | DE | de.example.com | 2027-03`. Separately, the name must
  contain ` | <shop_domain> | `. The goal and optimization tools refuse campaigns
  whose names fall outside the pattern, so the pattern also fences off campaigns
  this workflow must not touch.
- **`product_path_prefix`**: each ad group's final URL must be exactly
  `https://<shop_domain><prefix><ad group name>`, with no query string, fragment
  or credentials. So **the ad group name is the product handle** (or slug).
  Change the prefix for non-Shopify URL schemes (for example `/p/`).
- **`blocked_product_prefixes` / `allowed_product_prefixes`**: case-insensitive
  ad group (product handle) prefixes that must never get campaigns, with
  exceptions. Example: block `trial-` but allow `trial-pack-`. Use this for
  product families the advertiser must not promote (policy, licensing, supply).
- **`business_name_must_match_domain`**: when true, the business name the
  optimizer links must be exactly the shop domain.

Changing these rules is a configuration change for the user to approve; never
loosen them to make a payload pass.

## Reviewed-delivery tools (`mcp_search_delivery.py`)

All three default to `validate_only: true`, take one `request` object and
reject unknown fields. Outcomes: `validated`, a success status, a no-op status,
or `error` with Google's `request_id` and error list. An error without a
Google response carries `readback_required: true`: read back before any retry.

### `google_ads_create_reviewed_search_campaign`

Validates or atomically creates one PAUSED Search campaign: non-shared budget,
campaign, location and language criteria, ad groups, one RSA per group and
keywords, all in one mutate (`partial_failure` off).

```json
{"request": {
  "customer_id": "YOUR_CUSTOMER_ID", "expected_currency": "EUR",
  "campaign_name": "Brand | SEARCH | DE | de.example.com | 2027-03",
  "shop_domain": "de.example.com",
  "daily_budget_micros": 3000000, "cpc_bid_micros": 200000,
  "geo_id": "2276", "language_id": "1001",
  "groups": [{
    "name": "sample-bottle",
    "final_url": "https://de.example.com/products/sample-bottle",
    "path1": "sample-bottle", "path2": "kaufen",
    "headlines": ["... exactly 15 unique, each <= 30 ..."],
    "descriptions": ["... exactly 4, each <= 90 ..."],
    "keywords": [{"text": "sample bottle", "match": "EXACT"}]
  }],
  "validate_only": true
}}
```

Fixed settings: Google Search only (no search partners, no Display), PRESENCE
for positive and negative geo targeting, Manual CPC without enhanced CPC,
standard delivery, the EU political advertising declaration set to "does not
contain", and PAUSED campaign, ad groups and ads. Guards:

- `customer_id` is 10 digits; `expected_currency` must equal the live account
  currency; budget and CPC must be multiples of the currency's billable unit.
- `daily_budget_micros` 10,000 to 10,000,000 and `cpc_bid_micros` 1 to 1,000,000,
  i.e. at most 10 and 1 account-currency units. These are pilot caps: create
  within them and raise later only with explicit approval
  (`google_ads_update_campaign_budget`, bid tools).
- One `cpc_bid_micros` is applied to every ad group and keyword. Set different
  group or keyword bids afterwards.
- 1 to 9 groups with unique names; 1 to 40 unique EXACT/PHRASE keywords each
  (text up to 80); headlines unique and within limits; the `search_launch` rules.
- If a non-removed campaign with the same name exists, it returns
  `exists_no_mutation` with its IDs. Reconcile that campaign; never rename around it.
- Result `created_paused` lists the resource names of every created object.

### `google_ads_set_exact_campaign_conversion_goal`

```json
{"request": {"customer_id": "YOUR_CUSTOMER_ID", "campaign_ids": ["CAMPAIGN_ID"],
  "goal_name": "Brand | SEARCH | purchase", "conversion_action_ids": ["CONVERSION_ACTION_ID"],
  "validate_only": true}}
```

Up to 7 campaigns, each PAUSED, SEARCH and inside the naming pattern; 1 to 10
ENABLED conversion actions; `goal_name` letters, digits, spaces, `|`, `_`, `-`.
It reuses an enabled custom goal with the same name only when its actions are
identical (otherwise `custom_goal_action_drift`), sets that goal at campaign
level and makes every standard campaign conversion goal non-biddable for those
campaigns. Account defaults and conversion action definitions stay unchanged.
Use it when the account's standard goals would pool actions you do not want to
optimize for (for example two primary actions of the same category).

### `google_ads_optimize_reviewed_search`

Updates the text of existing RSAs in one reviewed campaign and links campaign
identity assets.

```json
{"request": {"customer_id": "YOUR_CUSTOMER_ID", "campaign_id": "CAMPAIGN_ID",
  "shop_domain": "de.example.com", "business_name": "de.example.com",
  "existing_logo_asset_id": "LOGO_ASSET_ID",
  "existing_callout_asset_ids": ["ID1", "ID2", "ID3", "ID4"],
  "ads": [{"ad_group_id": "AD_GROUP_ID", "ad_id": "AD_ID",
           "expected_content_sha256": "64 hex characters",
           "headlines": ["... 15 ..."], "descriptions": ["... 4 ..."]}],
  "validate_only": true}}
```

- Campaign: not removed, SEARCH, name in the pattern and containing the shop domain.
- Ads: 1 to 9 RSAs of that campaign; every final URL https on `shop_domain`;
  no pinned assets (pinned ads are rejected); 15 and 4 unique texts within limits.
- `expected_content_sha256` must match the ad's current content (below);
  otherwise `ad_content_drift_<ad_id>`: re-read, re-review, never force.
- Business name up to 25 characters: reuses an existing text asset with the same
  text or creates one. Logo: an existing ENABLED account-level BUSINESS_LOGO asset.
  Callouts: 4 to 8 existing callout assets. Conflicting business name or logo
  links already on the campaign stop the call.
- Preserves ad IDs, URLs, paths, statuses, keywords, bids and budgets. Returns
  `already_matches` when nothing would change.

Content hash, computed from a fresh `ad_group_ad` read
(`ad_group_ad.ad.final_urls`, `responsive_search_ad.headlines/descriptions/path1/path2`):

```python
import hashlib, json
content = {"headlines": [...], "descriptions": [...],   # text strings, current order
           "final_urls": [...], "path1": "...", "path2": "..."}
sha = hashlib.sha256(json.dumps(content, ensure_ascii=False, sort_keys=True,
                                separators=(",", ":")).encode()).hexdigest()
```

## Other tools used by this workflow

| Phase | Tools |
|---|---|
| Continuity | `batch_changelog_read`, `batch_changelog_add`, `batch_api_quota` |
| Account and history | `google_ads_get_account_summary`, `google_ads_execute_gaql` (`page_size` up to 1000; rows come back as protobuf text and are cut at `page_size`), `google_ads_list_campaigns`, `google_ads_list_campaign_criteria`, `google_ads_list_conversion_actions`, `google_ads_get_performance_report` |
| Keywords | `google_ads_list_keywords`, `google_ads_list_search_terms`, `batch_keyword_research`, `batch_category_keywords`, `batch_mine_country_keywords` (asynchronous job) |
| Market data | `batch_warmup_market_intel` only when its endpoint is configured in `config.json`; otherwise an external provider or official interfaces |
| Assets | `google_ads_create_sitelink_assets` (`sitelinks` with `link_text`, `description1`, `description2`, `final_urls`), `google_ads_create_callout_assets` (`callout_texts`), `google_ads_create_price_assets`, `google_ads_create_campaign_image_asset`, `google_ads_create_business_identity_assets`, `google_ads_list_assets`, `google_ads_remove_ad_group_asset_links` |
| Bids, budget, status | `google_ads_update_ad_group_bid`, `google_ads_update_keyword_bid`, `google_ads_update_campaign_budget`, `google_ads_update_ad_status`, `google_ads_update_ad_group_status`, `google_ads_update_campaign_status`, `google_ads_add_negative_keyword` |

Cautions:

- Asset tools link at campaign or ad group level depending on which ID you pass;
  check the resulting association, not just asset creation. Despite its name,
  `google_ads_create_campaign_image_asset` can target an ad group.
- `google_ads_create_responsive_search_ad` (for a genuine second RSA) has no
  `validate_only` and sets no status, so the ad takes the API default. Create it
  only inside approved scope, read back its status and pause it if it must stay staged.
- Promotion `percent_off`: pass a plain percent (`50` for 50%); the plugin converts it to
  the API unit (1,000,000 = 100%, so 50% = 500000). Create promotions only for a real,
  implemented offer and read the asset back to confirm the discount shown.
- Execute writes sequentially with durable intents (see
  [delivery-and-verification.md](delivery-and-verification.md)); check
  `batch_api_quota` before long sequences and stop, rather than loop, on quota errors.
