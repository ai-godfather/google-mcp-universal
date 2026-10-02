# Historical, keyword and competition research

## Account history

Resolve customer currency, time zone and campaign criteria live. Group by the
campaigns' positive country targeting deliberately: that is not the user's
physical location. Read `geographic_view` or `user_location_view` separately
when location of users matters. `search_term_view` may reject
`segments.geo_target_country`; attribute search terms through campaign targeting.

Use GAQL with explicit dates, an `advertising_channel_type = 'SEARCH'` filter,
an explicit `LIMIT` and stable partitions. `google_ads_execute_gaql` returns at
most `page_size` rows (max 1000) as protobuf text: a result whose count equals
the page size may be truncated. Split by campaign, ad group or date range, keep
parent and leaf coverage, and aggregate only complete leaves. Do not treat a
conversion-filtered discovery query as the full-period cost denominator.

Collect separate datasets:

- campaign and ad group cost, clicks, impressions, conversions and values;
- `keyword_view` for purchased keywords and match types;
- `search_term_view` for actual customer queries;
- conversion action segments, to know what each conversion metric counts;
- `ad_group_ad` RSAs, identity assets and their associations;
- geo and language criteria, bid strategies, negatives and goal configuration.

Persist each query with its raw response. Normalize protobuf text offline with
the Google Ads SDK (for example `google.protobuf.text_format` into
`GoogleAdsRow`) rather than fragile string splitting. Sum cost and clicks for
weighted CPC (total cost / clicks); never average row-level CPCs. Keep missing
values distinct from zero. An `asset_performance_label` of `NOT_APPLICABLE` or
`PENDING` is not evidence of a winning headline.

Before interpreting revenue or ROAS, inspect what each conversion action really
counts and how its value is produced (imports, offline uploads, transformations,
deduplication, attribution model). Different actions cannot simply be added as
unique sales, and conversion value / cost is not profit. Preserve attribution
gaps and keep denominators comparable.

## Build a bounded request matrix

Matrix rows: exact product/handle, country, language, seed query, endpoint kind,
device, geo code, dates, expected cost and purpose. Prioritize the user-selected
products. Use native purchase and price modifiers and verified product aliases,
not a suffix copied blindly from internal handles.

Free and plugin sources come first: account history (`batch_keyword_research`,
`google_ads_list_search_terms`, GAQL), Google Trends, Google Ads Keyword
Planner, autocomplete, a manual local SERP check and the Ads Transparency
Center. A paid keyword/SERP provider is optional. If you use one, these endpoint
roles proved useful (DataForSEO v3 names shown as an example):

| Kind | Example endpoint | Evidence |
|---|---|---|
| serp | `serp/google/organic/live/advanced` | Local paid and organic results, kept as separate types |
| volume | `keywords_data/google_ads/search_volume/live` | Monthly volume history and provider CPC estimates |
| trends | `keywords_data/google_trends/explore/live` | Interest series, top and rising related queries, regions |
| autocomplete | `serp/google/autocomplete/live/advanced` | Query suggestions, not measured volume |
| related | `dataforseo_labs/google/related_keywords/live` | Expansion candidates |
| ranked | `dataforseo_labs/google/ranked_keywords/live` | Product-filtered organic rankings of observed competitor domains |
| ads_archive | `serp/google/ads_search/live/advanced` | Ads Transparency creatives per advertiser/domain |

Check the provider's current schema and accepted parameters before writing the
matrix. Query observed competitor domains; do not pay for unrestricted domain
inventories when only a few product phrases matter.

## Paid-request discipline

- Dry-run the exact matrix; verify language, geo and payload and the task-wide
  allowance. A small capped pilot establishes real access before expansion.
- Cache key = endpoint + canonical payload (country, language, dates, device,
  query). Choose a cache validity that fits research freshness; one cached
  response is not complete coverage.
- Durable intent before payment, per-fingerprint lock and recheck, a combined
  task ledger (`research/cost-ledger.json`), request and currency caps, and
  rejection of matrix drift make resumes cheap. A partial or ambiguous paid
  result does not authorize an automatic retry.
- Respect the provider's documented rate limits; never remove caps to go faster.
- Keep the raw response, status, task ID, charge and partial observations even on
  failure. Payment success and research success are separate facts.

If access fails, distinguish credentials, unsupported geo or language, empty
results, throttling, provider outage and an explicit credit shortage, and report
the observed error and which account surface needs attention. Do not invent a
balance. For example, a DataForSEO status 40102 ("No Search Results") with zero
items is an empty result, not a credit problem. Label fallbacks; an ordinary web
search or Google Trends alone is not complete paid research.

## Interpretation and reporting

Trends is normalized relative interest, not search volume. Keep the request's
term set, dates and geo; separately normalized requests are not directly
comparable (use a shared anchor query if you need relative comparison). A sparse
or missing series is not zero demand. Label top versus rising queries.

Organic rankings do not prove paid targeting. A live SERP shows one rendered
combination at one place and time, not all headlines, descriptions or assets.
An archive record's title may be the advertiser name, not the ad headline.
Inspect the actual creative in the Ads Transparency Center, save advertiser,
domain, country and dates, and label lifetime dates, query filter and impression
window separately. Archive impression ranges are not spend, clicks or ROAS, and
an empty filtered result does not prove that nobody advertises.

Read competitor landing pages directly after stripping ad-click tracking; never
click paid placements. Record offer, price, pack, delivery, CTA, claims and
positioning, then adapt to verified facts about your own product instead of
copying risky claims.

Deliver per-product tables of demand, trend coverage, competition, positive and
negative hypotheses and confidence. Keep every missing source as an explicit
coverage gap. Each keyword decision explains the query, landing fit and
economics, not just popularity.
