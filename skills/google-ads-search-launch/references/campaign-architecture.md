# Campaign grouping: decide from controls and economics

## Evidence versus local judgment

Google places budgets and many targeting and bidding controls at campaign level;
related keyword themes belong in focused ad groups. Shared budget and targeting
can support one campaign; different campaign settings justify separation
([Google account organization](https://support.google.com/google-ads/answer/6372655?hl=en)).

For AI-based Search, Google favors simpler structures, clear objectives and
themed groups over unnecessary fragmentation. That does not mean merging every
product into one undifferentiated group, or expecting the same outcome from a
tiny manual-CPC pilot ([ABCs of account structure](https://support.google.com/google-ads/answer/14752782),
[Search simplification](https://business.google.com/uk/think/search-and-video/ai-for-search-advertising-campaign-strategy/)).

The rules below are planning judgments aligned with those sources, not
thresholds prescribed by Google. No rule says every product needs a campaign or
every country exactly one.

## What the reviewed creation tool fixes

`google_ads_create_reviewed_search_campaign` creates a campaign with one shop
domain, one location, one language, one non-shared budget, one starting CPC for
all groups and keywords, up to 9 ad groups and one RSA per group. Different
domains, languages or locations therefore mean different campaigns in this
workflow. That is a property of the tool and of the one-domain-per-campaign
convention, not a Google API restriction.

## Decision matrix

| Question | Usually share a campaign | Separate or investigate |
|---|---|---|
| Destination | Same verified shop domain, language and location | Different domain, language or location needs |
| Goal | Same meaningful conversion action and objective | Different objectives (lead vs purchase) or incompatible measurement |
| Economics | Comparable acceptable acquisition cost, or reliable values that express the differences | Very different margin, payout or approval rates not represented in bidding signals |
| Budget | Owner accepts allocation by demand | A product needs a reserved budget, a ceiling or a guaranteed test allocation |
| Strategy | Same strategy; different manual group or keyword bids are fine | Different strategy or a campaign-level target that cannot be shared |
| Demand | Small related cohorts benefit from not splitting budgets | One high-volume, low-margin product is likely to consume the pool |
| Intent | Product brand, purchase and price groups stay relevant | Generic discovery versus brand traffic needs separate economics or control |
| Lifecycle | Same availability, timing and priority | Separate launch, season or pause schedule |

Different product names or CPCs alone do not require different campaigns; the
same country does not prove identical economics.

## Required planning output (`campaign-decisions.json`)

For each proposed campaign:

```
campaign_key, campaign_name, country, shop_domain, language, products, goal,
bidding_strategy, shared_or_reserved_budget, daily_budget_account_currency,
starting_cpc, group_and_keyword_bid_overrides, alternative_considered,
why_chosen, economics_evidence, unknowns, ad_group_themes, product_asset_scope,
review_trigger
```

Daily click capacity = daily budget / plausible CPC, labeled as an
approximation. Use current keyword demand to check traffic plausibility and
explain any product whose expected sample is too small. Do not invent a
universal minimum of clicks or conversions for Smart Bidding. If the total
budget is tiny, propose a narrower rollout or a longer learning period instead
of silently increasing it or creating many starved campaigns. Never promise
equal spend between groups of one campaign.

A product that needs reserved spend needs its own campaign and budget; putting
it into a shared pool does not reserve anything. Reliable conversion values and
suitable targets may handle economic differences without separation; decide
from the actual bidding capabilities and data.

## Worked pattern and its boundary

A typical small launch: one campaign per shop domain and language, several
themed product groups inside it, each with its own landing page, keywords,
copy, prices and assets; campaign-level assets only when they are true for
every product. Such a structure can earn Excellent creative scores, which is
**not** proof that the grouping is the most profitable one. Split later when
real spend and qualified conversions show an allocation problem, or when the
owner asks for product-specific reservations.

Use meaningful tight themes. Do not recreate single-keyword ad groups or
duplicate every exact/phrase variant across campaigns out of habit. Prevent
cross-product copy and URL mix-ups, and check existing negatives before adding
exclusions.

## RSA count

Google's RSA guidance recommends at least two good or excellent RSAs per ad
group with distinct final URLs ([RSA guide](https://support.google.com/google-ads/answer/7684791?hl=en));
an older general page mentions three ads, so prefer the format-specific guidance.
Assess a genuinely different relevant landing and offer hypothesis. A one-RSA
pilot (what the reviewed tool creates) is a recorded decision, not a default.
Never create query-string or anchor variants, or duplicate products, just to
satisfy a count; a new URL needs its own identity, offer and conversion checks.

## Bidding and reassessment

Use account-currency weighted CPC = total cost / clicks from recent comparable
product and intent data; say so when only older country-level data exists.
Provider CPC estimates may be in USD while the account bills in another currency.
A starting CPC from history is a hypothesis, not a default or a permission to spend.

Under Manual CPC, keyword-level bids override the ad group default; plan and
read back both. Before raising bids for missing impressions, check the enabled
hierarchy, review status and eligibility, start date, low-volume flags, match
and negative conflicts, presence/geo/language, budget and rank indicators.
Auction Insights and Ad Preview are useful diagnostics where available. Raise
only within explicit limits, on sufficiently complete data, and never schedule
automatic increases from this skill alone.

Average daily budgets are not strict daily ceilings: for most campaigns Google
may spend up to 2 times the average on a day and up to 30.4 times it in a month,
with changes and partial periods handled separately
([spending limits](https://support.google.com/google-ads/answer/10486637?hl=en)).
