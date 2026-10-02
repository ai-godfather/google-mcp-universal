---
name: google-ads-search-launch
description: "Prepare and launch product Google Ads Search campaigns end to end: historical account audit, competitor ads, keywords and Google Trends, campaign grouping, landing-page and Shopify content corrections, an editable owner review page, PAUSED delivery through validate_only and then apply with the reviewed-delivery MCP tools, read-back and Ad Strength verification in the Google Ads editor, and later optimization of existing reviewed RSAs. Use for new or repeated product Search launches on the shops and markets configured in the plugin. Not for Shopping or Performance Max, and not for status-only requests."
---

# Google Ads Search launch: from research to a verified ad

Use the user's language for questions, review notes and reports. Reproduce the
**method**, never a previous run's products, dates, IDs, prices, budgets or
permissions. A careful run can reach Excellent Ad Strength, but nothing here
guarantees a score, approval, traffic or sales. Read each reference when its
phase starts.

## Configuration

The account, currency, markets and shop domains come from the plugin's
`config.json`: `account.customer_id`, `domains` (country → shop domain) and
`markets`. Never assume a default account. Confirm customer ID, currency and
time zone with a live read before planning; an MCC login is not the customer
selection.

The reviewed-delivery tools also enforce the optional `search_launch` section
of the same file (defaults shown):

```json
"search_launch": {
  "campaign_name_pattern": "^[A-Za-z0-9 _-]{1,40} \\| SEARCH \\| [A-Z]{2} \\| [a-z0-9.-]+ \\| \\d{4}-\\d{2}$",
  "product_path_prefix": "/products/",
  "blocked_product_prefixes": [],
  "allowed_product_prefixes": [],
  "business_name_must_match_domain": false
}
```

- `campaign_name_pattern`: full-match regex for reviewed campaign names, by
  default `<Prefix> | SEARCH | <CC> | <shop domain> | <YYYY-MM>`. The name must
  also contain ` | <shop domain> | `. Campaigns outside the pattern are off
  limits for the goal and optimization tools.
- `product_path_prefix`: the final URL must be exactly
  `https://<shop domain><prefix><ad group name>`, so each ad group is named
  after its product handle.
- `blocked_product_prefixes` / `allowed_product_prefixes`: product-name prefixes
  that must never get campaigns, with exceptions (case-insensitive).
- `business_name_must_match_domain`: the linked business name must equal the
  shop domain.

Tool fields, guards, return statuses and the RSA content hash:
[tools-and-config.md](references/tools-and-config.md).

## Start and resume

1. Read the plugin's main skill (`google-mcp-universal`) for setup, safety rules
   and tool conventions. Before UI work, read the instructions of the available
   browser automation tool, or plan to ask the user to check in their own browser.
2. Resolve products and markets, shop domains, account, history period, spend
   envelope and existing authorization. Approval given in this conversation
   holds for its stated scope; a new run never inherits a previous run's
   approvals, budgets or research allowance.
3. Create one run directory with `scripts/start_run.py` (offline; see
   [run-artifacts.md](references/run-artifacts.md)): manifest, checkpoint, cost
   ledger, raw receipts, review files and read-backs. On resume, read the
   checkpoint and continue the incomplete phase instead of repeating paid
   requests or writes.
4. Call `batch_changelog_read` and bounded read-only GAQL. Discover the current
   tool schemas before planning mutations; a tool name in this skill does not
   prove the tool is available.
5. For a handoff, a continuation by another agent, or "finish and deploy
   everything", read [agent-handoff.md](references/agent-handoff.md). Drafting a
   message does not authorize sending it or acting on it.

Use available evidence without unnecessary questions; ask only for missing
decisions that materially change scope, and continue independent work
meanwhile. A plan-only request stops at the owner review, without provider mutations.

## 1. Current products and Search history

Read [research-and-history.md](references/research-and-history.md).

- Inventory exact country / shop / handle / product ID rows from the store
  (admin API or catalog) and current reads. Keep offers, distinct products,
  shop pairs, drafted descriptions and confirmed writes apart.
- Check availability and ownership, active offer, product form and category,
  local language, variants, the market's real price and currency, image and
  landing page. Hold blocked families and product-specific problems without
  blocking qualified siblings.
- Default history: **16 calendar months ending yesterday in the account time
  zone**, plus separate 90- and 30-day slices, unless the user chooses
  otherwise. State exact dates.
- Audit Search per targeted country and comparable product or intent: cost,
  clicks, weighted CPC, purchased keywords, actual search terms, assets and the
  conversion actions behind the numbers. Keep raw GAQL and complete partitions;
  report sparse or stale samples, overlapping actions and currencies honestly.

## 2. Keywords, Trends and competition

- Research the requested qualified products first: native brand, purchase,
  price and closely related category hypotheses in the real language and geo.
- Sources: account history tools, Google Trends (16 months and 90 days, related
  and rising queries), keyword volume and CPC estimates, autocomplete and related
  queries, local SERP observations and the Ads Transparency Center. A paid data
  provider is optional; `batch_warmup_market_intel` works only when its endpoint is configured.
- Paid requests: read caches and earlier intents first, run an authorized capped
  pilot, then expand only the reviewed matrix. Record fingerprint, task ID,
  requested and returned geo, time, raw response and actual charge.
- Inspect real competitor creatives and landing pages. Paid ads, organic results,
  archive entries and keyword hypotheses are different evidence; the archive
  does not reveal keyword lists, full RSA assets, bids or profitability.
- When a provider fails, identify the real error. Empty results do not mean no
  demand, no competitors or no credits; label any fallback.

Output an evidence-to-keyword table: term, intent, match type, demand and trend
coverage, competitor source, landing fit and include / test / exclude. Do not
add irrelevant broad or symptom terms to improve a score.

## 3. Structure, budgets and starting bids

Read [campaign-architecture.md](references/campaign-architecture.md). Before
creation write `campaign-decisions.json`: alternatives, settings, economics,
reasons, unknowns, budget allocation and the reevaluation trigger.

- Same country is not enough to share a campaign. Compare shop domain, language
  and location, conversion goal, bid strategy, unit economics, budget control
  and demand. Prefer compact campaigns with tightly themed product groups when
  settings and budget can be shared; split when control or economics require it.
- A reviewed campaign has one shop domain, one location, one language, one
  non-shared budget, one starting CPC for every group and keyword, and up to 9
  ad groups. Keep product prices, URLs and assets at ad group level; only truly
  universal assets go at campaign level.
- Recommend conservative CPCs from comparable history and current demand, not
  the billing minimum. Budget / CPC is a rough click capacity, not a forecast.
  Check group defaults **and keyword overrides**. Manual CPC with exact and
  phrase match is a controlled pilot choice, not a universal alternative to
  Smart Bidding. Explain average daily budgets; no unauthorized increases or
  automatic escalation.

## 4. Landing page and offer

Read [landing-and-creative.md](references/landing-and-creative.md).

- Correct wrong descriptions only within authorized exact product scope, after
  confirming identity, form and size from a coherent source and the packaging.
  Keep IDs, handles and unrelated fields: a narrow body-only write with backup,
  a current-hash precondition, and admin **plus** storefront read-back. Never
  replay an old plan or use a broad upsert that clears omitted fields.
- Check the whole journey: price and currency, packs, delivery and payment, CTA
  and form language, real anchors. Never submit live orders or leads as a test.
  A wrong-language form holds that product alone. New bundles or discounts need
  separate commercial authorization; a mockup or compare-at price is not an
  applied promotion.

## 5. Copy, assets and the owner review page

- Write **15 distinct headlines and 4 distinct descriptions per RSA** (30 and 90
  characters, paths 15): native high-intent phrases, verified benefits, prices
  and packs, a clear call to action. No filler, unsupported claims, invented
  discounts, urgency or official status; no pinning. Count compliance is not
  quality: never fill slots with "order form" or "contact the shop" instead of
  verified product value.
- Prepare up to 6 useful localized sitelinks and 6 callouts per product when the
  site supports them, plus qualifying business name and logo, real product
  images and supported price assets. No fictional phone numbers, apps or
  snippet categories.
- Google recommends 2+ RSAs per ad group with distinct final URLs. Evaluate a
  genuinely different relevant landing; the reviewed tool creates one RSA per
  group, so a one-RSA pilot is a recorded decision, never a fabricated second URL.
- Build the owner review from the [bundled template](assets/review-template.html)
  under [review-template-contract.md](references/review-template-contract.md):
  a local, editable HTML page with exact settings, keywords, copy with counters
  and meanings, URLs and pack behavior, desktop and mobile combinations, assets,
  prices, research, grouping rationale, costs and holds. Preserve owner edits;
  the page never writes to Google Ads.
- If approval is needed, finish this concrete review first. Proceed without
  asking again for actions already approved. Do not expand paid research or spend.

## 6. MCP delivery: validate, apply, read back

Read [delivery-and-verification.md](references/delivery-and-verification.md).

1. Refresh product, price and settings. Build `delivery/requests.json` from the
   owner's final export and run `scripts/preflight.py RUN_DIR --through owner_review`.
2. Call `google_ads_create_reviewed_search_campaign` with `validate_only: true`
   (the default) per campaign. Fix errors and validate again. `exists_no_mutation`
   means the name exists: reconcile that campaign, never rename around it.
3. Apply with `validate_only: false` only inside the approved scope, one campaign
   at a time, with a durable intent written first. A timeout, partial failure or
   missing response does not prove nothing happened: read back before any retry.
4. Attach the exact conversion goal with `google_ads_set_exact_campaign_conversion_goal`
   (validate, then apply; up to 7 PAUSED reviewed campaigns). It turns off the
   campaigns' standard goals and leaves account defaults unchanged.
5. Add sitelinks, callouts, price assets and images at the intended level, and
   set group and keyword bids that differ from the starting CPC.
6. Read back with GAQL: IDs, statuses, URLs, 15/4 copy, budget, group and keyword
   bids, asset links and goals against the reviewed payload. HTTP success is not
   completeness; keep prepared, uploaded, linked, approved and enabled separate.

## 7. Editor verification

Use a browser automation tool if available; otherwise ask the user to check in
their own browser session. Reuse one browser session per goal; if the user must
sign in or take over, wait for their confirmation and continue in that session.

- Open the real RSA editor by verified IDs; compare saved copy, business name,
  logo and assets; read Ad Strength and every category.
- Let asynchronous assets load before judging: an initial Good with low sitelink
  coverage can become Excellent with no change once existing ad-group sitelinks
  load. Empty campaign-level slots are not missing ad-group assets; check
  associations before creating duplicates. Ad-blocker text in the DOM needs a
  visible obstruction before it matters.
- Fix real deficits within scope, preferably through MCP; use the UI only for
  operations MCP lacks. Save, reopen, read back. Never force Excellent with
  irrelevant keywords or claims, or report it from the local page.
- Record UI score and categories, IDs, time and a screenshot or snapshot. API Ad
  Strength may stay PENDING meanwhile. Ad Strength is not Quality Score, Ad Rank,
  eligibility or profitability.

## 8. Approved activation and handoff

When exact budget and scope are approved: enable ready ads and ad groups, verify
them under the paused campaigns, then enable the approved campaigns and read
back enabled **and** held scope. Do not toggle ads that are already enabled.

Report concisely: scope, budget in account currency, bids, editor score,
moderation, holds and the review link. Before raising bids for missing
impressions, diagnose review and eligibility, demand, negatives, geo, rank and
budget. Do not schedule monitoring or automatic raises without authorization.
Update the checkpoint and `batch_changelog_add`. Enabled does not prove
impressions or sales. For work passed between agents, reconcile every product
and remaining phase with the [handoff contract](references/agent-handoff.md).

## Later: optimize existing reviewed RSAs

For copy or identity improvements on a campaign inside the naming pattern:

1. Read the current ads with GAQL (final URLs, headlines, descriptions, paths)
   and compute each ad's content SHA-256 ([recipe](references/tools-and-config.md)).
2. Prepare complete replacement texts (15 and 4, unique, unpinned, within
   limits), the business name, an existing ENABLED account logo asset and 4 to 8
   existing callout assets; review them with the owner like a launch.
3. Call `google_ads_optimize_reviewed_search` with `validate_only: true`, then
   apply within approval. It keeps ad IDs, URLs, statuses, keywords, bids and
   budgets. A hash mismatch means the ad changed since your read: re-read and
   re-review, never force.
4. Read back copy and links, then repeat the editor check from phase 7.

## Fast repeat and completeness

- Batch independent reads; sequence dependent writes. Reuse exact caches,
  normalize offline and keep raw evidence.
- `scripts/preflight.py RUN_DIR` reports progress offline. Use `--through creative`
  before presenting the review, `--through owner_review` before any write, and
  the matching later phase for delivery or handoff. Passing it is not approval,
  a live validation or a substitute for account reads.
- Mark every phase completed (with evidence), not_needed (with reason), held
  (with resume condition) or pending. A research gap inventory is not completed
  research: unattempted Trends, related-query or competitor work stays pending,
  while an attempted empty result is a documented limitation.
- Refresh changing Google rules through [sources.md](references/sources.md)
  before new structural or policy decisions.
