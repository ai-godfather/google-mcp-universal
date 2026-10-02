# Deliver, read back, then inspect the real Google Ads editor

## Freeze the plan

Freeze customer, campaign names, shop domain, exact product URLs, geo and
language constant IDs, conversion goal and action IDs, networks, strategy,
budgets, group and keyword bids, statuses, RSA texts, negatives and assets.
Bind them to the owner's final export and the grouping decision. Read the real
input schemas instead of guessing signatures from tool names.

Scope authorizations separately: paid research allowance, exact landing
changes, PAUSED creation, activation budget, later CPC changes and RSA
optimization. The user can grant several at once; do not add repeated gates
after a concrete approval, and never reuse a previous run's approval. Prepare
the reviewable result before asking for any missing consequential decision.

## Validate, then apply

1. Run `scripts/preflight.py RUN_DIR --through owner_review` with
   `delivery/requests.json` in place (stored with `validate_only: true`).
2. Call `google_ads_create_reviewed_search_campaign` per campaign with
   `validate_only: true`. Fix any `error` (keep Google's `request_id`) and
   validate again. `exists_no_mutation` means a campaign with that name exists:
   read it and reconcile; do not rename around it.
3. Reuse an existing campaign instead only after checking its settings, goal
   and interference risk; otherwise keep the isolated structure.
4. Apply with `validate_only: false` only inside the approved scope, one
   campaign at a time. Write a durable intent (`delivery/intents/<n>.json` with
   request fingerprint and time) before the call and the result
   (`delivery/results/<n>.json`) after it. Never delete intent files.
5. A timeout, partial failure or missing response does not prove nothing was
   created: read the campaign by exact name and IDs before any retry, keep the
   uncertain evidence and reconcile it.

## Conversion goal

Choose current, verified, ENABLED conversion action IDs for the campaign's real
objective, never IDs copied from an older run. If the account's standard goals
would pool several primary actions (for example two of the same category),
`google_ads_set_exact_campaign_conversion_goal` gives the PAUSED reviewed
campaigns one exact custom goal and turns their standard goals off, leaving
account defaults and action definitions unchanged. Validate first, then apply,
then read `conversion_goal_campaign_config` and `campaign_conversion_goal` back.

## Assets, bids and read-back

- Check every expected association at its intended level, not only asset
  creation. Business name and logo are campaign identity; local universal
  callouts may sit at campaign level; product-specific sitelinks, callouts,
  prices and images stay in their own ad group. Reuse a valid account logo.
- The creation tool sets one CPC for all groups and keywords. Under Manual CPC
  keyword bids override group defaults; update and read back both within the plan.
- When a change is only copy or identity, preserve budget, URLs, IDs, statuses
  and keywords; `google_ads_optimize_reviewed_search` enforces this with the
  current-content hash. Re-read copy after every change, including UI edits.
- Read budgets from `FROM campaign` (`campaign_budget.amount_micros`) in a
  separate query; mixing budget fields into an `ad_group_ad` query can fail. Use
  compatible bounded GAQL instead of suppressing errors, and read held items too.
- Compare provider IDs and statuses, URLs, 15/4 copy, budgets, group and keyword
  bids, asset links and goals with the reviewed payload. HTTP success is not
  completeness; keep prepared, uploaded, linked, approved and enabled separate.

## Editor verification in a browser

Use a browser automation tool if one is available, after reading its own
instructions; otherwise ask the user to look in their browser and share what
they see. Keep one browser session per goal and record its identifier in
`checkpoint.json` (`browser_session`). Do not inspect or change browser profiles
unless asked. If the user must sign in or take over, wait for their confirmation
and continue in the same session; never open a substitute profile to get around it.

Open the RSA editor for the verified IDs from the Google Ads UI. One observed
editor URL shape was `/aw/ads/edit/search` with `campaignId`, `adGroupId`,
`adId`, `adGroupIdForAd` and `ocid`; `ocid` is a UI account identifier, **not**
the 10-digit customer ID, so never derive it. If a direct link is not verified,
navigate through the UI.

For each ad:

1. Wait until the editor shows the matching final URL and product; snapshot the full page.
2. Read Ad Strength and every category rating. Accessible labels such as
   "Ad strength overall score is Excellent" may exist; adapt to the observed UI
   language instead of imagined selectors.
3. Let existing assets finish loading and observe again only while the view is
   incomplete. An initial Good can become Excellent with no save once ad-group
   sitelinks load. If it is not Excellent after a bounded wait, record the
   actual deficits; do not loop indefinitely or treat elapsed time as success.
4. Compare all 15/4 texts with the saved payload, plus final URL, business name,
   logo and asset associations.
5. Fix a genuine deficit only through the relevant approved copy, link or
   association: MCP for supported writes, the UI only for missing capabilities.
   Reopen after saving and read back through MCP; do not save unchanged forms.
6. Record score, categories, IDs and time with a screenshot or snapshot in
   `ui/`. A score read while loading is provisional; keep before and after.

### Two common false alarms

- Campaign-level sitelink slots in the RSA editor can look empty while six
  ad-group sitelinks exist; the settled score credits them. Query associations
  before creating duplicates.
- Ad-blocker warning text can remain in the DOM without any visible dialog.
  Confirm a visible obstruction before reacting; never disable protections or
  stop on a text match alone.

## Separate statuses and activation

Record saved state, API Ad Strength, editor Ad Strength, policy review,
approval, asset eligibility, enabled hierarchy, impressions and performance
separately. Editor Excellent can coexist with API PENDING; do not edit just to
make the API catch up. Ad Strength is not Quality Score, an auction result or ROI.

After concrete approval and readiness: enable the intended ads and ad groups,
read them under the still-paused campaigns, then enable only the approved
campaigns and read everything back. Keep held products and markets paused with
explicit reasons. If something is already enabled, verify without toggling it.
Watch ad and asset moderation.

The final report uses fresh read-backs and separate UI observation times: one
useful review or report link, active scope, budget and bids, and unresolved
conditions. Never claim serving or conversions from a preview, an approval or
an API response.
