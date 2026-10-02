# Owner review page: template contract

The owner reviews and edits the exact proposal in a local HTML page built from
[`../assets/review-template.html`](../assets/review-template.html). Reusing the
same template keeps reviews comparable across runs: same views, controls,
limits and export. The page never calls Google Ads; edits stay in the owner's
browser until exported. If the owner prefers another editable document (a
shared sheet or doc), it must carry the same exact fields, character limits,
holds and an export the agent can read back; the HTML structure below remains
the reference.

## Render

Write the run's data to `review/review-data.json` (shape below; a complete
fictional example is [`../assets/review-data.example.json`](../assets/review-data.example.json)),
then replace the single `/*__REVIEW_DATA__*/` marker, which sits inside
`<script id="review-data" type="application/json">`:

```python
import json, pathlib
template = pathlib.Path("SKILL_DIR/assets/review-template.html").read_text(encoding="utf-8")
data = json.loads(pathlib.Path("RUN_DIR/review/review-data.json").read_text(encoding="utf-8"))
payload = json.dumps(data, ensure_ascii=False).replace("<", "\\u003c")  # keeps </script> and <!-- inert
pathlib.Path("RUN_DIR/review/review.html").write_text(template.replace("/*__REVIEW_DATA__*/", payload, 1), encoding="utf-8")
```

Opened without embedded data, the template shows a file picker that loads a
review-data JSON instead. Images must be relative files copied next to the page,
`https` URLs or small `data:image` URIs; the page must not depend on another
local server.

## Keep these views

Ten tabs in this order, plus the whole-structure overview:

1. **Ad preview**: desktop/mobile SERP simulation, three headline and two
   description selectors, combination presets, image/callout/sitelink toggles,
   price cards, meanings in the owner's language.
2. **Google Ads fields**: exact campaign and group settings (read-only) and the
   editable RSA (paths, 15 headlines, 4 descriptions) with live counters.
3. **Keywords & intent**: exact keywords, match types, sources, volumes, provider
   CPCs, decisions and reasons, negatives, post-launch evaluation.
4. **Assets & links**: editable callouts, sitelinks with counters, image, asset
   decisions, alternative offers.
5. **Packs & promotion**: real packs, pack-link behavior, exact price asset
   fields, promotion status.
6. **Trends & queries**: Trends series with gaps shown as gaps, top/rising
   queries, related volumes, query decisions.
7. **Competition & demand**: demand and monthly volumes, competitor pages,
   paid versus organic SERP observations, read parameters.
8. **Competitor ads**: archive coverage, reviewed creatives, limits.
9. **Account history**: comparable Search metrics, purchased keywords and
   search terms for the product and market.
10. **My review**: unreviewed / changes needed / accept copy, notes, export.

Show missing evidence inside its tab instead of removing the tab: a section set
to `null` renders as **pending** (not yet researched), while an attempted
section with empty arrays renders as an **empty result** (a limitation, not
zero demand). Do not replace the template with a simpler dashboard or drop
views unless the owner explicitly asks for a redesign; record such requests.

## Data contract

Top level:

| Field | Meaning |
|---|---|
| `version` | Run-specific string; also the local-storage namespace. Change it for every new run. |
| `title`, `generatedAt`, `locale`, `uiLang` | Page title, generation time, number-format locale, page language |
| `account` | `{customerId, currency}` read live; must equal the run manifest |
| `statusPill`, `banner` | Optional `{text, url}` and `{kind: info or hold, title, text, url}` describing the true current state |
| `historyPeriod`, `scopeNote`, `conversionGoal`, `googleAdsWrites` | Exact history dates, scope summary, `{name, actionIds, status}`, writes so far |
| `combos` | Optional presets `[{label, hint, h: [3 indexes], d: [2 indexes]}]` |
| `labels` | Optional overrides of any UI string, including `tabs` and `assetLabels`, to present the page in the owner's language |
| `products` | One item per product and market |

Per product (`status` `ready` or `held` must match the manifest):

| Field | Meaning |
|---|---|
| `id` | Stable key, e.g. `DE-sample-bottle`; owner edits are stored by it |
| `name`, `country`, `countryName`, `lang`, `languageName`, `sponsoredLabel` | Identity and local labels |
| `shop`, `handle`, `productId`, `url` | Exact shop domain, handle, existing product ID and final URL (`https://<shop><product_path_prefix><handle>`) |
| `price`, `currency`, `image`, `imageNote`, `logoUrl` | Verified price and currency, image source and notes |
| `status`, `reason` | `ready` or `held`, and why |
| `campaign`, `adGroup`, `geoId`, `languageId` | Exact campaign name (configured pattern), ad group name (= handle), Google constant IDs |
| `cpc`, `campaignBudget`, `groupBudgetShare`, `bidNote` | Account-currency starting CPC, campaign average daily budget, optional planned share, bid rationale |
| `businessName` | Up to 25 characters |
| `headlines` (15), `descriptions` (4), `callouts`, `path1`, `path2` | `{text, meaning}` items (plain strings accepted); `meaning` explains the text in the owner's language |
| `keywords` | `{text, match: EXACT or PHRASE, source, volume, providerCpc, providerCurrency, decision: include or test, reason}`; excluded ideas belong in `trends.candidates` |
| `negatives` | `{text, match, reason}` |
| `sitelinks` | `{link_text, description1, description2, final_urls: [url]}` |
| `assets` | Asset decisions keyed `businessName`, `logo`, `image`, `priceAsset`, `promotion`, `snippet` (free text) |
| `prices` | `{supported, type, languageCode, currency, offerings: [{header, description, price, final_url}], note}` |
| `packs`, `packUrl`, `packBehavior` | Real packs `{label, price, comparePrice, note, final_url}` and what the pack link actually does |
| `promotion` | `{status: none, proposed or implemented, text, basis, final_url}`; only `implemented` appears in the ad preview |
| `history`, `trends`, `research`, `archive` | Evidence sections (see the example file for their fields); `null` = pending |
| `alternatives`, `landingCorrection`, `defaultCopyUpdates`, `combos`, `afterLaunch` | Optional: other offers, a summary of a verified landing fix, default-text migrations, per-product presets, evaluation note |

`scripts/preflight.py` validates `review/review-data.json` for ready items
against the same rules as the delivery tool.

## Owner edits and new versions

The page stores edits and review choices in `localStorage` under
`search-launch-review-<version>`. When a new version replaces an older page:
ask the owner for (or read) the old export, merge their edits into the new data
by exact product `id`, and keep their notes. Never clear storage or overwrite
edits silently. `defaultCopyUpdates: [{field, index, oldText}]` refreshes a
default text only where the stored text still equals `oldText`; newly added
callouts are appended without touching saved ones. Save each version that the
owner sees under `review/versions/` (immutable); a new version needs a new
review of the changed parts.

The export (`Export review`) contains, per product, the exact `group` object in
the delivery tool's shape (`name`, `final_url`, `path1`, `path2`, `headlines`,
`descriptions`, `keywords`), assets, prices, promotion status, the owner's
decision and notes, `local_text_edits`, `invalid_fields` and
`activation_authorized: false`. Build `delivery/requests.json` from the owner's
final export, never from an older draft.

## Fresh data, not historical claims

Replace every date, budget, status banner, ID, URL and score claim with the
current run's facts. A proposed discount must not appear as an applied offer;
show the actual promotion status. A valid existing logo is not yet a campaign
association. Local acceptance or a local Excellent-looking preview is never a
Google Ads write or an actual Ad Strength result.

## Acceptance before presenting

Open the rendered page in a browser (a browser automation tool or the user's
own browser) and check one ready and one held item: all ten views and the
overview, desktop and mobile preview, combination presets and selectors,
counters and limit warnings, edit persistence after reload, and that the export
matches the visible copy. Record `review/template-provenance.json` (template
SHA-256, data file SHA-256, label overrides, owner-requested design changes)
and `ui/qa.json` (views checked, viewport sizes, gaps). Valid JSON or 15/4 text
alone does not prove the page works; if a capability cannot be kept, report the
exact gap and continue with independent work.
