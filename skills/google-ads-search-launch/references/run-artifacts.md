# Run files and offline helpers

Both helpers use only the Python 3.9+ standard library (with IANA time zone
data). They never call Google Ads, a store, a browser or a paid provider, and
they never grant authorization.

## Start once; resume by reading files

Run from the skill directory. `WORKSPACE` is any folder outside the plugin
folder (plugin updates may replace it):

```sh
python3 scripts/start_run.py WORKSPACE/search-runs/2027-03-15-de \
  --customer YOUR_CUSTOMER_ID --currency EUR --timezone Europe/Berlin \
  --products WORKSPACE/selected-products.json --intent prepare
python3 scripts/preflight.py WORKSPACE/search-runs/2027-03-15-de
```

Replace every placeholder with values read live from the account. The
bootstrap refuses an existing directory, so a resumed run is never reset. It
uses today in the account time zone (or `--date YYYY-MM-DD`) and derives 16
calendar months ending yesterday plus the last 90 and 30 complete days: start =
run date minus 16 calendar months (clamped to the month's last day), end = run
date minus one day; for 2027-03-15 that is 2025-11-15 to 2027-03-14. A
user-chosen interval may replace it; record why. `--intent prepare | paused |
activate | optimize` records the desired work, **not permission**.

Created files:

- `manifest.json`: account, run date, history intervals, products and approval
  records (`paid_research`, `landing_changes`, `paused_delivery`, `activation`,
  `bids`, `optimization`).
- `checkpoint.json`: phase statuses and `browser_session` (the browser session
  used for UI checks, if any).
- `research/cost-ledger.json`: caps start as `null`, and null never authorizes
  spending; each request records fingerprint, intent, task ID, charge and receipt.
- Folders `raw/`, `research/`, `landing/`, `review/`, `delivery/`, `ui/` for receipts.

Keep the manifest and checkpoint current without rewriting historical
evidence, owner edits or durable mutation and payment intents.

Selected products (fictional example, not a deployable cohort):

```json
[
  {
    "country": "DE",
    "shop_domain": "de.example.com",
    "handle": "sample-bottle",
    "product_id": "EXACT_EXISTING_ID",
    "final_url": "https://de.example.com/products/sample-bottle",
    "status": "pending",
    "reason": "Awaiting current admin, price and storefront reads",
    "evidence": []
  }
]
```

Set a product `ready` only with its exact existing ID, the exact final URL
(`https://<shop_domain><product_path_prefix><handle>`) and relative evidence
paths. Set `held` with a concrete reason for exclusions; products matching
`blocked_product_prefixes` must stay held. Every requested product stays in the
manifest, including excluded ones.

## Phases

`inventory, history, research, architecture, landing, creative, owner_review,
delivery, editor_review, activation, handoff`. Each has `status`, `reason` and
`evidence`:

- `pending`: not done.
- `completed`: requires evidence files inside the run directory.
- `not_needed`: requires a reason (for example no landing correction after a
  verified review, or activation outside a plan-only request).
- `held`: requires a concrete resume condition. A product-specific hold does
  not hold the whole phase when qualified siblings can proceed.

Record instructions the user actually gave under `approvals` with the exact
scope and a short transcript excerpt as evidence. That documents existing
approval; it does not require asking again, and elapsed time is never consent.

## Review and delivery files

- `campaign-decisions.json`: alternatives and decision fields from
  [campaign-architecture.md](campaign-architecture.md).
- `review/review-data.json`: the review page data
  ([review-template-contract.md](review-template-contract.md)); `review/review.html`
  the rendered page; `review/versions/` immutable copies of each version shown;
  `review/owner-export-*.json` the owner's exports.
- `delivery/requests.json`: an array of exact
  `google_ads_create_reviewed_search_campaign` requests built from the final
  owner export, stored with `validate_only: true`.
- `delivery/intents/` and `delivery/results/`: one intent before and one result
  after every write; never deleted.
- `ui/`: editor observations (score, categories, IDs, time, screenshot or snapshot path).

## Receipts and truthful history

For every provider observation or confirmed action keep a receipt:

- `id`: stable and unique (same id and payload = same receipt);
- `type`: `provider_observation`, `delivery_saved`, `activation_confirmed`,
  `pause_confirmed`, `ui_verified`, `research_completed` or `landing_verified`;
- `observed_at`: when you verified it; `occurred_at`: when the mutation actually
  happened, if known (required for activation and pause; never substitute the
  observation time or a file modification time);
- `source` and `evidence_sha256` binding the saved raw evidence;
- `resources`: campaign, group and ad IDs with status, name, budget, CPC,
  policy state and other observed fields (empty for research or UI receipts).

To recover past action times, query `change_event` read-only for the exact
campaign IDs within its retention window, keep old and new values, the resource
name and the account time zone, and convert `change_date_time` accordingly. Do
not infer times for events you cannot find; a first ENABLED read without an
action time stays a `provider_observation`. Older observations never overwrite
newer state. Keep saved, linked, moderated, enabled and serving distinct.

## Preflight

```sh
python3 scripts/preflight.py RUN_DIR                         # progress report
python3 scripts/preflight.py RUN_DIR --through creative      # before presenting the review
python3 scripts/preflight.py RUN_DIR --through owner_review  # before any write
python3 scripts/preflight.py RUN_DIR --through delivery --config /path/to/config.json
```

It reads `search_launch` from the plugin config (same discovery order as the
plugin, or `--config`) and checks:

- manifest and checkpoint schema, account fields, time zone and history dates;
- phase statuses, evidence paths inside the run, reasons for holds and skips;
- product identities, blocked prefixes, exact final URLs of ready products, and
  domains or markets missing from the config (warnings);
- from `--through creative`: `campaign-decisions.json`, no pending products, at
  least one ready product, and `review/review-data.json`;
- review items: campaign name pattern and shop-domain segment, final URL =
  `https://<shop><prefix><ad group>`, 15 unique headlines (30), 4 unique
  descriptions (90), paths (15), 1 to 40 unique EXACT/PHRASE keywords, callouts,
  sitelinks, business name, price offerings, promotion status, one shop,
  location, language and budget per campaign, at most 9 groups, tool budget and
  CPC caps (warnings);
- `delivery/requests.json` whenever present: exact request and group keys, IDs,
  currency, micros ranges, every group equal to the reviewed copy, URL, keywords,
  geo, language and budget, and (at `--through delivery`) every ready item covered.

Exit 0 means the offline checks passed; exit 2 lists errors. Pending phases are
reported, not hidden. It cannot check billable units, policy, claims, live
eligibility, commercial authorization or Ad Strength; the tools' `validate_only`
call and fresh read-backs remain required. Character counting is conservative
for double-width scripts; Google's validation governs.
