# Correct landing, persuasive copy, real assets

## Verify the offer before writing

For each exact shop / handle / product ID: store status and publication,
ownership or supply (the advertiser can actually sell it), source offer, form
and size, variants and the market's actual price and currency (for example the
Shopify Markets price list), then the public page and the browser journey. An
"active" flag or HTTP 200 is not enough. Hold products matching
`search_launch.blocked_product_prefixes`, and record any market-specific
currency or delivery hold with its evidence rather than copying it to every run.

Different categories under one brand are different offers: a cream is not a
capsule, an external spray is not an oral supplement, a refill is not a starter
kit. Never borrow dosage, ingredients, specifications or outcomes from another
offer. Packaging can establish visible form and size, not unstated ingredients,
efficacy or clinical claims.

## Narrow product description correction (Shopify example)

Change only what is wrong, inside an authorized exact-ID scope:

1. Retrieve the full product (Admin API) plus any downstream copy that mirrors
   it (feed, cache, CMS block). Save before-snapshots and hashes; use the
   existing authenticated integration, never new credentials.
2. Draft a localized, factual description from qualified same-product evidence;
   review the text and affected images. Remove contradictory claims only.
3. Re-read immediately before each write and compare against the expected
   before-hash. Send a body-only update, e.g.
   `PUT /admin/api/<version>/products/<id>.json` with
   `{"product": {"id": <id>, "body_html": "..."}}`. Never use a broad upsert or
   an old plan that could clear omitted fields (title, handle, status, variants,
   prices, images, metafields, routing).
4. Read the product back before continuing or retrying, then sync only the
   mirrored body field with a current-state precondition. Verify every untouched field.
5. Verify the storefront separately: content, price, variant and form/CTA language.

Shopify may normalize HTML (for example newlines between blocks) after a
successful write, so a raw hash mismatch is not proof of failure. Keep the raw
evidence, compare meaningful text, attributes, links and structure, and resume
from fresh state; never resend the same write just because a post-write
comparison differed.

Inactive existing products are reactivated by their exact IDs and handles inside
an authorized reactivation scope; do not create numbered duplicates.

## Brand names, packs and promotions

A preferred marketing name (for example dropping a sub-line word from the brand
in ads and keywords) does not authorize changing the product's URL, handle or title.

Inspect the existing pack selector and current prices. A link to an on-page
anchor opens the selector; it does **not** preselect a quantity. Describe the
actual behavior in the review. If real pack URLs or variant links exist, verify
and use them exactly. Do not invent bundle pages, duplicate products or discounts.

For a new promotion or bundle, first define exact components, quantities,
price and currency, the product choice, URLs and validity; apply it through a
separately authorized commerce change and verify it in the store admin and on
the storefront before advertising it. A compare-at price or a mockup is not an
implemented discount; validate the reference price under current rules.

## RSA and value-led writing

15 headlines and 4 descriptions, each sensible on its own and in any mixed
combination: headlines up to 30 characters, descriptions up to 90, paths up to
15 (double-width scripts count double; Google's validation is authoritative).
Make product plus local purchase or price phrases explicit and natural, for
example "<product> precio", "kúpiť <product>", "<product> prix", "<product> kaufen".

Cover different jobs: identity and category, verified benefit, price, pack
choice, delivery and payment, confidence supported by facts, direct CTA. Do not
repeat one promise 15 ways. Write assertively but truthfully; internal checks
("check contact before ordering") do not belong in sales copy, while
customer-relevant conditions must stay accurate.

Do not invent treatment effects, speed of results, clinical proof, exclusive or
official status, scarcity or savings. Older account ads and competitor claims
show wording, not proof that a claim is valid for this product. Keep legally
required text where needed; otherwise do not pin (the optimizer rejects pinned ads).

## Assets and localization

| Asset | Target and validation |
|---|---|
| Sitelinks | Up to 6 meaningful, distinct destinations per product on the same domain: text up to 25, descriptions up to 35 each. Product page, pack selector, delivery, FAQ, ordering and contact pages are typical when they really exist |
| Callouts | Up to 6 distinct local facts per group, up to 25 characters; reuse generic ones at campaign level only when true for every product |
| Business name | Up to 25 characters, matching the verified advertiser or domain and current policy (`business_name_must_match_domain` may require the domain) |
| Logo | Reuse a correct, approved existing account logo and verify its association; do not generate a replacement for a usable logo |
| Product images | The correct product, a real clean packshot, permitted crop/pad/scale with provenance; no misleading overlays or claim badges; never cut the packaging to fit a ratio |
| Prices | 3 to 8 real offerings (e.g. 1/2/3 packs) with the actual destination, in a supported language and the right currency |
| Promotion | Only a real implemented offer with a defensible basis and conditions; never just to fill a slot |

More distinct qualifying images help, but one verified image is better than
clones or invented product shots; do not describe one image as full coverage.

Price assets support only some languages, and the accepted language code can be
specific (for example a regional Portuguese code); check the current list. Where
unsupported, prices can stay accurate in RSA text and sitelinks.

Do not fill asset types with fiction: phone, message, app and structured snippet
assets need a real destination or category. In multi-product campaigns keep
product-specific prices and claims at ad group level so no product inherits
another's copy. Creation, association, moderation and eligibility are separate checks.

## Owner review before live action

Follow [review-template-contract.md](review-template-contract.md). Render exact
field values, resource scope and a localized preview: all 15/4 texts with
counters and meanings in the owner's language, actual URLs and pack-link
behavior, callouts, sitelinks, prices, images and business identity, the chosen
structure with its alternative, budget and CPC, evidence and holds.
Desktop/mobile combinations are labeled simulations. Preserve owner edits and
notes across regeneration. A saved-state report after delivery must read actual
provider snapshots and keep UI Ad Strength, API Ad Strength and moderation
separate. A local "accept" or "Excellent" badge never mutates or impersonates Google.
