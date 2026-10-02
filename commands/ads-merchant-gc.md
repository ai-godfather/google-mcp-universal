---
description: Merchant Center Garbage Collection — scan for disapproved products, blacklist or appeal
allowed-tools: ["mcp__google-ads__batch_merchant_gc", "mcp__google-ads__merchant_center_list_product_statuses", "mcp__google-ads__merchant_center_get_product_status", "mcp__google-ads__merchant_center_list_datafeeds", "mcp__google-ads__merchant_center_fetch_datafeed_now"]
---

# /ads-merchant-gc — Merchant Center Garbage Collection

Scan Google Merchant Center sub-accounts for DISAPPROVED products and take appropriate action.

## Arguments
- `{merchant_id}` (optional) — specific sub-account Merchant ID. If omitted or "ALL" (pass `merchant_id=None`), scans all sub-accounts under every MCA in config.json `merchant_center.mca_ids`.
- `{country_code}` (optional) — filter to specific country (e.g., "DE", "PL"); matched against the product feed label or target country

## Two Action Types

### 1. BLACKLIST (automatic)
Products with policy violations that **cannot be appealed** — they contain prohibited content:
- Healthcare and medicine: Prescription drugs
- Healthcare and medicine: misleading claims
- Dangerous products (general)
- Product policy violations
- Misrepresentation
- Other DISAPPROVED reasons NOT in the appeal list

**Action**: Add offerId to the **CORRECT** blacklist file (see below) and trigger feed refresh.

### 2. APPEAL (manual — generate links)
Products with violations that **CAN be appealed** — they are likely false positives:
- Alcoholic beverages
- Dangerous products (Tobacco products and related equipment)
- Guns and parts
- Sexual interests in personalized advertising
- Restricted adult content
- Adult-oriented content
- Personalized advertising: legal restrictions

**Action**: Generate Merchant Center UI links for each product so the user can manually click "Request review" → "I don't sell [category] products".

**NOTE**: Google Merchant Center API does NOT support programmatic review requests. Reviews MUST be done manually in the MC UI. The tool generates direct links to speed up the process.

---

## ⚠️ CRITICAL: Where Blacklisted Products Are Written

`batch_merchant_gc` appends BLACKLIST offer IDs to `dat/BLACKLIST/gMerchant/<shop host>.txt` (relative to the plugin root):

- `<shop host>` = host of the product links in that Merchant Center sub-account (e.g. `de.yourshop.com`) — **without any path**
- no product link found → `merchant_<merchant_id>.txt`
- each run appends a block that starts with a timestamped `UPDATE` header line

Your feed generator must read these files and drop the listed offer IDs from the feed — otherwise blacklisting has no effect.

### ⛔ DANGER ZONE: One Host, Several Language/Country Feeds

If one host serves several feeds under paths (e.g. `eu.yourshop.com/de`, `eu.yourshop.com/es`), the tool still writes every entry to `eu.yourshop.com.txt`, because the `domain` it sees is only the BASE host. If your feed generator reads one file per path (e.g. `eu.yourshop.com_de.txt`) or per country (`DE.txt`), you MUST move the new entries to the file that feed actually reads:

1. Check `blacklist_files_updated` (and the `domain` of each `to_blacklist` entry) in the result
2. Look up the entry's `merchant_id` in config.json (`merchant_center.merchant_ids`) to know which country/path it belongs to
3. Move the entries to the correct file before committing

**Example** (fake IDs): merchant 333333333 (DE) returns `domain: "eu.yourshop.com"` → the tool writes `eu.yourshop.com.txt`; if your DE feed reads `eu.yourshop.com_de.txt`, move the entries there.

Tip: keep your own merchant ID → blacklist file mapping (e.g. a CSV next to the blacklist files) for such multi-path shops and use it on every run.

---

## Workflow

### Step 1: DRY RUN (always start here)
```
batch_merchant_gc(
    merchant_id={merchant_id or None for ALL},
    country_code={country_code or None},
    dry_run=True
)
```

Present results in the user's language:
- Summary table: accounts scanned, products checked, to_blacklist count, to_appeal count, already_blacklisted
- BLACKLIST table: offerId, title, **correct blacklist file** (resolved per the DANGER ZONE rules — NOT just the base domain), reasons
- APPEAL table: offerId, title, domain, reasons, MC review link

⚠️ **VERIFY**: For each product in `to_blacklist`, confirm the correct blacklist file via its merchant_id. If the tool returns a base host that serves several language/country feeds, resolve it to the file that feed reads.

### Step 2: CONFIRM & EXECUTE (after user approval)
```
batch_merchant_gc(
    merchant_id={same},
    country_code={same},
    dry_run=False,
    refresh_feeds_after_blacklist=True
)
```

Present:
- Which blacklist files were updated + how many new entries
- ⚠️ **DOUBLE-CHECK** that `blacklist_files_updated` shows the files your feeds actually read (move entries for multi-path shops)
- Which feeds were refreshed
- APPEAL links table (user must click these manually)

### Step 3: GIT COMMIT & PUSH & DEPLOY (after blacklist updates)
If any blacklist files were updated in Step 2, commit, push and deploy to production:
```bash
# 1. Commit & push locally
cd /path/to/your-project
git add dat/BLACKLIST/gMerchant/*.txt
git commit -m "GC: blacklist [N] disapproved products ([reasons summary])"
git push origin main

# 2. Deploy to production (adjust to your deployment workflow)
# e.g.: ssh yourserver "cd /path/to/app && git pull origin main"
```
If production has local changes and fast-forward fails, use: `git merge origin/main --no-edit`

### Step 4: REFRESH ALL PRODUCT FEEDS in Merchant Center
After blacklist commit+push, refresh ALL data feeds in Merchant Center for the affected merchant accounts.

For each affected MC sub-account:
1. List all datafeeds: `merchant_center_list_datafeeds(merchant_id={mid})`
2. For each datafeed, trigger immediate fetch: `merchant_center_fetch_datafeed_now(merchant_id={mid}, datafeed_id={feed_id})`

**Present in the user's language**: list of refreshed feeds per MC account.

### Step 5: APPEAL SUMMARY
Generate a clean list of all appeal links grouped by merchant_id for the user to process in the MC UI.

## Important Notes
- ALWAYS start with dry_run=True
- ALWAYS present results in the user's language
- ALWAYS ask for confirmation before executing with dry_run=False
- After blacklisting: ALWAYS do git commit+push, then refresh ALL feeds in affected MC accounts
- The appeal MC UI links format: `https://merchants.google.com/mc/items/details?a={merchant_id}&offerId={offer_id}`
- For appeals, the user needs to select "I don't sell [category] products" and click "Request review"
- Account: Merchant Center access comes from your OAuth credentials; MCA IDs from config.json `merchant_center.mca_ids` (no Google Ads customer ID is needed)
- For hosts that serve several language/country feeds, **never leave entries in the base-host file** unless your feed generator reads that file
