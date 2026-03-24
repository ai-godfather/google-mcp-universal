---
description: Merchant Center Garbage Collection — scan for disapproved products, blacklist or appeal
allowed-tools: ["mcp__google-ads__batch_merchant_gc", "mcp__google-ads__merchant_center_list_product_statuses", "mcp__google-ads__merchant_center_get_product_status", "mcp__google-ads__merchant_center_list_datafeeds", "mcp__google-ads__merchant_center_fetch_datafeed_now"]
---

# /ads-merchant-gc — Merchant Center Garbage Collection

Scan Google Merchant Center sub-accounts for DISAPPROVED products and take appropriate action.

## Arguments
- `{merchant_id}` (optional) — specific sub-account Merchant ID. If "ALL", scans all sub-accounts under both MCAs.
- `{country_code}` (optional) — filter to specific country (e.g., "RO", "PL")

## Two Action Types

### 1. BLACKLIST (automatic)
Products with policy violations that **cannot be appealed** — they contain prohibited content:
- Healthcare and medicine: Prescription drugs
- Healthcare and medicine: misleading claims
- Dangerous products (general)
- Product policy violations
- Misrepresentation
- Other DISAPPROVED reasons NOT in the appeal list

**Action**: Add offerId to the **CORRECT** blacklist file (see mapping below) and trigger feed refresh.

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

## ⚠️ CRITICAL: Blacklist File Mapping Rules

The PHP feed generator (`WHITELIST-show-products.php`) reads **TWO files** per feed instance:

1. **Primary file**: Derived from the shop URL — slash `/` replaced with underscore `_`
2. **Country-specific blacklist file**: `{COUNTRY_CODE}.txt` (uppercase) — applied as additional layer

A product blocked in **EITHER** file is blocked in the feed.

### Mapping Logic (from WHITELIST-show-products.php)

```
Rule 1: Shop URL with path (eu.yourstore.com/de) → slash→underscore → eu.yourstore.com_de.txt
Rule 2: Shop URL without path (ro.yourstore.com) → ro.yourstore.com.txt
Rule 3: If countryCode param set (DE) → ALSO reads DE.txt as country-specific blacklist
Rule 4: Product blocked in EITHER primary OR country file → blocked in feed
CRITICAL: When adding products to blacklist — ALWAYS use the PRIMARY file for that shop instance!
```

### ⛔ DANGER ZONE: EU Multi-Language Instances

For `eu.yourstore.com` and `eu2.yourstore.com`, each language has its OWN blacklist file.
**`eu.yourstore.com.txt` is ONLY used when the shop URL has NO `/xx` path — it is NOT read for DE, ES, EL, IT, or SK feeds.**
**NEVER write DE/ES/IT/GR/SK/FR products to `eu.yourstore.com.txt` or `eu2.yourstore.com.txt`.**

| Merchant ID | Country | Shop URL | PRIMARY blacklist file | Country File |
|------------|---------|----------|----------------------|------------|
| MERCHANT_ID_EU_DE | DE | eu.yourstore.com/de | `eu.yourstore.com_de.txt` | `DE.txt` |
| MERCHANT_ID_EU_AT | AT | eu.yourstore.com/de | `eu.yourstore.com_de.txt` | `DE.txt` |
| MERCHANT_ID_EU_ES | ES | eu.yourstore.com/es | `eu.yourstore.com_es.txt` | `ES.txt` |
| MERCHANT_ID_EU_GR | GR | eu.yourstore.com/el | `eu.yourstore.com_el.txt` | `GR.txt` |
| MERCHANT_ID_EU_IT | IT | eu.yourstore.com/it | `eu.yourstore.com_it.txt` | `IT.txt` |
| MERCHANT_ID_EU_SK | SK | eu.yourstore.com/sk | `eu.yourstore.com_sk.txt` | `SK.txt` |
| MERCHANT_ID_EU2_FR | FR | eu2.yourstore.com/fr | `eu2.yourstore.com_fr.txt` | `FR.txt` |

### Complete Merchant ID → Blacklist File Mapping

#### Primary MCA (Account 1)

| Merchant ID | CC | Primary Blacklist File | Country File |
|------------|----|-----------------------|------------|
| MERCHANT_ID_1 | AE | `ae.yourstore.com.txt` | `AE.txt` |
| MERCHANT_ID_2 | AR | `ar.yourstore.com.txt` | `AR.txt` |
| MERCHANT_ID_3 | AU | `au.yourstore.com.txt` | `AU.txt` |
| MERCHANT_ID_4 | CA | `ca.yourstore.com.txt` | `CA.txt` |
| ... | ... | ... | ... |

#### Secondary MCA (Account 2)

| Merchant ID | CC | Primary Blacklist File | Country File | Notes |
|------------|----|-----------------------|------------|-------|
| MERCHANT_ID_EU_PL | PL | `pl.yourstore.com.txt` | `PL.txt` | |
| MERCHANT_ID_EU_DE | DE | `eu.yourstore.com_de.txt` | `DE.txt` | ⚠️ NOT eu.yourstore.com.txt! |
| MERCHANT_ID_EU_ES | ES | `eu.yourstore.com_es.txt` | `ES.txt` | ⚠️ NOT eu.yourstore.com.txt! |
| MERCHANT_ID_EU_GR | GR | `eu.yourstore.com_el.txt` | `GR.txt` | ⚠️ NOT eu.yourstore.com.txt! |
| ... | ... | ... | ... | |

### How to Determine the Correct File

When `batch_merchant_gc` returns a product with `domain: "eu.yourstore.com"`, you CANNOT just use `{domain}.txt`. You MUST:

1. Look up the **merchant_id** in the mapping table above
2. Use the **PRIMARY blacklist file** from the table
3. The `domain` field from the API is the BASE domain — it does NOT include the language path

**Example**: MC 621165580 returns `domain: "eu.yourstore.com"` → the correct file is `eu.yourstore.com_de.txt` (NOT `eu.yourstore.com.txt`)

**Canonical reference**: `dat/BLACKLIST/BLACKLIST_FILE_MAPPING.csv`

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

Present results in Polish:
- Summary table: accounts scanned, products checked, to_blacklist count, to_appeal count, already_blacklisted
- BLACKLIST table: offerId, title, **correct blacklist file** (from mapping — NOT just domain), reasons
- APPEAL table: offerId, title, domain, reasons, MC review link

⚠️ **VERIFY**: For each product in `to_blacklist`, confirm the correct blacklist file using the merchant_id mapping. If the tool returns `domain: "eu.yourstore.com"`, resolve it to the correct `_xx.txt` file.

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
- ⚠️ **DOUBLE-CHECK** that `blacklist_files_updated` shows the correct `_xx.txt` files, not the base domain file
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

**Present in Polish**: list of refreshed feeds per MC account.

### Step 5: APPEAL SUMMARY
Generate a clean list of all appeal links grouped by merchant_id for the user to process in the MC UI.

## Important Notes
- ALWAYS start with dry_run=True
- ALWAYS present results in Polish
- ALWAYS ask for confirmation before executing with dry_run=False
- After blacklisting: ALWAYS do git commit+push, then refresh ALL feeds in affected MC accounts
- The appeal MC UI links format: `https://merchants.google.com/mc/items/details?a={merchant_id}&offerId={offer_id}`
- For appeals, the user needs to select "I don't sell [category] products" and click "Request review"
- Customer ID: `YOUR_CUSTOMER_ID`
- **NEVER write products to `eu.yourstore.com.txt` or `eu2.yourstore.com.txt`** unless the product is specifically from the base domain with no language path
- The canonical reference for all mappings is `dat/BLACKLIST/BLACKLIST_FILE_MAPPING.csv`
