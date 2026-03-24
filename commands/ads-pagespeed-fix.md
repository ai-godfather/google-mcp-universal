---
description: Apply PageSpeed performance fixes to Shopify templates in REPOSITORIES/ACTIVE_SHOPS
allowed-tools: ["mcp__google-ads__pagespeed_apply_fixes", "mcp__google-ads__pagespeed_analyze_theme", "mcp__google-ads__pagespeed_scan_url"]
---

# /ads-pagespeed-fix — Apply PageSpeed Fixes

Apply performance optimizations to Shopify Liquid templates.

## Usage

Parse the user's input for: country code (or "all"), fix IDs, and mode (--dry-run or --apply).

### Step 1: Analyze (always)
1. Call `pagespeed_analyze_theme` for the target repo(s)
2. Show current findings and which fixes are applicable

### Step 2: Preview (--dry-run, default)
1. Call `pagespeed_apply_fixes` with fix_ids, repo_names, dry_run=true
2. Show unified diff for each file that would be changed
3. Show summary: N files across M repos
4. Ask user to confirm before applying

### Step 3: Apply (--apply, only after user confirms)
1. Call `pagespeed_apply_fixes` with dry_run=false
2. Show applied changes
3. Suggest: "Run `git commit` in each repo, then rescan with /ads-pagespeed to verify improvement"

## Available Fixes

| Fix ID | Name | Severity | Files | Est. Savings |
|--------|------|----------|-------|-------------|
| PSI-001 | Defer IP fetch in helperscripts | CRITICAL | snippets/helperscripts.liquid | -400ms |
| PSI-002 | Lazy-load popupExit | CRITICAL | snippets/popupExit.liquid | -350ms |
| PSI-003 | Defer phone button CSS | HIGH | layout/theme.liquid | -200ms |
| PSI-010 | Add defer to jQuery | CRITICAL | layout/theme.liquid | -300ms |
| PSI-011 | Add defer to Splide.js | HIGH | layout/theme.liquid | -150ms |
| PSI-012 | Remove console.log | LOW | multiple | -10ms |

## Safety Rules
- ALWAYS preview with --dry-run first
- Show diffs to user, wait for explicit "apply" or "--apply" confirmation
- NEVER modify files without user approval
- After applying: remind user to git commit and rescan
- Multi-instance impact: fixing MULTI2 affects 49 countries, MULTI1 affects 38 — warn user of scope

## Priority Order
Recommended fix order (biggest impact first):
1. PSI-010 (jQuery defer) — simplest change, biggest FCP impact
2. PSI-001 (helperscripts defer) — defers sync network call
3. PSI-011 (Splide defer) — another render-blocking script
4. PSI-002 (popupExit lazy) — heaviest JS payload
5. PSI-003 (phone CSS defer) — inline CSS optimization
6. PSI-012 (console.log cleanup) — minor cleanup
