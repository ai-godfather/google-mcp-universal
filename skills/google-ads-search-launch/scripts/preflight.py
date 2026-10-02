#!/usr/bin/env python3
"""Offline preflight for a Search launch run directory.

Checks the run files and the reviewed payloads against the rules that the reviewed-delivery MCP tools
enforce, using the "search_launch" section of the plugin config.json. It never calls Google Ads, a store,
a browser or a paid provider. Passing it is not approval, policy review, a live validate_only call, or
proof of Ad Strength, eligibility or serving.
"""
import argparse
from datetime import date
import json
import os
from pathlib import Path
import re
import sys
import unicodedata
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from start_run import PHASES

DEFAULT_NAME_PATTERN = r"^[A-Za-z0-9 _-]{1,40} \| SEARCH \| [A-Z]{2} \| [a-z0-9.-]+ \| \d{4}-\d{2}$"
HOST = re.compile(r"^[a-z0-9-]+(\.[a-z0-9-]+)+$")
REQUEST_KEYS = {"customer_id", "expected_currency", "campaign_name", "shop_domain", "daily_budget_micros",
                "cpc_bid_micros", "geo_id", "language_id", "groups", "validate_only"}
GROUP_KEYS = {"name", "final_url", "path1", "path2", "headlines", "descriptions", "keywords"}
BUDGET_MICROS, CPC_MICROS = (10_000, 10_000_000), (1, 1_000_000)  # caps of google_ads_create_reviewed_search_campaign


def ad_length(text):
    """Conservative count: double-width scripts count twice. Google Ads validation stays authoritative."""
    return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in str(text))


def text_of(item):
    return item.get("text") if isinstance(item, dict) else item


def load_config(explicit):
    """Same discovery order as the plugin loader: env var, plugin root config.json, ~/.google-ads-plugin."""
    if explicit is not None:
        return json.loads(explicit.read_text(encoding="utf-8")), str(explicit)
    env = os.environ.get("GOOGLE_ADS_PLUGIN_CONFIG")
    candidates = [Path(env)] if env else []
    parents = Path(__file__).resolve().parents
    if len(parents) > 3 and (parents[3] / "skills" / "google-mcp-universal").is_dir():
        candidates.append(parents[3] / "config.json")
    candidates.append(Path.home() / ".google-ads-plugin" / "config.json")
    for path in candidates:
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8")), str(path)
    return {}, None


def launch_rules(config):
    section = config.get("search_launch") or {}
    rules = {
        "campaign_name_pattern": section.get("campaign_name_pattern") or DEFAULT_NAME_PATTERN,
        "product_path_prefix": section.get("product_path_prefix", "/products/"),
        "blocked_product_prefixes": [str(x).lower() for x in section.get("blocked_product_prefixes") or []],
        "allowed_product_prefixes": [str(x).lower() for x in section.get("allowed_product_prefixes") or []],
        "business_name_must_match_domain": bool(section.get("business_name_must_match_domain", False)),
    }
    re.compile(rules["campaign_name_pattern"])
    return rules


def blocked(rules, name):
    """Mirror of the tool: a blocked prefix applies unless an allowed prefix also matches."""
    name, block, allow = str(name).lower(), tuple(rules["blocked_product_prefixes"]), tuple(rules["allowed_product_prefixes"])
    return bool(block) and name.startswith(block) and not (allow and name.startswith(allow))


def check_destination(ctx, campaign, shop, group, final_url, rules, errors):
    if not isinstance(shop, str) or not HOST.fullmatch(shop) or len(shop) > 253:
        errors.append(f"{ctx}: shop domain must be a lowercase host name")
        return
    if not isinstance(campaign, str) or re.fullmatch(rules["campaign_name_pattern"], campaign) is None:
        errors.append(f"{ctx}: campaign name {campaign!r} does not match search_launch.campaign_name_pattern")
    elif f" | {shop} | " not in campaign:
        errors.append(f"{ctx}: campaign name must contain ' | {shop} | '")
    if not isinstance(group, str) or not group or len(group) > 255:
        errors.append(f"{ctx}: ad group name (the product handle) required")
        return
    if blocked(rules, group):
        errors.append(f"{ctx}: ad group {group!r} matches search_launch.blocked_product_prefixes")
    prefix = rules["product_path_prefix"]
    url = urlsplit(final_url) if isinstance(final_url, str) else None
    if (url is None or url.scheme != "https" or url.hostname != shop or url.username or url.password
            or url.query or url.fragment or url.path != prefix + group):
        errors.append(f"{ctx}: final URL must be exactly https://{shop}{prefix}{group} (no query, fragment or credentials)")


def check_copy(ctx, headlines, descriptions, path1, path2, keywords, errors):
    for field, items, count, limit in (("headlines", headlines, 15, 30), ("descriptions", descriptions, 4, 90)):
        if not isinstance(items, list) or len(items) != count or not all(isinstance(t, str) for t in items):
            errors.append(f"{ctx}: exactly {count} {field} strings required")
            continue
        if any(not t.strip() or ad_length(t) > limit for t in items):
            errors.append(f"{ctx}: empty or over-{limit}-character {field}")
        if len({t.strip().casefold() for t in items}) != count:
            errors.append(f"{ctx}: duplicate {field}")
    for name, value in (("path1", path1), ("path2", path2)):
        if not isinstance(value, str) or ad_length(value) > 15:
            errors.append(f"{ctx}: {name} must be a string of at most 15 characters")
    if isinstance(path1, str) and isinstance(path2, str) and path2.strip() and not path1.strip():
        errors.append(f"{ctx}: path2 requires path1")
    if not isinstance(keywords, list) or not 1 <= len(keywords) <= 40:
        errors.append(f"{ctx}: 1-40 keywords required")
        return
    seen = set()
    for k in keywords:
        text, match = (k.get("text"), k.get("match")) if isinstance(k, dict) else (None, None)
        if not isinstance(text, str) or not text.strip() or len(text) > 80 or match not in ("EXACT", "PHRASE"):
            errors.append(f"{ctx}: keyword {k!r} needs text of 1-80 characters and match EXACT or PHRASE")
            continue
        if (text, match) in seen:
            errors.append(f"{ctx}: duplicate keyword {text!r} {match}")
        seen.add((text, match))
        if str(k.get("decision", "")).lower() == "exclude":
            errors.append(f"{ctx}: keyword {text!r} is marked exclude but listed for the ad group")


def check_assets(ctx, p, shop, rules, errors, warnings):
    callouts = [text_of(c) for c in p.get("callouts") or []]
    if any(not isinstance(c, str) or not c.strip() or ad_length(c) > 25 for c in callouts):
        errors.append(f"{ctx}: callouts must be nonempty and at most 25 characters")
    elif len({c.strip().casefold() for c in callouts}) != len(callouts):
        errors.append(f"{ctx}: duplicate callouts")
    for s in p.get("sitelinks") or []:
        urls = s.get("final_urls") or ([s["final_url"]] if s.get("final_url") else [])
        if not str(s.get("link_text", "")).strip() or ad_length(s.get("link_text", "")) > 25:
            errors.append(f"{ctx}: sitelink text must be 1-25 characters")
        if any(ad_length(s.get(f, "")) > 35 for f in ("description1", "description2")):
            errors.append(f"{ctx}: sitelink descriptions must be at most 35 characters")
        if not urls or any(urlsplit(u).scheme != "https" or urlsplit(u).hostname != shop for u in urls):
            errors.append(f"{ctx}: sitelink {s.get('link_text')!r} needs https URLs on {shop}")
    name = p.get("businessName")
    if name and ad_length(name) > 25:
        errors.append(f"{ctx}: business name must be at most 25 characters")
    if name and rules["business_name_must_match_domain"] and name != shop:
        errors.append(f"{ctx}: business_name_must_match_domain requires the business name {shop!r}")
    prices = p.get("prices") or {}
    if prices.get("supported"):
        offerings = prices.get("offerings") or []
        if not 3 <= len(offerings) <= 8:
            errors.append(f"{ctx}: a price asset needs 3-8 offerings")
        for o in offerings:
            url = urlsplit(str(o.get("final_url", "")))
            if (not str(o.get("header", "")).strip() or ad_length(o.get("header", "")) > 25
                    or ad_length(o.get("description", "")) > 25 or not isinstance(o.get("price"), (int, float))
                    or o["price"] <= 0 or url.scheme != "https" or url.hostname != shop):
                errors.append(f"{ctx}: price offering {o.get('header')!r} needs header/description <=25, a positive price and an https URL on {shop}")
    promotion = (p.get("promotion") or {}).get("status", "none")
    if promotion not in ("none", "proposed", "implemented"):
        errors.append(f"{ctx}: promotion status must be none, proposed or implemented")
    elif promotion == "proposed":
        warnings.append(f"{ctx}: promotion is only proposed; keep it out of ad copy and assets until it is implemented and authorized")


def check_review(data, manifest, inventory, rules, errors, warnings):
    """Validate review/review-data.json; return ready items keyed by (campaign, ad group)."""
    if not isinstance(data, dict) or not isinstance(data.get("products"), list) or not data["products"]:
        errors.append("review-data.json: object with a nonempty products array required")
        return {}
    if not str(data.get("version", "")).strip():
        errors.append("review-data.json: run-specific version (local storage namespace) required")
    account = data.get("account") or {}
    if str(account.get("customerId", "")) != manifest["account"].get("customer_id"):
        errors.append("review-data.json: account.customerId differs from the manifest")
    if account.get("currency") != manifest["account"].get("currency"):
        errors.append("review-data.json: account.currency differs from the manifest")
    ready, ids, campaigns = {}, set(), {}
    for p in data["products"]:
        if not isinstance(p, dict):
            errors.append("review-data.json: each product must be an object")
            continue
        ctx = f"review {p.get('id')}"
        if not p.get("id") or p["id"] in ids:
            errors.append(f"{ctx}: missing or duplicate id")
        ids.add(p.get("id"))
        key = (str(p.get("country", "")), str(p.get("shop", "")), str(p.get("handle", "")))
        item = inventory.get(key)
        if item is None:
            errors.append(f"{ctx}: {key} is not in the manifest inventory")
            continue
        status = p.get("status")
        if status != item.get("status", "pending"):
            errors.append(f"{ctx}: status {status!r} differs from the manifest ({item.get('status', 'pending')!r})")
        if status == "held":
            if not str(p.get("reason", "")).strip():
                errors.append(f"{ctx}: held item needs a reason")
            continue
        if status != "ready":
            errors.append(f"{ctx}: review items must be ready or held")
            continue
        if p.get("url") != item.get("final_url"):
            errors.append(f"{ctx}: url differs from the manifest final_url")
        check_destination(ctx, p.get("campaign"), key[1], p.get("adGroup"), p.get("url"), rules, errors)
        check_copy(ctx, [text_of(x) for x in p.get("headlines") or []], [text_of(x) for x in p.get("descriptions") or []],
                   text_of(p.get("path1", "")), text_of(p.get("path2", "")), p.get("keywords"), errors)
        check_assets(ctx, p, key[1], rules, errors, warnings)
        for field in ("geoId", "languageId"):
            if not re.fullmatch(r"\d+", str(p.get(field, ""))):
                errors.append(f"{ctx}: {field} must be a numeric Google constant ID")
        for field, cap in (("cpc", CPC_MICROS[1]), ("campaignBudget", BUDGET_MICROS[1])):
            value = p.get(field)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
                errors.append(f"{ctx}: {field} must be a positive number in account currency")
            elif value * 1_000_000 > cap:
                warnings.append(f"{ctx}: {field} exceeds the reviewed creation tool cap ({cap / 1_000_000:g}); "
                                "create within the cap and raise later only with explicit approval")
        camp = campaigns.setdefault(p.get("campaign"), {"shop": key[1], "country": key[0], "geo": str(p.get("geoId")),
                                                         "language": str(p.get("languageId")),
                                                         "budget": p.get("campaignBudget"), "cpc": set(), "groups": set()})
        for field, value in (("shop", key[1]), ("country", key[0]), ("geo", str(p.get("geoId"))),
                             ("language", str(p.get("languageId"))), ("budget", p.get("campaignBudget"))):
            if camp[field] != value:
                errors.append(f"{ctx}: campaign {p.get('campaign')!r} mixes {field} values; a reviewed campaign has one "
                              "shop, location, language and budget")
        if p.get("adGroup") in camp["groups"]:
            errors.append(f"{ctx}: duplicate ad group in campaign {p.get('campaign')!r}")
        camp["groups"].add(p.get("adGroup"))
        camp["cpc"].add(p.get("cpc"))
        ready[(p.get("campaign"), p.get("adGroup"))] = p
    for name, camp in campaigns.items():
        if len(camp["groups"]) > 9:
            errors.append(f"campaign {name!r}: more than 9 ad groups")
        if len(camp["cpc"]) > 1:
            warnings.append(f"campaign {name!r}: groups use different CPCs; the creation tool sets one starting CPC, "
                            "so apply the others afterwards with the bid tools")
        if (rules["campaign_name_pattern"] == DEFAULT_NAME_PATTERN and isinstance(name, str)
                and f" | {camp['country']} | " not in name):
            warnings.append(f"campaign {name!r}: country segment differs from the products' country")
    return ready


def check_requests(data, manifest, ready, rules, require_all, errors, warnings):
    """Validate delivery/requests.json (exact google_ads_create_reviewed_search_campaign requests)."""
    requests = data.get("requests") if isinstance(data, dict) else data
    if not isinstance(requests, list) or not requests:
        errors.append("delivery/requests.json: nonempty array of requests required")
        return
    covered, names = set(), set()
    for i, r in enumerate(requests):
        if not isinstance(r, dict):
            errors.append(f"request {i}: expected an object")
            continue
        ctx = f"request {i} ({r.get('campaign_name')})"
        extra, missing = set(r) - REQUEST_KEYS, REQUEST_KEYS - {"validate_only"} - set(r)
        if extra or missing:
            errors.append(f"{ctx}: unexpected keys {sorted(extra)}, missing keys {sorted(missing)}")
        if r.get("customer_id") != manifest["account"].get("customer_id"):
            errors.append(f"{ctx}: customer_id differs from the manifest")
        if r.get("expected_currency") != manifest["account"].get("currency"):
            errors.append(f"{ctx}: expected_currency differs from the manifest")
        if r.get("campaign_name") in names:
            errors.append(f"{ctx}: duplicate campaign name")
        names.add(r.get("campaign_name"))
        for field, (low, high) in (("daily_budget_micros", BUDGET_MICROS), ("cpc_bid_micros", CPC_MICROS)):
            value = r.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or not low <= value <= high:
                errors.append(f"{ctx}: {field} must be an integer from {low} to {high}")
        for field in ("geo_id", "language_id"):
            if not isinstance(r.get(field), str) or not re.fullmatch(r"\d+", r[field]):
                errors.append(f"{ctx}: {field} must be a string of digits")
        if r.get("validate_only") is False:
            warnings.append(f"{ctx}: stored with validate_only=false; keep stored requests validate-only and pass false "
                            "only in the approved apply call")
        groups = r.get("groups")
        if not isinstance(groups, list) or not 1 <= len(groups) <= 9:
            errors.append(f"{ctx}: 1-9 groups required")
            continue
        group_names = set()
        for g in groups:
            if not isinstance(g, dict):
                errors.append(f"{ctx}: each group must be an object")
                continue
            gctx = f"{ctx} group {g.get('name')!r}"
            extra, missing = set(g) - GROUP_KEYS, GROUP_KEYS - set(g)
            if extra or missing:
                errors.append(f"{gctx}: unexpected keys {sorted(extra)}, missing keys {sorted(missing)}")
            if g.get("name") in group_names:
                errors.append(f"{gctx}: duplicate group name")
            group_names.add(g.get("name"))
            keywords = g.get("keywords")
            if isinstance(keywords, list) and any(not isinstance(k, dict) or set(k) != {"text", "match"} for k in keywords):
                errors.append(f"{gctx}: keywords must be objects with exactly text and match")
            check_destination(gctx, r.get("campaign_name"), r.get("shop_domain"), g.get("name"), g.get("final_url"), rules, errors)
            check_copy(gctx, g.get("headlines"), g.get("descriptions"), g.get("path1"), g.get("path2"), keywords, errors)
            if ready is None:
                continue
            p = ready.get((r.get("campaign_name"), g.get("name")))
            if p is None:
                errors.append(f"{gctx}: no ready reviewed item with this campaign and ad group")
                continue
            covered.add((r.get("campaign_name"), g.get("name")))
            pairs = lambda items: sorted((k.get("text"), k.get("match")) for k in items or [] if isinstance(k, dict))
            reviewed = {"final_url": p.get("url"), "path1": text_of(p.get("path1", "")), "path2": text_of(p.get("path2", "")),
                        "headlines": [text_of(x) for x in p.get("headlines") or []],
                        "descriptions": [text_of(x) for x in p.get("descriptions") or []], "keywords": pairs(p.get("keywords"))}
            sent = {k: g.get(k) for k in reviewed if k != "keywords"}
            sent["keywords"] = pairs(keywords)
            diffs = [k for k in reviewed if reviewed[k] != sent[k]]
            if diffs:
                errors.append(f"{gctx}: differs from the reviewed copy in {diffs}; update the review and its approval, or the request")
            if (r.get("shop_domain") != p.get("shop") or r.get("geo_id") != str(p.get("geoId"))
                    or r.get("language_id") != str(p.get("languageId"))):
                errors.append(f"{gctx}: shop, geo or language differs from the review")
            if isinstance(p.get("campaignBudget"), (int, float)) and r.get("daily_budget_micros") != round(p["campaignBudget"] * 1_000_000):
                errors.append(f"{gctx}: daily budget differs from the reviewed campaign budget")
            if isinstance(p.get("cpc"), (int, float)) and r.get("cpc_bid_micros") != round(p["cpc"] * 1_000_000):
                warnings.append(f"{gctx}: starting CPC differs from this group's reviewed CPC; set the reviewed bid afterwards")
    if ready is not None and set(ready) - covered:
        (errors if require_all else warnings).append(
            f"Ready reviewed items not in delivery requests: {sorted(map(list, set(ready) - covered))}")


def audit(root, through=None, config_path=None):
    errors, warnings, pending = [], [], []
    config, source = load_config(config_path)
    rules = launch_rules(config)
    if source is None:
        warnings.append("No plugin config.json found; default search_launch rules were used")
    last = PHASES.index(through) if through else -1

    def reached(phase):
        return last >= PHASES.index(phase)

    def load(rel):
        return json.loads((root / rel).read_text(encoding="utf-8"))

    def evidence(refs, context):
        if not isinstance(refs, list) or not refs:
            errors.append(f"{context}: evidence paths missing")
            return
        for ref in refs:
            path = (root / ref).resolve() if isinstance(ref, str) else None
            if path is None or not path.is_relative_to(root.resolve()) or not path.is_file():
                errors.append(f"{context}: missing or outside-run evidence: {ref!r}")

    manifest, checkpoint = load("manifest.json"), load("checkpoint.json")
    if not isinstance(manifest, dict) or not isinstance(checkpoint, dict):
        raise ValueError("manifest.json and checkpoint.json must be JSON objects")
    if manifest.get("schema_version") != 1 or checkpoint.get("schema_version") != 1:
        errors.append("Unsupported manifest/checkpoint schema_version")
    account = manifest.setdefault("account", {})
    if not re.fullmatch(r"\d{10}", str(account.get("customer_id", ""))):
        errors.append("manifest: explicit 10-digit customer_id missing")
    if not re.fullmatch(r"[A-Z]{3}", str(account.get("currency", ""))):
        errors.append("manifest: explicit account currency missing")
    try:
        ZoneInfo(str(account.get("timezone", "")))
    except Exception:  # ZoneInfo raises several types for invalid keys
        errors.append("manifest: account timezone is not a valid IANA name")
    configured = str((config.get("account") or {}).get("customer_id", "")).replace("-", "")
    if configured and configured != account.get("customer_id"):
        warnings.append("manifest customer_id differs from config.json account.customer_id; confirm the intended account")
    domains = {v.lower() for k, v in (config.get("domains") or {}).items() if not k.startswith("_") and isinstance(v, str)}
    markets = {str(m).upper() for m in config.get("markets") or []}
    run_date = date.fromisoformat(str(manifest.get("run_date")))
    for key in ("16_calendar_months", "90_days", "30_days"):
        interval = (manifest.get("history_intervals") or {}).get(key) or {}
        try:
            start, end = date.fromisoformat(interval["from"]), date.fromisoformat(interval["to"])
        except (KeyError, TypeError, ValueError):
            errors.append(f"history interval {key}: missing or invalid dates")
            continue
        if start > end or end >= run_date:
            errors.append(f"history interval {key}: start after end, or the incomplete run day included")
    phases = checkpoint.get("phases") or {}
    for index, name in enumerate(PHASES):
        phase = phases.get(name) or {}
        status = phase.get("status", "pending")
        if status not in ("pending", "completed", "not_needed", "held"):
            errors.append(f"phase {name}: invalid status {status!r}")
        elif status == "completed":
            evidence(phase.get("evidence"), f"phase {name}")
        elif status == "not_needed":
            if not str(phase.get("reason", "")).strip():
                errors.append(f"phase {name}: not_needed requires a reason")
        else:
            pending.append({"phase": name, "status": status, "reason": phase.get("reason", "")})
            if status == "held" and not str(phase.get("reason", "")).strip():
                errors.append(f"phase {name}: held requires a concrete resume condition")
            if index <= last:
                errors.append(f"phase {name}: {status}; required for --through {through}")
    if set(phases) - set(PHASES):
        warnings.append(f"checkpoint has unknown phases: {sorted(set(phases) - set(PHASES))}")
    products = manifest.get("products", [])
    if not isinstance(products, list) or not all(isinstance(p, dict) for p in products):
        raise ValueError("manifest products must be an array of objects")
    inventory = {}
    for product in products:
        key = tuple(str(product.get(k, "")) for k in ("country", "shop_domain", "handle"))
        ctx = "product " + "/".join(key)
        if not all(key) or key in inventory:
            errors.append(f"{ctx}: missing or duplicate country/shop_domain/handle identity")
        inventory[key] = product
        status = product.get("status", "pending")
        if status not in ("pending", "ready", "held"):
            errors.append(f"{ctx}: invalid status {status!r}")
        if status == "held" and not str(product.get("reason", "")).strip():
            errors.append(f"{ctx}: held product needs a reason")
        if not re.fullmatch(r"[A-Z]{2}", key[0]) or not HOST.fullmatch(key[1]):
            errors.append(f"{ctx}: country must be a two-letter uppercase code and shop_domain a lowercase host")
        if blocked(rules, key[2]) and status != "held":
            errors.append(f"{ctx}: matches search_launch.blocked_product_prefixes and must stay held")
        if domains and key[1].lower() not in domains:
            warnings.append(f"{ctx}: shop domain is not listed in config.json domains")
        if markets and key[0] not in markets:
            warnings.append(f"{ctx}: country is not listed in config.json markets")
        if status == "ready":
            expected = f"https://{key[1]}{rules['product_path_prefix']}{key[2]}"
            if not product.get("product_id"):
                errors.append(f"{ctx}: ready product needs its exact existing product_id")
            if product.get("final_url") != expected:
                errors.append(f"{ctx}: ready product final_url must be exactly {expected}")
            evidence(product.get("evidence"), ctx)
    if not products:
        warnings.append("Product inventory is empty; complete it before campaign preparation")
        if reached("inventory"):
            errors.append("No products inventoried")
    ready = None
    if (root / "review" / "review-data.json").is_file():
        ready = check_review(load("review/review-data.json"), manifest, inventory, rules, errors, warnings)
    elif reached("creative"):
        errors.append("Missing review/review-data.json")
    if reached("creative"):
        if not (root / "campaign-decisions.json").is_file():
            errors.append("Missing campaign-decisions.json")
        if not any(p.get("status") == "ready" for p in products):
            errors.append("No ready products; held items cannot be delivered")
        if any(p.get("status", "pending") == "pending" for p in products):
            errors.append("Unreconciled pending products remain")
    if (root / "delivery" / "requests.json").is_file():
        check_requests(load("delivery/requests.json"), manifest, ready, rules, reached("delivery"), errors, warnings)
    elif reached("delivery"):
        errors.append("Missing delivery/requests.json")
    for key, record in (manifest.get("approvals") or {}).items():
        if isinstance(record, dict) and record.get("status") == "authorized":
            if not record.get("scope"):
                errors.append(f"approval {key}: needs its actual scope")
            evidence(record.get("evidence"), f"approval {key}")
    warnings.append("Offline check only: it cannot grant authorization, run validate_only, check billable units, "
                    "certify claims or prove Ad Strength or serving.")
    return {"offline_valid": not errors, "requested_checkpoint": through, "config_source": source, "rules": rules,
            "errors": errors, "pending": pending, "warnings": warnings}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--through", choices=PHASES, help="Fail when this phase or an earlier one is pending or held")
    parser.add_argument("--config", type=Path, help="Plugin config.json; default: the plugin loader's discovery order")
    args = parser.parse_args()
    try:
        result = audit(args.run_dir, args.through, args.config)
    except (ValueError, KeyError, TypeError, AttributeError, OSError, re.error) as exc:
        result = {"offline_valid": False, "errors": [f"Invalid input: {exc}"]}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    sys.exit(0 if result["offline_valid"] else 2)


if __name__ == "__main__":
    main()
