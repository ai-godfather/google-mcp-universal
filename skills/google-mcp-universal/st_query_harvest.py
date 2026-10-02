#!/usr/bin/env python3
"""Senior-PPC query harvest pipeline for Google Ads search-terms exports.

Reads BOTH CSVs (full + converting). Aggregates by (term, country) across the union,
applies harvest filters (conv >= MIN_CONV AND roi >= MIN_ROI AND clicks >= 5),
classifies source channel + brand-vs-generic, and emits action recommendations.

Usage:
  python3 st_query_harvest.py \\
      --full "/path/Search terms report (1).csv" \\
      --converting "/path/Search terms report.csv" \\
      --output-dir ./reports/harvest \\
      [--min-conv 2] [--min-roi 200]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


COUNTRY_RE = re.compile(r"^([A-Z]{2})\s*[\|\-]")
TOKEN_RE = re.compile(r"[\wąćęłńóśźżăâîșțáéíóúüöäőűçñßάέήίόύώ]+", re.UNICODE)


def parse_num(value: Any) -> float:
    s = str(value or "").strip().replace(",", "").replace("%", "").replace("£", "")
    if not s or s in ("--", "—"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def extract_country(camp: str) -> str:
    m = COUNTRY_RE.match(camp or "")
    return m.group(1) if m else "??"


def classify_channel(camp: str, match_type: str) -> str:
    h = ((camp or "") + " " + (match_type or "")).lower()
    if "performance max" in h or "pmax" in h:
        return "PMAX"
    if "shopping" in h:
        return "SHOPPING"
    if "search" in h:
        return "SEARCH"
    return "OTHER"


def is_brand_search_campaign(camp: str) -> bool:
    h = (camp or "").lower()
    return ("bw" in h or "brand" in h) and "search" in h


def load_csv(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    header_idx = None
    for i, l in enumerate(lines):
        if l.startswith("Search term,"):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(f"No 'Search term,' header in {path}")
    reader = csv.DictReader(lines[header_idx:])
    rows: List[Dict[str, Any]] = []
    for r in reader:
        term = (r.get("Search term") or "").strip()
        camp = (r.get("Campaign") or "").strip()
        if not term or not camp:
            continue
        if term.lower().startswith("total:"):
            continue
        cost = parse_num(r.get("Cost"))
        roi = parse_num(r.get("ROI"))
        conv = parse_num(r.get("Conversions"))
        clicks = parse_num(r.get("Clicks"))
        value = cost * roi / 100.0
        rows.append({
            "term": term,
            "term_lc": term.lower(),
            "camp": camp,
            "country": extract_country(camp),
            "channel": classify_channel(camp, r.get("Match type", "")),
            "is_brand_search": is_brand_search_campaign(camp),
            "cost": cost, "conv": conv, "clicks": clicks,
            "value": value, "roi": roi,
            "match_type": (r.get("Match type") or "").strip(),
        })
    return rows


def discover_brand_tokens(rows: List[Dict[str, Any]]) -> Set[str]:
    """Brand tokens = single-word terms that appear in Search BW campaigns with >=3 conv."""
    by_term: Dict[str, float] = defaultdict(float)
    for r in rows:
        if r["is_brand_search"]:
            by_term[r["term_lc"]] += r["conv"]
    brand_words: Set[str] = set()
    for term, conv in by_term.items():
        if conv >= 3:
            for tok in TOKEN_RE.findall(term):
                tok_l = tok.lower()
                if len(tok_l) >= 4 and not tok_l.isdigit():
                    brand_words.add(tok_l)
    # Your own product/brand names (config.json "search_terms.brand_tokens") are always BRAND.
    brand_words |= _configured_brand_tokens()
    return brand_words


def _configured_brand_tokens() -> Set[str]:
    """Optional brand list from config.json; the analyzer still works without it."""
    try:
        from accounts_config import load_config
        tokens = (load_config().get("search_terms", {}) or {}).get("brand_tokens", [])
    except Exception:
        tokens = []
    return {str(t).lower() for t in tokens if str(t).strip()}


def classify_brand_or_generic(term: str, brand_words: Set[str]) -> str:
    toks = [t.lower() for t in TOKEN_RE.findall(term)]
    if any(t in brand_words for t in toks):
        return "BRAND"
    if len(toks) == 1 and len(toks[0]) >= 5 and toks[0] not in {
        "amazon", "allegro", "ebay", "farmacia", "apteka", "apotheke",
        "cena", "pret", "precio", "preço", "price", "opinie", "recensioni",
    }:
        return "BRAND"  # likely a product name (single-token coined word)
    return "GENERIC"


def determine_harvest_action(
    source_channel: str, brand_or_generic: str, already_in_search_brand: bool
) -> str:
    if source_channel == "PMAX" and not already_in_search_brand:
        return "HARVEST_TO_SEARCH_EXACT"
    if source_channel == "SHOPPING" and not already_in_search_brand and brand_or_generic == "BRAND":
        return "HARVEST_TO_SEARCH_EXACT"
    if source_channel == "SHOPPING" and brand_or_generic == "GENERIC":
        return "KEEP_IN_SHOPPING_LISTING_GROUP_BID_UP"
    if source_channel == "SEARCH":
        return "LOCK_AS_EXACT_IN_SAME_ADGROUP"
    if source_channel == "PMAX" and brand_or_generic == "BRAND" and already_in_search_brand:
        return "KEEP_IN_PMAX_BRAND_EXCLUSION_NEEDED"
    return "MONITOR"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", required=True, type=Path)
    ap.add_argument("--converting", required=False, type=Path, default=None)
    ap.add_argument("--output-dir", required=False, type=Path,
                    default=Path("./reports/query_harvest"))
    ap.add_argument("--min-conv", type=float, default=2.0)
    ap.add_argument("--min-roi", type=float, default=200.0)
    args = ap.parse_args()

    out_dir: Path = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    full_rows = load_csv(args.full)
    conv_rows = load_csv(args.converting) if args.converting and args.converting.exists() else []
    all_rows = full_rows + conv_rows

    brand_words = discover_brand_tokens(all_rows)

    # Aggregate by (term_lc, country)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in all_rows:
        key = (r["term_lc"], r["country"])
        if key not in agg:
            agg[key] = {
                "term": r["term"], "country": r["country"],
                "cost": 0.0, "conv": 0.0, "value": 0.0, "clicks": 0.0,
                "channels": defaultdict(lambda: {"cost": 0.0, "conv": 0.0, "value": 0.0, "clicks": 0.0}),
                "already_in_search_brand": False,
            }
        b = agg[key]
        b["cost"] += r["cost"]; b["conv"] += r["conv"]; b["value"] += r["value"]; b["clicks"] += r["clicks"]
        ch = b["channels"][r["channel"]]
        ch["cost"] += r["cost"]; ch["conv"] += r["conv"]; ch["value"] += r["value"]; ch["clicks"] += r["clicks"]
        if r["is_brand_search"]:
            b["already_in_search_brand"] = True

    candidates: List[Dict[str, Any]] = []
    for (tlc, country), b in agg.items():
        roi = b["value"] / b["cost"] * 100 if b["cost"] > 0 else 0
        if b["conv"] < args.min_conv:
            continue
        if roi < args.min_roi:
            continue
        if b["clicks"] < 5:
            continue
        # Primary source channel = where most conv came from
        src_ch = max(b["channels"].items(), key=lambda x: (x[1]["conv"], x[1]["value"]))[0]
        cpa = b["cost"] / b["conv"]
        bog = classify_brand_or_generic(b["term"], brand_words)
        action = determine_harvest_action(src_ch, bog, b["already_in_search_brand"])
        candidates.append({
            "search_term": b["term"],
            "country": country,
            "source_channel": src_ch,
            "brand_or_generic": bog,
            "clicks": int(b["clicks"]),
            "cost": round(b["cost"], 2),
            "conv": round(b["conv"], 2),
            "roi_pct": round(roi, 2),
            "cpa": round(cpa, 2),
            "harvest_action": action,
            "suggested_match": "EXACT",
            "suggested_max_cpc": round(cpa * 0.6, 2),
            "already_in_search_brand": "Y" if b["already_in_search_brand"] else "N",
        })

    candidates.sort(key=lambda x: (-x["conv"], -x["roi_pct"]))

    # Write harvest CSV
    cols = [
        "search_term", "country", "source_channel", "brand_or_generic",
        "clicks", "cost", "conv", "roi_pct", "cpa",
        "harvest_action", "suggested_match", "suggested_max_cpc",
        "already_in_search_brand",
    ]
    with (out_dir / "harvest_pmax_to_search.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for c in candidates:
            w.writerow(c)

    # Brand-only split
    brand_only = [c for c in candidates if c["brand_or_generic"] == "BRAND"]
    with (out_dir / "harvest_search_brand_split.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for c in brand_only:
            w.writerow(c)

    # Summary
    by_source: Dict[str, int] = defaultdict(int)
    for c in candidates:
        by_source[c["source_channel"]] += 1
    by_country: Dict[str, int] = defaultdict(int)
    for c in candidates:
        by_country[c["country"]] += 1
    top_countries = sorted(by_country.items(), key=lambda x: -x[1])[:10]

    summary = {
        "total_candidates": len(candidates),
        "brand_candidates": len(brand_only),
        "generic_candidates": len(candidates) - len(brand_only),
        "total_profit_potential_gbp": round(sum((c["roi_pct"] / 100.0 - 1.0) * c["cost"] for c in candidates), 2),
        "avg_roi_pct": round(sum(c["roi_pct"] for c in candidates) / len(candidates), 2) if candidates else 0,
        "by_source_channel": dict(by_source),
        "top_10_countries_by_count": [{"country": c, "count": n} for c, n in top_countries],
        "min_conv": args.min_conv,
        "min_roi": args.min_roi,
        "brand_tokens_discovered": len(brand_words),
    }
    (out_dir / "harvest_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(json.dumps({
        "status": "success",
        "output_dir": str(out_dir),
        "files_written": [
            "harvest_pmax_to_search.csv",
            "harvest_search_brand_split.csv",
            "harvest_summary.json",
        ],
        "candidates": len(candidates),
        "brand_candidates": len(brand_only),
        "profit_potential_gbp": summary["total_profit_potential_gbp"],
    }))


if __name__ == "__main__":
    main()
