#!/usr/bin/env python3
"""Senior-PPC country priority matrix for Google Ads search-terms exports.

Loads BOTH search-terms CSVs (full + converting). Computes a priority decision per country:
SCALE_AGGRESSIVE / SCALE_CAUTIOUS / OPTIMIZE_NEGATIVES / INVESTIGATE / MONITOR / RESTRUCTURE / HOLD.

Usage:
  python3 st_country_priority.py \\
      --full "/path/Search terms report (1).csv" \\
      --converting "/path/Search terms report.csv" \\
      --output-dir ./reports/country
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


COUNTRY_RE = re.compile(r"^([A-Z]{2})\s*[\|\-]")


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


def load_csv(path: Path) -> Tuple[List[Dict[str, Any]], str]:
    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    date_range = lines[1].strip().strip('"') if len(lines) > 1 else ""
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
        impr = parse_num(r.get("Impr."))
        value = cost * roi / 100.0
        rows.append({
            "term": term, "camp": camp,
            "country": extract_country(camp),
            "channel": classify_channel(camp, r.get("Match type", "")),
            "cost": cost, "conv": conv, "clicks": clicks,
            "impr": impr, "value": value, "roi": roi,
            "source": path.name,
        })
    return rows, date_range


def determine_action(d: Dict[str, float]) -> str:
    cost = d["cost"]; conv = d["conv"]; roi = d["roi_pct"]
    waste_ratio = d["zero_conv_waste"] / cost if cost > 0 else 0
    profit = d["profit"]
    if cost > 1000 and profit < -200:
        return "RESTRUCTURE"
    if cost > 500 and roi < 50:
        return "INVESTIGATE"
    if roi > 200 and conv > 50:
        return "SCALE_AGGRESSIVE"
    if roi > 150 and conv > 20:
        return "SCALE_AGGRESSIVE"
    if roi > 100 and conv > 10:
        return "SCALE_CAUTIOUS"
    if waste_ratio > 0.5 and cost > 100:
        return "OPTIMIZE_NEGATIVES"
    if cost < 50:
        return "MONITOR"
    return "HOLD"


def aggregate_by_country(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = defaultdict(lambda: {
        "rows": 0, "clicks": 0.0, "cost": 0.0, "conv": 0.0,
        "value": 0.0, "impr": 0.0, "zero_conv_waste": 0.0,
    })
    for r in rows:
        c = r["country"]
        out[c]["rows"] += 1
        out[c]["clicks"] += r["clicks"]
        out[c]["cost"] += r["cost"]
        out[c]["conv"] += r["conv"]
        out[c]["value"] += r["value"]
        out[c]["impr"] += r["impr"]
        if r["conv"] == 0:
            out[c]["zero_conv_waste"] += r["cost"]
    for d in out.values():
        d["cpa"] = d["cost"] / d["conv"] if d["conv"] > 0 else 0.0
        d["roi_pct"] = d["value"] / d["cost"] * 100.0 if d["cost"] > 0 else 0.0
        d["conv_rate"] = d["conv"] / d["clicks"] if d["clicks"] > 0 else 0.0
        d["profit"] = d["value"] - d["cost"]
        d["waste_pct"] = d["zero_conv_waste"] / d["cost"] * 100.0 if d["cost"] > 0 else 0.0
        d["scale_score"] = d["clicks"] * max(0.0, d["roi_pct"] - 100.0) / 100.0
        d["cut_score"] = d["zero_conv_waste"] * (1.0 - min(1.0, d["roi_pct"] / 100.0))
        d["action"] = determine_action(d)
    return out


def write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", required=True, type=Path)
    ap.add_argument("--converting", required=False, type=Path, default=None)
    ap.add_argument("--output-dir", required=False, type=Path,
                    default=Path("./reports/country_priority"))
    args = ap.parse_args()

    out_dir: Path = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    full_rows, _ = load_csv(args.full)
    conv_rows: List[Dict[str, Any]] = []
    if args.converting and args.converting.exists():
        conv_rows, _ = load_csv(args.converting)

    # Union both CSVs: full has non-converting / waste-heavy rows, converting has
    # high-value rows from "Converting" asset groups and top-Shopping. They are
    # largely disjoint in practice (different campaign / asset-group slices).
    all_rows = full_rows + conv_rows

    by_country = aggregate_by_country(all_rows)

    acct = {
        "rows": len(all_rows),
        "cost": sum(r["cost"] for r in all_rows),
        "conv": sum(r["conv"] for r in all_rows),
        "value": sum(r["value"] for r in all_rows),
        "clicks": sum(r["clicks"] for r in all_rows),
    }
    acct["cpa"] = acct["cost"] / acct["conv"] if acct["conv"] > 0 else 0
    acct["roi_pct"] = acct["value"] / acct["cost"] * 100 if acct["cost"] > 0 else 0
    acct["profit"] = acct["value"] - acct["cost"]

    rows_for_csv = []
    for c, d in sorted(by_country.items(), key=lambda x: -x[1]["scale_score"]):
        rows_for_csv.append({
            "country": c,
            "rows": int(d["rows"]),
            "clicks": int(d["clicks"]),
            "cost": round(d["cost"], 2),
            "conv": round(d["conv"], 2),
            "value": round(d["value"], 2),
            "roi_pct": round(d["roi_pct"], 2),
            "cpa": round(d["cpa"], 2),
            "conv_rate": round(d["conv_rate"] * 100, 2),
            "profit": round(d["profit"], 2),
            "zero_conv_waste": round(d["zero_conv_waste"], 2),
            "waste_pct": round(d["waste_pct"], 2),
            "scale_score": round(d["scale_score"], 2),
            "cut_score": round(d["cut_score"], 2),
            "action": d["action"],
        })
    write_csv_rows(out_dir / "country_priority.csv", rows_for_csv, [
        "country", "rows", "clicks", "cost", "conv", "value", "roi_pct", "cpa",
        "conv_rate", "profit", "zero_conv_waste", "waste_pct",
        "scale_score", "cut_score", "action",
    ])

    md = [
        "# Country Priority Matrix",
        f"\n**Account totals (union of both CSVs):** cost £{acct['cost']:.2f}, conv {acct['conv']:.1f}, ROI {acct['roi_pct']:.2f}%, profit £{acct['profit']:.2f}",
        "\nThe matrix combines both Search Terms exports — *full* (non-converting + waste-heavy) and *converting* (high-value asset groups). Decisions favour countries with both volume AND positive margin.",
        "\n## Top SCALE candidates (sorted by scale_score)\n",
        "| Country | Action | Cost £ | Conv | ROI % | CPA £ | Profit £ | Scale score |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    top_scale = [r for r in rows_for_csv if r["action"] in ("SCALE_AGGRESSIVE", "SCALE_CAUTIOUS")][:25]
    for r in top_scale:
        md.append(f"| {r['country']} | {r['action']} | {r['cost']:.2f} | {r['conv']:.1f} | {r['roi_pct']:.1f} | {r['cpa']:.2f} | {r['profit']:.2f} | {r['scale_score']:.1f} |")

    md.append("\n## Top CUT / RESTRUCTURE candidates (sorted by cut_score)\n")
    md.append("| Country | Action | Cost £ | Conv | ROI % | Waste % | Profit £ | Cut score |")
    md.append("|---|---|---:|---:|---:|---:|---:|---:|")
    top_cut = sorted(
        [r for r in rows_for_csv if r["action"] in ("RESTRUCTURE", "INVESTIGATE", "OPTIMIZE_NEGATIVES")],
        key=lambda x: -x["cut_score"],
    )[:15]
    for r in top_cut:
        md.append(f"| {r['country']} | {r['action']} | {r['cost']:.2f} | {r['conv']:.1f} | {r['roi_pct']:.1f} | {r['waste_pct']:.1f} | {r['profit']:.2f} | {r['cut_score']:.2f} |")

    md.append("\n## All countries (top 50 by cost)\n")
    md.append("| Country | Action | Cost £ | Conv | ROI % | CPA £ | Waste £ |")
    md.append("|---|---|---:|---:|---:|---:|---:|")
    for r in sorted(rows_for_csv, key=lambda x: -x["cost"])[:50]:
        md.append(f"| {r['country']} | {r['action']} | {r['cost']:.2f} | {r['conv']:.1f} | {r['roi_pct']:.1f} | {r['cpa']:.2f} | {r['zero_conv_waste']:.2f} |")
    (out_dir / "country_priority.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    summary = {
        "account": {k: round(v, 2) if isinstance(v, float) else v for k, v in acct.items()},
        "countries_count": len(by_country),
        "by_country": {c: {k: round(v, 4) if isinstance(v, float) else v for k, v in d.items()}
                       for c, d in by_country.items()},
    }
    (out_dir / "country_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps({
        "status": "success",
        "output_dir": str(out_dir),
        "files_written": ["country_priority.csv", "country_priority.md", "country_summary.json"],
        "countries_count": len(by_country),
        "scale_candidates": len([r for r in rows_for_csv if r["action"] in ("SCALE_AGGRESSIVE", "SCALE_CAUTIOUS")]),
        "cut_candidates": len([r for r in rows_for_csv if r["action"] in ("RESTRUCTURE", "INVESTIGATE")]),
    }))


if __name__ == "__main__":
    main()
