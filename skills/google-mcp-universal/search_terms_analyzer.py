#!/usr/bin/env python3
"""
Search Terms Intelligence — offline analysis for Google Ads search-terms exports.

Parses Google Ads UI CSV exports (2-line header), classifies intent, finds waste,
scale winners, and exports actionable negative-keyword / exclusion lists.

Usage:
  python3 search_terms_analyzer.py \\
    --full "/path/Search terms report (1).csv" \\
    --converting "/path/Search terms report.csv" \\
    --output-dir ./reports/search-terms-2026-05-28

No API credentials required — works on downloaded reports only.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

COLUMNS = [
    "search_term",
    "match_type",
    "added_excluded",
    "campaign",
    "ad_group",
    "clicks",
    "impressions",
    "ctr",
    "currency",
    "avg_cpc",
    "cost",
    "roi",
    "conv_rate",
    "conversions",
    "cost_per_conv",
]

COLUMN_MAP = {
    "Search term": "search_term",
    "Match type": "match_type",
    "Added/Excluded": "added_excluded",
    "Campaign": "campaign",
    "Ad group": "ad_group",
    "Clicks": "clicks",
    "Impr.": "impressions",
    "CTR": "ctr",
    "Currency code": "currency",
    "Avg. CPC": "avg_cpc",
    "Cost": "cost",
    "ROI": "roi",
    "Conv. rate": "conv_rate",
    "Conversions": "conversions",
    "Cost / conv.": "cost_per_conv",
}


def parse_num(value: Any) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s or s in ("--", "—"):
        return 0.0
    s = s.replace(",", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def load_google_ads_search_terms_csv(path: Path) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Return normalized rows + optional date range from line 2."""
    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    date_range: Optional[str] = None
    if len(lines) >= 2 and lines[1].startswith('"'):
        date_range = lines[1].strip('"')

    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Search term,"):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(f"No 'Search term,' header row in {path}")

    raw_rows = list(csv.DictReader(lines[header_idx:]))
    rows: List[Dict[str, Any]] = []
    for raw in raw_rows:
        row: Dict[str, Any] = {"source_file": path.name}
        for src, dst in COLUMN_MAP.items():
            row[dst] = raw.get(src, "")
        row["clicks_n"] = parse_num(row["clicks"])
        row["impressions_n"] = parse_num(row["impressions"])
        row["cost_n"] = parse_num(row["cost"])
        row["conversions_n"] = parse_num(row["conversions"])
        row["roi_n"] = parse_num(row["roi"])
        row["cpa_n"] = (
            row["cost_n"] / row["conversions_n"]
            if row["conversions_n"] > 0
            else 0.0
        )
        row["channel"] = (
            "pmax"
            if "performance max" in str(row["match_type"]).lower()
            else "search_shopping"
        )
        rows.append(row)
    return rows, date_range


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

INTENT_RULES: List[Tuple[str, str]] = [
    ("marketplace", r"\b(amazon|allegro|ebay|emag|mercadona|carrefour|tei\.ro)\b"),
    ("forum_reviews", r"\b(forum|opinie|opinii|recensioni|avis|review|reviews|bugiardino|truffa|scam|απατη|escam|fake)\b"),
    ("competitor_brand", r"\b(cerave|nivea|oriflame|herbalife|fuxion)\b"),
    ("informational", r"\b(co to jest|que es|what is|para que sirve|ce este|wiki|wikipedia|sklad|ingredient)\b"),
    ("price_shopping", r"\b(cena|precio|prezzo|pret|price|costo|quanto costa|farmacia|apteka|apotheke)\b"),
    ("generic_category", r"\b(krem|tablet|capsule|medicament|lek|dieta|detox|weight loss|odchudzanie)\b"),
    ("wrong_product", r"\b(chainsaw|piła|parkside|drill|phone|iphone)\b"),
    ("brand_product", r""),  # fallback — product name queries
]


def classify_intent(search_term: str) -> str:
    st = (search_term or "").lower().strip()
    for intent, pattern in INTENT_RULES:
        if not pattern:
            continue
        if re.search(pattern, st, re.IGNORECASE):
            return intent
    return "brand_product"


def extract_country(campaign: str) -> str:
    c = campaign or ""
    m = re.match(r"^([A-Z]{2})\s*\|", c)
    if m:
        return m.group(1)
    m = re.match(r"^([A-Z]{2})\s*-\s*", c)
    if m:
        return m.group(1)
    return "??"


def campaign_family(campaign: str) -> str:
    c = campaign or ""
    if "Pmax" in c or "Performance Max" in c:
        return "pmax"
    if "Shopping" in c:
        return "shopping"
    if "Search" in c:
        return "search"
    return "other"


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------

@dataclass
class BucketStats:
    cost: float = 0.0
    conversions: float = 0.0
    clicks: float = 0.0
    impressions: float = 0.0
    terms: int = 0

    def add(self, row: Dict[str, Any]) -> None:
        self.cost += row["cost_n"]
        self.conversions += row["conversions_n"]
        self.clicks += row["clicks_n"]
        self.impressions += row["impressions_n"]
        self.terms += 1

    @property
    def cpa(self) -> float:
        return self.cost / self.conversions if self.conversions else 0.0

    @property
    def waste(self) -> float:
        return self.cost if self.conversions == 0 else 0.0


def aggregate(rows: Iterable[Dict[str, Any]], key_fn) -> Dict[str, BucketStats]:
    out: Dict[str, BucketStats] = defaultdict(BucketStats)
    for row in rows:
        out[key_fn(row)].add(row)
    return dict(out)


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

WASTE_MIN_COST = 2.0
SCALE_MIN_CONV = 2.0
SCALE_MAX_CPA = 3.0
NEGATIVE_MIN_COST = 1.0


def find_waste_terms(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    waste = [
        r
        for r in rows
        if not _is_aggregate_row(r)
        and r["cost_n"] >= WASTE_MIN_COST
        and r["conversions_n"] == 0
    ]
    waste.sort(key=lambda r: -r["cost_n"])
    return waste


def _is_aggregate_row(row: Dict[str, Any]) -> bool:
    st = (row.get("search_term") or "").strip().lower()
    return st.startswith("total:") or st == "total"


def find_scale_winners(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    winners = [
        r
        for r in rows
        if not _is_aggregate_row(r)
        and r["conversions_n"] >= SCALE_MIN_CONV
        and r["cpa_n"] > 0
        and r["cpa_n"] <= SCALE_MAX_CPA
        and r["cost_n"] >= 5
    ]
    winners.sort(key=lambda r: (-r["conversions_n"], r["cpa_n"]))
    return winners


def ngram_phrase_negatives(
    rows: List[Dict[str, Any]], min_cost: float = 10.0, min_terms: int = 3
) -> List[Dict[str, Any]]:
    """Find repeated words in zero-conv spend (phrase negative candidates)."""
    word_stats: Dict[str, BucketStats] = defaultdict(BucketStats)
    for row in rows:
        if row["conversions_n"] > 0 or row["cost_n"] < 1:
            continue
        words = re.findall(r"[a-zà-ž0-9]+", (row["search_term"] or "").lower())
        for w in words:
            if len(w) < 4:
                continue
            word_stats[w].add(row)

    candidates = []
    stop = {
        "pentru",
        "para",
        "avec",
        "with",
        "from",
        "your",
        "this",
        "that",
        "este",
        "sunt",
        "the",
        "and",
        "dla",
        "jak",
        "czy",
    }
    for word, stats in word_stats.items():
        if word in stop:
            continue
        if stats.cost < min_cost or stats.terms < min_terms:
            continue
        candidates.append(
            {
                "phrase": word,
                "wasted_cost": round(stats.cost, 2),
                "zero_conv_terms": stats.terms,
                "suggested_match": "phrase",
            }
        )
    candidates.sort(key=lambda x: -x["wasted_cost"])
    return candidates


def build_negative_export(waste_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Campaign-level negatives for Search (exact/phrase). Skip PMax — use account exclusions."""
    out: List[Dict[str, str]] = []
    seen: set = set()
    for row in waste_rows:
        if row["channel"] == "pmax":
            continue
        if row["cost_n"] < NEGATIVE_MIN_COST:
            continue
        key = (row["campaign"], row["search_term"].lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "campaign": row["campaign"],
                "country": extract_country(row["campaign"]),
                "search_term": row["search_term"],
                "match_type_suggest": "exact",
                "cost_gbp": f"{row['cost_n']:.2f}",
                "clicks": str(int(row["clicks_n"])),
                "intent": classify_intent(row["search_term"]),
            }
        )
    return out


def build_pmax_exclusion_candidates(
    waste_rows: List[Dict[str, Any]], min_cost: float = 5.0
) -> List[Dict[str, str]]:
    out = []
    seen: set = set()
    for row in waste_rows:
        if row["channel"] != "pmax":
            continue
        if row["cost_n"] < min_cost:
            continue
        st = row["search_term"].lower().strip()
        if st in seen:
            continue
        seen.add(st)
        out.append(
            {
                "search_term": row["search_term"],
                "campaign": row["campaign"],
                "country": extract_country(row["campaign"]),
                "cost_gbp": f"{row['cost_n']:.2f}",
                "intent": classify_intent(row["search_term"]),
                "action": "pmax_search_theme_exclusion_or_negative",
            }
        )
    out.sort(key=lambda x: -float(x["cost_gbp"]))
    return out


def markdown_report(
    full_rows: List[Dict[str, Any]],
    converting_rows: Optional[List[Dict[str, Any]]],
    date_range: Optional[str],
    output_dir: Path,
) -> str:
    total = BucketStats()
    for r in full_rows:
        total.add(r)

    waste_rows = find_waste_terms(full_rows)
    waste_cost = sum(r["cost_n"] for r in waste_rows)
    winners = find_scale_winners(
        converting_rows if converting_rows else full_rows
    )

    by_country = aggregate(full_rows, lambda r: extract_country(r["campaign"]))
    by_intent = aggregate(
        [r for r in full_rows if r["cost_n"] > 0],
        lambda r: classify_intent(r["search_term"]),
    )
    by_channel = aggregate(
        [r for r in full_rows if r["cost_n"] > 0],
        lambda r: r["channel"],
    )
    by_family = aggregate(
        [r for r in full_rows if r["cost_n"] > 0],
        lambda r: campaign_family(r["campaign"]),
    )

    camp_waste = aggregate(
        [r for r in full_rows if r["conversions_n"] == 0 and r["cost_n"] > 0],
        lambda r: r["campaign"],
    )
    top_camp_waste = sorted(
        camp_waste.items(), key=lambda x: -x[1].cost
    )[:20]

    lines = [
        "# Search Terms Intelligence Report — Google Ads",
        "",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Period:** {date_range or 'unknown'}",
        "",
        "## Executive summary",
        "",
        f"| Metric | Value |",
        f"|--------|------:|",
        f"| Search terms (rows) | {len(full_rows):,} |",
        f"| Total spend (GBP) | {total.cost:,.2f} |",
        f"| Total conversions | {total.conversions:,.2f} |",
        f"| Blended CPA | {total.cpa:.2f} |",
        f"| Zero-conv waste (≥{WASTE_MIN_COST} GBP/term) | {waste_cost:,.2f} ({100*waste_cost/total.cost if total.cost else 0:.1f}%) |",
        f"| Terms flagged for negatives | {len(waste_rows):,} |",
        f"| Scale candidates (CPA≤{SCALE_MAX_CPA}, ≥{SCALE_MIN_CONV} conv) | {len(winners):,} |",
        "",
        "## Strategic priorities (profit maximization)",
        "",
        "### 1. Stop bleed — negatives & exclusions (week 1)",
        "",
        "- **PMax** concentrates **irrelevant-query waste** — export `pmax_exclusions.csv` and add via "
        "campaign-level search term exclusions / account negative lists (see SKILL: `google_ads_add_negative_keyword`).",
        "- **Shopping/Search**: import `negatives_search_shopping.csv` as **exact** negatives per campaign "
        "(product-specific queries that spent with 0 conv).",
        "- Auto-block patterns: `marketplace`, `forum_reviews`, `wrong_product` — see `negatives_by_intent.csv`.",
        "",
        "### 2. Scale winners (week 1–2)",
        "",
        "- File `scale_winners.csv`: high volume + CPA ≤ target — increase budgets on parent campaigns, "
        "raise listing group bids (Shopping), or isolate in **Converting** PMax asset groups.",
        "- Cross-check with `ecom_labelizer_run` — promote HERO products matching winning queries.",
        "",
        "### 3. Query sculpting (week 2–4)",
        "",
        "- Use `phrase_negatives_candidates.csv` for **phrase** negatives on Search BW campaigns only "
        "(avoid on Shopping product campaigns).",
        "- Split **Converting vs Non-converting** PMax campaigns (already in account) — route feed labels via Labelizer.",
        "",
        "### 4. Monitoring cadence",
        "",
        "- Weekly: export search terms (last 7d) → run this script → diff vs previous `reports/` folder.",
        "- Monthly: full 90d export + merge with MCP `google_ads_list_search_terms` for live negatives sync.",
        "",
        "## Waste by channel",
        "",
        "| Channel | Spend | Conv | CPA |",
        "|---------|------:|-----:|----:|",
    ]
    for ch, stats in sorted(by_channel.items(), key=lambda x: -x[1].cost):
        lines.append(
            f"| {ch} | {stats.cost:,.2f} | {stats.conversions:,.1f} | {stats.cpa:.2f} |"
        )

    lines.extend(
        [
            "",
            "## Waste by campaign type",
            "",
            "| Type | Spend | Conv | CPA |",
            "|------|------:|-----:|----:|",
        ]
    )
    for fam, stats in sorted(by_family.items(), key=lambda x: -x[1].cost):
        lines.append(
            f"| {fam} | {stats.cost:,.2f} | {stats.conversions:,.1f} | {stats.cpa:.2f} |"
        )

    lines.extend(
        [
            "",
            "## Intent breakdown (spend > 0)",
            "",
            "| Intent | Spend | Conv | CPA | Terms |",
            "|--------|------:|-----:|----:|------:|",
        ]
    )
    for intent, stats in sorted(by_intent.items(), key=lambda x: -x[1].cost):
        lines.append(
            f"| {intent} | {stats.cost:,.2f} | {stats.conversions:,.1f} | {stats.cpa:.2f} | {stats.terms:,} |"
        )

    lines.extend(
        [
            "",
            "## Top countries by spend",
            "",
            "| CC | Spend | Conv | CPA |",
            "|----|------:|-----:|----:|",
        ]
    )
    for cc, stats in sorted(by_country.items(), key=lambda x: -x[1].cost)[:15]:
        lines.append(
            f"| {cc} | {stats.cost:,.2f} | {stats.conversions:,.1f} | {stats.cpa:.2f} |"
        )

    lines.extend(
        [
            "",
            f"## Top campaigns by zero-conversion spend",
            "",
            "| Campaign | Wasted GBP |",
            "|----------|-----------:|",
        ]
    )
    for camp, stats in top_camp_waste:
        lines.append(f"| {camp[:80]} | {stats.cost:,.2f} |")

    lines.extend(
        [
            "",
            "## Top 25 waste terms",
            "",
            "| Cost | Term | Campaign | Intent |",
            "|-----:|------|----------|--------|",
        ]
    )
    for r in waste_rows[:25]:
        lines.append(
            f"| {r['cost_n']:.2f} | {r['search_term'][:45]} | {r['campaign'][:35]} | {classify_intent(r['search_term'])} |"
        )

    lines.extend(
        [
            "",
            "## Top 15 scale candidates",
            "",
            "| Conv | CPA | Cost | Term | Campaign |",
            "|-----:|----:|-----:|------|----------|",
        ]
    )
    for r in winners[:15]:
        lines.append(
            f"| {r['conversions_n']:.1f} | {r['cpa_n']:.2f} | {r['cost_n']:.2f} | {r['search_term'][:40]} | {r['campaign'][:35]} |"
        )

    lines.extend(
        [
            "",
            "## Output files",
            "",
            "- `summary.json` — machine-readable KPIs",
            "- `negatives_search_shopping.csv` — exact negatives for Search/Shopping",
            "- `pmax_exclusions.csv` — PMax exclusion candidates",
            "- `negatives_by_intent.csv` — grouped by intent class",
            "- `phrase_negatives_candidates.csv` — n-gram phrase negatives",
            "- `scale_winners.csv` — bid/budget scale list",
            "- `waste_terms_all.csv` — full waste export",
            "",
        ]
    )

    return "\n".join(lines)


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def run_analysis(
    full_path: Path,
    converting_path: Optional[Path],
    output_dir: Path,
) -> Dict[str, Any]:
    full_rows, date_range = load_google_ads_search_terms_csv(full_path)
    converting_rows: Optional[List[Dict[str, Any]]] = None
    if converting_path and converting_path.exists():
        converting_rows, _ = load_google_ads_search_terms_csv(converting_path)

    output_dir.mkdir(parents=True, exist_ok=True)

    waste_rows = find_waste_terms(full_rows)
    winners = find_scale_winners(
        converting_rows if converting_rows else full_rows
    )
    negatives = build_negative_export(waste_rows)
    pmax_excl = build_pmax_exclusion_candidates(waste_rows)
    phrases = ngram_phrase_negatives(full_rows)

    # Intent-grouped negatives
    by_intent: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for n in negatives:
        by_intent[n["intent"]].append(n)

    intent_rows = []
    for intent, items in sorted(by_intent.items(), key=lambda x: -sum(float(i["cost_gbp"]) for i in x[1])):
        for item in items[:200]:
            item = dict(item)
            item["intent_group"] = intent
            intent_rows.append(item)

    total_cost = sum(r["cost_n"] for r in full_rows)
    total_conv = sum(r["conversions_n"] for r in full_rows)
    waste_cost = sum(r["cost_n"] for r in waste_rows)
    all_zero_conv_cost = sum(
        r["cost_n"] for r in full_rows if r["conversions_n"] == 0 and not _is_aggregate_row(r)
    )

    summary = {
        "date_range": date_range,
        "rows": len(full_rows),
        "total_cost_gbp": round(total_cost, 2),
        "total_conversions": round(total_conv, 2),
        "blended_cpa": round(total_cost / total_conv, 2) if total_conv else None,
        "waste_cost_gbp": round(waste_cost, 2),
        "waste_pct": round(100 * waste_cost / total_cost, 2) if total_cost else 0,
        "all_zero_conv_cost_gbp": round(all_zero_conv_cost, 2),
        "all_zero_conv_pct": round(100 * all_zero_conv_cost / total_cost, 2)
        if total_cost
        else 0,
        "negatives_search_count": len(negatives),
        "pmax_exclusion_count": len(pmax_excl),
        "scale_winners_count": len(winners),
        "generated_at": datetime.now().isoformat(),
    }

    write_csv(
        output_dir / "waste_terms_all.csv",
        [
            {
                "search_term": r["search_term"],
                "campaign": r["campaign"],
                "ad_group": r["ad_group"],
                "cost_gbp": f"{r['cost_n']:.2f}",
                "clicks": int(r["clicks_n"]),
                "intent": classify_intent(r["search_term"]),
                "channel": r["channel"],
            }
            for r in waste_rows
        ],
        [
            "search_term",
            "campaign",
            "ad_group",
            "cost_gbp",
            "clicks",
            "intent",
            "channel",
        ],
    )

    write_csv(
        output_dir / "negatives_search_shopping.csv",
        negatives,
        [
            "country",
            "campaign",
            "search_term",
            "match_type_suggest",
            "cost_gbp",
            "clicks",
            "intent",
        ],
    )

    write_csv(
        output_dir / "pmax_exclusions.csv",
        pmax_excl,
        ["country", "campaign", "search_term", "cost_gbp", "intent", "action"],
    )

    write_csv(
        output_dir / "negatives_by_intent.csv",
        intent_rows,
        [
            "intent_group",
            "country",
            "campaign",
            "search_term",
            "match_type_suggest",
            "cost_gbp",
            "intent",
        ],
    )

    write_csv(
        output_dir / "phrase_negatives_candidates.csv",
        phrases,
        ["phrase", "wasted_cost", "zero_conv_terms", "suggested_match"],
    )

    write_csv(
        output_dir / "scale_winners.csv",
        [
            {
                "search_term": r["search_term"],
                "campaign": r["campaign"],
                "country": extract_country(r["campaign"]),
                "conversions": f"{r['conversions_n']:.2f}",
                "cost_gbp": f"{r['cost_n']:.2f}",
                "cpa_gbp": f"{r['cpa_n']:.2f}",
                "clicks": int(r["clicks_n"]),
                "roi_pct": f"{r['roi_n']:.0f}",
            }
            for r in winners
        ],
        [
            "country",
            "search_term",
            "campaign",
            "conversions",
            "cost_gbp",
            "cpa_gbp",
            "clicks",
            "roi_pct",
        ],
    )

    report_md = markdown_report(full_rows, converting_rows, date_range, output_dir)
    (output_dir / "REPORT.md").write_text(report_md, encoding="utf-8")
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze Google Ads search terms CSV exports"
    )
    parser.add_argument(
        "--full",
        required=True,
        type=Path,
        help="Full search terms export (all queries)",
    )
    parser.add_argument(
        "--converting",
        type=Path,
        default=None,
        help="Optional: converting-only export for scale winner detection",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/search-terms-analysis"),
        help="Output directory for CSV + REPORT.md",
    )
    args = parser.parse_args()

    summary = run_analysis(args.full, args.converting, args.output_dir)
    print(json.dumps(summary, indent=2))
    print(f"\nReports written to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
