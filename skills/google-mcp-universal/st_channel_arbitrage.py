"""
Channel arbitrage analyzer for Google Ads search-terms exports.
Surfaces PMAX versus Search brand overlap and produces a channel efficiency matrix.
Usage: python3 st_channel_arbitrage.py --full "/path/Search terms report (1).csv" --output-dir ./reports/channel_arbitrage
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


COUNTRY_PRIMARY_RE = re.compile(r"^([A-Z]{2})\s*\|")
COUNTRY_FALLBACK_RE = re.compile(r"^([A-Z]{2})\s*-\s*")
TOKEN_RE = re.compile(r"[\wąćęłńóśźżăâîșțáéíóúüöäőűçñßάέήίόύώ]+", re.UNICODE)
COMPETITOR_TOKENS = {
    "cerave",
    "nivea",
    "herbalife",
    "oriflame",
    "fuxion",
    "vichy",
    "eucerin",
    "bioderma",
    "garnier",
    "neutrogena",
    "loreal",
    "weleda",
}
MARKETPLACE_TOKENS = {
    "amazon",
    "allegro",
    "ebay",
    "mercado",
    "mercadolibre",
    "mercadona",
    "emag",
    "skroutz",
    "benu",
    "rossmann",
    "lidl",
    "kaufland",
    "farmacia",
    "farmacie",
    "apteka",
    "apotheke",
    "drogaria",
    "tei",
    "ahorro",
}
REVIEW_TOKENS = {
    "opinie",
    "opinia",
    "recensioni",
    "recenze",
    "recenzie",
    "reviews",
    "avis",
    "forum",
    "comentarios",
    "opiniões",
    "opinioes",
    "κριτικες",
    "κριτικές",
    "vélemények",
}
PRICE_TOKENS = {
    "cena",
    "cenę",
    "pret",
    "preț",
    "precio",
    "preço",
    "preco",
    "price",
    "costo",
    "kosten",
    "quanto",
    "kosztuje",
    "τιμή",
    "τιμη",
    "ár",
}
SCAM_TOKENS = {
    "oszustwo",
    "scam",
    "fraud",
    "teapa",
    "țeapă",
    "contraindicatii",
    "contraindicații",
    "effetti",
    "collaterali",
    "bugiardino",
}


def parse_num(value: Any) -> float:
    text = str(value or "").strip().replace(",", "").replace("%", "").replace("£", "")
    if text and text not in ("--", "—"):
        try:
            return float(text)
        except ValueError:
            return 0.0
    else:
        return 0.0


def round2(value: float) -> float:
    return round(value + 1e-12, 2)


def tokenize(term: str) -> list[str]:
    return [token.casefold() for token in TOKEN_RE.findall(term or "") if token]


def discover_brand_tokens(rows: list[dict[str, Any]]) -> set[str]:
    conversions_by_term: dict[str, float] = defaultdict(float)
    for row in rows:
        if row["is_brand_campaign"]:
            conversions_by_term[row["search_term_lc"]] += float(row["conv"])
        else:
            pass
    return {term for term, conv in conversions_by_term.items() if conv >= 3.0}


def extract_country(campaign_name: str) -> str:
    match = COUNTRY_PRIMARY_RE.match(campaign_name or "")
    if match:
        return match.group(1)
    else:
        fallback = COUNTRY_FALLBACK_RE.match(campaign_name or "")
        if fallback:
            return fallback.group(1)
        else:
            return "??"


def classify_channel(campaign_name: str, match_type: str) -> str:
    haystack = f"{campaign_name or ''} {match_type or ''}".lower()
    if "performance max" in haystack or "pmax" in haystack:
        return "PMAX"
    elif "shopping" in haystack:
        return "SHOPPING"
    elif "search" in haystack:
        return "SEARCH"
    else:
        return "OTHER"


def is_brand_campaign_name(campaign_name: str) -> bool:
    lowered = (campaign_name or "").lower()
    if "bw" in lowered or "brand" in lowered:
        return True
    else:
        return False


def classify_intent(search_term: str, brand_terms: set[str], brand_words: set[str]) -> str:
    lowered = search_term.casefold()
    tokens = tokenize(search_term)
    token_set = set(tokens)
    if lowered in brand_terms or any(token in brand_words for token in tokens):
        return "brand"
    elif any(re.search(rf"\b{re.escape(token)}\b", lowered, flags=re.IGNORECASE) for token in COMPETITOR_TOKENS):
        return "competitor"
    elif token_set & MARKETPLACE_TOKENS:
        return "marketplace"
    elif token_set & SCAM_TOKENS:
        return "scam"
    elif token_set & REVIEW_TOKENS:
        return "review"
    elif token_set & PRICE_TOKENS:
        return "price"
    else:
        return "generic"


def load_search_terms_csv(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        handle.readline()
        handle.readline()
        reader = csv.DictReader(handle)
        for raw in reader:
            search_term = (raw.get("Search term") or "").strip()
            campaign = (raw.get("Campaign") or "").strip()
            if search_term.lower().startswith("total:") or not campaign:
                continue
            else:
                match_type = (raw.get("Match type") or "").strip()
                cost = parse_num(raw.get("Cost"))
                conversions = parse_num(raw.get("Conversions"))
                roi_pct = parse_num(raw.get("ROI"))
                rows.append(
                    {
                        "search_term": search_term,
                        "search_term_lc": search_term.casefold(),
                        "match_type": match_type,
                        "campaign": campaign,
                        "country": extract_country(campaign),
                        "channel": classify_channel(campaign, match_type),
                        "is_brand_campaign": is_brand_campaign_name(campaign),
                        "clicks": parse_num(raw.get("Clicks")),
                        "cost": cost,
                        "conv": conversions,
                        "value": cost * roi_pct / 100.0 if roi_pct > 0.0 else 0.0,
                    }
                )
    return rows


def format_cpa(value: float | None) -> str:
    if value is None:
        return ""
    else:
        return f"{round2(value):.2f}"


def build_pmax_brand_cannibalization(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["search_term_lc"], row["country"])
        if key not in grouped:
            grouped[key] = {
                "search_term": row["search_term_lc"],
                "country": row["country"],
                "channels": {
                    "PMAX": {"cost": 0.0, "conv": 0.0, "value": 0.0},
                    "SHOPPING": {"cost": 0.0, "conv": 0.0, "value": 0.0},
                    "SEARCH_BRAND": {"cost": 0.0, "conv": 0.0, "value": 0.0},
                },
            }
        else:
            pass
        bucket = grouped[key]
        if row["channel"] == "PMAX":
            target = bucket["channels"]["PMAX"]
        elif row["channel"] == "SHOPPING":
            target = bucket["channels"]["SHOPPING"]
        elif row["channel"] == "SEARCH" and row["is_brand_campaign"]:
            target = bucket["channels"]["SEARCH_BRAND"]
        else:
            continue
        target["cost"] += float(row["cost"])
        target["conv"] += float(row["conv"])
        target["value"] += float(row["value"])

    candidates: list[dict[str, Any]] = []
    for _, bucket in grouped.items():
        pmax = bucket["channels"]["PMAX"]
        shopping = bucket["channels"]["SHOPPING"]
        search_brand = bucket["channels"]["SEARCH_BRAND"]
        overlap_score = pmax["cost"] + shopping["cost"]
        if overlap_score <= 5.0 or search_brand["cost"] <= 0.0:
            continue
        elif pmax["cost"] <= 0.0 and shopping["cost"] <= 0.0:
            continue
        else:
            pass

        pmax_roi = (pmax["value"] / pmax["cost"] * 100.0) if pmax["cost"] > 0.0 else 0.0
        shopping_roi = (shopping["value"] / shopping["cost"] * 100.0) if shopping["cost"] > 0.0 else 0.0
        search_brand_roi = (search_brand["value"] / search_brand["cost"] * 100.0) if search_brand["cost"] > 0.0 else 0.0
        pmax_cpa = (pmax["cost"] / pmax["conv"]) if pmax["conv"] > 0.0 else None
        search_brand_cpa = (search_brand["cost"] / search_brand["conv"]) if search_brand["conv"] > 0.0 else None

        if pmax["cost"] > search_brand["cost"] and search_brand_roi > pmax_roi:
            recommended_action = "ADD_BRAND_EXCLUSION_TO_PMAX"
        elif (
            pmax["cost"] > 50.0
            and search_brand_cpa is not None
            and pmax_cpa is not None
            and search_brand_cpa < pmax_cpa * 0.6
        ):
            recommended_action = "BRAND_LIFT_TEST_THEN_EXCLUDE"
        else:
            recommended_action = "MONITOR"

        candidates.append(
            {
                "search_term": bucket["search_term"],
                "country": bucket["country"],
                "pmax_cost": round2(pmax["cost"]),
                "pmax_conv": round2(pmax["conv"]),
                "pmax_roi_pct": round2(pmax_roi),
                "shopping_cost": round2(shopping["cost"]),
                "shopping_conv": round2(shopping["conv"]),
                "shopping_roi_pct": round2(shopping_roi),
                "search_brand_cost": round2(search_brand["cost"]),
                "search_brand_conv": round2(search_brand["conv"]),
                "search_brand_roi_pct": round2(search_brand_roi),
                "overlap_score": round2(overlap_score),
                "recommended_action": recommended_action,
                "_pmax_cpa": pmax_cpa,
                "_search_brand_cpa": search_brand_cpa,
            }
        )

    candidates.sort(key=lambda item: (-item["overlap_score"], item["search_term"], item["country"]))
    return candidates


def build_channel_efficiency_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    country_totals: dict[str, dict[str, float]] = defaultdict(lambda: {"cost": 0.0, "conv": 0.0})
    grouped: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"cost": 0.0, "conv": 0.0, "value": 0.0, "clicks": 0.0}
    )
    for row in rows:
        country_totals[row["country"]]["cost"] += float(row["cost"])
        country_totals[row["country"]]["conv"] += float(row["conv"])
        bucket = grouped[(row["country"], row["channel"])]
        bucket["cost"] += float(row["cost"])
        bucket["conv"] += float(row["conv"])
        bucket["value"] += float(row["value"])
        bucket["clicks"] += float(row["clicks"])

    matrix_rows: list[dict[str, Any]] = []
    for (country, channel_type), bucket in grouped.items():
        cost = bucket["cost"]
        conv = bucket["conv"]
        value = bucket["value"]
        clicks = bucket["clicks"]
        cpa = (cost / conv) if conv > 0.0 else None
        roi_pct = (value / cost * 100.0) if cost > 0.0 else 0.0
        conv_rate = (conv / clicks * 100.0) if clicks > 0.0 else 0.0
        spend_share_pct = (cost / country_totals[country]["cost"] * 100.0) if country_totals[country]["cost"] > 0.0 else 0.0
        conv_share_pct = (conv / country_totals[country]["conv"] * 100.0) if country_totals[country]["conv"] > 0.0 else 0.0
        matrix_rows.append(
            {
                "country": country,
                "channel_type": channel_type,
                "cost": round2(cost),
                "conv": round2(conv),
                "value": round2(value),
                "cpa": format_cpa(cpa),
                "roi_pct": round2(roi_pct),
                "conv_rate": round2(conv_rate),
                "spend_share_pct": round2(spend_share_pct),
                "conv_share_pct": round2(conv_share_pct),
                "profit": round2(value - cost),
                "_country_total_cost": country_totals[country]["cost"],
            }
        )

    matrix_rows.sort(key=lambda item: (-item["_country_total_cost"], item["channel_type"], item["country"]))
    for row in matrix_rows:
        del row["_country_total_cost"]
    return matrix_rows


def build_intent_outputs(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, float]]:
    brand_terms = discover_brand_tokens(rows)
    brand_words: set[str] = set()
    for term in brand_terms:
        for token in tokenize(term):
            brand_words.add(token)

    term_rollups: dict[str, dict[str, Any]] = {}
    for row in rows:
        term = row["search_term_lc"]
        if term not in term_rollups:
            term_rollups[term] = {
                "search_term": term,
                "cost": 0.0,
                "conv": 0.0,
                "value": 0.0,
            }
        else:
            pass
        term_rollups[term]["cost"] += float(row["cost"])
        term_rollups[term]["conv"] += float(row["conv"])
        term_rollups[term]["value"] += float(row["value"])

    account_cost = sum(float(row["cost"]) for row in rows)
    class_buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"terms_count": 0, "total_cost": 0.0, "total_conv": 0.0, "total_value": 0.0}
    )
    sample_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for term, bucket in term_rollups.items():
        intent_class = classify_intent(term, brand_terms, brand_words)
        class_bucket = class_buckets[intent_class]
        class_bucket["terms_count"] += 1
        class_bucket["total_cost"] += float(bucket["cost"])
        class_bucket["total_conv"] += float(bucket["conv"])
        class_bucket["total_value"] += float(bucket["value"])
        sample_buckets[intent_class].append(
            {
                "search_term": term,
                "cost": round2(bucket["cost"]),
                "conv": round2(bucket["conv"]),
                "value": round2(bucket["value"]),
            }
        )

    intent_rows: list[dict[str, Any]] = []
    for intent_class, bucket in class_buckets.items():
        total_cost = float(bucket["total_cost"])
        intent_rows.append(
            {
                "intent_class": intent_class,
                "terms_count": int(bucket["terms_count"]),
                "total_cost": round2(total_cost),
                "total_conv": round2(bucket["total_conv"]),
                "total_value": round2(bucket["total_value"]),
                "avg_roi_pct": round2((bucket["total_value"] / total_cost * 100.0) if total_cost > 0.0 else 0.0),
                "spend_share_pct": round2((total_cost / account_cost * 100.0) if account_cost > 0.0 else 0.0),
            }
        )
    intent_rows.sort(key=lambda item: (-item["total_cost"], item["intent_class"]))

    sample_rows: list[dict[str, Any]] = []
    for intent_class in sorted(sample_buckets):
        top_terms = sorted(
            sample_buckets[intent_class],
            key=lambda item: (-item["cost"], -item["conv"], item["search_term"]),
        )[:20]
        for idx, item in enumerate(top_terms, start=1):
            sample_rows.append(
                {
                    "intent_class": intent_class,
                    "rank": idx,
                    "search_term": item["search_term"],
                    "cost": item["cost"],
                    "conv": item["conv"],
                    "value": item["value"],
                }
            )

    spend_map = {row["intent_class"]: float(row["spend_share_pct"]) for row in intent_rows}
    return intent_rows, sample_rows, spend_map


def ensure_output_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_csv_report(path: Path, rows: list[dict[str, Any]], headers: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row[column] for column in headers])


def write_json_report(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze PMAX/Search brand overlap and channel efficiency.")
    parser.add_argument("--full", required=True, help="Path to the full Google Ads Search terms report CSV.")
    parser.add_argument("--converting", required=False, help="Optional converting-only CSV; accepted for CLI compatibility.")
    parser.add_argument("--output-dir", default="./reports/channel_arbitrage", help="Directory for generated reports.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit:
        print("Argument parsing failed.", file=sys.stderr)
        return 1

    full_path = Path(args.full)
    if full_path.exists():
        rows = load_search_terms_csv(full_path)
    else:
        print(f"Full CSV not found: {full_path}", file=sys.stderr)
        return 1

    cannibalization_rows = build_pmax_brand_cannibalization(rows)
    matrix_rows = build_channel_efficiency_matrix(rows)
    intent_rows, sample_rows, spend_map = build_intent_outputs(rows)

    summary = {
        "cannibalization_candidates": len(cannibalization_rows),
        "projected_pmax_brand_save_gbp_estimate": round2(
            sum(float(row["pmax_cost"]) for row in cannibalization_rows if row["recommended_action"] == "ADD_BRAND_EXCLUSION_TO_PMAX")
        ),
        "brand_pct_of_account_spend": round2(spend_map.get("brand", 0.0)),
        "generic_pct_of_account_spend": round2(spend_map.get("generic", 0.0)),
        "competitor_pct_of_account_spend": round2(spend_map.get("competitor", 0.0)),
        "marketplace_pct_of_account_spend": round2(spend_map.get("marketplace", 0.0)),
    }

    output_dir = ensure_output_dir(Path(args.output_dir))
    files_written = [
        str(output_dir / "pmax_brand_cannibalization.csv"),
        str(output_dir / "channel_efficiency_matrix.csv"),
        str(output_dir / "intent_classification.csv"),
        str(output_dir / "intent_terms_sample.csv"),
        str(output_dir / "channel_arbitrage_summary.json"),
    ]

    write_csv_report(
        Path(files_written[0]),
        cannibalization_rows,
        [
            "search_term",
            "country",
            "pmax_cost",
            "pmax_conv",
            "pmax_roi_pct",
            "shopping_cost",
            "shopping_conv",
            "shopping_roi_pct",
            "search_brand_cost",
            "search_brand_conv",
            "search_brand_roi_pct",
            "overlap_score",
            "recommended_action",
        ],
    )
    write_csv_report(
        Path(files_written[1]),
        matrix_rows,
        [
            "country",
            "channel_type",
            "cost",
            "conv",
            "value",
            "cpa",
            "roi_pct",
            "conv_rate",
            "spend_share_pct",
            "conv_share_pct",
            "profit",
        ],
    )
    write_csv_report(
        Path(files_written[2]),
        intent_rows,
        ["intent_class", "terms_count", "total_cost", "total_conv", "total_value", "avg_roi_pct", "spend_share_pct"],
    )
    write_csv_report(
        Path(files_written[3]),
        sample_rows,
        ["intent_class", "rank", "search_term", "cost", "conv", "value"],
    )
    write_json_report(Path(files_written[4]), summary)

    print(
        json.dumps(
            {"status": "success", "output_dir": str(output_dir.resolve()), "files_written": files_written},
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
