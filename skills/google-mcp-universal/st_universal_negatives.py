"""
Universal negative-keyword analyzer for Google Ads search-terms exports.
Finds cross-country waste patterns that are safe candidates for account-wide negatives.
Usage: python3 st_universal_negatives.py --full "/path/Search terms report (1).csv" --output-dir ./reports/universal_negatives
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
STOPWORDS = {
    "and",
    "the",
    "for",
    "with",
    "from",
    "czy",
    "jak",
    "dla",
    "na",
    "do",
    "z",
    "w",
    "we",
    "si",
    "sa",
    "de",
    "la",
    "el",
    "en",
    "para",
    "que",
    "con",
    "por",
    "sin",
    "di",
    "da",
    "per",
    "del",
    "della",
    "che",
    "le",
    "les",
    "des",
    "pour",
    "και",
    "για",
    "στο",
    "στη",
    "την",
    "του",
    "το",
    "τι",
    "σε",
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
LOW_INTENT_TOKENS = {"gratis", "free", "pdf", "youtube", "wikipedia", "second", "hand"}
RISK_REVIEW_FLAGS = {"marketplace", "review", "price"}


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
    tokens = [token.casefold() for token in TOKEN_RE.findall(term or "")]
    return [token for token in tokens if len(token) >= 3 and not token.isdigit() and token not in STOPWORDS]


def classify_intent(ngram: str) -> str:
    tokens = tokenize(ngram)
    token_set = set(tokens)
    if token_set & MARKETPLACE_TOKENS:
        return "marketplace"
    elif token_set & SCAM_TOKENS:
        return "scam"
    elif token_set & REVIEW_TOKENS:
        return "review"
    elif token_set & PRICE_TOKENS:
        return "price"
    elif token_set & LOW_INTENT_TOKENS:
        return "low_intent"
    else:
        return "generic"


def discover_brand_tokens(rows: list[dict[str, Any]]) -> set[str]:
    conversions_by_term: dict[str, float] = defaultdict(float)
    for row in rows:
        lowered_campaign = str(row["campaign"]).casefold()
        if "bw" in lowered_campaign or "brand" in lowered_campaign:
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
                        "clicks": parse_num(raw.get("Clicks")),
                        "cost": cost,
                        "conv": conversions,
                        "value": cost * roi_pct / 100.0 if roi_pct > 0.0 else 0.0,
                    }
                )
    return rows


def build_ngrams(tokens: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for token in tokens:
        if token not in seen:
            seen.add(token)
            ordered.append(token)
        else:
            pass
    for idx in range(len(tokens) - 1):
        bigram = f"{tokens[idx]} {tokens[idx + 1]}"
        if bigram not in seen:
            seen.add(bigram)
            ordered.append(bigram)
        else:
            pass
    return ordered


def aggregate_negatives(
    rows: list[dict[str, Any]],
    min_countries: int,
    min_cost: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    account_cost = sum(float(row["cost"]) for row in rows)
    account_conv = sum(float(row["conv"]) for row in rows)
    account_cpa = (account_cost / account_conv) if account_conv > 0.0 else 0.0

    ngram_totals: dict[str, dict[str, Any]] = {}
    country_term_rollup: dict[tuple[str, str], dict[str, Any]] = {}

    for row in rows:
        ngrams = build_ngrams(tokenize(row["search_term"]))
        if ngrams:
            pass
        else:
            continue
        for ngram in ngrams:
            if ngram not in ngram_totals:
                ngram_totals[ngram] = {
                    "ngram": ngram,
                    "total_cost": 0.0,
                    "total_conv": 0.0,
                    "total_clicks": 0.0,
                    "countries": set(),
                    "per_country": defaultdict(lambda: {"cost": 0.0, "conv": 0.0, "clicks": 0.0, "value": 0.0}),
                }
            else:
                pass
            bucket = ngram_totals[ngram]
            bucket["total_cost"] += float(row["cost"])
            bucket["total_conv"] += float(row["conv"])
            bucket["total_clicks"] += float(row["clicks"])
            country_bucket = bucket["per_country"][row["country"]]
            country_bucket["cost"] += float(row["cost"])
            country_bucket["conv"] += float(row["conv"])
            country_bucket["clicks"] += float(row["clicks"])
            country_bucket["value"] += float(row.get("value", 0.0))
            if country_bucket["cost"] > 0.0:
                bucket["countries"].add(row["country"])
            else:
                pass

            country_key = (row["country"], ngram)
            if country_key not in country_term_rollup:
                country_term_rollup[country_key] = {
                    "country": row["country"],
                    "ngram": ngram,
                    "intent_flag": classify_intent(ngram),
                    "cost": 0.0,
                    "conv": 0.0,
                    "clicks": 0.0,
                }
            else:
                pass
            country_term_rollup[country_key]["cost"] += float(row["cost"])
            country_term_rollup[country_key]["conv"] += float(row["conv"])
            country_term_rollup[country_key]["clicks"] += float(row["clicks"])

    account_rows: list[dict[str, Any]] = []
    risk_review_rows: list[dict[str, Any]] = []
    tier_counter: Counter[str] = Counter()

    for ngram, bucket in ngram_totals.items():
        countries = sorted(bucket["countries"])
        if len(countries) < min_countries:
            continue
        else:
            pass
        tier = ""
        # HOLD_SHARED_NEGATIVE check (priority): n-gram waste in some countries
        # AND converts in ≥1 country with ROI > 100% AND conv > 1 → never make
        # this a shared/account-wide negative; whitelist required, campaign-level
        # negative only. This guard prevents killing converting variants like
        # "uro up forte" (RO £472 / 87 conv / ROI 171%) when blocking "forte".
        converting_countries = []
        for c, d in bucket["per_country"].items():
            if d["conv"] > 1.0 and d.get("value", 0.0) > d["cost"]:
                converting_countries.append(c)
        wasting_countries = [
            c for c, d in bucket["per_country"].items()
            if d["cost"] >= min_cost and d["conv"] == 0.0
        ]
        if converting_countries and wasting_countries:
            tier = "HOLD_SHARED_NEGATIVE"
        elif all(
            bucket["per_country"][country]["cost"] >= min_cost and bucket["per_country"][country]["conv"] == 0.0
            for country in countries
        ):
            tier = "HIGH_CONFIDENCE_NEGATIVE"
        elif all(
            bucket["per_country"][country]["cost"] > 3.0 * account_cpa and bucket["per_country"][country]["conv"] <= 1.0
            for country in countries
        ):
            tier = "REVIEW_NEEDED_NEGATIVE"
        else:
            continue

        intent_flag = classify_intent(ngram)
        row = {
            "ngram": ngram,
            "tier": tier,
            "intent_flag": intent_flag,
            "countries_count": len(countries),
            "countries_list": "|".join(countries),
            "total_cost": round2(bucket["total_cost"]),
            "total_conv": round2(bucket["total_conv"]),
            "total_clicks": round2(bucket["total_clicks"]),
            "monthly_save_estimate": round2(bucket["total_cost"] / 7.5),
            "recommended_match_type": "PHRASE" if " " in ngram else "EXACT",
        }
        tier_counter[tier] += 1
        if intent_flag in RISK_REVIEW_FLAGS:
            risk_review_rows.append(row)
        else:
            account_rows.append(row)

    account_rows.sort(key=lambda item: (-item["monthly_save_estimate"], item["ngram"]))
    risk_review_rows.sort(key=lambda item: (-item["monthly_save_estimate"], item["ngram"]))

    by_country_rows: list[dict[str, Any]] = []
    for _, row in country_term_rollup.items():
        if row["cost"] > 10.0 and row["conv"] == 0.0:
            by_country_rows.append(
                {
                    "country": row["country"],
                    "ngram": row["ngram"],
                    "intent_flag": row["intent_flag"],
                    "cost": round2(row["cost"]),
                    "conv": round2(row["conv"]),
                    "clicks": round2(row["clicks"]),
                    "recommended_match_type": "PHRASE" if " " in row["ngram"] else "EXACT",
                }
            )
        else:
            pass
    by_country_rows.sort(key=lambda item: (-item["cost"], item["country"], item["ngram"]))

    summary = {
        "tier_a_count": int(tier_counter["HIGH_CONFIDENCE_NEGATIVE"]),
        "tier_b_count": int(tier_counter["REVIEW_NEEDED_NEGATIVE"]),
        "hold_shared_negative_count": int(tier_counter["HOLD_SHARED_NEGATIVE"]),
        "risk_review_count": len(risk_review_rows),
        "projected_monthly_save_gbp": round2(sum(float(row["monthly_save_estimate"]) for row in account_rows)),
        "top_10_ngrams": [
            {
                "ngram": row["ngram"],
                "monthly_save_estimate": row["monthly_save_estimate"],
                "tier": row["tier"],
                "intent_flag": row["intent_flag"],
            }
            for row in account_rows[:10]
        ],
    }
    return account_rows, by_country_rows, risk_review_rows, summary


def ensure_output_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_account_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = [
        "ngram",
        "tier",
        "intent_flag",
        "countries_count",
        "countries_list",
        "total_cost",
        "total_conv",
        "total_clicks",
        "monthly_save_estimate",
        "recommended_match_type",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row[column] for column in headers])


def write_country_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = ["country", "ngram", "intent_flag", "cost", "conv", "clicks", "recommended_match_type"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row[column] for column in headers])


def write_json_report(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Identify universal negative-keyword candidates across countries.")
    parser.add_argument("--full", required=True, help="Path to the full Google Ads Search terms report CSV.")
    parser.add_argument("--converting", required=False, help="Optional converting-only CSV; accepted for CLI compatibility.")
    parser.add_argument("--min-countries", type=int, default=3, help="Minimum country count required for an account-wide negative.")
    parser.add_argument("--min-cost", type=float, default=5.0, help="Minimum per-country cost for Tier A qualification.")
    parser.add_argument("--output-dir", default="./reports/universal_negatives", help="Directory for generated reports.")
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

    # Union with --converting CSV so HOLD_SHARED_NEGATIVE tier can detect
    # n-grams that convert in one country while wasting in another (the
    # converting evidence lives in the CONVERTING-filtered CSV, not FULL).
    if args.converting:
        conv_path = Path(args.converting)
        if conv_path.exists():
            rows = rows + load_search_terms_csv(conv_path)
        else:
            print(f"Converting CSV not found: {conv_path}", file=sys.stderr)

    account_rows, by_country_rows, risk_review_rows, summary = aggregate_negatives(
        rows,
        min_countries=args.min_countries,
        min_cost=float(args.min_cost),
    )
    output_dir = ensure_output_dir(Path(args.output_dir))
    files_written = [
        str(output_dir / "universal_negatives_account.csv"),
        str(output_dir / "negatives_by_country.csv"),
        str(output_dir / "negatives_risk_review.csv"),
        str(output_dir / "universal_negatives_summary.json"),
    ]

    write_account_csv(Path(files_written[0]), account_rows)
    write_country_csv(Path(files_written[1]), by_country_rows)
    write_account_csv(Path(files_written[2]), risk_review_rows)
    write_json_report(Path(files_written[3]), summary)

    print(
        json.dumps(
            {"status": "success", "output_dir": str(output_dir.resolve()), "files_written": files_written},
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
