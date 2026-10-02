#!/usr/bin/env python3
"""
Offline Google Ads search terms profit analyzer.

Reads Google Ads "Search terms report" CSV exports and produces actionable
files for multi-country e-commerce optimization:
- positive keyword candidates to harvest into Search campaigns,
- negative keyword candidates with risk notes,
- n-gram winners and waste clusters,
- country/campaign/ad-group rollups,
- a Polish strategy report for the current dataset.

This script is intentionally stdlib-only so it can be run on fresh exports
without installing the MCP server dependencies.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_TARGET_ROI_PERCENT = 200.0
DEFAULT_MIN_NEGATIVE_COST = 5.0
DEFAULT_MIN_NEGATIVE_CLICKS = 25.0
DEFAULT_MIN_SCALE_CONVERSIONS = 2.0
DEFAULT_MIN_SCALE_ROI_PERCENT = 150.0


MARKETPLACE_TERMS = {
    "amazon", "allegro", "mercado", "mercadolibre", "mercado libre", "ebay",
    "olx", "skroutz", "emag", "benu", "rossmann", "dm", "lidl", "kaufland",
    "mercadona", "farmacia", "farmacie", "apteka", "apotheke", "shop apotheke",
    "drogaria", "farmacia tei", "farmacia del ahorro",
}

REVIEW_TERMS = {
    "opinie", "opinia", "recenzja", "recensioni", "recensioni negative",
    "reviews", "avis", "forum", "comentarios", "opiniões", "opinioes",
    "vélemények", "recenze", "recenzie", "κριτικες", "κριτικές",
}

PRICE_TERMS = {
    "cena", "cenę", "pret", "preț", "precio", "preço", "preco", "price",
    "quanto costa", "ile kosztuje", "τιμη", "τιμή", "ár", "costo", "kosten",
}

SCAM_TERMS = {
    "oszustwo", "scam", "fraud", "teapa", "țeapă", "bugiardino", "negative",
    "contraindicatii", "contraindicații", "effetti collaterali",
}

LOW_INTENT_TERMS = {
    "gratis", "free", "pdf", "youtube", "wikipedia", "olx", "second hand",
}

STOPWORDS = {
    "and", "the", "for", "with", "from", "how", "what", "where", "when",
    "czy", "jak", "dla", "bez", "oraz", "or", "na", "do", "w", "we", "z",
    "si", "sa", "de", "la", "el", "en", "para", "que", "con", "por", "sin",
    "di", "da", "per", "del", "della", "che", "le", "les", "des", "pour",
    "και", "για", "στο", "στη", "την", "του", "το", "τι", "σε",
}


@dataclass
class Metrics:
    impressions: float = 0.0
    clicks: float = 0.0
    cost: float = 0.0
    conversions: float = 0.0
    value: float = 0.0
    row_count: int = 0

    def add(self, other: "Metrics") -> None:
        self.impressions += other.impressions
        self.clicks += other.clicks
        self.cost += other.cost
        self.conversions += other.conversions
        self.value += other.value
        self.row_count += other.row_count

    @property
    def ctr(self) -> float:
        return self.clicks / self.impressions if self.impressions > 0 else 0.0

    @property
    def cpc(self) -> float:
        return self.cost / self.clicks if self.clicks > 0 else 0.0

    @property
    def cpa(self) -> float:
        return self.cost / self.conversions if self.conversions > 0 else 0.0

    @property
    def conv_rate(self) -> float:
        return self.conversions / self.clicks if self.clicks > 0 else 0.0

    @property
    def roi_percent(self) -> float:
        return (self.value / self.cost) * 100.0 if self.cost > 0 else 0.0

    @property
    def profit(self) -> float:
        return self.value - self.cost


@dataclass
class Row:
    source: str
    date_range: str
    search_term: str
    match_type: str
    added_excluded: str
    campaign: str
    ad_group: str
    currency: str
    metrics: Metrics
    raw_roi_percent: float

    @property
    def country(self) -> str:
        match = re.match(r"\s*([A-Z]{2})\b", self.campaign)
        return match.group(1) if match else "??"

    @property
    def campaign_type(self) -> str:
        haystack = f"{self.match_type} {self.campaign}".lower()
        if "performance max" in haystack or "pmax" in haystack:
            return "PMAX"
        if "shopping" in haystack:
            return "SHOPPING"
        if "search" in haystack:
            return "SEARCH"
        if "ai max" in haystack:
            return "AI_MAX"
        return "OTHER"


@dataclass
class Aggregate:
    key: Tuple[str, ...]
    metrics: Metrics = field(default_factory=Metrics)
    rows: List[Row] = field(default_factory=list)

    def add(self, row: Row) -> None:
        self.metrics.add(row.metrics)
        self.rows.append(row)

    @property
    def search_term(self) -> str:
        return self.key[0] if self.key else ""

    @property
    def country(self) -> str:
        countries = Counter(row.country for row in self.rows)
        return countries.most_common(1)[0][0] if countries else ""

    @property
    def campaign_type(self) -> str:
        types = Counter(row.campaign_type for row in self.rows)
        return types.most_common(1)[0][0] if types else ""

    @property
    def top_campaign(self) -> str:
        campaigns = Counter(row.campaign for row in self.rows)
        return campaigns.most_common(1)[0][0] if campaigns else ""

    @property
    def top_ad_group(self) -> str:
        ad_groups = Counter(row.ad_group for row in self.rows)
        return ad_groups.most_common(1)[0][0] if ad_groups else ""


def parse_number(value: str) -> float:
    cleaned = (value or "").strip().replace(",", "").replace("%", "")
    if cleaned in {"", "--"}:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def find_header_index(lines: Sequence[str]) -> int:
    for index, line in enumerate(lines):
        if line.startswith("Search term,"):
            return index
    raise ValueError("Could not find Google Ads Search term CSV header")


def read_google_ads_csv(path: Path) -> Tuple[str, List[Row]]:
    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    header_index = find_header_index(lines)
    date_range = lines[1].strip().strip('"') if len(lines) > 1 else ""

    reader = csv.DictReader(lines[header_index:])
    rows: List[Row] = []
    for record in reader:
        term = (record.get("Search term") or "").strip()
        campaign = (record.get("Campaign") or "").strip()
        if not term or not campaign:
            continue

        cost = parse_number(record.get("Cost", ""))
        roi_percent = parse_number(record.get("ROI", ""))
        value = cost * (roi_percent / 100.0) if cost > 0 and roi_percent > 0 else 0.0

        metrics = Metrics(
            impressions=parse_number(record.get("Impr.", "")),
            clicks=parse_number(record.get("Clicks", "")),
            cost=cost,
            conversions=parse_number(record.get("Conversions", "")),
            value=value,
            row_count=1,
        )

        rows.append(
            Row(
                source=path.name,
                date_range=date_range,
                search_term=term,
                match_type=(record.get("Match type") or "").strip(),
                added_excluded=(record.get("Added/Excluded") or "").strip(),
                campaign=campaign,
                ad_group=(record.get("Ad group") or "").strip(),
                currency=(record.get("Currency code") or "").strip(),
                metrics=metrics,
                raw_roi_percent=roi_percent,
            )
        )
    return date_range, rows


def aggregate(rows: Iterable[Row], key_func) -> Dict[Tuple[str, ...], Aggregate]:
    groups: Dict[Tuple[str, ...], Aggregate] = {}
    for row in rows:
        key = key_func(row)
        if key not in groups:
            groups[key] = Aggregate(key=key)
        groups[key].add(row)
    return groups


def tokenize(term: str) -> List[str]:
    cleaned = term.lower()
    tokens = re.findall(r"[\wąćęłńóśźżăâîșțáéíóúüöäőűçñßά-ώα-ωΑ-Ω]+", cleaned, flags=re.UNICODE)
    return [token for token in tokens if len(token) >= 3 and token not in STOPWORDS and not token.isdigit()]


def ngrams(tokens: Sequence[str], max_n: int = 3) -> Iterable[str]:
    for n in range(1, max_n + 1):
        for index in range(0, len(tokens) - n + 1):
            yield " ".join(tokens[index:index + n])


def classify_intent(term: str) -> List[str]:
    lower = term.lower()
    tags = []
    buckets = [
        ("marketplace_or_offline_retail", MARKETPLACE_TERMS),
        ("review_research", REVIEW_TERMS),
        ("price_sensitive", PRICE_TERMS),
        ("scam_or_complaint", SCAM_TERMS),
        ("low_intent_free_info", LOW_INTENT_TERMS),
    ]
    for label, words in buckets:
        if any(word in lower for word in words):
            tags.append(label)
    if not tags:
        tags.append("product_or_generic_intent")
    return tags


def format_money(value: float) -> str:
    return f"{value:.2f}"


def format_percent(value: float) -> str:
    return f"{value:.2f}%"


def round_metric(value: float, digits: int = 2) -> float:
    if math.isfinite(value):
        return round(value, digits)
    return 0.0


def aggregate_to_dict(agg: Aggregate, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    metrics = agg.metrics
    data: Dict[str, Any] = {
        "rows": metrics.row_count,
        "impressions": round_metric(metrics.impressions, 0),
        "clicks": round_metric(metrics.clicks, 0),
        "cost": round_metric(metrics.cost, 2),
        "conversions": round_metric(metrics.conversions, 2),
        "conversion_value_est": round_metric(metrics.value, 2),
        "profit_est": round_metric(metrics.profit, 2),
        "ctr_percent": round_metric(metrics.ctr * 100, 2),
        "avg_cpc": round_metric(metrics.cpc, 4),
        "conv_rate_percent": round_metric(metrics.conv_rate * 100, 2),
        "cpa": round_metric(metrics.cpa, 2),
        "roi_percent": round_metric(metrics.roi_percent, 2),
    }
    if extra:
        data.update(extra)
    return data


def action_for_positive(agg: Aggregate) -> str:
    if agg.campaign_type == "PMAX":
        return "Harvest: dodaj jako Exact/Phrase w dedykowanym Search ad group; utrzymaj PMax jako discovery."
    if agg.campaign_type == "SHOPPING":
        return "Harvest: zbuduj Search exact dla tej frazy lub podnieś bid/listing segment dla produktu."
    if agg.campaign_type == "SEARCH":
        return "Scale: podnieś bid/budżet ostrożnie i rozbij dokładne warianty w osobnym ad group."
    return "Scale: wykorzystaj jako keyword/ad copy angle po ręcznej walidacji."


def negative_match_recommendation(agg: Aggregate) -> str:
    tags = classify_intent(agg.search_term)
    if agg.metrics.clicks >= 50 or "low_intent_free_info" in tags:
        return "PHRASE"
    if agg.metrics.cost >= 10 and agg.metrics.conversions == 0:
        return "EXACT first, PHRASE only if n-gram waste confirms pattern"
    return "EXACT"


def risk_note_for_negative(agg: Aggregate) -> str:
    tags = classify_intent(agg.search_term)
    if any(tag in tags for tag in {"review_research", "price_sensitive", "scam_or_complaint", "marketplace_or_offline_retail"}):
        return "Uwaga: intencja bywa konwertująca w tym koncie; negatywować tylko na poziomie kampanii/produktu po sprawdzeniu row-level."
    return "Niskie ryzyko, jeśli fraza nie pasuje do produktu ani rynku."


def build_positive_candidates(
    term_groups: Dict[Tuple[str, ...], Aggregate],
    min_conversions: float,
    min_roi_percent: float,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for agg in term_groups.values():
        metrics = agg.metrics
        if metrics.conversions < min_conversions:
            continue
        if metrics.roi_percent < min_roi_percent and metrics.profit < 1:
            continue
        score = metrics.profit + (metrics.conversions * 3.0) + (metrics.conv_rate * 10.0)
        candidates.append(
            aggregate_to_dict(
                agg,
                {
                    "search_term": agg.search_term,
                    "country": agg.country,
                    "campaign_type": agg.campaign_type,
                    "top_campaign": agg.top_campaign,
                    "top_ad_group": agg.top_ad_group,
                    "intent_tags": "|".join(classify_intent(agg.search_term)),
                    "recommended_action": action_for_positive(agg),
                    "priority_score": round_metric(score, 2),
                },
            )
        )
    return sorted(candidates, key=lambda item: (item["priority_score"], item["conversions"], item["profit_est"]), reverse=True)


def build_negative_candidates(
    term_groups: Dict[Tuple[str, ...], Aggregate],
    min_cost: float,
    min_clicks: float,
    target_roi_percent: float,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for agg in term_groups.values():
        metrics = agg.metrics
        no_conversion_waste = metrics.conversions == 0 and (metrics.cost >= min_cost or metrics.clicks >= min_clicks)
        low_roi_waste = metrics.cost >= min_cost and 0 < metrics.conversions < 1 and metrics.roi_percent < target_roi_percent * 0.5
        click_waste = metrics.clicks >= min_clicks * 2 and metrics.conv_rate < 0.002
        if not (no_conversion_waste or low_roi_waste or click_waste):
            continue

        waste_score = metrics.cost + (metrics.clicks * 0.05) + max(0.0, target_roi_percent - metrics.roi_percent) / 100.0
        candidates.append(
            aggregate_to_dict(
                agg,
                {
                    "search_term": agg.search_term,
                    "country": agg.country,
                    "campaign_type": agg.campaign_type,
                    "top_campaign": agg.top_campaign,
                    "top_ad_group": agg.top_ad_group,
                    "intent_tags": "|".join(classify_intent(agg.search_term)),
                    "negative_match": negative_match_recommendation(agg),
                    "risk_note": risk_note_for_negative(agg),
                    "recommended_action": "Add negative after manual spot-check of product fit and campaign scope.",
                    "priority_score": round_metric(waste_score, 2),
                },
            )
        )
    return sorted(candidates, key=lambda item: (item["priority_score"], item["cost"], item["clicks"]), reverse=True)


def annotate_negative_conflicts(
    negative_candidates: List[Dict[str, Any]],
    positive_candidates: List[Dict[str, Any]],
) -> None:
    positive_by_term = {str(row["search_term"]).lower(): row for row in positive_candidates}
    for row in negative_candidates:
        positive = positive_by_term.get(str(row["search_term"]).lower())
        if not positive:
            row["positive_conflict"] = "NO"
            row["positive_signal"] = ""
            continue

        row["positive_conflict"] = "YES"
        row["positive_signal"] = (
            f"{positive['conversions']} conv / ROI {positive['roi_percent']}% / "
            f"{positive['country']} / {positive['campaign_type']}"
        )
        row["negative_match"] = "HOLD_SHARED_NEGATIVE"
        row["risk_note"] = (
            "Conflict: this same term appears in the scale-source winner list. "
            "Do not add shared/account negative; if needed, use campaign-level exact negative only where waste is proven."
        )
        row["recommended_action"] = (
            "Split by market/campaign first; harvest profitable instance and isolate losing instance with campaign-level control."
        )


def build_impression_waste(term_groups: Dict[Tuple[str, ...], Aggregate]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for agg in term_groups.values():
        metrics = agg.metrics
        if metrics.impressions < 500 or metrics.clicks > 0:
            continue
        items.append(
            aggregate_to_dict(
                agg,
                {
                    "search_term": agg.search_term,
                    "country": agg.country,
                    "campaign_type": agg.campaign_type,
                    "top_campaign": agg.top_campaign,
                    "top_ad_group": agg.top_ad_group,
                    "recommended_action": "Feed/query relevance cleanup: wysokie wyświetlenia bez klików, niekoniecznie negative.",
                },
            )
        )
    return sorted(items, key=lambda item: item["impressions"], reverse=True)


def build_ngram_groups(rows: Sequence[Row]) -> Dict[Tuple[str, ...], Aggregate]:
    groups: Dict[Tuple[str, ...], Aggregate] = {}
    for row in rows:
        seen = set(ngrams(tokenize(row.search_term), max_n=3))
        for gram in seen:
            key = (gram,)
            if key not in groups:
                groups[key] = Aggregate(key=key)
            groups[key].add(row)
    return groups


def build_ngram_winners(ngram_groups: Dict[Tuple[str, ...], Aggregate]) -> List[Dict[str, Any]]:
    items = []
    for agg in ngram_groups.values():
        metrics = agg.metrics
        if metrics.conversions < 5:
            continue
        if metrics.roi_percent < 150 and metrics.profit < 5:
            continue
        items.append(
            aggregate_to_dict(
                agg,
                {
                    "ngram": agg.search_term,
                    "countries": ",".join(country for country, _ in Counter(row.country for row in agg.rows).most_common(5)),
                    "campaign_types": ",".join(kind for kind, _ in Counter(row.campaign_type for row in agg.rows).most_common()),
                    "recommended_action": "Użyj jako keyword theme, headline angle lub osobny ad group, jeśli pasuje do produktu.",
                },
            )
        )
    return sorted(items, key=lambda item: (item["profit_est"], item["conversions"], item["cost"]), reverse=True)


def build_ngram_waste(
    ngram_groups: Dict[Tuple[str, ...], Aggregate],
    min_cost: float,
    target_roi_percent: float,
) -> List[Dict[str, Any]]:
    items = []
    for agg in ngram_groups.values():
        metrics = agg.metrics
        if metrics.cost < min_cost:
            continue
        if metrics.conversions > 0 and metrics.roi_percent >= target_roi_percent * 0.5:
            continue
        items.append(
            aggregate_to_dict(
                agg,
                {
                    "ngram": agg.search_term,
                    "countries": ",".join(country for country, _ in Counter(row.country for row in agg.rows).most_common(5)),
                    "campaign_types": ",".join(kind for kind, _ in Counter(row.campaign_type for row in agg.rows).most_common()),
                    "recommended_action": "Kandydat na phrase negative lub podział kampanii, ale sprawdź wyjątki konwertujące.",
                },
            )
        )
    return sorted(items, key=lambda item: (item["cost"], item["clicks"]), reverse=True)


def build_rollup(groups: Dict[Tuple[str, ...], Aggregate], key_names: Sequence[str]) -> List[Dict[str, Any]]:
    rows = []
    for key, agg in groups.items():
        extra = {name: key[index] if index < len(key) else "" for index, name in enumerate(key_names)}
        rows.append(aggregate_to_dict(agg, extra))
    return sorted(rows, key=lambda item: item["cost"], reverse=True)


def summarize_sources(source_rows: Dict[str, List[Row]], source_dates: Dict[str, str]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    for source, rows in source_rows.items():
        agg = Aggregate(key=(source,))
        for row in rows:
            agg.add(row)
        countries = Counter(row.country for row in rows)
        match_types = Counter(row.match_type for row in rows)
        summary[source] = aggregate_to_dict(
            agg,
            {
                "date_range": source_dates.get(source, ""),
                "countries_top": countries.most_common(10),
                "match_types": match_types.most_common(),
                "unique_terms": len({row.search_term.lower() for row in rows}),
            },
        )
    return summary


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def top(items: Sequence[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    return list(items[:limit])


def build_strategy_markdown(
    output_dir: Path,
    primary_source: str,
    scale_source: str,
    source_summary: Dict[str, Any],
    positive_candidates: List[Dict[str, Any]],
    negative_candidates: List[Dict[str, Any]],
    ngram_winners: List[Dict[str, Any]],
    ngram_waste: List[Dict[str, Any]],
    country_rollup: List[Dict[str, Any]],
    campaign_rollup: List[Dict[str, Any]],
    impression_waste: List[Dict[str, Any]],
    target_roi_percent: float,
) -> str:
    primary = source_summary[primary_source]
    scale = source_summary[scale_source]
    lines = [
        "# Google Ads Search Terms Profit Strategy",
        "",
        f"**Waste / coverage source**: `{primary_source}`",
        f"**Scale / harvest source**: `{scale_source}`",
        f"**Date range**: {primary.get('date_range', 'unknown')}",
        f"**Target ROI threshold**: {target_roi_percent:.0f}%",
        "",
        "## Executive Summary",
        "",
        (
            f"Primary export contains {int(primary['rows'])} rows, "
            f"{int(primary['clicks'])} clicks, {format_money(primary['cost'])} spend, "
            f"{format_money(primary['conversions'])} conversions, estimated CPA {format_money(primary['cpa'])}, "
            f"and estimated ROI {format_percent(primary['roi_percent'])}."
        ),
        (
            f"Scale source contains {int(scale['rows'])} rows, "
            f"{int(scale['clicks'])} clicks, {format_money(scale['cost'])} spend, "
            f"{format_money(scale['conversions'])} conversions, estimated CPA {format_money(scale['cpa'])}, "
            f"and estimated ROI {format_percent(scale['roi_percent'])}."
        ),
        "",
        "The strongest opportunity is not a simple negative-keyword sweep. The account has many "
        "queries with marketplace, pharmacy, price, review, and even complaint intent that still convert. "
        "Optimization should therefore use a two-lane system: harvest profitable search terms into controllable "
        "Search/Shopping structures, and only negate waste after checking campaign/product scope.",
        "",
        "## Immediate Actions",
        "",
        f"1. Harvest top {min(100, len(positive_candidates))} profitable terms from `positive_keyword_candidates.csv` into Exact/Phrase Search ad groups.",
        f"2. Review top {min(100, len(negative_candidates))} waste candidates from `negative_keyword_candidates.csv`; apply campaign-level negatives first, shared negatives only after n-gram confirmation.",
        f"3. Use `ngram_winners.csv` to create keyword themes and ad-copy angles by market.",
        f"4. Use `ngram_waste.csv` to find phrase-negative patterns, but whitelist exceptions with conversions before applying.",
        "5. Reallocate budget by `country_summary.csv` and `campaign_summary.csv`: scale profit/ROI pockets, cap spend on low-ROI/no-conversion pockets.",
        "",
        "## Top Positive Terms",
        "",
    ]

    for row in top(positive_candidates, 15):
        lines.append(
            f"- `{row['search_term']}` ({row['country']}, {row['campaign_type']}): "
            f"{row['conversions']} conv, cost {row['cost']}, ROI {row['roi_percent']}%, profit {row['profit_est']}."
        )

    lines.extend(["", "## Top Negative Candidates", ""])
    for row in top(negative_candidates, 15):
        lines.append(
            f"- `{row['search_term']}` ({row['country']}, {row['campaign_type']}): "
            f"{row['clicks']} clicks, cost {row['cost']}, conv {row['conversions']}, "
            f"ROI {row['roi_percent']}%, match {row['negative_match']}."
        )

    lines.extend(["", "## Top N-Gram Winners", ""])
    for row in top(ngram_winners, 15):
        lines.append(
            f"- `{row['ngram']}`: {row['conversions']} conv, ROI {row['roi_percent']}%, profit {row['profit_est']}, countries {row['countries']}."
        )

    lines.extend(["", "## Top N-Gram Waste", ""])
    for row in top(ngram_waste, 15):
        lines.append(
            f"- `{row['ngram']}`: cost {row['cost']}, clicks {row['clicks']}, conv {row['conversions']}, countries {row['countries']}."
        )

    lines.extend(["", "## Country Prioritization", ""])
    for row in top(country_rollup, 12):
        lines.append(
            f"- `{row['country']}`: cost {row['cost']}, conv {row['conversions']}, "
            f"CPA {row['cpa']}, ROI {row['roi_percent']}%, profit {row['profit_est']}."
        )

    lines.extend(["", "## Campaign Risk Review", ""])
    for row in top(campaign_rollup, 12):
        lines.append(
            f"- `{row['campaign']}`: cost {row['cost']}, conv {row['conversions']}, "
            f"CPA {row['cpa']}, ROI {row['roi_percent']}%."
        )

    if impression_waste:
        lines.extend(["", "## Impression Waste / Feed Relevance", ""])
        for row in top(impression_waste, 10):
            lines.append(
                f"- `{row['search_term']}` ({row['country']}): {row['impressions']} impressions, 0 clicks. Treat as feed/ad relevance cleanup first."
            )

    lines.extend(
        [
            "",
            "## Operating Rules",
            "",
            "- Do not add account-level shared negatives from single-term evidence. Start campaign-level, then promote to shared list only when the same n-gram wastes spend across countries/products.",
            "- PMax converting terms should become Search exact/phrase keywords so budget and ad copy become controllable.",
            "- Shopping query winners should trigger listing-group/product bid review, not only keyword creation.",
            "- Marketplace/pharmacy/review/scam terms are not automatically bad in this account. They require ROI-based treatment because several convert profitably.",
            "- Re-run this script weekly and diff the output folders. New waste should be reviewed before it compounds; new winners should be harvested before competitors bid them up.",
            "",
            "## Generated Files",
            "",
        ]
    )
    for file_name in [
        "summary.json",
        "positive_keyword_candidates.csv",
        "negative_keyword_candidates.csv",
        "ngram_winners.csv",
        "ngram_waste.csv",
        "country_summary.csv",
        "campaign_summary.csv",
        "ad_group_summary.csv",
        "impression_waste.csv",
        "campaign_type_summary.csv",
        "scale_country_summary.csv",
    ]:
        lines.append(f"- `{output_dir / file_name}`")

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Google Ads Search Terms report CSV exports.")
    parser.add_argument("csv_paths", nargs="+", help="Path(s) to Google Ads Search terms report CSV exports.")
    parser.add_argument("--primary", help="Primary CSV filename/path. Defaults to the largest row-count export.")
    parser.add_argument(
        "--scale-source",
        help="CSV filename/path used for positive keyword harvesting. Defaults to the source with the strongest ROI/conversion signal.",
    )
    parser.add_argument("--output-dir", help="Directory for generated report files.")
    parser.add_argument("--target-roi-percent", type=float, default=DEFAULT_TARGET_ROI_PERCENT)
    parser.add_argument("--min-negative-cost", type=float, default=DEFAULT_MIN_NEGATIVE_COST)
    parser.add_argument("--min-negative-clicks", type=float, default=DEFAULT_MIN_NEGATIVE_CLICKS)
    parser.add_argument("--min-scale-conversions", type=float, default=DEFAULT_MIN_SCALE_CONVERSIONS)
    parser.add_argument("--min-scale-roi-percent", type=float, default=DEFAULT_MIN_SCALE_ROI_PERCENT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    csv_paths = [Path(path).expanduser().resolve() for path in args.csv_paths]

    source_rows: Dict[str, List[Row]] = {}
    source_dates: Dict[str, str] = {}
    for path in csv_paths:
        date_range, rows = read_google_ads_csv(path)
        source_rows[path.name] = rows
        source_dates[path.name] = date_range

    if args.primary:
        primary_path = Path(args.primary).name
        if primary_path not in source_rows:
            raise SystemExit(f"--primary must match one of the input file names. Got: {primary_path}")
        primary_source = primary_path
    else:
        primary_source = max(source_rows, key=lambda name: len(source_rows[name]))

    if args.scale_source:
        scale_path = Path(args.scale_source).name
        if scale_path not in source_rows:
            raise SystemExit(f"--scale-source must match one of the input file names. Got: {scale_path}")
        scale_source = scale_path
    else:
        source_summary_preview = summarize_sources(source_rows, source_dates)
        scale_source = max(
            source_rows,
            key=lambda name: (
                source_summary_preview[name]["roi_percent"],
                source_summary_preview[name]["conversions"],
                -source_summary_preview[name]["rows"],
            ),
        )

    primary_rows = source_rows[primary_source]
    scale_rows = source_rows[scale_source]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else Path("google-ads-mcp/reports") / f"search-terms-{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    source_summary = summarize_sources(source_rows, source_dates)
    term_groups = aggregate(primary_rows, lambda row: (row.search_term.lower(),))
    scale_term_groups = aggregate(scale_rows, lambda row: (row.search_term.lower(),))
    ngram_groups = build_ngram_groups(primary_rows)
    scale_ngram_groups = build_ngram_groups(scale_rows)

    positive_candidates = build_positive_candidates(
        scale_term_groups,
        min_conversions=args.min_scale_conversions,
        min_roi_percent=args.min_scale_roi_percent,
    )
    negative_candidates = build_negative_candidates(
        term_groups,
        min_cost=args.min_negative_cost,
        min_clicks=args.min_negative_clicks,
        target_roi_percent=args.target_roi_percent,
    )
    annotate_negative_conflicts(negative_candidates, positive_candidates)
    impression_waste = build_impression_waste(term_groups)
    ngram_winners = build_ngram_winners(scale_ngram_groups)
    ngram_waste = build_ngram_waste(
        ngram_groups,
        min_cost=max(args.min_negative_cost * 2, 10),
        target_roi_percent=args.target_roi_percent,
    )

    country_rollup = build_rollup(aggregate(primary_rows, lambda row: (row.country,)), ("country",))
    campaign_rollup = build_rollup(aggregate(primary_rows, lambda row: (row.campaign,)), ("campaign",))
    ad_group_rollup = build_rollup(
        aggregate(primary_rows, lambda row: (row.country, row.campaign, row.ad_group)),
        ("country", "campaign", "ad_group"),
    )
    campaign_type_rollup = build_rollup(aggregate(primary_rows, lambda row: (row.campaign_type,)), ("campaign_type",))
    scale_country_rollup = build_rollup(aggregate(scale_rows, lambda row: (row.country,)), ("country",))

    write_csv(output_dir / "positive_keyword_candidates.csv", positive_candidates)
    write_csv(output_dir / "negative_keyword_candidates.csv", negative_candidates)
    write_csv(output_dir / "impression_waste.csv", impression_waste)
    write_csv(output_dir / "ngram_winners.csv", ngram_winners)
    write_csv(output_dir / "ngram_waste.csv", ngram_waste)
    write_csv(output_dir / "country_summary.csv", country_rollup)
    write_csv(output_dir / "campaign_summary.csv", campaign_rollup)
    write_csv(output_dir / "ad_group_summary.csv", ad_group_rollup)
    write_csv(output_dir / "campaign_type_summary.csv", campaign_type_rollup)
    write_csv(output_dir / "scale_country_summary.csv", scale_country_rollup)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "primary_source": primary_source,
        "scale_source": scale_source,
        "inputs": [str(path) for path in csv_paths],
        "thresholds": {
            "target_roi_percent": args.target_roi_percent,
            "min_negative_cost": args.min_negative_cost,
            "min_negative_clicks": args.min_negative_clicks,
            "min_scale_conversions": args.min_scale_conversions,
            "min_scale_roi_percent": args.min_scale_roi_percent,
        },
        "source_summary": source_summary,
        "decision_counts": {
            "positive_keyword_candidates": len(positive_candidates),
            "negative_keyword_candidates": len(negative_candidates),
            "impression_waste": len(impression_waste),
            "ngram_winners": len(ngram_winners),
            "ngram_waste": len(ngram_waste),
            "countries": len(country_rollup),
            "campaigns": len(campaign_rollup),
            "ad_groups": len(ad_group_rollup),
        },
        "top_positive_candidates": top(positive_candidates, 25),
        "top_negative_candidates": top(negative_candidates, 25),
        "top_ngram_winners": top(ngram_winners, 25),
        "top_ngram_waste": top(ngram_waste, 25),
        "top_countries_by_cost": top(country_rollup, 25),
        "top_campaigns_by_cost": top(campaign_rollup, 25),
        "top_campaign_types": campaign_type_rollup,
        "top_scale_countries_by_profit_signal": top(scale_country_rollup, 25),
        "notes": [
            "ROI is interpreted as conversion value divided by cost, based on Google Ads CSV ROI percent.",
            "Profit is estimated as cost * ROI% - cost. If the export's ROI field is not revenue/cost, adjust strategy thresholds.",
            "Multiple input files are summarized separately. They are not added together because Google Ads exports may represent filtered views of the same date range.",
            "Primary source is used for waste/negative analysis. Scale source is used for positive keyword harvesting.",
        ],
    }
    write_json(output_dir / "summary.json", summary)

    strategy = build_strategy_markdown(
        output_dir=output_dir,
        primary_source=primary_source,
        scale_source=scale_source,
        source_summary=source_summary,
        positive_candidates=positive_candidates,
        negative_candidates=negative_candidates,
        ngram_winners=ngram_winners,
        ngram_waste=ngram_waste,
        country_rollup=country_rollup,
        campaign_rollup=campaign_rollup,
        impression_waste=impression_waste,
        target_roi_percent=args.target_roi_percent,
    )
    (output_dir / "strategy.md").write_text(strategy, encoding="utf-8")

    print(json.dumps({
        "status": "success",
        "output_dir": str(output_dir),
        "primary_source": primary_source,
        "decision_counts": summary["decision_counts"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
