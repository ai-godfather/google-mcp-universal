#!/usr/bin/env python3
"""Create a new offline evidence workspace for a Search launch run.

Never contacts Google Ads, a store, a browser or a paid provider, and never grants approval.
The run directory can live anywhere; keep it outside the plugin folder, which updates may replace.
"""
import argparse
import calendar
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

PHASES = (
    "inventory", "history", "research", "architecture", "landing", "creative",
    "owner_review", "delivery", "editor_review", "activation", "handoff",
)
APPROVALS = ("paid_research", "landing_changes", "paused_delivery", "activation", "bids", "optimization")
SUBDIRS = ("raw", "research", "landing", "review", "delivery", "ui")


def intervals(run_date):
    """16 calendar months ending yesterday, plus the last 90 and 30 complete days."""
    month = run_date.year * 12 + run_date.month - 1 - 16
    year, month = divmod(month, 12)
    month += 1
    start = date(year, month, min(run_date.day, calendar.monthrange(year, month)[1]))
    end = run_date - timedelta(days=1)
    return {
        "16_calendar_months": {"from": str(start), "to": str(end)},
        "90_days": {"from": str(end - timedelta(days=89)), "to": str(end)},
        "30_days": {"from": str(end - timedelta(days=29)), "to": str(end)},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="New directory for this run (must not exist)")
    parser.add_argument("--customer", required=True, help="Google Ads customer ID verified live (10 digits, not the MCC)")
    parser.add_argument("--currency", required=True, help="Account currency read live (ISO 4217, e.g. EUR)")
    parser.add_argument("--timezone", required=True, help="Account time zone read live (IANA, e.g. Europe/Berlin)")
    parser.add_argument("--date", type=date.fromisoformat, help="Account-local run date; default: today there")
    parser.add_argument("--products", type=Path, help="JSON array of selected products; see references/run-artifacts.md")
    parser.add_argument("--intent", choices=("prepare", "paused", "activate", "optimize"), default="prepare",
                        help="Desired work; recorded only, never a permission")
    args = parser.parse_args()
    customer = args.customer.replace("-", "")
    if not re.fullmatch(r"\d{10}", customer):
        parser.error("--customer must be the 10-digit customer ID (dashes allowed), not a placeholder")
    if not re.fullmatch(r"[A-Z]{3}", args.currency):
        parser.error("--currency must be an uppercase three-letter code")
    try:
        timezone = ZoneInfo(args.timezone)
        products = json.loads(args.products.read_text(encoding="utf-8")) if args.products else []
        if not isinstance(products, list) or not all(isinstance(p, dict) for p in products):
            raise ValueError("--products must contain a JSON array of product objects")
    except (ValueError, OSError, KeyError) as exc:
        parser.error(str(exc))
    run_date = args.date or datetime.now(timezone).date()
    manifest = {
        "schema_version": 1, "run_date": str(run_date), "intent": args.intent,
        "account": {"customer_id": customer, "currency": args.currency, "timezone": args.timezone},
        "history_intervals": intervals(run_date), "products": products,
        "approvals": {key: {"status": "not_recorded", "scope": {}, "evidence": []} for key in APPROVALS},
        "note": "Approval records document instructions the user actually gave; they never request or imply consent.",
    }
    checkpoint = {
        "schema_version": 1, "browser_session": None,
        "phases": {key: {"status": "pending", "reason": "", "evidence": []} for key in PHASES},
    }
    # Exclusive creation: a resumed or ambiguous run is never reset.
    try:
        args.run_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("Run directory already exists. Read its checkpoint and resume; do not overwrite.")
    for name in SUBDIRS:
        (args.run_dir / name).mkdir()
    for name, data in (("manifest.json", manifest), ("checkpoint.json", checkpoint)):
        (args.run_dir / name).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.run_dir / "research" / "cost-ledger.json").write_text(json.dumps({
        "schema_version": 1, "authorized_usd_cap": None, "authorized_request_cap": None, "requests": [],
        "note": "Null caps never authorize spending. Each request records fingerprint, intent, task_id, actual charge and receipt.",
    }, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"created": str(args.run_dir.resolve()), "intervals": intervals(run_date),
                      "provider_calls": 0, "approvals_granted": 0}, ensure_ascii=False))


if __name__ == "__main__":
    main()
