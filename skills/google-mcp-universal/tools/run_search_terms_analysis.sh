#!/usr/bin/env bash
# Run offline search terms analysis on Google Ads CSV exports.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANALYZER="${SCRIPT_DIR}/../search_terms_analyzer.py"
DATE_TAG="$(date +%Y-%m-%d)"
OUT="${SCRIPT_DIR}/../../reports/search-terms-${DATE_TAG}"

FULL="${1:-}"
CONV="${2:-}"

if [[ -z "${FULL}" ]]; then
  echo "Usage: $0 <full_search_terms.csv> [converting_only.csv]"
  echo "Example:"
  echo "  $0 ~/Downloads/Search\\ terms\\ report\\ \\(1\\).csv ~/Downloads/Search\\ terms\\ report.csv"
  exit 1
fi

ARGS=(--full "${FULL}" --output-dir "${OUT}")
if [[ -n "${CONV}" ]]; then
  ARGS+=(--converting "${CONV}")
fi

python3 "${ANALYZER}" "${ARGS[@]}"
echo "Open: ${OUT}/REPORT.md"
