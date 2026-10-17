#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== Lint =="
ruff check src/ dags/ spark/ tests/
echo "== Tests =="
pytest -v
