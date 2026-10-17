#!/usr/bin/env bash
# One-time local dev setup: create .env, virtualenv, and data directories.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example (RUN_MODE=local by default -- no S3/Spark cluster needed)."
else
  echo ".env already exists, leaving it alone."
fi

# Pick an interpreter in the 3.11-3.13 range explicitly, rather than trusting
# whatever `python3` happens to resolve to. requirements.txt pins
# pyspark>=3.5,<4.0, and that PySpark line only became compatible with
# Python 3.14 in Spark 4.1 (SPARK-51169) -- on a bare `python3` that resolves
# to 3.14+ (e.g. a brand-new machine where 3.14 is the only/default Python),
# `pip install -r requirements-dev.txt` will either fail outright or, worse,
# install "successfully" and then fail at runtime inside Spark's pickling/
# serialization layer with a confusing error. If you deliberately want to run
# on Spark 4.1+/Python 3.14 instead, see the README's
# "Running on Python 3.14 / Spark 4.x" note first.
PYTHON_BIN=""
for candidate in python3.11 python3.12 python3.13 python3; do
  if command -v "$candidate" >/dev/null 2>&1; then
    ver="$("$candidate" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
    case "$ver" in
      3.11|3.12|3.13) PYTHON_BIN="$candidate"; break ;;
    esac
  fi
done

if [ -z "$PYTHON_BIN" ]; then
  echo "ERROR: no Python 3.11-3.13 interpreter found on PATH." >&2
  echo "  requirements.txt pins pyspark>=3.5,<4.0, which is not verified against" >&2
  echo "  Python 3.14+ (only Spark 4.1+ officially added 3.14 support)." >&2
  echo "  Install Python 3.11 alongside whatever you already have, e.g.:" >&2
  echo "    macOS:   brew install python@3.11" >&2
  echo "    Ubuntu:  sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt install python3.11 python3.11-venv" >&2
  echo "    Windows: winget install Python.Python.3.11   (then use 'py -3.11' instead of 'python3')" >&2
  echo "  Then re-run this script." >&2
  exit 1
fi
echo "Using $PYTHON_BIN ($($PYTHON_BIN -c 'import sys; print(sys.version.split()[0])')) for the virtualenv."

"$PYTHON_BIN" -m venv .venv
. .venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements-dev.txt -q

mkdir -p data/raw data/bronze data/silver data/gold data/quarantine

echo "Done. Activate the environment with:  source .venv/bin/activate"
echo "Then run the sample pipeline with:    ./scripts/run_pipeline_local.sh"
