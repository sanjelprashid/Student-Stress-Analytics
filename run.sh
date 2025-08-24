#!/usr/bin/env bash
set -euo pipefail
python etl/extract/execute.py data/raw data/extract
python etl/transform/execute.py data/extract data/transform
# Example load (edit creds):
# python etl/load/execute.py data/transform postgres mypass localhost postgres 5432