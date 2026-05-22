#!/usr/bin/env bash
# Record asciinema demos and convert to GIF for README embeds.
# Requires: asciinema, agg
#   brew install asciinema agg
#
# Usage: ./demos/record_gifs.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ASSETS="$ROOT/docs/assets"

mkdir -p "$ASSETS"
cd "$ROOT"

echo "==> Recording pipeline demo..."
PYTHONPATH=. asciinema rec \
  --overwrite \
  --cols 88 \
  --rows 30 \
  --title "pipeline demo" \
  "$ASSETS/pipeline.cast" \
  -c "python3 demos/demo_pipeline.py"

echo "==> Recording infra demo..."
PYTHONPATH=. asciinema rec \
  --overwrite \
  --cols 88 \
  --rows 36 \
  --title "infra demo" \
  "$ASSETS/infra.cast" \
  -c "python3 demos/demo_infra.py"

echo "==> Recording observability demo..."
PYTHONPATH=. asciinema rec \
  --overwrite \
  --cols 88 \
  --rows 36 \
  --title "observability demo" \
  "$ASSETS/observability.cast" \
  -c "python3 demos/demo_observability.py"

echo "==> Converting to GIF..."
for cast in pipeline infra observability; do
  agg \
    --theme monokai \
    --font-size 14 \
    --speed 1.5 \
    "$ASSETS/${cast}.cast" \
    "$ASSETS/${cast}.gif"
  echo "    → docs/assets/${cast}.gif"
done

echo "Done. Embed with: ![demo](docs/assets/pipeline.gif)"
