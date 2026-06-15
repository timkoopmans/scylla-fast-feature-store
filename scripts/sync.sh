#!/usr/bin/env bash
# Sync this webinar's repo to the demo host (set FS_HOST). Data + cluster live there.
set -euo pipefail
HOST="${FS_HOST:-localhost}"
DEST="${FS_DEST:-scylla-fast-feature-store}"
SRC="$(cd "$(dirname "$0")/.." && pwd)/"
ssh "$HOST" "mkdir -p '$DEST'"
rsync -az --delete \
  --exclude '.venv' --exclude '.git' --exclude '__pycache__' \
  --exclude '*.pyc' --exclude 'sample_keys.csv' --exclude 'results' \
  --exclude 'data' \
  "$SRC" "$HOST:$DEST/"
echo "synced $SRC -> $HOST:$DEST/"
