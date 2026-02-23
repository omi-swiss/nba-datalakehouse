#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../data/landing"

STAMP="$(date +%F)"
OUTDIR="../../data/zips"
mkdir -p "$OUTDIR"

ZIPPATH="${OUTDIR}/landing_${STAMP}.zip"

rm -f "$ZIPPATH"
zip -r "$ZIPPATH" . >/dev/null

ls -lah "$ZIPPATH"
