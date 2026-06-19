#!/usr/bin/env bash
# Fetch a standard ann-benchmarks dataset and convert it to the .csb binary the
# Go bench harness reads. One-time, offline. Idempotent: skips work already done.
#
# The conversion needs python3 + numpy + h5py (pip install h5py). The Go harness
# itself has NO python/HDF5 dependency — it only reads the produced .csb file.
#
# Usage:
#   cmd/bench/fetch_dataset.sh [dataset]
# dataset defaults to glove-25-angular. Other ann-benchmarks names work too
# (e.g. sift-128-euclidean, fashion-mnist-784-euclidean) as long as they expose
# train/test/neighbors with an angular|euclidean distance attr.
set -euo pipefail

DATASET="${1:-glove-25-angular}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
OUT="$REPO/testdata/ann"
HDF5="$OUT/$DATASET.hdf5"
CSB="$OUT/$DATASET.csb"
PY="${PYTHON:-python3}"

mkdir -p "$OUT"

if [[ -f "$CSB" ]]; then
  echo "already converted: $CSB"
  exit 0
fi

if [[ ! -f "$HDF5" ]]; then
  echo "downloading $DATASET.hdf5 ..."
  curl -fL --retry 3 -o "$HDF5" "http://ann-benchmarks.com/$DATASET.hdf5"
fi

echo "converting -> $CSB ..."
"$PY" "$HERE/convert_hdf5.py" "$HDF5" "$CSB"
echo "done: $CSB"
