# CorkScrewDB ANN benchmark

A reproducible, Go-native benchmark over a standard [ann-benchmarks](http://ann-benchmarks.com)
dataset. It reports **recall@10 vs exact ground-truth**, **QPS** (serial + parallel),
**index build time**, and **bytes/vector** for the flat quantized index and HNSW.

The Go harness has **no Python/HDF5 dependency** — it reads a small self-describing
binary (`.csb`). A one-time offline step downloads the HDF5 dataset and converts it.

## 1. Fetch + convert the dataset (one-time, offline)

Needs `python3` + `numpy` + `h5py` (`pip install h5py`):

```sh
cmd/bench/fetch_dataset.sh glove-25-angular
```

This downloads `glove-25-angular.hdf5` (~122 MB) and writes
`testdata/ann/glove-25-angular.csb`. Both are git-ignored and never committed.

`glove-25-angular` is an **angular (cosine)** dataset, which matches CorkScrewDB's
inner-product scorer exactly once vectors are L2-normalized (done in the converter).
It ships exact ground-truth neighbors, so recall@10 is measured against the dataset
authors' own exact neighbors — no recomputation when the full base is used.

## 2. Run

Full base (1,183,514 vectors — the dataset-provided ground-truth applies verbatim;
build is fsync-bound and takes a while at this scale):

```sh
go run ./cmd/bench -data testdata/ann/glove-25-angular.csb -maxbase 0
```

Documented 100K subset (re-runnable in minutes; exact ground-truth is recomputed
by brute force over the subset, so recall stays honest):

```sh
go run ./cmd/bench -data testdata/ann/glove-25-angular.csb -maxbase 100000 -maxquery 1000
```

Sweep bit widths (recall climbs with bits; the default and headline is 2):

```sh
go run ./cmd/bench -data testdata/ann/glove-25-angular.csb -maxbase 100000 -bits 2,4,8
```

## Flags

| flag | default | meaning |
|------|---------|---------|
| `-data` | `testdata/ann/glove-25-angular.csb` | dataset path |
| `-bits` | `2` | comma-separated quantizer bit widths |
| `-seed` | `12345` | quantizer seed (reproducibility) |
| `-maxbase` | `0` | cap base vectors (0 = full; subset recomputes exact ground-truth) |
| `-maxquery` | `1000` | cap query vectors (0 = all 10,000) |
| `-k` | `10` | recall@k |
| `-parallel` | `NumCPU` | workers for the parallel-QPS measure |
| `-efsearch` / `-efconstruction` / `-m` | `100` / `200` / `16` | HNSW params |
| `-rebuild` | `false` | build HNSW via `RebuildIndex` (default params) instead of an HNSW-typed collection |
| `-skiphnsw` | `false` | flat only |

## Reported columns

- `recall@10` — fraction of the exact top-10 neighbors recovered, averaged over queries.
- `qps_serial` — single-thread queries/sec.
- `qps_parallel` — queries/sec across `-parallel` workers.
- `build` — index build time (insert loop; for HNSW the graph is built during insertion).
- `resident B/vec` — live Go heap attributable to the whole collection (index + history) per vector.
- `code B/vec` — packed quantized code size per vector (`ceil(dim*(b-1)/8) + ceil(dim/8) + 4`).
- `disk B/vec` — durable snapshot file size per vector (quantized-only collection).

## Notes / honesty

- `glove-25-angular` is a genuinely hard low-dimensional angular set. At the default
  bit width 2 the flat (exact-brute-force-over-quantized-codes) recall is modest;
  it rises sharply with bit width (see the `-bits 2,4,8` sweep). The flat number is
  the *pure quantization fidelity* — no approximation beyond the codes themselves.
- HNSW shares the same quantized codes, so its recall is upper-bounded by the flat
  recall at the same bit width; HNSW trades a little recall for large QPS gains.
- Build time is dominated by the WAL's fsync-per-write durability default.
