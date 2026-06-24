# CorkScrewDB Benchmarks

## TL;DR

CorkScrewDB is **not** built to win the ANN recall/QPS leaderboard, and these numbers
don't pretend it does. It is a **quantization-first, embedded, versioned** vector DB.
The axis where it is genuinely competitive **and** directly comparable is **footprint**:
its 2-bit codes are **8–16× smaller than float32**. It trades leaderboard recall/QPS for
tiny storage, in-process operation (no server, no cgo), and full version history. Its
inner-product scorer is strongest on **cosine/angular** data — the dominant case for
text-embedding RAG.

If you need the last few points of recall@10 at maximum QPS on a billion-scale corpus,
use a dedicated ANN engine (hnswlib, FAISS, ScaNN, Milvus). If you want a single Go
binary that embeds a *versioned* vector store with a tiny on-disk footprint, this is a
different point in the design space.

## Why this is a different benchmark

CorkScrewDB optimizes for things the standard leaderboards don't measure, and is built
differently from the systems on them:

- **Embedded / in-process.** No server, no container, no network hop, no cgo — it's a Go
  library. The fair comparison is to in-process libraries (hnswlib, FAISS, ScaNN), **not**
  to networked/managed DBs (the VectorDBBench cohort).
- **Quantization-first.** The default is **2-bit** TurboQuant codes. Storage is the
  primary objective; recall is a tunable dial (`WithBitWidth`).
- **Versioned.** Every vector keeps full history under hybrid logical clocks, with
  point-in-time `At()` views. No ANN leaderboard measures this — it's extra work we carry.
- **Recompute cold tier (v0.4.0).** Optionally drop raw vectors entirely and re-derive
  them from stored text for exact rerank (see the main README). Footprint goes lower still.
- **Inner-product scorer.** Strongest on cosine/angular (normalized) vectors — i.e. text
  embeddings — and weaker on raw Euclidean data.

## Our measured numbers

Flat index (exhaustive scan over quantized codes — *pure quantization fidelity*, no ANN
approximation), quantized-only storage (`WithoutRawStore`), `recall@10` vs exact
ground-truth recomputed by brute force over the subset.

- **Hardware:** Intel Core Ultra 9 285 (20 threads), WSL2, Go 1.26.
- **Base:** 12,000-vector subset of each ann-benchmarks dataset, 500 queries.
- `code B/vec` = packed code size (`ceil(dim·(b-1)/8) + ceil(dim/8) + 4`).
- `disk B/vec` = durable snapshot bytes per vector (code + per-record framing; the fixed
  framing ~77 B amortizes away at scale and at higher dimensions).

| dataset | metric | bits | recall@10 | qps_serial | qps_parallel | code B/vec | disk B/vec |
|---|---|---|---|---|---|---|---|
| glove-25 | angular | 2 | 0.223 | 2628 | 32467 | 12 | 89 |
| glove-25 | angular | 4 | 0.590 | 471 | 6160 | 18 | 95 |
| glove-25 | angular | 8 | **0.936** | 282 | 3295 | 30 | 107 |
| sift-128 | euclidean | 2 | 0.243 | 2164 | 29555 | 36 | 113 |
| sift-128 | euclidean | 4 | 0.528 | 117 | 1379 | 68 | 145 |
| sift-128 | euclidean | 8 | 0.877 | 58 | 514 | 132 | 209 |
| fashion-mnist-784 | euclidean | 2 | 0.283 | 517 | 6469 | 200 | 277 |
| fashion-mnist-784 | euclidean | 4 | 0.415 | 19 | 193 | 396 | 473 |
| fashion-mnist-784 | euclidean | 8 | 0.502 | 10 | 90 | 788 | 865 |

Reading the table:

- **Recall climbs steeply with bit width.** 2-bit is storage-optimized and lossy; 8-bit
  recovers most neighbors on the metric we're built for.
- **Cosine/angular is our strength.** glove-25 (angular) reaches **0.94 @ 8-bit** — and
  text-embedding RAG queries are virtually always cosine/normalized, so this is the case
  that matters in practice.
- **Raw high-dimensional Euclidean is our weak case.** fashion-mnist (784-dim image
  vectors, L2) tops out around 0.50 even at 8-bit. The IP scorer is a poor fit for L2 on
  this kind of data; if you have a heavy raw-Euclidean workload, this is the honest floor.
- **QPS here is flat full-scan over a 12 K base** — it is *not* an ANN number and is not
  comparable to the leaderboard QPS below (those are approximate-NN over 1 M+ vectors). For
  ANN QPS use HNSW (`WithIndexType(IndexHNSW)`; see the README's HNSW table).

## Footprint — the directly comparable axis

Unlike recall/QPS (hardware- and base-size-dependent), **bytes per vector are exact and
base-size-independent**. This is where the comparison is honest and where CorkScrewDB wins.

| dim | float32 (`4·d`) | SQ8 (`d`) | PQ M=16 (`16`) | CorkScrewDB 2-bit | 4-bit | 8-bit |
|---|---|---|---|---|---|---|
| 25 (glove) | 100 | 25 | 16 | **12** | 18 | 30 |
| 128 (sift) | 512 | 128 | 16 | **36** | 68 | 132 |
| 768 (Cohere) | 3072 | 768 | 16 | ~196 | ~381 | ~836 |
| 784 (f-mnist) | 3136 | 784 | 16 | **200** | 396 | 788 |

- **vs float32** (the default storage for hnswlib / FAISS-Flat / most graph indexes):
  CorkScrewDB 2-bit is **8.3× smaller at 25-dim, 14× at 128-dim, 16× at 784-dim**.
- **vs SQ8** (scalar 8-bit, the common "lite" mode in Qdrant/Milvus, 1 byte/dim): our 2-bit
  is **~2–4× smaller**; our 8-bit is essentially SQ8-class (≈1 byte/dim). This matches
  Qdrant's own published compression classes (their docs list 2-bit ≈ 16× vs float32).
- **vs PQ** (FAISS IVFPQ — the most compact mainstream option): PQ is dimension-independent
  (`M` bytes), so at **high dimension PQ is more compact** than our codes (16 B vs ~200 B at
  784-dim). We trade that for **no codebook training**, exact inner-product *within* the
  code space, and a pure-Go/embedded implementation. At very low dimension our codes are
  smaller than PQ (12 B vs 16 B at 25-dim).

Put in collection terms: 1 M SIFT vectors as float32-in-a-graph (e.g. hnswlib) is
**~650 MB**; the same 1 M vectors as CorkScrewDB 2-bit codes are **~36 MB** (~110 MB on
disk with framing) — roughly an order of magnitude smaller, before the recompute cold tier
removes the raw store entirely.

## How the field compares (published — NOT the same hardware or base size)

These are published [ann-benchmarks](https://ann-benchmarks.com) results — in-process
libraries, the right cohort to compare an embedded engine against. **They are not directly
comparable to our numbers** (see Caveats): different hardware (AWS `r6i.16xlarge` Xeon vs
our desktop WSL2 core), different base size (full 1 M+ datasets vs our 12 K subset), and a
recall/QPS *Pareto curve* reduced to one operating point. Index sizes are the
authors' approximate per-algorithm figures.

Representative frontier leaders (highest QPS at recall@10 ≥ 0.90, single-CPU serial):

| dataset (full base) | system | recall@10 | QPS | index size |
|---|---|---|---|---|
| fashion-mnist-784 (60 K) | hnswlib | 0.955 | 7,870 | ~187 MB (~3,115 B/vec) |
| fashion-mnist-784 (60 K) | glass | 0.953 | 19,194 | ~95 MB |
| glove-25 (1.18 M) | scann | 0.907 | 23,436 | ~166 MB |
| glove-25 (1.18 M) | hnswlib | 0.915 | 4,164 | ~305–481 MB |
| sift-128 (1 M) | hnswlib | 0.901 | 7,935 | ~647 MB (~647 B/vec) |
| sift-128 (1 M) | faiss-ivfpqfs (PQ) | 0.913 | 5,458 | ~558 MB |

The honest read: dedicated ANN engines deliver **recall@10 ≥ 0.90 at thousands of QPS on
million-scale corpora**, with indexes of **~150 MB to ~2 GB**. CorkScrewDB targets a
different point — **a fraction of that footprint, embedded and versioned**, at modest recall
by default (2-bit) rising to comparable *fidelity* at 8-bit on cosine data. We do not claim
to beat them on recall or QPS, and our subset numbers above must not be read as a head-to-head
win on recall (a smaller base inflates recall). The footprint table is the comparison we
stand behind.

> VectorDBBench (Zilliz) numbers are deliberately omitted from a head-to-head: it measures
> networked/managed DBs at recall@**100** on Cohere/OpenAI 768–1536-dim corpora normalized to
> a $1,000/month spend — a different metric, cohort, and cost model than an embedded library.

## What no leaderboard entry has

- **Embedded, pure-Go, single binary** — no server, no container, no cgo, no Python runtime.
- **Versioned** — full per-vector history (HLC) and point-in-time `At()` views.
- **Recompute cold tier (v0.4.0)** — drop the raw store and re-derive vectors from text for
  exact rerank, with a per-candidate drift-check.
- **Pluggable Scorer seam** — optional CUDA GPU scoring (build tag `cuda`).

## Reproduce

One-time dataset prep (needs `python3` + `h5py`; downloads + converts to a self-describing
`.csb`, both git-ignored):

```sh
cmd/bench/fetch_dataset.sh glove-25-angular
cmd/bench/fetch_dataset.sh sift-128-euclidean
cmd/bench/fetch_dataset.sh fashion-mnist-784-euclidean
```

Run the subset sweep used above:

```sh
go run ./cmd/bench -data testdata/ann/glove-25-angular.csb        -maxbase 12000 -maxquery 500 -bits 2,4,8 -skiphnsw
go run ./cmd/bench -data testdata/ann/sift-128-euclidean.csb      -maxbase 12000 -maxquery 500 -bits 2,4,8 -skiphnsw
go run ./cmd/bench -data testdata/ann/fashion-mnist-784-euclidean.csb -maxbase 12000 -maxquery 500 -bits 2,4,8 -skiphnsw
```

Drop `-skiphnsw` for HNSW, raise `-maxbase 0` for the full dataset (build is fsync-bound and
slow at scale), and sweep `-efsearch`/`-m` for the HNSW recall/QPS frontier. See
`cmd/bench/README.md` for all flags.

## Caveats (read before quoting any number)

1. **Hardware mismatch.** Our numbers: Intel Core Ultra 9 285 / WSL2 (one desktop core,
   virtualized I/O). ann-benchmarks: AWS `r6i.16xlarge` server Xeon, single-CPU serial.
   Absolute QPS is not comparable across these.
2. **Base size differs.** Our recall/QPS are on a **12 K subset**; the leaderboard is on the
   **full** datasets (60 K – 1.18 M). A smaller base inflates recall and QPS — do not read
   the two recall columns as head-to-head.
3. **Pareto operating point.** Each leaderboard system is a recall/QPS *curve*; we quote the
   best QPS at recall ≥ 0.90. Change the threshold and the ranking shifts.
4. **Flat vs ANN.** Our table is *flat* (exhaustive scan over codes) — pure quantization
   fidelity, not an ANN index. Leaderboard QPS is approximate-NN. Use HNSW for ANN QPS.
5. **Serial vs batched.** All numbers here and on ann-benchmarks are single-query serial.
6. **Embedded vs server.** Compare CorkScrewDB to in-process libraries, not to managed DBs.
7. **Footprint is the comparable axis.** Bytes/vector are exact and base-independent; that
   table is the one cross-system claim we stand behind.
