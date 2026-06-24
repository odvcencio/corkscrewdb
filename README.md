# CorkScrewDB

CorkScrewDB is a distributed, versioned vector database in pure Go.

- Text-in and vector-in collection APIs
- Version history per ID with hybrid logical clocks
- TurboQuant-backed quantized flat and HNSW search
- Sparse vectors and hybrid dense+sparse retrieval with RRF or weighted fusion
- Append-only WAL persistence with snapshot recovery
- Quantized index persistence (`.tqi`) and HNSW graph persistence (`graph.hnsw`)
- Content-addressed raw vector store (blake3-keyed `.rvs` segments)
- Recompute cold tier: drop the raw store and re-derive vectors from text for exact rerank (LEANN-style), with a per-candidate drift-check
- Embedding-space config enforcement
- Metadata filters and point-in-time collection views with LRU view cache
- gRPC transport with `Connect(...)` and `Serve(...)`
- Embedded federation with hash-based write routing and fan-out search
- Explicit shard metadata with persisted ownership ranges
- 2PC cluster rebalance with freeze→barrier→pull + recovery + force-abort
- Live gRPC WAL streaming replication with snapshot catch-up
- Cold storage offload (sealed WAL segments + snapshots)
- Pluggable Scorer seam with optional CUDA GPU scorer (build tag `cuda`)
- Standalone server binary (`cmd/corkscrewdb`)

## Agent Skill

Agents working with CorkScrewDB should use the [using-corkscrewdb](https://github.com/odvcencio/m31labs-skills/blob/main/skills/using-corkscrewdb/SKILL.md) skill.

## Status

`v0.4.0` — the recompute cold tier has shipped: `WithRecomputeRawFromText()` drops the raw store and re-derives vectors from stored text for exact rerank with a per-candidate drift-check, `WithRerank(c)` controls overfetch depth, `ReQuantize` re-quantizes a collection's full history at new parameters, and backend-fingerprint enforcement guards against silently-degraded rerank on reopen. This builds on v0.3.0: sparse vectors, hybrid SearchMulti with RRF/weighted fusion, real HNSW (RNG/hub-protected pruning, O(degree) tombstone delete, build-from-codes), content-addressed raw vector store, 2PC cluster rebalance, pluggable Scorer seam (optional CUDA GPU scorer), and LRU point-in-time view cache.

## Install

```bash
go get github.com/odvcencio/corkscrewdb
```

## Quick Start

```go
package main

import (
	"fmt"
	"log"

	"github.com/odvcencio/corkscrewdb"
)

func main() {
	db, err := corkscrewdb.Open("./example.csdb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("documents", corkscrewdb.WithBitWidth(2))
	if err := coll.Put("doc-1", corkscrewdb.Entry{
		Text:     "the auth module uses WebAuthn passkeys",
		Metadata: map[string]string{"source": "review"},
	}); err != nil {
		log.Fatal(err)
	}

	results, err := coll.Search("passkeys", 5, corkscrewdb.Filter("source", "review"))
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		fmt.Println(result.ID, result.Score, result.Text)
	}
}
```

## Embeddings

`Open()` works out of the box with a built-in deterministic text embedding provider. For production embedding quality, pass your own provider with `corkscrewdb.WithProvider(...)`.

```go
db, err := corkscrewdb.Open("./prod.csdb", corkscrewdb.WithProvider(myProvider))
```

Embedding config is persisted in `manifest.json`. Reopening a database with a different embedding space is rejected to keep search results coherent.

## Sparse Vectors and Hybrid Search

Enable the sparse channel on a collection with `WithSparse()`. Each `Entry` or `PutVector` call can carry a `SparseVector` alongside the dense embedding. `SparseVector.Indices` must be sorted ascending and unique; `Values` is parallel to `Indices`.

```go
coll := db.Collection("docs",
    corkscrewdb.WithBitWidth(8),
    corkscrewdb.WithSparse(),
)

// Write with a sparse channel alongside the dense text embedding.
if err := coll.Put("doc-1", corkscrewdb.Entry{
    Text: "WebAuthn passkeys replace passwords",
    Sparse: &corkscrewdb.SparseVector{
        Indices: []uint32{42, 137, 512},
        Values:  []float32{0.8, 0.6, 0.4},
    },
}); err != nil {
    log.Fatal(err)
}

// Hybrid search: dense text query fused with sparse query vector.
results, err := coll.SearchMulti(corkscrewdb.MultiQuery{
    Text: "passkeys",
    Sparse: &corkscrewdb.SparseVector{
        Indices: []uint32{42, 137},
        Values:  []float32{0.9, 0.5},
    },
    Fusion: corkscrewdb.RRFFusion{K: 60}, // default when Fusion is nil
}, 10)
```

`MultiQuery.Fusion` selects how the two ranked lists are combined:

- `RRFFusion{K: 60}` — Reciprocal Rank Fusion (default; `K: 0` also means 60).
- `WeightedFusion{Dense: 0.7, Sparse: 0.3}` — min-max-normalized linear combination.

`CollectionView.SearchMulti` provides the same hybrid search against a point-in-time view.

## HNSW

Use `WithIndexType(IndexHNSW)` at collection creation to enable approximate nearest-neighbor search. Custom parameters are set at creation time with `WithHNSWParams`; these are persisted in the manifest.

```go
coll := db.Collection("vectors",
    corkscrewdb.WithBitWidth(8),
    corkscrewdb.WithIndexType(corkscrewdb.IndexHNSW),
    corkscrewdb.WithHNSWParams(corkscrewdb.HNSWParams{
        M:              32,
        EfConstruction: 400,
        EfSearch:       100,
    }),
)
```

To switch an existing flat collection to HNSW after the fact:

```go
if err := coll.RebuildIndex(corkscrewdb.IndexHNSW); err != nil {
    log.Fatal(err)
}
```

Note: `WithHNSWParams` is honored when set at collection creation. `RebuildIndex` uses the default params (`M=16`, `EfConstruction=200`, `EfSearch=50`).

## Raw Vector Store

By default, collections persist raw float32 vectors in a blake3-keyed content-addressed store (`.rvs` segments alongside the WAL). This enables replication to pull raw vectors by hash and supports operations that need the original embedding.

To opt out and store only quantized codes (smaller on-disk footprint, no raw retrieval):

```go
coll := db.Collection("compact",
    corkscrewdb.WithBitWidth(4),
    corkscrewdb.WithoutRawStore(),
)
```

`WithoutRawStore()` replaces the old `WithQuantizedOnlyPersistence()` / `WithVectorStorage(VectorStorageQuantizedOnly)` options, which are no longer present.

## Recompute Cold Tier (exact rerank without the raw store)

For corpora where storing raw float vectors is too expensive but exact-rerank accuracy still matters, `WithRecomputeRawFromText()` drops the raw store entirely and re-derives raw vectors from each entry's stored text on demand. Searches overfetch quantized candidates and exact-rerank them against the recomputed vectors, with a per-candidate drift-check: if a recomputed vector does not reproduce the stored quantized code, that candidate falls back to its quantized score. A ranking is therefore never wrong — at worst it degrades to codes-only.

This mode requires a deterministic embedding provider (collection creation is rejected otherwise, and the silent built-in fallback is refused when a real provider was intended). Set the rerank depth with `WithRerank(c)`: searches overfetch `c × k` candidates before reranking; depth ≤ 1 is plain codes-only. Recompute mode is mutually exclusive with `WithoutRawStore()`, and `PutVector` / `PutMultiVector` without text are rejected (text is the source of truth).

```go
coll := db.Collection("notes",
    corkscrewdb.WithBitWidth(2),
    corkscrewdb.WithRecomputeRawFromText(),
    corkscrewdb.WithRerank(8),
    corkscrewdb.WithProvider(myDeterministicProvider), // required: must be deterministic
)

// Every entry carries text — recompute re-derives the vector from it.
if err := coll.Put("n-1", corkscrewdb.Entry{Text: "WebAuthn passkeys replace passwords"}); err != nil {
    log.Fatal(err)
}

// Search overfetches 8×k candidates and exact-reranks them.
results, _ := coll.Search("passkeys", 5)
```

Point-in-time views and `SearchMulti` rerank the dense channel the same way. On reopen, a recompute collection refuses to open if the embedding backend fingerprint changed (`ErrRecomputeBackendMismatch`), preventing a silently-degraded rerank.

To re-quantize an entire collection's history at new parameters (for example, raising the bit width for better recall), call `ReQuantize`. It recomputes and re-quantizes every version from text and makes the new codes durable via a fresh snapshot:

```go
// Re-derive every version's vector from text and re-quantize at 8-bit.
if err := coll.ReQuantize(8, newSeed); err != nil {
    log.Fatal(err)
}
```

`ReQuantize` aborts with `ErrEmbedderDriftDetected` if the provider no longer reproduces the stored codes (the embedder has drifted); pass `WithAllowReQuantizeDrift()` to re-quantize anyway.

## Choosing Bit Width

The default bit width is 2. 2-bit quantization minimizes on-disk and in-memory footprint but is lossy, especially at lower embedding dimensions. Use 4–8 bit for recall-critical workloads or for low-dimensional embeddings.

| Bit width | glove-25 recall@10 | sift-128 recall@10 | Notes |
|-----------|-------------------|--------------------|-------|
| 2-bit     | 0.13              | 0.19               | Storage-optimized; best at higher dimensions |
| 4-bit     | 0.47              | 0.47               | Good balance of size and recall |
| 8-bit     | 0.91              | 0.85               | Near-exact recall at standard benchmark dims |

Use 4–8 bit for recall-critical or low-dimensional embeddings. 2-bit is storage-optimized and works best at higher dimensions (recall climbs sharply with both bit width and embedding dimension).

## Remote Mode

`Connect(...)` works for remote access using the same collection API over gRPC:

```go
db, err := corkscrewdb.Connect("127.0.0.1:4040", corkscrewdb.WithToken("agent-token-xxx"))
```

Expose an embedded DB over TCP with:

```go
if err := db.ListenAndServe("127.0.0.1:4040"); err != nil {
    log.Fatal(err)
}
```

## Embedded Federation

Embedded nodes can also work with configured peers:

```go
db, err := corkscrewdb.Open(
    "./vectors.csdb",
    corkscrewdb.WithPeers("corkscrewdb-0.corkscrewdb.svc:4040"),
    corkscrewdb.WithToken("agent-token-xxx"),
)
```

Current behavior:

- text and vector searches fan out across the local node and configured peers
- writes and deletes route to a hash-selected owner across the local node plus peers
- history lookups route to the owning node

For explicit ownership, persist shard ranges instead of relying on peer-list hashing:

```go
db, err := corkscrewdb.Open(
    "./vectors.csdb",
    corkscrewdb.WithPeers("node-b:4040"),
    corkscrewdb.WithShards(
        corkscrewdb.ShardAssignment{ID: "shard-a", Owner: corkscrewdb.LocalShardOwner, Start: 0, End: (^uint64(0)) / 2},
        corkscrewdb.ShardAssignment{ID: "shard-b", Owner: "node-b:4040", Start: (^uint64(0))/2 + 1, End: ^uint64(0)},
    ),
)
```

When `WithShards(...)` is present, routed writes and point ownership come from the persisted shard ranges. `WithPeers(...)` remains the legacy fallback and the remote seed list for shard owners.

## Rebalancing

Shard layouts can be updated in place with data handoff. `RebalanceShards(...)` is the single-node form when you want to drive the phases yourself:

```go
err := db.RebalanceShards(
    corkscrewdb.ShardAssignment{ID: "shard-a", Owner: corkscrewdb.LocalShardOwner, Start: 0, End: (^uint64(0)) * 3 / 4},
    corkscrewdb.ShardAssignment{ID: "shard-b", Owner: "node-b:4040", Start: (^uint64(0))*3/4 + 1, End: ^uint64(0)},
)
```

For a lost-write-safe cluster-wide cutover, use `OrchestrateRebalance` with an explicit `WithShards` layout. This runs a two-phase-commit protocol (freeze→barrier→pull, durable decide, prune) across the local node and all reachable peers:

```go
err := db.OrchestrateRebalance(
    corkscrewdb.ShardAssignment{ID: "shard-a", Owner: "node-a:4040", Start: 0, End: (^uint64(0)) * 3 / 4},
    corkscrewdb.ShardAssignment{ID: "shard-b", Owner: "node-b:4040", Start: (^uint64(0))*3/4 + 1, End: ^uint64(0)},
)
```

The 2PC protocol freezes writes to keys being moved, waits for a quorum barrier, pulls data from old owners, durably records the decision, flips routing, and prunes handed-off data. Crash recovery and force-abort are supported; the coordinator must have `WithShards` configured for the lost-write-safe path.

## Replication

WAL entries stream from primary to followers over gRPC. Followers use live entry streams when the transport supports it and fall back to polling otherwise. New followers still catch up via snapshot transfer + WAL tail replay before switching to live updates. In v0.3.0, the streamer is rebuilt from WAL on restart, and replication can pull raw vectors by hash from the raw store.

## Cold Storage Offload

Sealed WAL segments and snapshots push to a configurable backend on a schedule. A filesystem backend ships for testing; additional backends are selectable via build tags.

## Server Binary

```bash
go build ./cmd/corkscrewdb/
./corkscrewdb -data ./my-data -addr 0.0.0.0:4040 -token secret
```

## Benchmarks

Standard ANN-benchmarks dataset runs. Hardware: Intel Core Ultra 9 285, 20 threads, Go 1.26, WSL2. Recall@10 is measured against exact ground-truth neighbors. These are subset runs; reproduce with `cmd/bench` (see `cmd/bench/README.md`).

### glove-25-angular (25-dim, 100K subset)

| Bit width | Recall@10 | Serial QPS | Parallel QPS | Code bytes/vec |
|-----------|-----------|-----------|--------------|----------------|
| 2-bit     | 0.13      | 215       | 2692         | 12             |
| 4-bit     | 0.47      | —         | —            | 18             |
| 8-bit     | 0.91      | —         | —            | 30             |

HNSW (2-bit): recall 0.12, 758 serial / 15228 parallel QPS. HNSW (4-bit): recall 0.47. HNSW (8-bit): recall 0.90.

### sift-128-euclidean (128-dim, 50K subset, flat)

| Bit width | Recall@10 | Serial QPS | Parallel QPS | Code bytes/vec |
|-----------|-----------|-----------|--------------|----------------|
| 2-bit     | 0.19      | 368       | 4391         | 36             |
| 4-bit     | 0.47      | —         | —            | 68             |
| 8-bit     | 0.85      | —         | —            | 132            |

**Caveats:** low-dim + low-bit is the hard case; recall climbs sharply with both bit width and embedding dimension (glove-25 at 2-bit is expected to be low). HNSW trades a small amount of recall for large QPS gains. Subset sizes are noted above. Reproduce via `go run ./cmd/bench` (see `cmd/bench/README.md` for dataset prep).

### Micro-benchmarks

384-dimensional vectors, 2-bit TurboQuant IP quantization, Intel Core Ultra 9 285:

| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| **Put (vector + WAL)** | 933us | 34 | quantize + WAL append + fsync |
| **Put (text + WAL)** | 942us | 35 | encode + quantize + WAL + fsync |
| **Search top-10 (1K vectors)** | 82us | 23 | prepared-query LUT scoring |
| **Search top-10 (10K vectors)** | 546us | 23 | |
| **Search top-10 (100K vectors)** | 5.5ms | 23 | |
| **Search parallel (10K, 20 cores)** | 49us | 23 | linear scaling under concurrency |
| **Search with filter (10K)** | 719us | 5023 | metadata match + quantized scoring |
| **History (100 versions)** | 32us | 101 | full version clone |
| **Open + Close (1K vectors)** | 31ms | 69607 | snapshot load + WAL replay + snapshot write |

Memory per vector at 384-dim, 2-bit: ~144 bytes (96B MSE + 48B signs + metadata).

```bash
go test -bench=. -benchmem -run=^$ .
```

## Development

```bash
go test ./...
go test -race -count=1 ./...
```
