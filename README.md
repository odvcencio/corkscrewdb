# CorkScrewDB

CorkScrewDB is a distributed, versioned vector database in pure Go.

- Text-in and vector-in collection APIs
- Version history per ID with hybrid logical clocks
- TurboQuant-backed quantized flat and HNSW search
- Append-only WAL persistence with snapshot recovery
- Quantized index persistence (`.tqi`)
- Embedding-space config enforcement
- Metadata filters and point-in-time collection views
- gRPC transport with `Connect(...)` and `Serve(...)`
- Embedded federation with hash-based write routing and fan-out search
- Explicit shard metadata with persisted ownership ranges
- Manual shard rebalance and handoff via snapshot + WAL catch-up
- Coordinated cluster rebalance orchestration over gRPC
- Live gRPC WAL streaming replication with snapshot catch-up
- Cold storage offload (sealed WAL segments + snapshots)
- Standalone server binary (`cmd/corkscrewdb`)

## Agent Skill

Agents working with CorkScrewDB should use the [using-corkscrewdb](https://github.com/odvcencio/m31labs-skills/blob/main/skills/using-corkscrewdb/SKILL.md) skill.

## Status

`v0.2.0` — HLC clocks, v2 storage formats, HNSW persistence, gRPC transport, explicit shard metadata, manual shard handoff, coordinated rebalance orchestration, and live replication streaming have shipped.

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

## Vector Storage

Collections persist raw float embeddings by default. For local flat collections that only need quantized search and metadata/history text, use quantized-only persistence:

```go
coll := db.Collection(
    "child_vectors",
    corkscrewdb.WithBitWidth(8),
    corkscrewdb.WithQuantizerSeed(5581486560434873699),
    corkscrewdb.WithQuantizedOnlyPersistence(),
)
```

`WithQuantizedOnlyPersistence()` is shorthand for `WithVectorStorage(VectorStorageQuantizedOnly)`. In this mode, WAL and snapshots store TurboQuant payloads plus text, metadata, clocks, and tombstones. Raw embeddings are used only at write time to build the quantized payload; they are not retained in version history or durable snapshots, and CorkScrewDB does not write a separate flat `.tqi` index file because the snapshot is the durable full-state copy.

Current limits are intentional: `quantized_only` is for embedded local flat search. HNSW creation/rebuild is rejected, remote collections reject the option, and replication snapshot/WAL export is unsupported until those paths can carry quantized-only state safely. Choose the default raw mode when you need raw vectors in history, HNSW, remote operation, or replication.

An Eos SciFact child-vector smoke using 12,468 128-d vectors with fixed quantizer seed `5581486560434873699` measured closed DB sizes of `4,251,167` bytes (`0.066749x`) for q8, `3,453,215` bytes (`0.054220x`) for q4, and `3,054,239` bytes (`0.047956x`) for q2. In that smoke, q8 exhaustive search matched the cache evaluator to rounding with nDCG@10 `0.413312` and recall@100 `0.743556`; serving-style overfetch100 recall was `0.729111`. These are local smoke numbers, not a general benchmark.

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

Shard layouts can be updated in place with data handoff:

```go
err := db.RebalanceShards(
    corkscrewdb.ShardAssignment{ID: "shard-a", Owner: corkscrewdb.LocalShardOwner, Start: 0, End: (^uint64(0)) * 3 / 4},
    corkscrewdb.ShardAssignment{ID: "shard-b", Owner: "node-b:4040", Start: (^uint64(0))*3/4 + 1, End: ^uint64(0)},
)
```

Current behavior:

- a gaining node pulls snapshot data plus WAL tail from the old owner for the ranges it is taking over
- the new shard layout is then persisted locally
- IDs no longer owned by the node are pruned from local search/history after the cutover
- `RebalanceShards(...)` is still the single-node/manual form when you want to drive the phases yourself

For cluster-wide cutover, coordinate the same phases from one node:

```go
err := db.OrchestrateRebalance(
    corkscrewdb.ShardAssignment{ID: "shard-a", Owner: "node-a:4040", Start: 0, End: (^uint64(0)) * 3 / 4},
    corkscrewdb.ShardAssignment{ID: "shard-b", Owner: "node-b:4040", Start: (^uint64(0))*3/4 + 1, End: ^uint64(0)},
)
```

Current behavior:

- the coordinator runs prepare, commit, and prune across the local node plus reachable peers
- gaining nodes import data before the layout flips cluster-wide
- routing changes once the commit phase runs
- old owners prune handed-off data in the final phase
- orchestration is sequential and best-effort; there is no distributed transaction or rollback yet

## Replication

WAL entries stream from primary to followers over gRPC. Followers use live entry streams when the transport supports it and fall back to polling otherwise. New followers still catch up via snapshot transfer + WAL tail replay before switching to live updates.

## Cold Storage Offload

Sealed WAL segments and snapshots push to a configurable backend on a schedule. A filesystem backend ships for testing; S3/GCS backends are planned behind build tags.

## Server Binary

```bash
go build ./cmd/corkscrewdb/
./corkscrewdb -data ./my-data -addr 0.0.0.0:4040 -token secret
```

## Benchmarks

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

Recall@10 at 64-dim, 4-bit: 0.80 (vs exact brute-force).

```bash
go test -bench=. -benchmem -run=^$ .
```

## Development

```bash
go test ./...
go test -race -count=1 ./...
```
