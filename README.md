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
- WAL streaming replication (primary → follower with catch-up)
- Cold storage offload (sealed WAL segments + snapshots)
- Standalone server binary (`cmd/corkscrewdb`)

## Status

`v0.2.0-dev` — HLC clocks, v2 storage formats, HNSW persistence, and gRPC transport are in on the dev branch. Remaining roadmap work is shard metadata/rebalancing, richer replication, pluggable cloud offload backends, and the bundled model.

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

## Replication

WAL entries stream from primary to followers via pull-based gRPC. Followers apply entries through CRDT merge (last-writer-wins by HLC value). New followers catch up via snapshot transfer + WAL tail replay.

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

## Roadmap (v0.2.0)

- Explicit shard metadata and rebalancing
- Cross-region replication
- Pluggable S3/GCS storage backends
- Bundled embedding model

## Development

```bash
go test ./...
go test -race -count=1 ./...
```
