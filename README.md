# CorkScrewDB

CorkScrewDB is an embedded, versioned vector database in pure Go.

Phase 1 ships the single-process core:

- Text-in and vector-in collection APIs
- Version history per ID with Lamport clocks
- TurboQuant-backed quantized flat search
- Append-only WAL persistence
- Snapshot recovery on reopen
- Quantized index persistence to `collections/<name>/index/quantized.tqi`
- Embedding-space config enforcement in `manifest.json`
- Metadata filters and point-in-time collection views

## Status

`v0.1.0-dev` is still pre-cluster. Embedded mode is complete enough to use, and single-node remote access works over the built-in RPC transport. Sharding, replication, and scatter-gather search are not implemented yet.

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

`Connect(...)` now works for single-node remote access using the same collection API over the wire:

```go
db, err := corkscrewdb.Connect("127.0.0.1:4040", corkscrewdb.WithToken("agent-token-xxx"))
```

Expose an embedded DB over TCP with:

```go
if err := db.ListenAndServe("127.0.0.1:4040"); err != nil {
    log.Fatal(err)
}
```

## Future Cluster Surface

The spec-facing option surface for embedded federation config is still present:

```go
db, err := corkscrewdb.Open(
    "./vectors.csdb",
    corkscrewdb.WithPeers("corkscrewdb-0.corkscrewdb.svc:4040"),
    corkscrewdb.WithToken("agent-token-xxx"),
)
```

`WithPeers(...)` and `WithToken(...)` are persisted/configured now, but multi-peer routing, sharding, replication, and gRPC transport are still ahead.

## Development

```bash
go test ./...
go test -race -count=1 ./...
```
