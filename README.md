# CorkScrewDB

CorkScrewDB is an embedded, versioned vector database in pure Go.

Phase 1 ships the single-process core:

- Text-in and vector-in collection APIs
- Version history per ID with Lamport clocks
- TurboQuant-backed quantized flat search
- Append-only WAL persistence
- Snapshot recovery on reopen
- Metadata filters and point-in-time collection views

## Status

`v0.1.0-dev` is the embedded-core milestone. There is no networking, sharding, or replication in this repo yet.

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

## Development

```bash
go test ./...
go test -race -count=1 ./...
```
