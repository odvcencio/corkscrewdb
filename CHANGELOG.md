# Changelog

All notable changes to CorkScrewDB are documented here.

## v0.1.0 — 2026-04-07

First stable release. Embedded core with transport, federation, replication, and cold storage.

### Added

- **Embedded database** — `corkscrewdb.Open()` with zero-config text-in/results-out
- **Collections** — `Put`, `PutVector`, `Search`, `SearchVector`, `History`, `Delete`
- **Versioned entries** — append-only version history per ID with Lamport clocks and actor ID tiebreak
- **Point-in-time queries** — `coll.At(lamportClock)` for snapshot views at a logical time
- **Metadata filters** — `Filter("key", "value")` restricts search results
- **TurboQuant quantized index** — 2-bit IP quantization by default, configurable per collection
- **WAL persistence** — append-only write-ahead log with CRC-checked binary format and segment rotation
- **Snapshot recovery** — periodic snapshots with fast recovery (snapshot + WAL tail replay)
- **Quantized index persistence** — `.tqi` files for fast index reload
- **Embedding provider interface** — `EmbeddingProvider` with bundled deterministic 384-dim provider
- **Embedding config enforcement** — manifest rejects reopening with mismatched embedding space
- **Built-in RPC transport** — `Connect(...)`, `Serve(...)`, `ListenAndServe(...)`
- **Token authentication** — `WithToken(...)` for remote access control
- **Embedded federation** — `WithPeers(...)` with hash-based write routing and scatter-gather search
- **WAL streaming replication** — `replica/` package with Streamer, Follower, and catch-up from snapshot + WAL tail
- **Cold storage offload** — `offload/` package with `Backend` interface and filesystem backend
- **Server binary** — `cmd/corkscrewdb` with flag-driven config for data dir, addr, token, peers, offload
