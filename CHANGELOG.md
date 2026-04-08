# Changelog

All notable changes to CorkScrewDB are documented here.

## v0.2.0-dev — 2026-04-07

### Added

- **gRPC transport** — `Connect(...)`, `Serve(...)`, and `ListenAndServe(...)` now run over generated protobuf stubs instead of `net/rpc`, with larger message limits for snapshot and replication traffic
- **HNSW index persistence** — approximate nearest-neighbor search now survives restarts alongside the existing quantized flat index
- **Proto definitions** — `proto/corkscrewdb.proto` and generated `grpc/` stubs define the remote DB and replication pull surface
- **Explicit shard metadata** — `WithShards(...)` persists contiguous ownership ranges in `manifest.json`, replacing peer-list hashing when configured
- **Manual shard handoff** — `RebalanceShards(...)` pulls snapshot + WAL tail from old owners before applying the new local layout and pruning handed-off IDs

### Changed

- **Hybrid logical clocks** — HLC now backs version ordering while preserving the existing clock-shaped API and stored fields
- **Format version bump** — WAL, snapshot, and index formats moved to v2 for the HLC/HNSW line
- **Transport abstraction** — remote DB operations now flow through the extracted `remoteClient` interface so transport and cluster work can evolve independently
- **Federation routing** — write ownership and scatter-gather fanout now prefer explicit shard assignments, falling back to the older peer-hash behavior only when shard metadata is absent
- **Remote metadata surface** — `Info()` now returns collection and shard metadata so rebalancing code can discover what a peer owns before pulling data

## v0.1.1 — 2026-04-07

### Fixed

- **WAL durability** — writer now fsyncs after every append by default (`SyncEvery`). Configurable via `SyncMode` for throughput-sensitive workloads (`SyncOnRotate`).
- **Snapshot atomicity** — snapshot writes now use write-to-temp + rename to prevent corruption on crash
- **WAL replay dedup** — duplicate WAL entries (same actor ID + Lamport clock) are skipped during recovery
- **Federation merge** — fast path for single-shard search results avoids map allocation

### Added

- **`DropCollection`** — remove a named collection and all its data from disk, wired through RPC transport
- **`RPCPuller` / `DBApplier`** — exported replication adapters in `replicate.go` for setting up followers without copy-pasting test code
- **`SyncMode` / `ManagerConfig`** — configurable WAL sync policy for balancing durability and throughput
- **Builtin provider documentation** — clarified that the default provider uses keyword hashing, not semantic embeddings

### Removed

- **`snapshot/float.go`** — unnecessary wrapper functions replaced with direct `math.Float32bits` / `math.Float32frombits` calls

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
