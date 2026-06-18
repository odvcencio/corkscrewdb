# Changelog

All notable changes to CorkScrewDB are documented here.

## v0.3.0 — 2026-06-18

### Added

- **Sparse vectors and hybrid search** — `SparseVector{Indices, Values}` on `Entry`; `WithSparse()` collection option; `SearchMulti(MultiQuery{Dense, Sparse, Text, Filters, Fusion}, k)` on both `Collection` and `CollectionView` for point-in-time hybrid search.
- **Fusion policies** — `RRFFusion{K}` (Reciprocal Rank Fusion, default `K=60`) and `WeightedFusion{Dense, Sparse}` (min-max-normalized linear combination); selectable via `MultiQuery.Fusion`.
- **Content-addressed raw vector store** — raw float32 vectors retained by default in blake3-keyed `.rvs` segments; `WithoutRawStore()` opts out (quantized-codes-only mode). Replaces the removed `WithVectorStorage` / `WithQuantizedOnlyPersistence`.
- **Real HNSW index** — RNG/hub-protected neighbor pruning (LEANN), O(degree) tombstone delete with free-list slot reuse, build-from-quantized-codes (no raw vectors needed at rebuild), persisted node IDs in `graph.hnsw v2`.
- **`RebuildIndex(IndexHNSW)`** — switches a flat collection to HNSW in place; uses default params (`M=16`, `EfConstruction=200`, `EfSearch=50`).
- **Query-time two-tier prune** — `turboquant.ScoreUpperBound` early-out in the flat scorer loop (D3); reduces scoring work when the residual-norm bound is tighter than the current k-th score.
- **Batched-multi scorer** — `ScoreTopKMulti(pqs, k, accept)` on the `Scorer` interface; single corpus pass for all queries using `InnerProductPreparedBatchTo`.
- **Pluggable `Scorer` seam** — `WithScorer(Scorer)` collection option; optional CUDA GPU scorer via build tag `cuda` (`scorer_cuda.go` / `scorer_nocuda.go`).
- **LRU point-in-time view cache** — `At()` / `AtTime()` views are memoized in a per-collection LRU to avoid repeated index reconstruction on identical clock values.
- **Distribution: code-carrying replication** — WAL entries carry quantized codes so followers can reconstruct the index without pulling raw vectors for every entry.
- **Distribution: raw pull-by-hash** — followers can retrieve raw vectors from the raw store by blake3 hash when needed (e.g., HNSW rebuild on a replica).
- **Distribution: streamer rebuilt from WAL** — the replication `Streamer` reconstructs its send position from the WAL on restart rather than requiring a durable cursor file.
- **Distribution: 2PC rebalance** — `OrchestrateRebalance(...)` runs a two-phase-commit protocol: freeze writes to migrating keys, wait for a quorum barrier, pull data from old owners, durably record the decision (`writeManifest` with fsync), flip routing, prune. Crash recovery (resume from persisted phase) and force-abort are both supported.

### Changed

- **`WithoutRawStore()` replaces `WithVectorStorage` / `WithQuantizedOnlyPersistence`** — the old storage-mode enum and its options are removed. Use `WithoutRawStore()` for quantized-codes-only collections; the default (raw store enabled) is unchanged.
- **HNSW params honored at creation** — `WithHNSWParams(HNSWParams{M, EfConstruction, EfSearch})` is persisted in the manifest and applied at creation time. `RebuildIndex` uses the default params regardless.
- **Index/graph cache re-wire** — on reopen, the HNSW graph is adopted from the persisted file before WAL replay; a deferred-build path skips costly graph construction when the cache is fresh.
- **Format versions (greenfield v0.3.0 floor — no v0.2.0 migration):** WAL v5, snapshot v6, manifest v2, `.tqi` v3, `graph.hnsw` v2, `.rvs` v1.

### Fixed

- **AtTime same-millisecond ordering** — `AtTime` now correctly orders entries when multiple versions share the same wall-clock millisecond by falling back to Lamport clock for tiebreak.
- **Bounded HLC Witness** — the HLC `Witness` call now caps the accepted wall-clock offset to prevent unbounded clock drift from a rogue or misconfigured peer.
- **O(log v) history insert** — version history insertion uses binary search to maintain sorted order in O(log v) rather than a linear scan.
- **WAL interior-corruption surfacing** — interior-frame CRC failures now return a distinct error rather than being silently swallowed as a clean end-of-segment.
- **Fuzz-hardened decoders** — `.tqi`, `graph.hnsw`, `.rvs`, and WAL decoders enforce bounded allocations on all length-prefixed fields; malformed inputs cannot cause unbounded memory growth.

## v0.2.0 — 2026-04-10

### Added

- **gRPC transport** — `Connect(...)`, `Serve(...)`, and `ListenAndServe(...)` now run over generated protobuf stubs instead of `net/rpc`, with larger message limits for snapshot and replication traffic
- **HNSW index persistence** — approximate nearest-neighbor search now survives restarts alongside the existing quantized flat index
- **Proto definitions** — `proto/corkscrewdb.proto` and generated `grpc/` stubs define the remote DB and replication pull surface
- **Explicit shard metadata** — `WithShards(...)` persists contiguous ownership ranges in `manifest.json`, replacing peer-list hashing when configured
- **Manual shard handoff** — `RebalanceShards(...)` pulls snapshot + WAL tail from old owners before applying the new local layout and pruning handed-off IDs
- **Coordinated rebalance orchestration** — `OrchestrateRebalance(...)` drives prepare, commit, and prune phases across the local node plus reachable peers
- **Live replication streams** — followers can now consume continuous gRPC WAL updates instead of relying only on periodic pull loops

### Changed

- **Hybrid logical clocks** — HLC now backs version ordering while preserving the existing clock-shaped API and stored fields
- **Format version bump** — WAL, snapshot, and index formats moved to v2 for the HLC/HNSW line
- **Transport abstraction** — remote DB operations now flow through the extracted `remoteClient` interface so transport and cluster work can evolve independently
- **Federation routing** — write ownership and scatter-gather fanout now prefer explicit shard assignments, falling back to the older peer-hash behavior only when shard metadata is absent
- **Remote metadata surface** — `Info()` now returns collection and shard metadata so rebalancing code can discover what a peer owns before pulling data
- **Remote admin surface** — gRPC now carries prepare/commit/prune rebalance calls so one node can coordinate cluster cutover
- **Follower runtime** — replication followers auto-upgrade from poll mode to server-driven streaming when the puller supports it, while preserving snapshot catch-up and polling fallback

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
