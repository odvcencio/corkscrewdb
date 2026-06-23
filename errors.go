package corkscrewdb

import "errors"

// ErrFormatTooOld is returned when an on-disk artifact predates the v0.3.0 floor.
var ErrFormatTooOld = errors.New("corkscrewdb: on-disk format is older than the v0.3.0 floor")

// ErrRawStoreRequired is returned when an operation needs the raw vector store
// but none is available. This includes collections created WithoutRawStore and
// collections in recompute mode (cold_tier:"recompute"), where raw floats are
// re-derived from text rather than stored. Recompute mode makes exact rerank
// succeed without a raw store, so this error is only returned for operations
// that explicitly require stored raw bytes (e.g. getRaw by hash). A v0.3.x
// reader opening a v0.4.0 recompute collection receives this error only at
// the raw-needing call site — not at Open or Collection (§9.3, Fix 2).
var ErrRawStoreRequired = errors.New("corkscrewdb: operation requires the raw vector store")

// ErrInvalidSparseVector is returned when a SparseVector violates its invariants.
var ErrInvalidSparseVector = errors.New("corkscrewdb: invalid sparse vector")

// ErrRawUnavailable is the terminal error returned when a raw blob cannot be
// fetched by hash (the source reports not-found / the blob is gone). It is
// distinct from a retriable transient/integrity failure, which callers should
// retry against the same or another source.
var ErrRawUnavailable = errors.New("corkscrewdb: raw vector unavailable")

// ErrWrongOwner is returned when a non-internal direct client write reaches a
// server that does not own the target key under the current shard layout. The
// server rejects rather than fanning out (only a federating client fans out).
var ErrWrongOwner = errors.New("corkscrewdb: key is owned by a different shard")

// errMultiVectorRemoteUnsupported is returned for SearchMulti against a remote
// or federated collection. Hybrid (dense+sparse) fan-out is out of scope for
// v0.3.0; the call remains unsupported across the network boundary.
var errMultiVectorRemoteUnsupported = errors.New(
	"corkscrewdb: SearchMulti is unsupported over remote/federated collections in v0.3.0")

// ErrRebalanceInProgress is returned (retriable) when a write targets a key
// whose ownership is moving in an in-flight rebalance. The client should retry
// after the cutover completes.
var ErrRebalanceInProgress = errors.New("corkscrewdb: write rejected, key is rebalancing")

// ErrStaleEpoch is returned when a rebalance RPC carries an epoch at or below
// the committed floor (a stale coordinator fenced out by a newer epoch or a
// force-abort).
var ErrStaleEpoch = errors.New("corkscrewdb: rebalance epoch is stale")

// ErrAlreadyCommitted is returned when an Abort targets a rebalance that the
// participant has already committed (a committed participant must never roll
// back).
var ErrAlreadyCommitted = errors.New("corkscrewdb: rebalance already committed")

// errPullTooOld is reserved for the v0.3.1 streamer retention-window path: a
// pull whose since-clock predates the retained history floor. Unexported because
// no code path returns it yet; exporting it would widen the public surface with
// a sentinel callers can never match via errors.Is.
var errPullTooOld = errors.New("corkscrewdb: pull since-clock is older than retained history")

// ErrLegacyRebalanceUnsafe is returned when a fenced cluster rebalance's layout
// diff cannot be expressed as explicit shard ranges (it falls through to legacy
// peer-hash-mod for a moving key). A safe rebalance requires explicit WithShards.
var ErrLegacyRebalanceUnsafe = errors.New("corkscrewdb: fenced cluster rebalance requires explicit shard ranges")

// ErrRecomputeRequiresDeterministicProvider is returned when a collection is
// created with WithRecomputeRawFromText() but the active embedding provider is
// not deterministic (does not implement deterministicProvider and return true),
// or when the provider is the silent builtin fallback when Eos was the intended
// (default) provider.
var ErrRecomputeRequiresDeterministicProvider = errors.New("corkscrewdb: recompute requires a deterministic embedding provider")

// ErrRecomputeMultiVectorUnsupported is returned when WithRecomputeRawFromText()
// is used together with multi-vector operations, which are excluded in v0.4.0.
var ErrRecomputeMultiVectorUnsupported = errors.New("corkscrewdb: recompute is unsupported for multi-vector collections in v0.4.0")

// ErrRecomputeRequiresText is returned when a put reaches a recompute-mode
// collection without the text field set (raw float fallback is unavailable).
var ErrRecomputeRequiresText = errors.New("corkscrewdb: recompute collection requires text on every write")

// ErrRecomputeBackendMismatch is returned when a recompute collection is opened
// on a host whose embedding backend fingerprint differs from the one pinned at
// create time (§9.4 backend-fingerprint fast-fail gate).
var ErrRecomputeBackendMismatch = errors.New("corkscrewdb: recompute collection backend fingerprint mismatch")

// ErrEmbedderDriftDetected is returned when the drift-check on a recomputed raw
// vector detects that the re-quantized codes do not byte-match the stored codes,
// indicating that the embedding backend has drifted since the codes were written.
var ErrEmbedderDriftDetected = errors.New("corkscrewdb: embedder drift detected; recomputed codes do not match stored codes")
