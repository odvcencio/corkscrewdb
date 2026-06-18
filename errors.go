package corkscrewdb

import "errors"

// ErrFormatTooOld is returned when an on-disk artifact predates the v0.3.0 floor.
var ErrFormatTooOld = errors.New("corkscrewdb: on-disk format is older than the v0.3.0 floor")

// ErrRawStoreRequired is returned when an operation needs the raw vector store
// but the collection was created WithoutRawStore.
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
