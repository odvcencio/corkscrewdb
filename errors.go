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
