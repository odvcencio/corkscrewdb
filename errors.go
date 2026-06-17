package corkscrewdb

import "errors"

// ErrFormatTooOld is returned when an on-disk artifact predates the v0.3.0 floor.
var ErrFormatTooOld = errors.New("corkscrewdb: on-disk format is older than the v0.3.0 floor")

// ErrRawStoreRequired is returned when an operation needs the raw vector store
// but the collection was created WithoutRawStore.
var ErrRawStoreRequired = errors.New("corkscrewdb: operation requires the raw vector store")

// ErrInvalidSparseVector is returned when a SparseVector violates its invariants.
var ErrInvalidSparseVector = errors.New("corkscrewdb: invalid sparse vector")
