package corkscrewdb

import (
	"testing"

	walpkg "m31labs.dev/corkscrewdb/wal"
)

// TestRPCReplicaEntryCarriesCodes verifies the transport-layer wire structs
// carry the WAL v5 code payload (quantized + sparse + children + raw hash +
// dim) rather than an inline float embedding. This is a package corkscrewdb
// test; it does not compile/run until Task 6 closes the cluster.
func TestRPCReplicaEntryCarriesCodes(t *testing.T) {
	rawHash := make([]byte, 32)
	for i := range rawHash {
		rawHash[i] = byte(i)
	}
	entry := RPCReplicaEntry{
		Kind:         walpkg.EntryPut,
		CollectionID: "docs",
		VectorID:     "vec-1",
		Quantized:    &walpkg.QuantizedVector{MSE: []byte{1, 2, 3}, Signs: []byte{0xFF}, ResNorm: 1.25},
		Dim:          8,
		RawHash:      rawHash,
		Sparse:       &walpkg.SparseBlock{Indices: []uint32{0, 2}, Values: []float32{0.5, 0.25}},
		Children:     []walpkg.ChildVector{{ID: "c0", Quantized: &walpkg.QuantizedVector{MSE: []byte{9}, Signs: []byte{1}, ResNorm: 0.5}, Dim: 8}},
		Text:         "hello",
		Metadata:     map[string]string{"k": "v"},
		LamportClock: 10,
		ActorID:      "a",
	}
	if entry.Quantized == nil || entry.Quantized.ResNorm != 1.25 {
		t.Fatalf("quantized not carried: %+v", entry.Quantized)
	}
	if entry.Dim != 8 || len(entry.RawHash) != 32 {
		t.Fatalf("dim/rawhash not carried: dim=%d rawhash=%d", entry.Dim, len(entry.RawHash))
	}
	if entry.Sparse == nil || len(entry.Sparse.Indices) != 2 {
		t.Fatalf("sparse not carried: %+v", entry.Sparse)
	}
	if len(entry.Children) != 1 || entry.Children[0].ID != "c0" {
		t.Fatalf("children not carried: %+v", entry.Children)
	}

	sv := RPCSnapshotVersion{
		Quantized:    &walpkg.QuantizedVector{MSE: []byte{4, 5}, Signs: []byte{0x0F}, ResNorm: 2.5},
		Dim:          8,
		RawHash:      rawHash,
		Sparse:       &walpkg.SparseBlock{Indices: []uint32{1}, Values: []float32{1.0}},
		Children:     []walpkg.ChildVector{{ID: "c1", Dim: 8}},
		Text:         "snap",
		LamportClock: 11,
		ActorID:      "a",
		Tombstone:    false,
	}
	if sv.Quantized == nil || sv.Quantized.ResNorm != 2.5 {
		t.Fatalf("snapshot quantized not carried: %+v", sv.Quantized)
	}
	if sv.Dim != 8 || len(sv.RawHash) != 32 || sv.Sparse == nil || len(sv.Children) != 1 {
		t.Fatalf("snapshot codes not carried: %+v", sv)
	}
}
