package corkscrewdb

import (
	"bytes"
	"path/filepath"
	"testing"

	"m31labs.dev/corkscrewdb/replica"
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

// TestDBApplierReconstructsCodes proves the follower-apply path reconstructs a
// Version directly from the wire codes (NO re-quantization) and the applied
// version's quantized codes are byte-identical to the source.
func TestDBApplierReconstructsCodes(t *testing.T) {
	primary, err := Open(filepath.Join(t.TempDir(), "primary.csdb"), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()

	coll := primary.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "hello reconstruct"}); err != nil {
		t.Fatal(err)
	}

	// Pull the live entry (codes intact) from the primary's streamer.
	pr := primary.streamer.Pull("docs", 0, 100)
	if len(pr.Entries) != 1 {
		t.Fatalf("streamer entries = %d, want 1", len(pr.Entries))
	}
	src := pr.Entries[0]
	if src.Quantized == nil {
		t.Fatal("source entry carries no quantized codes")
	}

	follower, err := Open(filepath.Join(t.TempDir(), "follower.csdb"), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	applier, err := NewDBApplier(follower)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.ApplyReplicatedEntry("docs", src); err != nil {
		t.Fatalf("apply replicated entry: %v", err)
	}

	versions, err := follower.Collection("docs").historyFor("doc-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 {
		t.Fatalf("follower history len = %d, want 1", len(versions))
	}
	got := versions[0]
	if got.Quantized == nil {
		t.Fatal("follower version carries no quantized codes (re-truncated)")
	}
	if !bytes.Equal(got.Quantized.MSE, src.Quantized.MSE) || !bytes.Equal(got.Quantized.Signs, src.Quantized.Signs) {
		t.Fatalf("quantized codes re-quantized: got MSE=%x Signs=%x want MSE=%x Signs=%x",
			got.Quantized.MSE, got.Quantized.Signs, src.Quantized.MSE, src.Quantized.Signs)
	}
	if got.Quantized.ResNorm != src.Quantized.ResNorm {
		t.Fatalf("res norm mismatch: %v != %v", got.Quantized.ResNorm, src.Quantized.ResNorm)
	}

	results, err := follower.Collection("docs").Search("hello reconstruct", 5)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(results, "doc-1") {
		t.Fatalf("follower search results = %v, want doc-1", results)
	}
}

// TestDBApplierApplySnapshotReconstructsCodes proves ApplySnapshot rebuilds each
// Version from the VersionEntry code fields.
func TestDBApplierApplySnapshotReconstructsCodes(t *testing.T) {
	primary, err := Open(filepath.Join(t.TempDir(), "primary-snap.csdb"), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()

	coll := primary.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "snapshot reconstruct"}); err != nil {
		t.Fatal(err)
	}

	pr := primary.streamer.Pull("docs", 0, 100)
	if len(pr.Entries) != 1 {
		t.Fatalf("streamer entries = %d, want 1", len(pr.Entries))
	}
	src := pr.Entries[0]

	// Build a SnapshotData carrying the source codes verbatim.
	data := replica.SnapshotData{
		Collection: "docs",
		BitWidth:   2,
		Seed:       primary.Collection("docs").seed,
		Dim:        src.Dim,
		MaxLamport: src.LamportClock,
		Entries: []replica.VersionRecord{
			{
				ID: "doc-1",
				Versions: []replica.VersionEntry{
					{
						Quantized:    cloneWALQuantized(src.Quantized),
						Dim:          src.Dim,
						RawHash:      append([]byte(nil), src.RawHash...),
						Sparse:       cloneWALSparse(src.Sparse),
						Children:     cloneWALChildren(src.Children),
						Text:         src.Text,
						LamportClock: src.LamportClock,
						ActorID:      src.ActorID,
						WallClock:    src.WallClock,
					},
				},
			},
		},
	}

	follower, err := Open(filepath.Join(t.TempDir(), "follower-snap.csdb"), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	applier, err := NewDBApplier(follower)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.ApplySnapshot(data); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	versions, err := follower.Collection("docs").historyFor("doc-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0].Quantized == nil {
		t.Fatalf("follower snapshot history = %+v", versions)
	}
	if !bytes.Equal(versions[0].Quantized.MSE, src.Quantized.MSE) {
		t.Fatalf("snapshot codes re-quantized")
	}
}
