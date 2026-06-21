package corkscrewdb

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"m31labs.dev/corkscrewdb/replica"
	walpkg "m31labs.dev/corkscrewdb/wal"
	"m31labs.dev/turboquant"
)

// TestReplicationCatchUpPreservesSparseFlag is the Bug 3 regression: the
// replication snapshot path dropped the primary's WithSparse() choice, so the
// follower created the collection without the sparse channel. Its SearchMulti
// then rejected sparse queries even though the sparse data was indexed. With the
// fix, SnapshotData carries SparseEnabled and ApplySnapshot recreates the
// follower collection WithSparse().
func TestReplicationCatchUpPreservesSparseFlag(t *testing.T) {
	primaryPath := filepath.Join(t.TempDir(), "primary-sparse.csdb")
	primaryDB, err := Open(primaryPath, WithProvider(&mockProvider{dim: 8}), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer primaryDB.Close()

	// Hybrid (dense + sparse) collection on the primary.
	coll := primaryDB.Collection("hy", WithBitWidth(2), WithSparse())
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	put := func(id string, dense []float32, sv *SparseVector) {
		if err := coll.Put(id, Entry{Vector: dense, Sparse: sv}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
	put("a", []float32{1, 0, 0, 0, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{0, 1}, Values: []float32{1, 1}})
	put("b", []float32{0, 1, 0, 0, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}})
	put("c", []float32{0, 0, 1, 0, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{5}, Values: []float32{1}})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() { _ = primaryDB.Serve(listener) }()

	followerPath := filepath.Join(t.TempDir(), "follower-sparse.csdb")
	followerDB, err := Open(followerPath, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer followerDB.Close()

	client, err := Connect(listener.Addr().String(), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	puller, err := NewRPCPuller(client)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewDBApplier(followerDB)
	if err != nil {
		t.Fatal(err)
	}

	follower, err := replica.NewFollower(replica.FollowerConfig{
		Collection: "hy",
		Applier:    applier,
		Puller:     puller,
		Interval:   time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.CatchUp("secret"); err != nil {
		t.Fatal(err)
	}

	followerColl := followerDB.Collection("hy")
	if followerColl.err != nil {
		t.Fatal(followerColl.err)
	}
	// The follower collection MUST have the sparse channel enabled.
	if !followerColl.sparseEnabled {
		t.Fatal("follower collection is not sparse-enabled after CatchUp (Bug 3)")
	}

	// A sparse query must succeed and rank "a" first (it shares indices 0 and 1).
	sq := &SparseVector{Indices: []uint32{0, 1}, Values: []float32{1, 1}}
	fused, err := followerColl.SearchMulti(MultiQuery{Sparse: sq}, 3)
	if err != nil {
		t.Fatalf("follower SearchMulti sparse query failed (Bug 3): %v", err)
	}
	if len(fused) == 0 || fused[0].ID != "a" {
		t.Fatalf("follower sparse search top = %v, want a", ids(fused))
	}
}

// TestApplySnapshotPreservesSparseAndRawStore directly exercises the
// DBApplier.ApplySnapshot mapping of SparseEnabled / RawStore from SnapshotData
// onto the follower collection options (Bug 3), independent of the RPC stack.
func TestApplySnapshotPreservesSparseAndRawStore(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "applysnap.csdb"), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	applier, err := NewDBApplier(db)
	if err != nil {
		t.Fatal(err)
	}

	data := replica.SnapshotData{
		Collection:    "follower-hy",
		BitWidth:      2,
		Dim:           4,
		MaxLamport:    0,
		RawStore:      false, // primary opted out of the raw store
		SparseEnabled: true,  // primary enabled the sparse channel
	}
	if err := applier.ApplySnapshot(data); err != nil {
		t.Fatal(err)
	}

	coll := db.Collection("follower-hy")
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	if !coll.sparseEnabled {
		t.Fatal("ApplySnapshot did not enable the sparse channel (Bug 3)")
	}
	if coll.rawStoreEnabled {
		t.Fatal("ApplySnapshot did not honor RawStore=false (Bug 3)")
	}
}

// TestApplyReplicatedEntryCreatesSparseCollection covers the entry-stream-only
// path (no prior CatchUp): the very first replicated entry carrying a sparse
// payload must create the follower collection WithSparse() so a later sparse
// SearchMulti is accepted (Bug 3).
func TestApplyReplicatedEntryCreatesSparseCollection(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "entrysparse.csdb"), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	applier, err := NewDBApplier(db)
	if err != nil {
		t.Fatal(err)
	}

	qv := turboquant.NewIPWithSeed(4, 2, 7).Quantize([]float32{1, 0, 0, 0})
	entry := replica.Entry{}
	entry.Kind = walpkg.EntryPut
	entry.CollectionID = "stream"
	entry.VectorID = "a"
	entry.Dim = 4
	entry.Quantized = toWALQuantized(&qv)
	entry.Sparse = toWALSparse(&SparseVector{Indices: []uint32{0, 1}, Values: []float32{1, 1}})
	entry.LamportClock = 1
	entry.ActorID = "primary"

	if err := applier.ApplyReplicatedEntry("stream", entry); err != nil {
		t.Fatal(err)
	}

	coll := db.Collection("stream")
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	if !coll.sparseEnabled {
		t.Fatal("entry-stream applier did not create a sparse collection for a sparse entry (Bug 3)")
	}
	if _, err := coll.SearchMulti(MultiQuery{Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}, 1); err != nil {
		t.Fatalf("sparse SearchMulti rejected on entry-stream follower (Bug 3): %v", err)
	}
}
