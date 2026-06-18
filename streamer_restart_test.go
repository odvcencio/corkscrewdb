package corkscrewdb

import (
	"path/filepath"
	"testing"
)

// TestStreamerSeededFromWALOnRestart proves the replication streamer is rebuilt
// from the WAL on Open so a follower asking pullEntries(sinceClock=0) after a
// primary restart gets the pre-restart entries with their codes, in clock order.
func TestStreamerSeededFromWALOnRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "primary.csdb")
	db, err := Open(path, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("doc-2", Entry{Text: "second"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	// Force the collection (and its history) to load.
	db2.Collection("docs")

	pr := db2.streamer.Pull("docs", 0, 100)
	if len(pr.Entries) != 2 {
		t.Fatalf("streamer after restart returned %d entries, want 2", len(pr.Entries))
	}
	if pr.Entries[0].LamportClock > pr.Entries[1].LamportClock {
		t.Fatalf("streamer entries not in ascending clock order: %d then %d",
			pr.Entries[0].LamportClock, pr.Entries[1].LamportClock)
	}
	for i, e := range pr.Entries {
		if e.Quantized == nil {
			t.Fatalf("streamer entry %d carries no quantized codes", i)
		}
	}
}

// TestStreamerSeededAfterSnapshotPrune proves the streamer is the union of the
// snapshot live versions (clock <= snapshotMax) and the post-snapshot WAL tail,
// sorted, with no duplicate (id, clock) — even after the pre-snapshot WAL
// segments have been pruned.
func TestStreamerSeededAfterSnapshotPrune(t *testing.T) {
	path := filepath.Join(t.TempDir(), "primary.csdb")
	db, err := Open(path, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "pre-snapshot"}); err != nil {
		t.Fatal(err)
	}
	// Snapshot then prune the WAL: pre-snapshot history now lives only in the
	// snapshot, not the WAL.
	if err := coll.persistSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := db.pruneWAL("docs"); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("doc-2", Entry{Text: "post-snapshot"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	db2.Collection("docs")

	pr := db2.streamer.Pull("docs", 0, 100)
	if len(pr.Entries) != 2 {
		t.Fatalf("streamer after snapshot+prune returned %d entries, want 2 (union)", len(pr.Entries))
	}
	seen := make(map[string]struct{})
	var lastClock uint64
	for i, e := range pr.Entries {
		key := e.VectorID
		if _, dup := seen[key]; dup {
			t.Fatalf("duplicate vector %q in streamer", key)
		}
		seen[key] = struct{}{}
		if i > 0 && e.LamportClock < lastClock {
			t.Fatalf("streamer entries not sorted by clock: %d after %d", e.LamportClock, lastClock)
		}
		lastClock = e.LamportClock
	}
	if _, ok := seen["doc-1"]; !ok {
		t.Fatal("snapshot-seeded pre-prune entry doc-1 missing from streamer")
	}
	if _, ok := seen["doc-2"]; !ok {
		t.Fatal("post-snapshot WAL tail entry doc-2 missing from streamer")
	}
}
