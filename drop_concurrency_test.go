package corkscrewdb

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestDropCollectionConcurrentFirstWriteNoDeadlock is the Bug 1 regression: a
// concurrent first-write (which takes c.mu then db.mu via persistCollectionMeta)
// interleaved with DropCollection (which previously took db.mu then c.mu via
// coll.close -> persistSnapshot) produced an AB-BA deadlock that wedged the
// whole DB. A watchdog timeout catches the hang; the test passes once
// DropCollection releases db.mu before calling coll.close().
//
// Run under -race to also exercise the lock-ordering invariant.
func TestDropCollectionConcurrentFirstWriteNoDeadlock(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "drop.csdb"), WithProvider(&mockProvider{dim: 16}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const rounds = 30
	done := make(chan struct{})

	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < rounds; i++ {
			name := fmt.Sprintf("c%d", i)
			// Create the collection so DropCollection has something to remove and
			// the writer has a target. The first Put is the "first write" that
			// triggers createdDim -> persistCollectionMeta (c.mu -> db.mu).
			coll := db.Collection(name, WithBitWidth(2))
			if coll.err != nil {
				t.Errorf("Collection(%s): %v", name, coll.err)
				return
			}
			wg.Add(2)
			go func() {
				defer wg.Done()
				// First write: under c.mu, persistCollectionMeta grabs db.mu.
				_ = coll.Put("k", Entry{Text: "first write payload"})
			}()
			go func(n string) {
				defer wg.Done()
				// Drop: previously grabbed db.mu then c.mu via close().
				_ = db.DropCollection(n)
			}(name)
			wg.Wait()
		}
	}()

	// The watchdog catches a TRUE deadlock (an indefinite hang). A generous bound
	// avoids a false positive under heavy parallel load (each round does several
	// fsync'd manifest/snapshot writes); a real AB-BA cycle never completes, so any
	// finite bound distinguishes pass from hang.
	select {
	case <-done:
		// completed without deadlock
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: concurrent first-write + DropCollection did not finish (AB-BA lock cycle)")
	}
}

// TestCollectionCloseRaceWithInFlightWrite is the Bug 2 regression: close()
// previously read c.dirty (and c.wal/c.rawStore) with no lock, racing the
// markDirty write in applyVersionLocked. A stale c.dirty==false read could skip
// persistSnapshot and lose a just-written version on a clean drop. The race
// detector flags the unsynchronized read; this test drives a write in-flight
// while close() runs (via DropCollection).
//
// Run under -race; without the fix the detector reports a data race on c.dirty.
func TestCollectionCloseRaceWithInFlightWrite(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "closerace.csdb"), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("r%d", i)
		coll := db.Collection(name, WithBitWidth(2))
		if coll.err != nil {
			t.Fatal(coll.err)
		}
		// Seed one row so the collection is non-empty (dirty path in close()).
		if err := coll.Put("seed", Entry{Text: "seed row"}); err != nil {
			t.Fatal(err)
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = coll.Put("k", Entry{Text: "racing write"})
		}()
		go func(n string) {
			defer wg.Done()
			_ = db.DropCollection(n)
		}(name)
	}
	wg.Wait()
}
