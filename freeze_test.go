package corkscrewdb

import (
	"errors"
	"path/filepath"
	"testing"
)

// freezeLayouts builds two explicit two-shard layouts (L0, L1) that move the
// cut between addrA and addrB, so some keys change owner. The local node is
// addrA (LocalShardOwner). It returns L0, L1 plus the resolved local member id.
func freezeLayouts(t *testing.T, db *DB, remote string) (l0, l1 []ShardAssignment) {
	t.Helper()
	maxKey := ^uint64(0)
	oldCut := (maxKey / 4) * 3
	newCut := maxKey / 2 // local LOSES the (newCut, oldCut] band to the remote
	l0 = []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: remote, Start: oldCut + 1, End: maxKey},
	}
	l1 = []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: newCut},
		{ID: "shard-b", Owner: remote, Start: newCut + 1, End: maxKey},
	}
	return l0, l1
}

func TestFreezeRejectsMovingKeyWrite(t *testing.T) {
	remote := "127.0.0.1:1"
	l0, l1 := freezeLayouts(t, nil, remote)
	db, err := Open(filepath.Join(t.TempDir(), "freeze.csdb"), WithProvider(nil), WithShards(l0...))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Resolve L1 to concrete owners (LocalShardOwner -> local id) for the freeze.
	local := db.localMemberID()
	l1resolved := cloneShardAssignments(l1)
	for i := range l1resolved {
		if l1resolved[i].Owner == LocalShardOwner {
			l1resolved[i].Owner = local
		}
	}

	// Find a key that moves between L0 and L1 (owner changes) and one that does not.
	oldMembers := db.legacyShardMembers()
	var movingID, stayingID string
	for i := 0; i < 100000 && (movingID == "" || stayingID == ""); i++ {
		id := "doc-" + itoa(i)
		o0 := db.ownerForLayout("vecs", id, l0, oldMembers)
		o1 := db.ownerForLayout("vecs", id, l1resolved, oldMembers)
		if o0 != o1 && o0 == local && movingID == "" {
			movingID = id
		}
		if o0 == o1 && o0 == local && stayingID == "" {
			stayingID = id
		}
	}
	if movingID == "" || stayingID == "" {
		t.Fatalf("could not find moving/staying keys: moving=%q staying=%q", movingID, stayingID)
	}

	db.installFreeze(1, "", l1resolved)

	coll := db.Collection("vecs")
	if err := coll.PutVector(movingID, []float32{1, 0, 0, 0}); !errors.Is(err, ErrRebalanceInProgress) {
		t.Fatalf("moving-key write err = %v, want ErrRebalanceInProgress", err)
	}
	if err := coll.PutVector(stayingID, []float32{1, 0, 0, 0}); err != nil {
		t.Fatalf("non-moving-key write err = %v, want nil", err)
	}

	// Clearing the freeze re-allows the moving key.
	db.clearRebalance()
	if err := coll.PutVector(movingID, []float32{0, 1, 0, 0}); err != nil {
		t.Fatalf("post-clear moving-key write err = %v, want nil", err)
	}
}

func TestFreezeRangeIsSymmetricDiff(t *testing.T) {
	remote := "127.0.0.1:1"
	l0, l1 := freezeLayouts(t, nil, remote)
	db, err := Open(filepath.Join(t.TempDir(), "symdiff.csdb"), WithProvider(nil), WithShards(l0...))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	local := db.localMemberID()
	l1resolved := cloneShardAssignments(l1)
	for i := range l1resolved {
		if l1resolved[i].Owner == LocalShardOwner {
			l1resolved[i].Owner = local
		}
	}
	db.installFreeze(1, "", l1resolved)

	oldMembers := db.legacyShardMembers()
	for i := 0; i < 5000; i++ {
		id := "k-" + itoa(i)
		o0 := db.ownerForLayout("vecs", id, l0, oldMembers)
		o1 := db.ownerForLayout("vecs", id, l1resolved, oldMembers)
		wantFrozen := o0 != o1
		gotFrozen := db.isFrozenKey("vecs", id)
		if wantFrozen != gotFrozen {
			t.Fatalf("id %q: isFrozenKey = %v, want %v (o0=%q o1=%q)", id, gotFrozen, wantFrozen, o0, o1)
		}
	}
}

func TestEpochMonotonicAndStaleRejected(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "epoch.csdb"), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	e1 := db.beginRebalanceEpoch()
	e2 := db.beginRebalanceEpoch()
	if e2 <= e1 {
		t.Fatalf("beginRebalanceEpoch not monotonic: e1=%d e2=%d", e1, e2)
	}

	// committedEpoch starts at 0; epoch 0 is stale.
	if err := db.checkEpoch(0); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("checkEpoch(0) = %v, want ErrStaleEpoch", err)
	}
	if err := db.checkEpoch(1); err != nil {
		t.Fatalf("checkEpoch(1) = %v, want nil", err)
	}

	// After committing epoch 2, epoch 2 and below are stale.
	db.installFreeze(2, "", nil)
	if err := db.commitEpoch(2); err != nil {
		t.Fatal(err)
	}
	if err := db.checkEpoch(2); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("checkEpoch(2) after commit = %v, want ErrStaleEpoch", err)
	}
	if err := db.checkEpoch(3); err != nil {
		t.Fatalf("checkEpoch(3) = %v, want nil", err)
	}
}

func TestManifestPersistsEpochAndCoordinator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.csdb")
	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}

	pending := []ShardAssignment{{ID: "s", Owner: "coord-addr", Start: 0, End: ^uint64(0)}}
	db.installFreeze(7, "coord-addr:1234", pending)
	db.markPrepared()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	reopened.mu.RLock()
	gotEpoch := reopened.rebalance.epoch
	gotCoord := reopened.rebalance.coordinator
	gotPhase := reopened.rebalance.phase
	gotPending := len(reopened.rebalance.pending)
	reopened.mu.RUnlock()

	if gotEpoch != 7 {
		t.Fatalf("rebalance epoch = %d, want 7", gotEpoch)
	}
	if gotCoord != "coord-addr:1234" {
		t.Fatalf("rebalance coordinator = %q, want coord-addr:1234", gotCoord)
	}
	if gotPhase != phasePrepared {
		t.Fatalf("rebalance phase = %v, want phasePrepared", gotPhase)
	}
	if gotPending != 1 {
		t.Fatalf("rebalance pending len = %d, want 1", gotPending)
	}
}

// itoa is a tiny local strconv.Itoa to avoid an import churn in this test file.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
