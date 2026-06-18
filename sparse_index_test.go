package corkscrewdb

import (
	"path/filepath"
	"testing"
)

func sparseEqual(a, b *SparseVector) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.Indices) != len(b.Indices) || len(a.Values) != len(b.Values) {
		return false
	}
	for i := range a.Indices {
		if a.Indices[i] != b.Indices[i] {
			return false
		}
	}
	for i := range a.Values {
		if a.Values[i] != b.Values[i] {
			return false
		}
	}
	return true
}

func TestSparseSetActiveSetMaintenance(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "set.csdb"), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll := db.Collection("docs", WithBitWidth(2), WithSparse())

	sv1 := &SparseVector{Indices: []uint32{0, 2}, Values: []float32{1, 2}}
	if err := coll.Put("a", Entry{Vector: []float32{1, 0, 0, 0}, Sparse: sv1}); err != nil {
		t.Fatal(err)
	}
	if got := coll.sparseSet["a"]; !sparseEqual(got, sv1) {
		t.Fatalf("sparseSet[a] = %v, want %v", got, sv1)
	}

	// Replace a with a different sparse channel.
	sv2 := &SparseVector{Indices: []uint32{1, 3}, Values: []float32{5, 6}}
	if err := coll.Put("a", Entry{Vector: []float32{0, 1, 0, 0}, Sparse: sv2}); err != nil {
		t.Fatal(err)
	}
	if got := coll.sparseSet["a"]; !sparseEqual(got, sv2) {
		t.Fatalf("after replace: sparseSet[a] = %v, want %v", got, sv2)
	}

	// Every key in sparseSet must correspond to a live (non-tombstone) id.
	for k := range coll.sparseSet {
		versions := coll.history[k]
		if len(versions) == 0 {
			t.Fatalf("sparseSet key %q has no history", k)
		}
		if versions[len(versions)-1].Tombstone {
			t.Fatalf("sparseSet key %q points at a tombstoned id", k)
		}
	}
}

// TestSparseSetUnconditionalDelete pins the §3.2 fix: delete(c.sparseSet, id)
// must run UNCONDITIONALLY, outside the `if c.index != nil` guard, so a
// tombstone or a sparse-less replacement always clears the prior sparse entry.
func TestSparseSetUnconditionalDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "del.csdb"), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll := db.Collection("docs", WithBitWidth(2), WithSparse())

	// (a) Put then tombstone -> absent.
	if err := coll.Put("a", Entry{Vector: []float32{1, 0, 0, 0}, Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := coll.sparseSet["a"]; !ok {
		t.Fatal("expected sparseSet[a] present after Put")
	}
	if err := coll.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := coll.sparseSet["a"]; ok {
		t.Fatal("sparseSet[a] must be absent after tombstone (unconditional delete)")
	}

	// (b) Put with sparse, then replace with a latest that has NO sparse -> absent.
	if err := coll.Put("b", Entry{Vector: []float32{0, 1, 0, 0}, Sparse: &SparseVector{Indices: []uint32{1}, Values: []float32{2}}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := coll.sparseSet["b"]; !ok {
		t.Fatal("expected sparseSet[b] present after Put")
	}
	if err := coll.Put("b", Entry{Vector: []float32{0, 0, 1, 0}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := coll.sparseSet["b"]; ok {
		t.Fatal("sparseSet[b] must be absent after sparse-less replacement (unconditional delete + skipped set)")
	}
}
