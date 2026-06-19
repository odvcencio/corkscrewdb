package corkscrewdb

import (
	"path/filepath"
	"sort"
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

func TestSparseSearchTopKAndPushdown(t *testing.T) {
	query := &SparseVector{Indices: []uint32{0, 1, 2}, Values: []float32{1, 1, 1}}
	set := map[string]*SparseVector{
		"a": {Indices: []uint32{0, 1, 2}, Values: []float32{1, 1, 1}}, // dot = 3
		"b": {Indices: []uint32{0, 1}, Values: []float32{1, 1}},       // dot = 2
		"c": {Indices: []uint32{2}, Values: []float32{1}},             // dot = 1
		"d": {Indices: []uint32{5, 6}, Values: []float32{1, 1}},       // dot = 0
		"e": {Indices: []uint32{0, 1, 2}, Values: []float32{2, 2, 2}}, // dot = 6 (excluded by accept)
	}

	scored := 0
	accept := func(id string) bool { scored++; return id != "e" }
	hydrate := func(id string) (string, map[string]string, uint64) {
		return "text-" + id, map[string]string{"id": id}, 7
	}

	// Brute-force reference (excluding "e"), top-k by score desc, id asc.
	type ref struct {
		id    string
		score float32
	}
	var refs []ref
	for id, sv := range set {
		if id == "e" {
			continue
		}
		refs = append(refs, ref{id, sparseDot(query, sv)})
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].score != refs[j].score {
			return refs[i].score > refs[j].score
		}
		return refs[i].id < refs[j].id
	})

	k := 3
	results := sparseSearch(set, query, k, accept, hydrate)
	if len(results) != k {
		t.Fatalf("got %d results, want %d", len(results), k)
	}
	for _, r := range results {
		if r.ID == "e" {
			t.Fatal("filtered id e must never appear")
		}
	}
	for i := 0; i < k; i++ {
		if results[i].ID != refs[i].id {
			t.Fatalf("result[%d].ID = %q, want %q (order %v)", i, results[i].ID, refs[i].id, results)
		}
		if results[i].Score != refs[i].score {
			t.Fatalf("result[%d].Score = %v, want %v", i, results[i].Score, refs[i].score)
		}
		if results[i].Text != "text-"+refs[i].id {
			t.Fatalf("result[%d].Text = %q, want hydrated", i, results[i].Text)
		}
		if results[i].Version != 7 {
			t.Fatalf("result[%d].Version = %d, want 7", i, results[i].Version)
		}
	}
	// accept must be evaluated for every candidate (pushdown before scoring).
	if scored != len(set) {
		t.Fatalf("accept evaluated %d times, want %d", scored, len(set))
	}

	// k <= 0 -> nil
	if got := sparseSearch(set, query, 0, accept, hydrate); got != nil {
		t.Fatalf("k=0 returned %v, want nil", got)
	}
	// k > len(set) -> all accepted candidates
	all := sparseSearch(set, query, 100, func(id string) bool { return true }, hydrate)
	if len(all) != len(set) {
		t.Fatalf("k>len: got %d, want %d", len(all), len(set))
	}
}

func TestSparseSearchReopenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "round.csdb")
	provider := &mockProvider{dim: 4}

	db, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs", WithBitWidth(2), WithSparse())
	svA := &SparseVector{Indices: []uint32{0, 1}, Values: []float32{1, 1}}
	svB := &SparseVector{Indices: []uint32{2, 3}, Values: []float32{1, 1}}
	if err := coll.Put("a", Entry{Vector: []float32{1, 1, 0, 0}, Sparse: svA}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("b", Entry{Vector: []float32{0, 0, 1, 1}, Sparse: svB}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	coll2 := db2.Collection("docs")
	if !sparseEqual(coll2.sparseSet["a"], svA) {
		t.Fatalf("reopen sparseSet[a] = %v, want %v", coll2.sparseSet["a"], svA)
	}
	if !sparseEqual(coll2.sparseSet["b"], svB) {
		t.Fatalf("reopen sparseSet[b] = %v, want %v", coll2.sparseSet["b"], svB)
	}

	query := &SparseVector{Indices: []uint32{0, 1}, Values: []float32{1, 1}}
	hydrate := func(id string) (string, map[string]string, uint64) { return "", nil, 0 }
	results := sparseSearch(coll2.sparseSet, query, 1, func(string) bool { return true }, hydrate)
	if len(results) != 1 || results[0].ID != "a" {
		t.Fatalf("reopen sparseSearch = %v, want top hit a", results)
	}
}
