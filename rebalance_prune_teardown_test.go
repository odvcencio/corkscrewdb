package corkscrewdb

import (
	"path/filepath"
	"testing"
)

// TestRemoveLocalIDsFullTeardown is the Bug 4 regression: removeLocalIDs (the
// rebalance prune teardown) previously did only delete(history) + index.Remove.
// It left behind (a) packed multivector children in the flat index (which still
// surfaced from SearchParents), (b) the sparse active-set entry, and (c) stale
// cached At() view indices. The fix mirrors applyVersionLocked's full teardown:
// removePackedChildren, delete(sparseSet), and a full view-cache invalidation.
func TestRemoveLocalIDsFullTeardown(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "prune.csdb"), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// (a) packed-multivector parents in a flat collection.
	mvColl := db.Collection("mv", WithBitWidth(2))
	if mvColl.err != nil {
		t.Fatal(mvColl.err)
	}
	if err := mvColl.PutMultiVector("parent-prune", MultiVectorEntry{
		Text: "to be pruned",
		Children: []MultiVectorChild{
			{ID: "c1", Vector: []float32{1, 0, 0, 0}, Text: "child one"},
			{ID: "c2", Vector: []float32{0, 1, 0, 0}, Text: "child two"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := mvColl.PutMultiVector("parent-keep", MultiVectorEntry{
		Text: "kept",
		Children: []MultiVectorChild{
			{ID: "k1", Vector: []float32{0, 0, 1, 0}, Text: "keep child"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Before pruning, the pruned parent's children are discoverable.
	pre, err := mvColl.SearchParentsVector([]float32{1, 0, 0, 0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !hasParent(pre, "parent-prune") {
		t.Fatalf("setup: parent-prune not found pre-prune: %v", parentIDs(pre))
	}

	if err := mvColl.removeLocalIDs([]string{"parent-prune"}); err != nil {
		t.Fatal(err)
	}

	post, err := mvColl.SearchParentsVector([]float32{1, 0, 0, 0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if hasParent(post, "parent-prune") {
		t.Fatalf("Bug 4(a): pruned parent-prune children still surface from SearchParents: %v", parentIDs(post))
	}

	// (b) sparse active-set teardown + (c) view-cache invalidation in a hybrid
	// collection.
	hy := db.Collection("hy", WithBitWidth(2), WithSparse())
	if hy.err != nil {
		t.Fatal(hy.err)
	}
	if err := hy.Put("sparse-prune", Entry{
		Vector: []float32{1, 1, 0, 0},
		Sparse: &SparseVector{Indices: []uint32{0, 1}, Values: []float32{1, 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := hy.Put("sparse-keep", Entry{
		Vector: []float32{0, 0, 1, 1},
		Sparse: &SparseVector{Indices: []uint32{5}, Values: []float32{1}},
	}); err != nil {
		t.Fatal(err)
	}

	hy.mu.RLock()
	_, sparsePresentPre := hy.sparseSet["sparse-prune"]
	headClock := hy.clock.Current()
	hy.mu.RUnlock()
	if !sparsePresentPre {
		t.Fatal("setup: sparse-prune missing from sparseSet")
	}

	// Build + cache an At(headClock) view whose flat index includes sparse-prune.
	view := hy.At(headClock)
	if view.err != nil {
		t.Fatal(view.err)
	}
	cachedHits, err := view.SearchVector([]float32{1, 1, 0, 0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(cachedHits, "sparse-prune") {
		t.Fatalf("setup: cached view did not contain sparse-prune: %v", ids(cachedHits))
	}

	if err := hy.removeLocalIDs([]string{"sparse-prune"}); err != nil {
		t.Fatal(err)
	}

	// (b) sparse active-set entry is gone.
	hy.mu.RLock()
	_, sparsePresentPost := hy.sparseSet["sparse-prune"]
	hy.mu.RUnlock()
	if sparsePresentPost {
		t.Fatal("Bug 4(b): pruned sparse-prune still present in sparseSet (leak)")
	}

	// (c) a view rebuilt at the same clock must NOT serve the pruned id from a
	// stale cached flat index.
	view2 := hy.At(headClock)
	if view2.err != nil {
		t.Fatal(view2.err)
	}
	hits2, err := view2.SearchVector([]float32{1, 1, 0, 0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if hasResult(hits2, "sparse-prune") {
		t.Fatalf("Bug 4(c): cached At() view still returns the pruned id: %v", ids(hits2))
	}
}

func hasParent(results []ParentSearchResult, id string) bool {
	for _, r := range results {
		if r.ID == id {
			return true
		}
	}
	return false
}

func parentIDs(results []ParentSearchResult) []string {
	out := make([]string, len(results))
	for i, r := range results {
		out[i] = r.ID
	}
	return out
}
