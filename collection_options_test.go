package corkscrewdb

import (
	"path/filepath"
	"strings"
	"testing"

	"m31labs.dev/turboquant"
)

// stubScorer is a no-op Scorer used only to verify runtime injection wiring.
type stubScorer struct{ id string }

func (s *stubScorer) ScoreTopK(pq turboquant.PreparedQuery, k int, accept func(i int) bool) []ScoredHit {
	return nil
}
func (s *stubScorer) ScoreTopKMulti(pqs []turboquant.PreparedQuery, k int, accept func(i int) bool) [][]ScoredHit {
	return nil
}
func (s *stubScorer) Close() error { return nil }

// TestCollectionReopenReappliesScorer is the Bug 5(a) regression: WithScorer is
// runtime-only (not persisted), and the existing-collection path in Collection()
// dropped all options other than seed/bitWidth/rawStore. So after a restart there
// was no way to (re-)inject a runtime scorer. The fix re-applies WithScorer on
// the resident collection.
func TestCollectionReopenReappliesScorer(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "scorer.csdb")
	db, err := Open(dir, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create the collection (no scorer), then write a row so it is resident.
	coll := db.Collection("docs", WithBitWidth(2))
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	if err := coll.Put("a", Entry{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	if coll.scorer != nil {
		t.Fatal("setup: scorer should be nil before injection")
	}

	// Re-open the SAME resident collection with a runtime scorer.
	injected := &stubScorer{id: "gpu"}
	reopened := db.Collection("docs", WithScorer(injected))
	if reopened.err != nil {
		t.Fatal(reopened.err)
	}
	if reopened.scorer == nil {
		t.Fatal("Bug 5(a): WithScorer was dropped on the resident collection")
	}
	if got, ok := reopened.scorer.(*stubScorer); !ok || got.id != "gpu" {
		t.Fatalf("Bug 5(a): scorer = %#v, want injected stubScorer{gpu}", reopened.scorer)
	}
	// It must be the SAME resident collection, now carrying the scorer.
	if coll.scorer == nil {
		t.Fatal("Bug 5(a): injected scorer not visible on the original handle")
	}
}

// TestCollectionWithSparseMismatchErrors is the Bug 5(b) regression: WithSparse
// on a collection created without it was a silent no-op that later surfaced as a
// confusing SearchMulti rejection. The fix returns a clear mismatch error,
// mirroring the seed/bitWidth/rawStore mismatch behavior.
func TestCollectionWithSparseMismatchErrors(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sparsemismatch.csdb"), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create WITHOUT sparse.
	coll := db.Collection("docs", WithBitWidth(2))
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	if err := coll.Put("a", Entry{Text: "hello"}); err != nil {
		t.Fatal(err)
	}

	// Re-request WITH sparse -> clear mismatch error.
	mism := db.Collection("docs", WithSparse())
	if mism.err == nil {
		t.Fatal("Bug 5(b): WithSparse on a non-sparse collection did not error")
	}
	if !strings.Contains(mism.err.Error(), "sparse") {
		t.Fatalf("Bug 5(b): error %q does not mention the sparse mismatch", mism.err)
	}
}

// TestCollectionWithIndexTypeMismatchErrors confirms WithIndexType conflicting
// with the persisted index type surfaces a clear error rather than being
// silently dropped (Bug 5).
func TestCollectionWithIndexTypeMismatchErrors(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "indextypemismatch.csdb"), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("docs", WithBitWidth(2)) // flat
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	if err := coll.Put("a", Entry{Text: "hello"}); err != nil {
		t.Fatal(err)
	}

	mism := db.Collection("docs", WithIndexType(IndexHNSW))
	if mism.err == nil {
		t.Fatal("Bug 5: WithIndexType conflicting with persisted flat index did not error")
	}
	if !strings.Contains(mism.err.Error(), "index type") {
		t.Fatalf("Bug 5: error %q does not mention the index-type mismatch", mism.err)
	}

	// Re-requesting the SAME (flat) index type is fine and returns the resident.
	same := db.Collection("docs", WithIndexType(IndexFlat))
	if same.err != nil {
		t.Fatalf("re-requesting the same index type errored: %v", same.err)
	}
}

// TestFlatCollectionHNSWParamsHonoredOnRebuild is the Bug 6 regression: HNSW
// params were persisted only for HNSW collections, so a flat collection created
// WithHNSWParams then RebuildIndex(IndexHNSW)'d built with DEFAULT params (and a
// reopen wiped the supplied params). The fix persists meta.HNSW whenever
// WithHNSWParams was supplied, surviving a reopen so RebuildIndex honors it.
func TestFlatCollectionHNSWParamsHonoredOnRebuild(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hnswparams.csdb")
	want := HNSWParams{M: 7, EfConstruction: 123, EfSearch: 31}

	db, err := Open(dir, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	// FLAT collection created WithHNSWParams.
	coll := db.Collection("docs", WithBitWidth(2), WithHNSWParams(want))
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	if coll.indexType != IndexFlat {
		t.Fatalf("setup: index type = %v, want flat", coll.indexType)
	}
	if err := coll.Put("a", Entry{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("b", Entry{Text: "world"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the supplied params must survive in the manifest (the first-write
	// persistCollectionMeta must not have erased them).
	db2, err := Open(dir, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	coll2 := db2.Collection("docs")
	if coll2.err != nil {
		t.Fatal(coll2.err)
	}
	if coll2.hnsw != want {
		t.Fatalf("Bug 6: reopened flat collection hnsw params = %+v, want %+v", coll2.hnsw, want)
	}

	if err := coll2.RebuildIndex(IndexHNSW); err != nil {
		t.Fatal(err)
	}
	hw, ok := coll2.index.(*hnswIndex)
	if !ok {
		t.Fatalf("Bug 6: after RebuildIndex the index is %T, want *hnswIndex", coll2.index)
	}
	if hw.params != want {
		t.Fatalf("Bug 6: RebuildIndex(HNSW) used params %+v, want supplied %+v", hw.params, want)
	}
}
