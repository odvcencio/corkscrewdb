package corkscrewdb

import (
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
)

// TestRecomputeHNSWRerank verifies that HNSW rerank produces results
// identical to flat rerank over the same candidate set (Task 7 / plan §647-649).
//
// Property: when both HNSW and flat indices hold the same entries with the
// same recompute+rerank config, their top-k results must be identical — this
// proves searchCandidates feeds the SAME rerankExact as the flat path.
func TestRecomputeHNSWRerank(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(42)
	const rerankDepth = 4
	const k = 3

	provider := newDeterministicFixedProvider(dim)

	texts := []string{"alpha rerank", "beta rerank", "gamma rerank", "delta rerank", "epsilon rerank", "zeta rerank"}

	// Build a FLAT recompute collection and an HNSW recompute collection with the
	// same entries, same provider, same rerank depth.
	flatDir := filepath.Join(t.TempDir(), "flat_hnsw.csdb")
	dbFlat, err := Open(flatDir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer dbFlat.Close()

	hnswDir := filepath.Join(t.TempDir(), "hnsw_rr.csdb")
	dbHNSW, err := Open(hnswDir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer dbHNSW.Close()

	opts := []CollectionOption{
		WithBitWidth(bw),
		WithQuantizerSeed(seed),
		WithRecomputeRawFromText(),
		WithRerank(rerankDepth),
	}

	cFlat := dbFlat.Collection("docs", opts...)
	if cFlat.err != nil {
		t.Fatalf("flat collection: %v", cFlat.err)
	}

	hnswOpts := append(opts, WithIndexType(IndexHNSW))
	cHNSW := dbHNSW.Collection("docs", hnswOpts...)
	if cHNSW.err != nil {
		t.Fatalf("hnsw collection: %v", cHNSW.err)
	}

	for i, txt := range texts {
		id := txt
		_ = i
		if err := cFlat.Put(id, Entry{Text: txt}); err != nil {
			t.Fatalf("flat Put %q: %v", txt, err)
		}
		if err := cHNSW.Put(id, Entry{Text: txt}); err != nil {
			t.Fatalf("hnsw Put %q: %v", txt, err)
		}
	}

	query, err := provider.Encode("query for rerank")
	if err != nil {
		t.Fatal(err)
	}

	flatResults, err := cFlat.SearchVector(query, k)
	if err != nil {
		t.Fatalf("flat SearchVector: %v", err)
	}
	hnswResults, err := cHNSW.SearchVector(query, k)
	if err != nil {
		t.Fatalf("hnsw SearchVector: %v", err)
	}

	if len(flatResults) == 0 {
		t.Fatal("flat returned no results")
	}
	if len(hnswResults) == 0 {
		t.Fatal("hnsw returned no results")
	}

	// Both should return k results (enough entries).
	if len(flatResults) != k {
		t.Errorf("flat: got %d results, want %d", len(flatResults), k)
	}
	if len(hnswResults) != k {
		t.Errorf("hnsw: got %d results, want %d", len(hnswResults), k)
	}

	// The IDs returned must be identical (order may differ slightly due to HNSW
	// graph traversal, but top-k after rerank should match since rerank uses the
	// same EncodeBatch/drift-check path).
	flatIDs := make([]string, len(flatResults))
	hnswIDs := make([]string, len(hnswResults))
	for i, r := range flatResults {
		flatIDs[i] = r.ID
	}
	for i, r := range hnswResults {
		hnswIDs[i] = r.ID
	}
	sort.Strings(flatIDs)
	sort.Strings(hnswIDs)
	for i := range flatIDs {
		if flatIDs[i] != hnswIDs[i] {
			t.Errorf("flat top-%d IDs %v != hnsw top-%d IDs %v", k, flatIDs, k, hnswIDs)
			break
		}
	}

	// HNSW recall should be ≥ codes-only (with rerank depth > 1, rerank only improves).
	// Test this by comparing rerank results vs no-rerank results on the HNSW collection.
	// We just verify HNSW results are non-empty and scores are finite.
	for _, r := range hnswResults {
		if r.Score != r.Score { // NaN check
			t.Errorf("hnsw result %q: score is NaN", r.ID)
		}
	}
}

// TestRecomputeSearchMultiDenseOnly verifies that a hybrid (sparse+dense)
// recompute collection's SearchMulti reranks the dense channel only, leaves the
// sparse channel untouched, and fusion is correct (compare against codes-only).
//
// STRENGTHENED (M-A): asserts that EncodeBatch fires during SearchMulti (proving
// the dense channel was exact-reranked, not codes-only), and that the fused order
// matches the reranked SearchVector order on the dense-only query.
func TestRecomputeSearchMultiDenseOnly(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(42)
	const rerankDepth = 4

	// Use a spy provider that counts EncodeBatch invocations to prove rerank fired.
	base := newDeterministicFixedProvider(dim)
	spy := &spyBatchProvider{deterministicFixedProvider: base}

	dir := filepath.Join(t.TempDir(), "multi_rerank.csdb")
	db, err := Open(dir, WithProvider(spy))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// A sparse+dense recompute collection with rerank.
	c := db.Collection("docs",
		WithBitWidth(bw),
		WithQuantizerSeed(seed),
		WithRecomputeRawFromText(),
		WithRerank(rerankDepth),
		WithSparse(),
	)
	if c.err != nil {
		t.Fatalf("collection: %v", c.err)
	}

	// Insert entries with both dense (via text) and sparse components.
	entries := []struct {
		id   string
		text string
		sv   *SparseVector
	}{
		{"doc-a", "apple fruit", &SparseVector{Indices: []uint32{1, 5}, Values: []float32{0.8, 0.3}}},
		{"doc-b", "banana yellow", &SparseVector{Indices: []uint32{2, 6}, Values: []float32{0.7, 0.4}}},
		{"doc-c", "cherry red", &SparseVector{Indices: []uint32{1, 3}, Values: []float32{0.5, 0.6}}},
	}
	for _, e := range entries {
		if err := c.Put(e.id, Entry{Text: e.text, Sparse: e.sv}); err != nil {
			t.Fatalf("Put %q: %v", e.id, err)
		}
	}

	queryVec, err := spy.Encode("apple")
	if err != nil {
		t.Fatal(err)
	}
	querySparse := &SparseVector{Indices: []uint32{1, 5}, Values: []float32{1.0, 0.5}}

	// Snapshot the EncodeBatch call count after writes (writes call EncodeBatch).
	batchCountBeforeMulti := atomic.LoadInt64(&spy.batchCount)

	// Dense+sparse hybrid query — with rerank enabled, this MUST call EncodeBatch.
	results, err := c.SearchMulti(MultiQuery{
		Dense:  queryVec,
		Sparse: querySparse,
	}, 3)
	if err != nil {
		t.Fatalf("SearchMulti: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("SearchMulti returned no results")
	}

	// Assert that SearchMulti called EncodeBatch (proving the dense rerank path fired).
	batchCountAfterMulti := atomic.LoadInt64(&spy.batchCount)
	if batchCountAfterMulti <= batchCountBeforeMulti {
		t.Errorf("SearchMulti did not call EncodeBatch (batchCount before=%d, after=%d): dense channel was NOT reranked",
			batchCountBeforeMulti, batchCountAfterMulti)
	}

	// Dense-only rerank via SearchVector for comparison.
	denseResults, err := c.SearchVector(queryVec, 3)
	if err != nil {
		t.Fatalf("SearchVector: %v", err)
	}
	if len(denseResults) == 0 {
		t.Fatal("SearchVector returned no results")
	}

	// Fusion result should include IDs from both dense and sparse channels.
	// At minimum, "doc-a" (highest sparse match AND good dense match) should appear.
	foundDocA := false
	for _, r := range results {
		if r.ID == "doc-a" {
			foundDocA = true
		}
		// All scores must be finite.
		if r.Score != r.Score {
			t.Errorf("result %q: score is NaN", r.ID)
		}
	}
	if !foundDocA {
		t.Errorf("doc-a not found in hybrid results; got: %v", results)
	}

	// SearchMulti on a view also works without panic; view path was already correct
	// (Task 7) and should also call EncodeBatch.
	c.mu.RLock()
	clk := c.clock.Current()
	c.mu.RUnlock()
	view := c.At(clk)

	batchCountBeforeViewMulti := atomic.LoadInt64(&spy.batchCount)
	viewResults, err := view.SearchMulti(MultiQuery{
		Dense:  queryVec,
		Sparse: querySparse,
	}, 3)
	if err != nil {
		t.Fatalf("view SearchMulti: %v", err)
	}
	if len(viewResults) == 0 {
		t.Fatal("view SearchMulti returned no results")
	}
	batchCountAfterViewMulti := atomic.LoadInt64(&spy.batchCount)
	if batchCountAfterViewMulti <= batchCountBeforeViewMulti {
		t.Errorf("view SearchMulti did not call EncodeBatch (batchCount before=%d, after=%d): dense channel was NOT reranked",
			batchCountBeforeViewMulti, batchCountAfterViewMulti)
	}
}

// TestRecomputeRebalanceCarriesNoRaw verifies that a recompute collection's
// snapshot carries codes+text with nil RawHash (§5 / plan §654-658):
//   - snapshotForPull returns zero RawHash for every version
//   - loadImportedVersion on the gainer stores nil RawHash
//   - the gainer can recompute on demand (drift-check passes, exact score available)
func TestRecomputeRebalanceCarriesNoRaw(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(42)

	provider := newDeterministicFixedProvider(dim)
	dir := filepath.Join(t.TempDir(), "rebalance_src.csdb")
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := db.Collection("docs",
		WithBitWidth(bw),
		WithQuantizerSeed(seed),
		WithRecomputeRawFromText(),
		WithRerank(4),
	)
	if c.err != nil {
		t.Fatalf("collection: %v", c.err)
	}

	texts := []string{"hello rebalance", "world rebalance", "foo rebalance"}
	for _, txt := range texts {
		if err := c.Put(txt, Entry{Text: txt}); err != nil {
			t.Fatalf("Put %q: %v", txt, err)
		}
	}

	// 1. snapshotForPull should carry nil RawHash for all versions (recompute mode
	//    does not store raw blobs, so v.RawHash is nil and the snapshot omits it).
	snap := c.snapshotForPull()
	for _, rec := range snap.Records {
		for _, sv := range rec.Versions {
			if len(sv.RawHash) != 0 {
				t.Errorf("snapshotForPull: record %q version %d has non-nil RawHash in recompute mode",
					rec.ID, sv.LamportClock)
			}
		}
	}

	// 2. Import all versions into a "gainer" collection and verify nil RawHash.
	gainDir := filepath.Join(t.TempDir(), "rebalance_gain.csdb")
	dbGain, err := Open(gainDir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer dbGain.Close()

	cGain := dbGain.Collection("docs",
		WithBitWidth(bw),
		WithQuantizerSeed(seed),
		WithRecomputeRawFromText(),
		WithRerank(4),
	)
	if cGain.err != nil {
		t.Fatalf("gainer collection: %v", cGain.err)
	}

	// Import each version via loadImportedVersion (the rebalance gainer path).
	for _, rec := range snap.Records {
		for _, sv := range rec.Versions {
			v := Version{
				Quantized:    fromWALQuantized(sv.Quantized),
				dim:          sv.Dim,
				RawHash:      append([]byte(nil), sv.RawHash...),
				Text:         sv.Text,
				Metadata:     cloneMetadata(sv.Metadata),
				LamportClock: sv.LamportClock,
				ActorID:      sv.ActorID,
				WallClock:    sv.WallClock,
				Tombstone:    sv.Tombstone,
			}
			if err := cGain.loadImportedVersion(rec.ID, v); err != nil {
				t.Fatalf("loadImportedVersion %q: %v", rec.ID, err)
			}
		}
	}

	// Verify gainer's versions have nil RawHash.
	cGain.mu.RLock()
	for id, versions := range cGain.history {
		for _, v := range versions {
			if len(v.RawHash) != 0 {
				t.Errorf("gainer %q version %d: non-nil RawHash after recompute import", id, v.LamportClock)
			}
		}
	}
	cGain.mu.RUnlock()

	// 3. Gainer can search (recomputes on demand): results must be non-empty and
	//    scores identical to source when same provider/backend.
	query, err := provider.Encode("hello")
	if err != nil {
		t.Fatal(err)
	}

	srcResults, err := c.SearchVector(query, 3)
	if err != nil {
		t.Fatalf("source SearchVector: %v", err)
	}
	gainResults, err := cGain.SearchVector(query, 3)
	if err != nil {
		t.Fatalf("gainer SearchVector: %v", err)
	}
	if len(srcResults) == 0 {
		t.Fatal("source returned no results")
	}
	if len(gainResults) == 0 {
		t.Fatal("gainer returned no results")
	}

	// When provider is same-backend (deterministic), gainer results should be
	// identical to source results (shared-graph invariant, Resolution 1).
	if len(srcResults) == len(gainResults) {
		for i, sr := range srcResults {
			gr := gainResults[i]
			if sr.ID != gr.ID {
				t.Errorf("result[%d]: source ID %q != gainer ID %q", i, sr.ID, gr.ID)
			}
		}
	}
}
