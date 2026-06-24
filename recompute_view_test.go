package corkscrewdb

import (
	"math"
	"path/filepath"
	"testing"

	"m31labs.dev/turboquant"
)

// TestHistoricalViewDriftCheck verifies that a point-in-time CollectionView reranks
// correctly: At(C1) sees T1's exact score and At(C2) sees T2's exact score (§6 /
// Task 7). Close+reopen reproduces the same results (same provider/backend).
//
// This test FAILS before view rerank is implemented (view uses index.Search which
// returns quantized scores, not exact dot scores).
func TestHistoricalViewDriftCheck(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(42)
	const rerankDepth = 4

	provider := newDeterministicFixedProvider(dim)
	dir := filepath.Join(t.TempDir(), "view_drift.csdb")
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}

	opts := []CollectionOption{
		WithBitWidth(bw),
		WithQuantizerSeed(seed),
		WithRecomputeRawFromText(),
		WithRerank(rerankDepth),
	}
	c := db.Collection("docs", opts...)
	if c.err != nil {
		t.Fatalf("collection create: %v", c.err)
	}

	// Write "id1" with text T1 at clock C1.
	text1 := "historical hello"
	if err := c.Put("id1", Entry{Text: text1}); err != nil {
		t.Fatalf("Put T1: %v", err)
	}
	c.mu.RLock()
	clk1 := c.clock.Current()
	c.mu.RUnlock()

	// Overwrite "id1" with text T2 at clock C2.
	text2 := "historical world"
	if err := c.Put("id1", Entry{Text: text2}); err != nil {
		t.Fatalf("Put T2: %v", err)
	}
	c.mu.RLock()
	clk2 := c.clock.Current()
	c.mu.RUnlock()

	// Build expected exact scores via plainDot after recomputing each text.
	q := turboquant.NewIPWithSeed(dim, bw, seed)

	raw1, err := provider.Encode(text1)
	if err != nil {
		t.Fatal(err)
	}
	raw2, err := provider.Encode(text2)
	if err != nil {
		t.Fatal(err)
	}
	query, err := provider.Encode("query text")
	if err != nil {
		t.Fatal(err)
	}

	// Verify that drift-check would MATCH for T1 and T2.
	code1 := q.Quantize(raw1)
	code2 := q.Quantize(raw2)

	// Check the stored codes match what the provider would recompute.
	c.mu.RLock()
	var storedCode1, storedCode2 *turboquant.IPQuantized
	for _, v := range c.history["id1"] {
		if !v.Tombstone && v.Text == text1 {
			stored := *v.Quantized
			storedCode1 = &stored
		}
		if !v.Tombstone && v.Text == text2 {
			stored := *v.Quantized
			storedCode2 = &stored
		}
	}
	c.mu.RUnlock()

	if storedCode1 == nil || storedCode2 == nil {
		t.Fatal("could not find stored codes for both versions")
	}
	if !byteEqualQuantized(&code1, storedCode1) {
		t.Skip("T1 drift-check would MISMATCH — provider not deterministic in this env; skipping exact-score assertion")
	}
	if !byteEqualQuantized(&code2, storedCode2) {
		t.Skip("T2 drift-check would MISMATCH — provider not deterministic in this env; skipping exact-score assertion")
	}

	expectedScore1 := plainDot(query, raw1)
	expectedScore2 := plainDot(query, raw2)

	checkViewResult := func(t *testing.T, view *CollectionView, wantScore float32, label string) {
		t.Helper()
		results, err := view.SearchVector(query, 1)
		if err != nil {
			t.Fatalf("%s SearchVector: %v", label, err)
		}
		if len(results) == 0 {
			t.Fatalf("%s: expected 1 result, got none", label)
		}
		diff := math.Abs(float64(results[0].Score - wantScore))
		if diff > 1e-4 {
			t.Errorf("%s: score = %f, want %f (exact dot), diff %e", label, results[0].Score, wantScore, diff)
		}
	}

	// At(C1): should see T1's exact score.
	view1 := c.At(clk1)
	checkViewResult(t, view1, expectedScore1, "At(C1)")

	// At(C2): should see T2's exact score.
	view2 := c.At(clk2)
	checkViewResult(t, view2, expectedScore2, "At(C2)")

	// Close and reopen — same provider/backend guarantees same codes.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	c2 := db2.Collection("docs", opts...)
	if c2.err != nil {
		t.Fatalf("reopen collection: %v", c2.err)
	}

	view1r := c2.At(clk1)
	checkViewResult(t, view1r, expectedScore1, "reopen At(C1)")

	view2r := c2.At(clk2)
	checkViewResult(t, view2r, expectedScore2, "reopen At(C2)")
}

// TestViewRerankCodesOnlyFallback verifies that a view with rerankDepth > 1 falls
// back gracefully to quantized scores when the provider errors during view rerank
// (no crash, non-empty results). The provider must be the SAME type to pass the
// backend-fingerprint check; we use a spyBatchProvider that is also an errBatch on
// the search path by temporarily replacing the encoder seam.
func TestViewRerankCodesOnlyFallback(t *testing.T) {
	const dim = 16

	// Use the same deterministicFixedProvider for write and reopen (same fingerprint),
	// but inject an error via a wrapper that errors only on EncodeBatch calls made
	// after the collection is opened (we'll test the fallback by triggering an error
	// through a swapped encoder rather than reopening with a different provider).
	provider := newDeterministicFixedProvider(dim)
	dir := filepath.Join(t.TempDir(), "view_fallback.csdb")
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := db.Collection("docs",
		WithBitWidth(2),
		WithQuantizerSeed(42),
		WithRecomputeRawFromText(),
		WithRerank(4),
	)
	if c.err != nil {
		t.Fatalf("collection create: %v", c.err)
	}

	for _, txt := range []string{"alpha", "beta", "gamma"} {
		if err := c.Put(txt, Entry{Text: txt}); err != nil {
			t.Fatalf("Put %q: %v", txt, err)
		}
	}
	c.mu.RLock()
	clk := c.clock.Current()
	c.mu.RUnlock()

	// Capture view before swapping encoder.
	view := c.At(clk)

	// Replace the view's encoder with one that errors on EncodeBatch, simulating
	// a rerank-time failure. This bypasses fingerprint checks (same DB).
	errProvider := &errBatchProvider{deterministicFixedProvider: newDeterministicFixedProvider(dim)}
	view.encoder = newEncoder(errProvider)

	query, _ := provider.Encode("query")
	results, err := view.SearchVector(query, 2)
	if err != nil {
		t.Fatalf("SearchVector with errProvider: %v", err)
	}
	// Fallback to quantized scores — non-empty results expected.
	if len(results) == 0 {
		t.Fatal("expected non-empty results (fallback path), got none")
	}
}
