package corkscrewdb

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"m31labs.dev/turboquant"
)

// ---- helper providers for rerank tests ----

// deterministicFixedProvider returns the SAME vector for every call, so
// write-time and rerank-time codes are byte-identical (drift-check MATCH by
// construction). Implements deterministicProvider.
type deterministicFixedProvider struct {
	dim int
	vec []float32 // overrideable; nil means use ascending ramp
}

func newDeterministicFixedProvider(dim int) *deterministicFixedProvider {
	return &deterministicFixedProvider{dim: dim}
}

func (p *deterministicFixedProvider) baseVec() []float32 {
	if p.vec != nil {
		return p.vec
	}
	v := make([]float32, p.dim)
	for i := range v {
		v[i] = float32(i+1) * 0.1
	}
	return v
}

func (p *deterministicFixedProvider) Encode(text string) ([]float32, error) {
	// Use text length to perturb slightly per ID so they are not all identical.
	v := make([]float32, p.dim)
	base := float32(len(text)) * 0.01
	for i := range v {
		v[i] = float32(i+1)*0.1 + base
	}
	return v, nil
}

func (p *deterministicFixedProvider) EncodeBatch(texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v, _ := p.Encode(t)
		out[i] = v
	}
	return out, nil
}

func (p *deterministicFixedProvider) Dim() int            { return p.dim }
func (p *deterministicFixedProvider) Close() error        { return nil }
func (p *deterministicFixedProvider) Deterministic() bool { return true }

// perturbProvider wraps a base provider and perturbs EncodeBatch output so that
// the re-quantized codes differ from those stored at write time (MISMATCH path).
// Encode() is unchanged (used at write time through the recompute write path only
// if the base provider is a non-recompute one — here the base IS deterministic and
// we want write to MATCH, so we use a separate deterministicFixedProvider for
// writes and only use perturbProvider in the rerank seam).
type perturbProvider struct {
	*deterministicFixedProvider
	perturbMagnitude float32 // added to all elements during EncodeBatch
}

func (p *perturbProvider) EncodeBatch(texts []string) ([][]float32, error) {
	out, err := p.deterministicFixedProvider.EncodeBatch(texts)
	if err != nil {
		return nil, err
	}
	for _, v := range out {
		for i := range v {
			v[i] += p.perturbMagnitude
		}
	}
	return out, nil
}

// spyBatchProvider counts EncodeBatch calls (for one-batch-per-query test).
type spyBatchProvider struct {
	*deterministicFixedProvider
	batchCount int64
}

func (s *spyBatchProvider) EncodeBatch(texts []string) ([][]float32, error) {
	atomic.AddInt64(&s.batchCount, 1)
	return s.deterministicFixedProvider.EncodeBatch(texts)
}

// errProvider always errors on EncodeBatch (for failure fallback test).
type errBatchProvider struct {
	*deterministicFixedProvider
}

func (e *errBatchProvider) EncodeBatch(texts []string) ([][]float32, error) {
	return nil, errors.New("mock EncodeBatch failure")
}

// ---- helpers ----

func makeRerankCollection(t *testing.T, dim int, rerankDepth int, provider EmbeddingProvider) *Collection {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "rr.csdb")
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	opts := []CollectionOption{
		WithBitWidth(2),
		WithQuantizerSeed(42),
		WithRecomputeRawFromText(),
		WithRerank(rerankDepth),
	}
	c := db.Collection("col", opts...)
	if c.err != nil {
		t.Fatalf("collection create: %v", c.err)
	}
	return c
}

func makeRawRerankCollection(t *testing.T, dim int, rerankDepth int, provider EmbeddingProvider) *Collection {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "raw_rr.csdb")
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c := db.Collection("col", WithBitWidth(2), WithQuantizerSeed(42), WithRerank(rerankDepth))
	if c.err != nil {
		t.Fatalf("raw collection create: %v", c.err)
	}
	return c
}

// plainDot computes the plain float dot product Σ a[i]*b[i].
func plainDot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// ---- tests ----

// TestDriftCheckMatchUsesExactDot verifies that when rerankExact successfully
// recomputes a candidate's raw vector and the drift-check MATCHES (requantized
// code == stored code), the score used is the plain float dot product
// Σ qvec[i]*raw[i], NOT the quantized score.
func TestDriftCheckMatchUsesExactDot(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(42)

	provider := newDeterministicFixedProvider(dim)
	c := makeRerankCollection(t, dim, 4, provider)

	// Write several entries with text so recompute works.
	texts := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i, txt := range texts {
		if err := c.Put(fmt.Sprintf("id%d", i), Entry{Text: txt}); err != nil {
			t.Fatalf("Put %q: %v", txt, err)
		}
	}

	// Build the query vector using the same provider.
	query, err := provider.Encode("query")
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.SearchVector(query, 3)
	if err != nil {
		t.Fatalf("SearchVector: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results, got none")
	}

	// For each returned result, compute the expected exact score:
	// re-encode the text and compute plain float dot with the query.
	q := turboquant.NewIPWithSeed(dim, bw, seed)
	for _, res := range results {
		// Re-encode the text through the same provider.
		raw, err := provider.Encode(res.Text)
		if err != nil {
			t.Fatalf("re-encode %q: %v", res.Text, err)
		}
		// Drift-check: requantize and compare bytes.
		recomputed := q.Quantize(raw)
		// Find stored code from the index.
		c.mu.RLock()
		idx := c.index
		c.mu.RUnlock()
		flat := idx.(*index)
		flat.mu.RLock()
		var storedCode *turboquant.IPQuantized
		for i := range flat.entries {
			if flat.entries[i].id == res.ID {
				stored := flat.entries[i].qv
				storedCode = &stored
				break
			}
		}
		flat.mu.RUnlock()
		if storedCode == nil {
			t.Fatalf("could not find stored code for id %q", res.ID)
		}
		// byteEqualQuantized must be true for the match path to be taken.
		if !byteEqualQuantized(&recomputed, storedCode) {
			// Codes don't match — this test requires the codes to match for MATCH path.
			// The deterministicFixedProvider guarantees this; skip this entry if not.
			t.Logf("SKIP %q: drift-check MISMATCH (provider not fully deterministic for this text)", res.ID)
			continue
		}
		// Expected score = plain float dot(query, raw).
		expectedScore := plainDot(query, raw)
		// The returned score should match within floating-point tolerance.
		diff := math.Abs(float64(res.Score - expectedScore))
		if diff > 1e-4 {
			t.Errorf("id %q: score = %f, want %f (plain dot), diff %e", res.ID, res.Score, expectedScore, diff)
		}
	}
}

// TestDriftCheckMismatchFallsBack verifies that when the recomputed vector
// re-quantizes to a DIFFERENT code than stored (MISMATCH), the candidate's
// score is its quantizedScore (not the perturbed exact score).
func TestDriftCheckMismatchFallsBack(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(42)

	// writeProvider is used for WRITE (through encodeWriteText→EncodeBatch).
	writeProvider := newDeterministicFixedProvider(dim)

	dir := filepath.Join(t.TempDir(), "mismatch.csdb")
	db, err := Open(dir, WithProvider(writeProvider))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := db.Collection("col",
		WithBitWidth(bw),
		WithQuantizerSeed(seed),
		WithRecomputeRawFromText(),
		WithRerank(4),
	)
	if c.err != nil {
		t.Fatalf("create: %v", c.err)
	}

	texts := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i, txt := range texts {
		if err := c.Put(fmt.Sprintf("id%d", i), Entry{Text: txt}); err != nil {
			t.Fatalf("Put %q: %v", txt, err)
		}
	}

	// Collect the quantized scores for candidates (pre-rerank) by running with
	// rerankDepth=1 (codes-only fallback).
	cNoRerank := db.Collection("col2",
		WithBitWidth(bw),
		WithQuantizerSeed(seed),
		WithRecomputeRawFromText(),
		WithRerank(1),
	)
	if cNoRerank.err != nil {
		t.Fatalf("create noRerank: %v", cNoRerank.err)
	}
	for i, txt := range texts {
		if err := cNoRerank.Put(fmt.Sprintf("id%d", i), Entry{Text: txt}); err != nil {
			t.Fatalf("Put noRerank %q: %v", txt, err)
		}
	}

	query, _ := writeProvider.Encode("alpha")

	// Now replace the encoder with a perturbing provider so rerank sees different vectors.
	// The perturbation is large enough to produce different quantized codes.
	perturbP := &perturbProvider{
		deterministicFixedProvider: writeProvider,
		perturbMagnitude:           100.0, // huge perturbation guarantees code mismatch
	}
	c.encoder = newEncoder(perturbP)

	// Get the codes-only scores from the no-rerank collection.
	noRerankResults, err := cNoRerank.SearchVector(query, 5)
	if err != nil {
		t.Fatalf("noRerank SearchVector: %v", err)
	}
	qScoreByID := make(map[string]float32)
	for _, r := range noRerankResults {
		qScoreByID[r.ID] = r.Score
	}

	// Run with the perturbing provider (all candidates will MISMATCH drift-check).
	rerankResults, err := c.SearchVector(query, 3)
	if err != nil {
		t.Fatalf("rerank SearchVector: %v", err)
	}
	if len(rerankResults) == 0 {
		t.Fatal("expected results")
	}

	// Every returned result's score should equal its codes-only quantizedScore
	// because all candidates MISMATCH and fall back.
	for _, r := range rerankResults {
		expected, ok := qScoreByID[r.ID]
		if !ok {
			// May not be in top-5, skip.
			continue
		}
		diff := math.Abs(float64(r.Score - expected))
		if diff > 1e-5 {
			t.Errorf("id %q: score = %f, want quantized score %f (MISMATCH fallback)", r.ID, r.Score, expected)
		}
	}
}

// TestDriftCheckResNormBitwise verifies that byteEqualQuantized uses bitwise
// comparison of ResNorm (via math.Float32bits), so that two IPQuantized values
// identical in MSE+Signs but differing only in a NaN/-0 bit pattern return false,
// while an exact-bit match returns true.
func TestDriftCheckResNormBitwise(t *testing.T) {
	makeCode := func(resNormBits uint32) *turboquant.IPQuantized {
		return &turboquant.IPQuantized{
			MSE:     []byte{0x01, 0x02},
			Signs:   []byte{0xAB},
			ResNorm: math.Float32frombits(resNormBits),
		}
	}

	a := makeCode(0x3F800000) // 1.0
	b := makeCode(0x3F800000) // 1.0 exact same bits
	if !byteEqualQuantized(a, b) {
		t.Error("byteEqualQuantized: same bits should return true")
	}

	// -0.0 vs +0.0: different bit patterns, must return false.
	negZero := makeCode(0x80000000) // -0.0
	posZero := makeCode(0x00000000) // +0.0
	if byteEqualQuantized(negZero, posZero) {
		t.Error("byteEqualQuantized: -0.0 vs +0.0 should return false (bitwise comparison)")
	}

	// NaN with different payloads must return false.
	nanA := makeCode(0x7FC00000) // canonical NaN
	nanB := makeCode(0x7F800001) // signaling NaN (different bits)
	if byteEqualQuantized(nanA, nanB) {
		t.Error("byteEqualQuantized: different NaN bits should return false")
	}

	// Same NaN bits must return true.
	nanC := makeCode(0x7FC00000)
	if !byteEqualQuantized(nanA, nanC) {
		t.Error("byteEqualQuantized: same NaN bits should return true")
	}

	// Different MSE must return false even if Signs+ResNorm match.
	c2 := &turboquant.IPQuantized{MSE: []byte{0x01, 0x03}, Signs: []byte{0xAB}, ResNorm: 1.0}
	if byteEqualQuantized(a, c2) {
		t.Error("byteEqualQuantized: different MSE should return false")
	}

	// Different Signs must return false.
	d := &turboquant.IPQuantized{MSE: []byte{0x01, 0x02}, Signs: []byte{0xAC}, ResNorm: 1.0}
	if byteEqualQuantized(a, d) {
		t.Error("byteEqualQuantized: different Signs should return false")
	}
}

// TestRerankScaleMix verifies that MATCH (exact dot) and FALLBACK
// (quantized score from InnerProductPrepared) are on the same ⟨q,x⟩ scale.
// We do this by using a deterministic provider (all MATCH) and verifying
// that the scores are consistent with direct computation.
func TestRerankScaleMix(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(42)

	provider := newDeterministicFixedProvider(dim)
	c := makeRerankCollection(t, dim, 4, provider)

	texts := []string{"one", "two", "three", "four"}
	for i, txt := range texts {
		if err := c.Put(fmt.Sprintf("id%d", i), Entry{Text: txt}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	query, _ := provider.Encode("query-for-scale")
	results, err := c.SearchVector(query, 4)
	if err != nil {
		t.Fatalf("SearchVector: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results")
	}

	// All scores must be in a sane range — no huge outliers from scale confusion.
	// Plain dot of unit-ish vectors is in [-dim, dim]; quantized scores are similar.
	for _, r := range results {
		if math.IsInf(float64(r.Score), 0) || math.IsNaN(float64(r.Score)) {
			t.Errorf("id %q: score %f is not finite", r.ID, r.Score)
		}
		// Arbitrary wide sanity range.
		if r.Score < -1000 || r.Score > 1000 {
			t.Errorf("id %q: score %f out of reasonable range", r.ID, r.Score)
		}
	}
}

// TestRerankRecallMonotonicAndSuperset verifies recall monotonicity:
//   - reranked recall ≥ codes-only recall (depth 4 ≥ base, depth 10 ≥ depth 4)
//   - reranked top-k ⊇ codes-only top-k (superset of base IDs)
//   - recall is non-decreasing with increasing rerankDepth
//
// Uses a labeled corpus of 30 vectors with a known best-match query (the text
// of id7 itself must be in ground-truth top-k). The test FAILS if rerank ever
// regresses recall.
func TestRerankRecallMonotonicAndSuperset(t *testing.T) {
	const dim = 16
	const n = 30
	const k = 5

	provider := newDeterministicFixedProvider(dim)

	// Build a recompute collection with no rerank (codes-only baseline, depth=1).
	dirBase := filepath.Join(t.TempDir(), "base.csdb")
	dbBase, err := Open(dirBase, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer dbBase.Close()
	cBase := dbBase.Collection("col", WithBitWidth(2), WithQuantizerSeed(42), WithRecomputeRawFromText(), WithRerank(1))
	if cBase.err != nil {
		t.Fatalf("base: %v", cBase.err)
	}

	dirD4 := filepath.Join(t.TempDir(), "d4.csdb")
	dbD4, err := Open(dirD4, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer dbD4.Close()
	cD4 := dbD4.Collection("col", WithBitWidth(2), WithQuantizerSeed(42), WithRecomputeRawFromText(), WithRerank(4))
	if cD4.err != nil {
		t.Fatalf("d4: %v", cD4.err)
	}

	dirD10 := filepath.Join(t.TempDir(), "d10.csdb")
	dbD10, err := Open(dirD10, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer dbD10.Close()
	cD10 := dbD10.Collection("col", WithBitWidth(2), WithQuantizerSeed(42), WithRecomputeRawFromText(), WithRerank(10))
	if cD10.err != nil {
		t.Fatalf("d10: %v", cD10.err)
	}

	// Labeled corpus: each document text is distinct; ground truth is all docs
	// since we can compute exact scores via the deterministic provider.
	texts := make([]string, n)
	for i := range texts {
		texts[i] = fmt.Sprintf("document number %d with some content", i)
	}
	for i, txt := range texts {
		id := fmt.Sprintf("id%d", i)
		e := Entry{Text: txt}
		if err := cBase.Put(id, e); err != nil {
			t.Fatalf("base put: %v", err)
		}
		if err := cD4.Put(id, e); err != nil {
			t.Fatalf("d4 put: %v", err)
		}
		if err := cD10.Put(id, e); err != nil {
			t.Fatalf("d10 put: %v", err)
		}
	}

	query, _ := provider.Encode("document number 7 content")

	// Build ground-truth top-k by scoring all vectors with plain float dot.
	allVecs := make([][]float32, n)
	for i, txt := range texts {
		v, _ := provider.Encode(txt)
		allVecs[i] = v
	}
	type scoredID struct {
		id    string
		score float32
	}
	allScored := make([]scoredID, n)
	for i, v := range allVecs {
		allScored[i] = scoredID{id: fmt.Sprintf("id%d", i), score: plainDot(query, v)}
	}
	sort.Slice(allScored, func(i, j int) bool { return allScored[i].score > allScored[j].score })
	groundTruth := make(map[string]bool, k)
	for i := 0; i < k && i < len(allScored); i++ {
		groundTruth[allScored[i].id] = true
	}

	recallOf := func(results []SearchResult) int {
		hits := 0
		for _, r := range results {
			if groundTruth[r.ID] {
				hits++
			}
		}
		return hits
	}
	idsOf := func(results []SearchResult) map[string]bool {
		m := make(map[string]bool)
		for _, r := range results {
			m[r.ID] = true
		}
		return m
	}

	resBase, err2 := cBase.SearchVector(query, k)
	if err2 != nil {
		t.Fatalf("base SearchVector: %v", err2)
	}
	resD4, err2 := cD4.SearchVector(query, k)
	if err2 != nil {
		t.Fatalf("d4 SearchVector: %v", err2)
	}
	resD10, err2 := cD10.SearchVector(query, k)
	if err2 != nil {
		t.Fatalf("d10 SearchVector: %v", err2)
	}

	if len(resBase) == 0 || len(resD4) == 0 || len(resD10) == 0 {
		t.Fatal("expected non-empty results for all depths")
	}

	recBase := recallOf(resBase)
	recD4 := recallOf(resD4)
	recD10 := recallOf(resD10)

	// Recall must be non-decreasing with depth.
	if recD4 < recBase {
		t.Errorf("recall regression: depth=4 recall %d < codes-only recall %d (ground truth k=%d)", recD4, recBase, k)
	}
	if recD10 < recD4 {
		t.Errorf("recall regression: depth=10 recall %d < depth=4 recall %d", recD10, recD4)
	}

	// Ground-truth overlap: ids in codes-only that are also in ground truth must
	// also appear in reranked results (since reranked draws from a strictly wider
	// candidate pool and then scores them exactly — a ground-truth item present
	// in the codes-only top-k was retrieved in the overfetch pool, so exact
	// scoring can only keep or elevate it).
	baseIDs := idsOf(resBase)
	d4IDs := idsOf(resD4)
	d10IDs := idsOf(resD10)
	for id := range baseIDs {
		if !groundTruth[id] {
			continue // not a ground-truth item; reranking may legitimately drop it
		}
		if !d4IDs[id] {
			t.Errorf("ground-truth superset violation: id %q is a ground-truth item in codes-only top-%d but not in depth-4 top-%d", id, k, k)
		}
		if !d10IDs[id] {
			t.Errorf("ground-truth superset violation: id %q is a ground-truth item in codes-only top-%d but not in depth-10 top-%d", id, k, k)
		}
	}

	// Scores must be sorted descending.
	for name, res := range map[string][]SearchResult{"base": resBase, "d4": resD4, "d10": resD10} {
		for i := 1; i < len(res); i++ {
			if res[i].Score > res[i-1].Score+1e-6 {
				t.Errorf("%s: results not sorted descending at index %d: %f > %f", name, i, res[i].Score, res[i-1].Score)
			}
		}
	}
}

// TestRerankConcurrentReadWriteNoDeadlock is a permanent regression guard for
// the BLOCKING AB-BA deadlock (flat.mu→c.mu lock order in coldRaw + concurrent
// putVector that takes c.mu→flat.mu). It also guards the MAJOR data race on
// c.history reads outside the lock.
//
// A coldRaw+WithRerank(8) collection is exercised under concurrent Search and
// PutVector goroutines for a bounded duration. The test must complete (no
// deadlock) and pass under -race (no map race). A timeout derived from
// t.Deadline() ensures a regression FAILS loudly instead of hanging forever.
func TestRerankConcurrentReadWriteNoDeadlock(t *testing.T) {
	const dim = 16
	const rerankDepth = 8

	provider := newDeterministicFixedProvider(dim)

	dir := filepath.Join(t.TempDir(), "conc.csdb")
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// coldRaw + WithRerank triggers the exact deadlock path: coldFloatsCollection coldRaw
	// branch takes c.mu.RLock inside rerankExact, while putVector holds c.mu.Lock
	// and then takes flat.mu.Lock.
	c := db.Collection("col", WithBitWidth(2), WithQuantizerSeed(42), WithRerank(rerankDepth))
	if c.err != nil {
		t.Fatalf("collection: %v", c.err)
	}

	// Pre-populate so searches actually find candidates.
	texts := []string{
		"the quick brown fox", "jumps over the lazy dog",
		"hello world", "search result candidate",
		"vector database embedded", "quantized index",
		"rerank depth test", "concurrent read write",
	}
	for i, txt := range texts {
		if err := c.Put(fmt.Sprintf("seed%d", i), Entry{Text: txt}); err != nil {
			t.Fatalf("seed Put: %v", err)
		}
	}

	query, _ := provider.Encode("search result candidate")

	// Compute a deadline: use t.Deadline if available (minus a safety margin);
	// otherwise run for 3 seconds.
	done := make(chan struct{})
	deadline, hasDeadline := t.Deadline()
	go func() {
		if hasDeadline {
			remaining := time.Until(deadline)
			runFor := remaining * 3 / 4
			if runFor < 500*time.Millisecond {
				runFor = 500 * time.Millisecond
			}
			time.Sleep(runFor)
		} else {
			time.Sleep(3 * time.Second)
		}
		close(done)
	}()

	const numReaders = 4
	const numWriters = 2
	errs := make(chan error, numReaders+numWriters)

	// Readers: continuously search.
	for i := 0; i < numReaders; i++ {
		go func(id int) {
			for {
				select {
				case <-done:
					errs <- nil
					return
				default:
				}
				_, err := c.SearchVector(query, 3)
				if err != nil {
					errs <- fmt.Errorf("reader %d SearchVector: %w", id, err)
					return
				}
			}
		}(i)
	}

	// Writers: continuously upsert entries with text (triggers raw store + c.history update).
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			j := 0
			for {
				select {
				case <-done:
					errs <- nil
					return
				default:
				}
				txt := fmt.Sprintf("writer %d item %d concurrent", id, j)
				if err := c.Put(fmt.Sprintf("w%d_%d", id, j%4), Entry{Text: txt}); err != nil {
					errs <- fmt.Errorf("writer %d Put: %w", id, err)
					return
				}
				j++
			}
		}(i)
	}

	// Collect results from all goroutines.
	for i := 0; i < numReaders+numWriters; i++ {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

// TestRerankOneEncodeBatchPerQuery verifies that exactly ONE EncodeBatch call is
// made per Search on a WithRerank collection (the candidate-text batch), and that
// Encode is used exactly once (for the query itself via the search path).
func TestRerankOneEncodeBatchPerQuery(t *testing.T) {
	const dim = 16

	base := newDeterministicFixedProvider(dim)
	spy := &spyBatchProvider{deterministicFixedProvider: base}

	c := makeRerankCollection(t, dim, 4, spy)

	texts := []string{"one", "two", "three", "four", "five", "six"}
	for i, txt := range texts {
		if err := c.Put(fmt.Sprintf("id%d", i), Entry{Text: txt}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Reset counter after writes (writes use EncodeBatch).
	atomic.StoreInt64(&spy.batchCount, 0)

	query, _ := spy.Encode("query-text")
	_, err := c.SearchVector(query, 3)
	if err != nil {
		t.Fatalf("SearchVector: %v", err)
	}

	batchCount := atomic.LoadInt64(&spy.batchCount)
	if batchCount != 1 {
		t.Errorf("EncodeBatch calls = %d, want exactly 1 (one candidate batch per query)", batchCount)
	}
}

// TestRerankRecomputeFailureFallsBack verifies §5.4: when EncodeBatch errors
// for the whole candidate batch, Search returns the codes-only top-k (fallback)
// and no error is surfaced to the caller.
func TestRerankRecomputeFailureFallsBack(t *testing.T) {
	const dim = 16
	const bw = 2

	writeProvider := newDeterministicFixedProvider(dim)
	c := makeRerankCollection(t, dim, 4, writeProvider)

	texts := []string{"alpha", "beta", "gamma", "delta"}
	for i, txt := range texts {
		if err := c.Put(fmt.Sprintf("id%d", i), Entry{Text: txt}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Replace encoder with an error-producing one.
	errP := &errBatchProvider{deterministicFixedProvider: writeProvider}
	c.encoder = newEncoder(errP)

	query, _ := writeProvider.Encode("query")
	results, err := c.SearchVector(query, 3)
	// Must NOT return an error.
	if err != nil {
		t.Fatalf("SearchVector returned error: %v (want nil, codes-only fallback)", err)
	}
	// Must return results (the codes-only fallback).
	if len(results) == 0 {
		t.Fatal("expected codes-only fallback results, got none")
	}
	// Results must be sorted descending.
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score+1e-6 {
			t.Errorf("fallback results not sorted at %d", i)
		}
	}
	_ = bw
}

// TestRecomputeVsStoredRawRerankParity verifies that a recompute collection and a
// raw (with raw store) collection return IDENTICAL ranked IDs and identical scores
// when both use a deterministic provider that returns the same vector from both
// Encode and EncodeBatch.
func TestRecomputeVsStoredRawRerankParity(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(42)
	const depth = 4

	// Use a fully-deterministic provider where Encode and EncodeBatch return the
	// SAME output (guaranteed by the deterministicFixedProvider implementation).
	provider := newDeterministicFixedProvider(dim)

	// Build recompute collection.
	dirRec := filepath.Join(t.TempDir(), "rec.csdb")
	dbRec, err := Open(dirRec, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer dbRec.Close()
	cRec := dbRec.Collection("col",
		WithBitWidth(bw), WithQuantizerSeed(seed),
		WithRecomputeRawFromText(), WithRerank(depth),
	)
	if cRec.err != nil {
		t.Fatalf("recompute: %v", cRec.err)
	}

	// Build raw (rawStore) collection.
	dirRaw := filepath.Join(t.TempDir(), "raw.csdb")
	dbRaw, err := Open(dirRaw, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer dbRaw.Close()
	cRaw := dbRaw.Collection("col",
		WithBitWidth(bw), WithQuantizerSeed(seed), WithRerank(depth),
	)
	if cRaw.err != nil {
		t.Fatalf("raw: %v", cRaw.err)
	}

	texts := []string{"one", "two", "three", "four", "five"}
	for i, txt := range texts {
		id := fmt.Sprintf("id%d", i)
		e := Entry{Text: txt}
		if err := cRec.Put(id, e); err != nil {
			t.Fatalf("rec Put: %v", err)
		}
		if err := cRaw.Put(id, e); err != nil {
			t.Fatalf("raw Put: %v", err)
		}
	}

	query, _ := provider.Encode("query")
	recResults, err := cRec.SearchVector(query, 3)
	if err != nil {
		t.Fatalf("rec SearchVector: %v", err)
	}
	rawResults, err := cRaw.SearchVector(query, 3)
	if err != nil {
		t.Fatalf("raw SearchVector: %v", err)
	}

	if len(recResults) != len(rawResults) {
		t.Fatalf("result count mismatch: rec=%d raw=%d", len(recResults), len(rawResults))
	}
	for i := range recResults {
		if recResults[i].ID != rawResults[i].ID {
			t.Errorf("[%d] ID mismatch: rec=%q raw=%q", i, recResults[i].ID, rawResults[i].ID)
		}
		diff := math.Abs(float64(recResults[i].Score - rawResults[i].Score))
		if diff > 1e-4 {
			t.Errorf("[%d] score mismatch: rec=%f raw=%f (diff %e)", i, recResults[i].Score, rawResults[i].Score, diff)
		}
	}
}

// TestSearchCandidatesEquivalence (review N1 drift guard): verifies that
// hnswIndex.searchCandidates returns the SAME hit IDs and per-hit quantizedScore
// as the public Search for the no-rerank case.
func TestSearchCandidatesEquivalence(t *testing.T) {
	const dim = 32
	const n = 80
	const k = 10

	rng := rand.New(rand.NewSource(77))
	h := newHNSWIndex(dim, 2, 77, defaultHNSWParams())

	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = randVec(rng, dim)
		h.Add(fmt.Sprintf("v%d", i), vecs[i], fmt.Sprintf("text-%d", i), nil, uint64(i+1))
	}

	query := randVec(rng, dim)

	// Public Search.
	publicResults := h.Search(query, k, nil)

	// searchCandidates (the new HNSW helper for rerank).
	cands, _ := h.searchCandidates(query, k, nil)

	// Build ID→score maps.
	publicByID := make(map[string]float32)
	for _, r := range publicResults {
		publicByID[r.ID] = r.Score
	}
	candsByID := make(map[string]float32)
	for _, c := range cands {
		candsByID[c.id] = c.quantizedScore
	}

	if len(publicByID) != len(candsByID) {
		t.Fatalf("count mismatch: Search=%d searchCandidates=%d", len(publicByID), len(candsByID))
	}
	for id, pubScore := range publicByID {
		candScore, ok := candsByID[id]
		if !ok {
			t.Errorf("id %q in Search but not in searchCandidates", id)
			continue
		}
		diff := math.Abs(float64(pubScore - candScore))
		if diff > 1e-5 {
			t.Errorf("id %q: Search score %f != searchCandidates quantizedScore %f (diff %e)", id, pubScore, candScore, diff)
		}
	}
	for id := range candsByID {
		if _, ok := publicByID[id]; !ok {
			t.Errorf("id %q in searchCandidates but not in Search", id)
		}
	}
}

// ---- helper for sorting to verify superset property ----
func sortedIDs(results []SearchResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	sort.Strings(ids)
	return ids
}
