package corkscrewdb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	walpkg "m31labs.dev/corkscrewdb/wal"
	"m31labs.dev/turboquant"
)

// ---- provider helpers for requantize tests ----

// recomputeTestProvider is a deterministic provider for requantize tests.
// It implements deterministicProvider so it can be used with WithRecomputeRawFromText.
type recomputeTestProvider struct {
	dim int
}

func (p *recomputeTestProvider) Encode(text string) ([]float32, error) {
	v := make([]float32, p.dim)
	for i := range v {
		v[i] = float32(len(text)+i+1) * 0.01
	}
	return v, nil
}

func (p *recomputeTestProvider) EncodeBatch(texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v, _ := p.Encode(t)
		out[i] = v
	}
	return out, nil
}

func (p *recomputeTestProvider) Dim() int     { return p.dim }
func (p *recomputeTestProvider) Close() error { return nil }

// Deterministic marks this provider as deterministic (required for recompute mode).
func (p *recomputeTestProvider) Deterministic() bool { return true }

// driftingRecomputeProvider returns DIFFERENT vectors on subsequent EncodeBatch
// calls to simulate embedder drift (the re-quantized code at old params won't
// match the stored code).
type driftingRecomputeProvider struct {
	dim   int
	calls int
}

func (p *driftingRecomputeProvider) Encode(text string) ([]float32, error) {
	v := make([]float32, p.dim)
	for i := range v {
		v[i] = float32(len(text)+i+1) * 0.01
	}
	return v, nil
}

func (p *driftingRecomputeProvider) EncodeBatch(texts []string) ([][]float32, error) {
	p.calls++
	out := make([][]float32, len(texts))
	// After the first call (write time), perturb significantly so re-quantization
	// at old params produces different codes.
	perturbation := float32(0)
	if p.calls > 1 {
		perturbation = float32(100.0) // large enough to change quantized codes
	}
	for i, t := range texts {
		v := make([]float32, p.dim)
		for j := range v {
			v[j] = float32(len(t)+j+1)*0.01 + perturbation
		}
		out[i] = v
	}
	return out, nil
}

func (p *driftingRecomputeProvider) Dim() int            { return p.dim }
func (p *driftingRecomputeProvider) Close() error        { return nil }
func (p *driftingRecomputeProvider) Deterministic() bool { return true }

// errEncodeBatchProvider returns an error on EncodeBatch calls (for failure test).
type errEncodeBatchProvider struct {
	dim    int
	failOn int // fail starting from this call number (1-indexed)
	calls  int
}

func (p *errEncodeBatchProvider) Encode(text string) ([]float32, error) {
	v := make([]float32, p.dim)
	for i := range v {
		v[i] = float32(len(text)+i+1) * 0.01
	}
	return v, nil
}

func (p *errEncodeBatchProvider) EncodeBatch(texts []string) ([][]float32, error) {
	p.calls++
	if p.failOn > 0 && p.calls >= p.failOn {
		return nil, errors.New("mock EncodeBatch failure for requantize test")
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v, _ := p.Encode(t)
		out[i] = v
	}
	return out, nil
}

func (p *errEncodeBatchProvider) Dim() int            { return p.dim }
func (p *errEncodeBatchProvider) Close() error        { return nil }
func (p *errEncodeBatchProvider) Deterministic() bool { return true }

// ---- helpers ----

// makeRecomputeCollection creates a recompute-mode collection using the given
// provider, with the specified initial bitWidth and seed.
func makeRecomputeCollection(t *testing.T, dir string, provider EmbeddingProvider, bitWidth int, seed int64) (*DB, *Collection) {
	t.Helper()
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c := db.Collection("requantize",
		WithBitWidth(bitWidth),
		WithQuantizerSeed(seed),
		WithRecomputeRawFromText(),
	)
	if c.err != nil {
		t.Fatalf("Collection: %v", c.err)
	}
	return db, c
}

// walEntriesForCollection scans all WAL segments for the collection and returns
// all EntryPut entries for the given vectorID.
func walEntriesForCollection(t *testing.T, dir string, collName string, vectorID string) []walpkg.Entry {
	t.Helper()
	walDir := filepath.Join(dir, collName, "wal")
	segments, err := walpkg.ListSegments(walDir)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	var entries []walpkg.Entry
	for _, seg := range segments {
		r, err := walpkg.NewReader(seg)
		if err != nil {
			t.Fatalf("NewReader(%s): %v", seg, err)
		}
		for r.Next() {
			e := r.Entry()
			if e.Kind == walpkg.EntryPut && e.CollectionID == collName && e.VectorID == vectorID {
				entries = append(entries, e)
			}
		}
		if err := r.Err(); err != nil {
			t.Fatalf("WAL reader error on %s: %v", seg, err)
		}
		r.Close()
	}
	return entries
}

// ---- tests ----

// TestReQuantizeFullHistoryReDerivesAllCodes verifies that ReQuantize rewrites
// every version's Quantized — live AND historical — to the new (bitWidth, seed).
func TestReQuantizeFullHistoryReDerivesAllCodes(t *testing.T) {
	const dim = 16
	const oldBW = 2
	const oldSeed = int64(11)
	const newBW = 4
	const newSeed = int64(22)

	dir := t.TempDir()
	provider := &recomputeTestProvider{dim: dim}
	dbPath := filepath.Join(dir, "rq.csdb")
	db, c := makeRecomputeCollection(t, dbPath, provider, oldBW, oldSeed)
	defer db.Close()

	// Write two versions of "id1" and one of "id2".
	if err := c.Put("id1", Entry{Text: "hello world"}); err != nil {
		t.Fatalf("Put id1 v1: %v", err)
	}
	if err := c.Put("id1", Entry{Text: "hello world updated"}); err != nil {
		t.Fatalf("Put id1 v2: %v", err)
	}
	if err := c.Put("id2", Entry{Text: "another entry"}); err != nil {
		t.Fatalf("Put id2: %v", err)
	}

	// Perform the re-quantization.
	if err := c.ReQuantize(newBW, newSeed); err != nil {
		t.Fatalf("ReQuantize: %v", err)
	}

	// Verify the collection-level params changed.
	if c.bitWidth != newBW {
		t.Errorf("bitWidth = %d, want %d", c.bitWidth, newBW)
	}
	if c.seed != newSeed {
		t.Errorf("seed = %d, want %d", c.seed, newSeed)
	}

	newQ := turboquant.NewIPWithSeed(dim, newBW, newSeed)

	// Verify every version of id1 and id2 is consistent with the new params.
	for _, id := range []string{"id1", "id2"} {
		versions, err := c.History(id)
		if err != nil {
			t.Fatalf("History(%s): %v", id, err)
		}
		if len(versions) == 0 {
			t.Fatalf("no versions for %s", id)
		}
		for vi, v := range versions {
			if v.Tombstone || v.Quantized == nil {
				continue
			}
			// Recompute: raw' = EncodeBatch([v.Text])[0]
			rawBatch, err := provider.EncodeBatch([]string{v.Text})
			if err != nil {
				t.Fatalf("EncodeBatch for %s v%d: %v", id, vi, err)
			}
			expected := newQ.Quantize(rawBatch[0])
			if !byteEqualQuantized(&expected, v.Quantized) {
				t.Errorf("%s v%d: Quantized mismatch after ReQuantize", id, vi)
			}
		}
	}

	// Search should still work (index was rebuilt).
	queryVec, err := provider.Encode("hello world")
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.SearchVector(queryVec, 2)
	if err != nil {
		t.Fatalf("SearchVector after ReQuantize: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected search results after ReQuantize, got none")
	}
}

// TestReQuantizePersistsViaSnapshotNotWAL is the load-bearing durability test
// (Resolution 5). After ReQuantize, Close, and reopen, the codes must be the
// NEW-param codes loaded from the snapshot — not the old codes. Additionally,
// the WAL must not contain any pre-requant EntryPut for the affected ids
// (the snapshot superseded them and the WAL was reset).
func TestReQuantizePersistsViaSnapshotNotWAL(t *testing.T) {
	const dim = 16
	const oldBW = 2
	const oldSeed = int64(111)
	const newBW = 4
	const newSeed = int64(222)

	dir := t.TempDir()
	provider := &recomputeTestProvider{dim: dim}
	dbPath := filepath.Join(dir, "persist.csdb")

	// Phase 1: create, populate, re-quantize, close.
	{
		db, c := makeRecomputeCollection(t, dbPath, provider, oldBW, oldSeed)
		if err := c.Put("a", Entry{Text: "foo"}); err != nil {
			t.Fatalf("Put a: %v", err)
		}
		if err := c.Put("b", Entry{Text: "bar baz"}); err != nil {
			t.Fatalf("Put b: %v", err)
		}
		// Write a second version of "a".
		if err := c.Put("a", Entry{Text: "foo updated"}); err != nil {
			t.Fatalf("Put a v2: %v", err)
		}

		if err := c.ReQuantize(newBW, newSeed); err != nil {
			t.Fatalf("ReQuantize: %v", err)
		}

		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: verify WAL is empty (snapshot superseded it, WAL was reset).
	walEntries := walEntriesForCollection(t, dbPath, "requantize", "a")
	if len(walEntries) != 0 {
		t.Errorf("expected no WAL EntryPut for 'a' after ReQuantize, got %d", len(walEntries))
	}

	// Phase 3: reopen and assert codes are at the new params.
	newQ := turboquant.NewIPWithSeed(dim, newBW, newSeed)
	{
		db2, err := Open(dbPath, WithProvider(provider))
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer db2.Close()

		c2 := db2.Collection("requantize",
			WithBitWidth(newBW),
			WithQuantizerSeed(newSeed),
			WithRecomputeRawFromText(),
		)
		if c2.err != nil {
			t.Fatalf("reopen collection: %v", c2.err)
		}

		for _, id := range []string{"a", "b"} {
			versions, err := c2.History(id)
			if err != nil {
				t.Fatalf("History(%s) after reopen: %v", id, err)
			}
			if len(versions) == 0 {
				t.Fatalf("no versions for %s after reopen", id)
			}
			for vi, v := range versions {
				if v.Tombstone || v.Quantized == nil {
					continue
				}
				rawBatch, err := provider.EncodeBatch([]string{v.Text})
				if err != nil {
					t.Fatalf("EncodeBatch: %v", err)
				}
				expected := newQ.Quantize(rawBatch[0])
				if !byteEqualQuantized(&expected, v.Quantized) {
					t.Errorf("%s v%d: codes are NOT at new params after reopen — snapshot durability failed", id, vi)
				}
			}
		}

		// Verify collection-level params were persisted.
		if c2.bitWidth != newBW {
			t.Errorf("bitWidth after reopen = %d, want %d", c2.bitWidth, newBW)
		}
		if c2.seed != newSeed {
			t.Errorf("seed after reopen = %d, want %d", c2.seed, newSeed)
		}
	}
}

// TestReQuantizeOldParamDriftAborts verifies that when EncodeBatch returns a vector
// that re-quantizes (at OLD params) to a DIFFERENT code than stored, ReQuantize
// returns ErrEmbedderDriftDetected and leaves all codes unchanged.
// With WithAllowReQuantizeDrift(), it proceeds.
func TestReQuantizeOldParamDriftAborts(t *testing.T) {
	const dim = 16
	const oldBW = 2
	const oldSeed = int64(33)
	const newBW = 4
	const newSeed = int64(44)

	dir := t.TempDir()
	provider := &driftingRecomputeProvider{dim: dim}
	dbPath := filepath.Join(dir, "drift.csdb")
	db, c := makeRecomputeCollection(t, dbPath, provider, oldBW, oldSeed)
	defer db.Close()

	if err := c.Put("x", Entry{Text: "hello drift"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Capture the current codes.
	versionsBefore, err := c.History("x")
	if err != nil {
		t.Fatalf("History before: %v", err)
	}
	if len(versionsBefore) == 0 {
		t.Fatal("no versions")
	}
	codesBefore := make([]*turboquant.IPQuantized, len(versionsBefore))
	for i, v := range versionsBefore {
		if v.Quantized != nil {
			q := cloneQuantized(*v.Quantized)
			codesBefore[i] = &q
		}
	}

	// After the first EncodeBatch (write), subsequent calls are perturbed =>
	// drift-detect mismatch.
	err = c.ReQuantize(newBW, newSeed)
	if err == nil {
		t.Fatal("expected ErrEmbedderDriftDetected, got nil")
	}
	if !errors.Is(err, ErrEmbedderDriftDetected) {
		t.Fatalf("err = %v, want ErrEmbedderDriftDetected", err)
	}

	// Codes must be UNCHANGED.
	versionsAfter, err := c.History("x")
	if err != nil {
		t.Fatalf("History after drift abort: %v", err)
	}
	for i, v := range versionsAfter {
		if v.Quantized == nil || codesBefore[i] == nil {
			continue
		}
		if !byteEqualQuantized(codesBefore[i], v.Quantized) {
			t.Errorf("v%d: code changed after drift abort (must be unchanged)", i)
		}
	}
	// Params must be unchanged.
	if c.bitWidth != oldBW {
		t.Errorf("bitWidth changed after drift abort: got %d, want %d", c.bitWidth, oldBW)
	}
	if c.seed != oldSeed {
		t.Errorf("seed changed after drift abort: got %d, want %d", c.seed, oldSeed)
	}

	// WithAllowReQuantizeDrift() must allow proceeding.
	provider2 := &driftingRecomputeProvider{dim: dim}
	dbPath2 := filepath.Join(dir, "drift_allow.csdb")
	db2, c2 := makeRecomputeCollection(t, dbPath2, provider2, oldBW, oldSeed)
	defer db2.Close()
	if err := c2.Put("x", Entry{Text: "hello drift"}); err != nil {
		t.Fatalf("Put(c2): %v", err)
	}
	if err := c2.ReQuantize(newBW, newSeed, WithAllowReQuantizeDrift()); err != nil {
		t.Fatalf("ReQuantize with override: %v", err)
	}
	if c2.bitWidth != newBW {
		t.Errorf("override: bitWidth = %d, want %d", c2.bitWidth, newBW)
	}
}

// TestReQuantizeRecomputeFailureAborts verifies that when EncodeBatch errors,
// ReQuantize returns a wrapped error and leaves codes/index/params unchanged.
func TestReQuantizeRecomputeFailureAborts(t *testing.T) {
	const dim = 16
	const oldBW = 2
	const oldSeed = int64(55)
	const newBW = 4
	const newSeed = int64(66)

	dir := t.TempDir()
	// failOn=2: first call (write) succeeds; second call (ReQuantize) fails.
	provider := &errEncodeBatchProvider{dim: dim, failOn: 2}
	dbPath := filepath.Join(dir, "encfail.csdb")
	db, c := makeRecomputeCollection(t, dbPath, provider, oldBW, oldSeed)
	defer db.Close()

	if err := c.Put("y", Entry{Text: "will fail requantize"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	versionsBefore, err := c.History("y")
	if err != nil {
		t.Fatalf("History before: %v", err)
	}
	codesBefore := make([]*turboquant.IPQuantized, len(versionsBefore))
	for i, v := range versionsBefore {
		if v.Quantized != nil {
			q := cloneQuantized(*v.Quantized)
			codesBefore[i] = &q
		}
	}

	err = c.ReQuantize(newBW, newSeed)
	if err == nil {
		t.Fatal("expected error from EncodeBatch failure, got nil")
	}

	// Codes must be unchanged.
	versionsAfter, err := c.History("y")
	if err != nil {
		t.Fatalf("History after: %v", err)
	}
	for i, v := range versionsAfter {
		if v.Quantized == nil || codesBefore[i] == nil {
			continue
		}
		if !byteEqualQuantized(codesBefore[i], v.Quantized) {
			t.Errorf("v%d: code changed after EncodeBatch failure", i)
		}
	}
	if c.bitWidth != oldBW {
		t.Errorf("bitWidth changed after failure: got %d, want %d", c.bitWidth, oldBW)
	}
	if c.seed != oldSeed {
		t.Errorf("seed changed after failure: got %d, want %d", c.seed, oldSeed)
	}
}

// TestReQuantizeRejectedOnNonRecompute verifies that ReQuantize is rejected for
// raw and none (non-recompute) collections, since they have no per-version text
// to recompute from.
func TestReQuantizeRejectedOnNonRecompute(t *testing.T) {
	const dim = 8

	t.Run("raw", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "raw.csdb")
		db, err := Open(dir, WithProvider(&mockProvider{dim: dim}))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		c := db.Collection("raw_col", WithBitWidth(2))
		if c.err != nil {
			t.Fatalf("create: %v", c.err)
		}
		if err := c.Put("a", Entry{Text: "test", Vector: []float32{1, 2, 3, 4, 5, 6, 7, 8}}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		err = c.ReQuantize(4, 99)
		if err == nil {
			t.Fatal("expected error for raw collection, got nil")
		}
		// Should not be nil; specific sentinel or contains message
	})

	t.Run("none", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "none.csdb")
		db, err := Open(dir, WithProvider(&mockProvider{dim: dim}))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		c := db.Collection("none_col", WithBitWidth(2), WithoutRawStore())
		if c.err != nil {
			t.Fatalf("create: %v", c.err)
		}
		vec := []float32{1, 2, 3, 4, 5, 6, 7, 8}
		if err := c.PutVector("a", vec); err != nil {
			t.Fatalf("PutVector: %v", err)
		}
		err = c.ReQuantize(4, 99)
		if err == nil {
			t.Fatal("expected error for none collection, got nil")
		}
	})
}

// TestRebuildIndexDoesNotRequantize is a regression test: RebuildIndex switches
// the index algorithm but leaves every version's Quantized byte-identical.
func TestRebuildIndexDoesNotRequantize(t *testing.T) {
	const dim = 16
	const bw = 2
	const seed = int64(77)

	dir := filepath.Join(t.TempDir(), "rebuild.csdb")
	provider := &recomputeTestProvider{dim: dim}
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := db.Collection("ri", WithBitWidth(bw), WithQuantizerSeed(seed), WithRecomputeRawFromText())
	if c.err != nil {
		t.Fatalf("create: %v", c.err)
	}

	texts := []string{"apple", "banana", "cherry"}
	for i, txt := range texts {
		if err := c.Put(txt, Entry{Text: txt}); err != nil {
			t.Fatalf("Put %q: %v", txt, err)
		}
		_ = i
	}

	// Capture codes before rebuild.
	type versionKey struct {
		id  string
		idx int
	}
	codesBefore := make(map[versionKey][]byte)
	for _, id := range texts {
		versions, err := c.History(id)
		if err != nil {
			t.Fatalf("History(%s): %v", id, err)
		}
		for vi, v := range versions {
			if v.Quantized != nil {
				key := versionKey{id, vi}
				mse := append([]byte(nil), v.Quantized.MSE...)
				signs := append([]byte(nil), v.Quantized.Signs...)
				codesBefore[key] = append(mse, signs...)
			}
		}
	}

	// RebuildIndex to HNSW and back to flat.
	if err := c.RebuildIndex(IndexHNSW); err != nil {
		t.Fatalf("RebuildIndex(IndexHNSW): %v", err)
	}
	if err := c.RebuildIndex(IndexFlat); err != nil {
		t.Fatalf("RebuildIndex(IndexFlat): %v", err)
	}

	// Codes must be identical.
	for _, id := range texts {
		versions, err := c.History(id)
		if err != nil {
			t.Fatalf("History(%s) after rebuild: %v", id, err)
		}
		for vi, v := range versions {
			if v.Quantized == nil {
				continue
			}
			key := versionKey{id, vi}
			before, ok := codesBefore[key]
			if !ok {
				t.Errorf("%s v%d: no code recorded before rebuild", id, vi)
				continue
			}
			combined := append(append([]byte(nil), v.Quantized.MSE...), v.Quantized.Signs...)
			if !bytes.Equal(before, combined) {
				t.Errorf("%s v%d: code changed by RebuildIndex (must be unchanged)", id, vi)
			}
		}
	}
}

// TestReQuantizeViewConsistency verifies that after ReQuantize, historical views
// created via At() reflect the new codes (the view cache was invalidated).
// This test does not depend on Task 7's view rerank — it checks that the codes
// returned by History() on a view are at the new params.
func TestReQuantizeViewConsistency(t *testing.T) {
	const dim = 16
	const oldBW = 2
	const oldSeed = int64(88)
	const newBW = 4
	const newSeed = int64(99)

	dir := filepath.Join(t.TempDir(), "viewconsist.csdb")
	provider := &recomputeTestProvider{dim: dim}
	db, c := makeRecomputeCollection(t, dir, provider, oldBW, oldSeed)
	defer db.Close()

	if err := c.Put("z", Entry{Text: "view test"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	versions1, err := c.History("z")
	if err != nil || len(versions1) == 0 {
		t.Fatal("expected version")
	}
	clock1 := versions1[0].LamportClock

	if err := c.ReQuantize(newBW, newSeed); err != nil {
		t.Fatalf("ReQuantize: %v", err)
	}

	// Historical view at clock1 should have the NEW codes (ReQuantize rewrote
	// in-memory history and invalidated the view cache).
	view := c.At(clock1)
	viewVersions, err := view.History("z")
	if err != nil {
		t.Fatalf("view.History: %v", err)
	}
	if len(viewVersions) == 0 {
		t.Fatal("no versions in view")
	}

	newQ := turboquant.NewIPWithSeed(dim, newBW, newSeed)
	for vi, v := range viewVersions {
		if v.Tombstone || v.Quantized == nil {
			continue
		}
		rawBatch, err := provider.EncodeBatch([]string{v.Text})
		if err != nil {
			t.Fatalf("EncodeBatch: %v", err)
		}
		expected := newQ.Quantize(rawBatch[0])
		if !byteEqualQuantized(&expected, v.Quantized) {
			t.Errorf("view v%d: code is not at new params after ReQuantize+view rebuild", vi)
		}
	}
}

// TestReQuantizeConcurrentRaceClean is the spec §12 mandated concurrent race test.
// It verifies that concurrent SearchVector, At()+view-search, PutVector, and
// ReQuantize operations produce no data race (go test -race) and no deadlock.
//
// Correctness property: no panic, no data race reported by the race detector,
// and the goroutines exit cleanly within the deadline (no deadlock).
func TestReQuantizeConcurrentRaceClean(t *testing.T) {
	const dim = 16
	const bw1 = 2
	const seed1 = int64(11)
	const bw2 = 4
	const seed2 = int64(22)
	const numSearchers = 4
	const numWriters = 2
	const numReQuantizers = 1

	provider := &recomputeTestProvider{dim: dim}
	dir := filepath.Join(t.TempDir(), "race_clean.csdb")
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := db.Collection("race",
		WithBitWidth(bw1),
		WithQuantizerSeed(seed1),
		WithRecomputeRawFromText(),
		WithRerank(4),
	)
	if c.err != nil {
		t.Fatalf("collection: %v", c.err)
	}

	// Pre-populate the corpus so searches find real candidates.
	corpus := []string{
		"apple fruit", "banana yellow", "cherry red",
		"database vector", "quantized index", "rerank depth",
		"concurrent safe", "drift check",
	}
	for i, txt := range corpus {
		if err := c.Put(fmt.Sprintf("seed%d", i), Entry{Text: txt}); err != nil {
			t.Fatalf("seed Put %d: %v", i, err)
		}
	}

	// Compute a run duration. Cap at 5s so the concurrent test does not dominate
	// the full suite runtime. Use the test deadline minus a safety margin when it
	// would give a shorter window (CI with a tight timeout).
	const maxRun = 5 * time.Second
	runFor := maxRun
	if dl, ok := t.Deadline(); ok {
		rem := time.Until(dl) * 3 / 4
		if rem > 500*time.Millisecond && rem < runFor {
			runFor = rem
		}
	}

	ctx := make(chan struct{})
	time.AfterFunc(runFor, func() { close(ctx) })

	errs := make(chan error, numSearchers+numWriters+numReQuantizers)

	queryVec, err := provider.Encode("vector database")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	// Goroutines: concurrent SearchVector (live path, uses liveRerankCtx snapshot).
	for i := 0; i < numSearchers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx:
					return
				default:
				}
				_, err := c.SearchVector(queryVec, 3)
				if err != nil {
					errs <- fmt.Errorf("SearchVector: %w", err)
					return
				}
			}
		}()
	}

	// Goroutines: At() + view SearchVector (point-in-time snapshot, reads
	// bitWidth/seed under c.mu.RLock per B1a fix).
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx:
					return
				default:
				}
				c.mu.RLock()
				clk := c.clock.Current()
				c.mu.RUnlock()
				view := c.At(clk)
				_, err := view.SearchVector(queryVec, 3)
				if err != nil {
					errs <- fmt.Errorf("view.SearchVector: %w", err)
					return
				}
			}
		}()
	}

	// Goroutines: concurrent PutVector with text (triggers write path + index rebuild).
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-ctx:
					return
				default:
				}
				id := fmt.Sprintf("live%d-%d", idx, n)
				txt := fmt.Sprintf("live write %d %d", idx, n)
				if err := c.Put(id, Entry{Text: txt}); err != nil {
					errs <- fmt.Errorf("Put: %w", err)
					return
				}
				n++
			}
		}()
	}

	// Goroutine: periodic ReQuantize alternating between two param sets.
	// This is the writer that mutates c.bitWidth/c.seed under c.mu.Lock.
	wg.Add(numReQuantizers)
	go func() {
		defer wg.Done()
		flip := false
		for {
			select {
			case <-ctx:
				return
			default:
			}
			bw := bw2
			seed := seed2
			if flip {
				bw = bw1
				seed = seed1
			}
			flip = !flip
			// ReQuantize holds c.mu.Lock for the duration; readers must not race.
			if err := c.ReQuantize(bw, seed); err != nil {
				// Failures from concurrent writes (e.g. drift) are expected and ignored;
				// only unexpected errors (panics etc.) matter for the race test.
				_ = err
			}
			// Brief yield to let readers proceed between ReQuantize calls.
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
