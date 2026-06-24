package corkscrewdb

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"m31labs.dev/turboquant"
)

// spyProvider records which encode path was called and returns DISTINGUISHABLE
// vectors: EncodeBatch returns vectorA, Encode returns vectorB.
type spyProvider struct {
	dim         int
	encodeCalls int
	batchCalls  int
}

func newSpyProvider(dim int) *spyProvider { return &spyProvider{dim: dim} }

// vectorA is returned by EncodeBatch (the recompute path).
func (s *spyProvider) vectorA() []float32 {
	v := make([]float32, s.dim)
	for i := range v {
		v[i] = float32(i+1) * 0.1
	}
	return v
}

// vectorB is returned by Encode (the non-recompute path).
func (s *spyProvider) vectorB() []float32 {
	v := make([]float32, s.dim)
	for i := range v {
		v[i] = float32(i+1) * 0.2
	}
	return v
}

func (s *spyProvider) Encode(text string) ([]float32, error) {
	s.encodeCalls++
	return s.vectorB(), nil
}

func (s *spyProvider) EncodeBatch(texts []string) ([][]float32, error) {
	s.batchCalls++
	result := make([][]float32, len(texts))
	for i := range texts {
		v := make([]float32, s.dim)
		copy(v, s.vectorA())
		result[i] = v
	}
	return result, nil
}

func (s *spyProvider) Dim() int     { return s.dim }
func (s *spyProvider) Close() error { return nil }

// spyProvider also implements deterministicProvider so recompute collections can be created.
func (s *spyProvider) Deterministic() bool { return true }

// TestRecomputeWriteUsesEncodeBatchGraph verifies §5.6(a):
//   - a RECOMPUTE collection's text-only Put routes through EncodeBatch (vectorA → codes match quantize(A))
//   - a RAW collection's text-only Put routes through Encode (vectorB → codes match quantize(B))
//
// This proves BOTH the recompute path AND that the non-recompute path is unchanged.
func TestRecomputeWriteUsesEncodeBatchGraph(t *testing.T) {
	const dim = 8
	const bw = 2
	const seed = int64(42)

	spy := newSpyProvider(dim)
	dir := filepath.Join(t.TempDir(), "spy.csdb")
	db, err := Open(dir, WithProvider(spy))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a RECOMPUTE collection with a fixed seed so we can reproduce codes.
	cRecompute := db.Collection("recompute", WithBitWidth(bw), WithQuantizerSeed(seed), WithRecomputeRawFromText())
	if cRecompute.err != nil {
		t.Fatalf("recompute collection create: %v", cRecompute.err)
	}

	// Create a RAW collection with the same seed.
	cRaw := db.Collection("raw", WithBitWidth(bw), WithQuantizerSeed(seed))
	if cRaw.err != nil {
		t.Fatalf("raw collection create: %v", cRaw.err)
	}

	// Reset spy counters before writes.
	spy.encodeCalls = 0
	spy.batchCalls = 0

	// Write to recompute collection via text-only Put.
	if err := cRecompute.Put("id1", Entry{Text: "hello"}); err != nil {
		t.Fatalf("recompute Put: %v", err)
	}

	// The recompute path must use EncodeBatch, not Encode.
	if spy.batchCalls != 1 {
		t.Errorf("recompute Put: EncodeBatch calls = %d, want 1", spy.batchCalls)
	}
	if spy.encodeCalls != 0 {
		t.Errorf("recompute Put: Encode calls = %d, want 0 (must use EncodeBatch)", spy.encodeCalls)
	}

	// The stored code must match quantize(vectorA).
	expectedA := turboquant.NewIPWithSeed(dim, bw, seed).Quantize(spy.vectorA())
	hist, err := cRecompute.History("id1")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) == 0 {
		t.Fatal("recompute: no history for id1")
	}
	gotA := hist[0].Quantized
	if gotA == nil {
		t.Fatal("recompute: Quantized is nil")
	}
	if string(gotA.MSE) != string(expectedA.MSE) || string(gotA.Signs) != string(expectedA.Signs) {
		t.Errorf("recompute: stored code does not match quantize(vectorA)")
	}

	// Reset spy counters before the RAW write.
	spy.encodeCalls = 0
	spy.batchCalls = 0

	// Write to raw collection via text-only Put.
	if err := cRaw.Put("id2", Entry{Text: "hello"}); err != nil {
		t.Fatalf("raw Put: %v", err)
	}

	// The raw path must use Encode, not EncodeBatch.
	if spy.encodeCalls != 1 {
		t.Errorf("raw Put: Encode calls = %d, want 1", spy.encodeCalls)
	}
	if spy.batchCalls != 0 {
		t.Errorf("raw Put: EncodeBatch calls = %d, want 0 (must use Encode)", spy.batchCalls)
	}

	// The stored code must match quantize(vectorB).
	expectedB := turboquant.NewIPWithSeed(dim, bw, seed).Quantize(spy.vectorB())
	hist2, err := cRaw.History("id2")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist2) == 0 {
		t.Fatal("raw: no history for id2")
	}
	gotB := hist2[0].Quantized
	if gotB == nil {
		t.Fatal("raw: Quantized is nil")
	}
	if string(gotB.MSE) != string(expectedB.MSE) || string(gotB.Signs) != string(expectedB.Signs) {
		t.Errorf("raw: stored code does not match quantize(vectorB)")
	}
}

// TestRecomputeRejectsTextlessDenseWrite verifies that putVector on a recompute
// collection rejects calls without text (ErrRecomputeRequiresText), while
// PutVector(WithText) and text-only Put succeed.
func TestRecomputeRejectsTextlessDenseWrite(t *testing.T) {
	spy := newSpyProvider(8)
	dir := filepath.Join(t.TempDir(), "textless.csdb")
	db, err := Open(dir, WithProvider(spy))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
	if c.err != nil {
		t.Fatalf("create: %v", c.err)
	}

	vec := make([]float32, 8)
	for i := range vec {
		vec[i] = float32(i+1) * 0.05
	}

	// PutVector with no text must return ErrRecomputeRequiresText.
	err = c.PutVector("id1", vec)
	if !errors.Is(err, ErrRecomputeRequiresText) {
		t.Errorf("PutVector no text: got %v, want ErrRecomputeRequiresText", err)
	}

	// History must be empty (write was rejected).
	hist, _ := c.History("id1")
	if len(hist) != 0 {
		t.Errorf("history after rejected write: got %d versions, want 0", len(hist))
	}

	// PutVector with text must succeed.
	if err := c.PutVector("id2", vec, WithText("hello")); err != nil {
		t.Errorf("PutVector with text failed: %v", err)
	}

	// Text-only Put must succeed.
	if err := c.Put("id3", Entry{Text: "world"}); err != nil {
		t.Errorf("Put text-only failed: %v", err)
	}
}

// TestRecomputeNoRawStoreDir verifies that after writes to a recompute collection
// and Close, no .rvs raw store directory exists on disk, and that codes+text
// match a WithoutRawStore collection built from the same corpus.
func TestRecomputeNoRawStoreDir(t *testing.T) {
	const dim = 8
	const bw = 2

	// Use a deterministic provider that returns the SAME vector for both Encode
	// and EncodeBatch so we can compare codes across the two collections.
	det := &deterministicMockProvider{&mockProvider{dim: dim}}

	const fixedSeed = int64(99)

	// Build RECOMPUTE collection with a fixed seed so codes are reproducible.
	dirRecomp := filepath.Join(t.TempDir(), "recomp.csdb")
	db1, err := Open(dirRecomp, WithProvider(det))
	if err != nil {
		t.Fatal(err)
	}
	c1 := db1.Collection("col", WithBitWidth(bw), WithQuantizerSeed(fixedSeed), WithRecomputeRawFromText())
	if c1.err != nil {
		t.Fatalf("recompute create: %v", c1.err)
	}
	texts := []string{"apple", "banana", "cherry"}
	for i, txt := range texts {
		id := strings.Repeat("x", i+1)
		if err := c1.Put(id, Entry{Text: txt}); err != nil {
			t.Fatalf("recompute Put %q: %v", id, err)
		}
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	// Assert no raw store directory.
	rawDir := db1.rawStoreDir("col")
	if _, statErr := os.Stat(rawDir); !os.IsNotExist(statErr) {
		t.Errorf("recompute collection: raw store dir %q should not exist, stat: %v", rawDir, statErr)
	}

	// Build WithoutRawStore collection using the same provider AND the same seed.
	dirNone := filepath.Join(t.TempDir(), "none.csdb")
	db2, err := Open(dirNone, WithProvider(det))
	if err != nil {
		t.Fatal(err)
	}
	c2 := db2.Collection("col", WithBitWidth(bw), WithQuantizerSeed(fixedSeed), WithoutRawStore())
	if c2.err != nil {
		t.Fatalf("none create: %v", c2.err)
	}
	for i, txt := range texts {
		id := strings.Repeat("x", i+1)
		if err := c2.Put(id, Entry{Text: txt}); err != nil {
			t.Fatalf("none Put %q: %v", id, err)
		}
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen both and compare codes+text for every version.
	db1r, err := Open(dirRecomp, WithProvider(det))
	if err != nil {
		t.Fatal(err)
	}
	defer db1r.Close()
	c1r := db1r.Collection("col")

	db2r, err := Open(dirNone, WithProvider(det))
	if err != nil {
		t.Fatal(err)
	}
	defer db2r.Close()
	c2r := db2r.Collection("col")

	for i, txt := range texts {
		id := strings.Repeat("x", i+1)
		h1, err := c1r.History(id)
		if err != nil {
			t.Fatalf("recompute history %q: %v", id, err)
		}
		h2, err := c2r.History(id)
		if err != nil {
			t.Fatalf("none history %q: %v", id, err)
		}
		if len(h1) == 0 || len(h2) == 0 {
			t.Fatalf("missing history for %q", id)
		}
		v1, v2 := h1[0], h2[0]

		// Text must match.
		if v1.Text != txt || v2.Text != txt {
			t.Errorf("id %q: texts %q/%q, want %q", id, v1.Text, v2.Text, txt)
		}
		// RawHash must be nil in both.
		if v1.RawHash != nil {
			t.Errorf("recompute id %q: RawHash is not nil", id)
		}
		if v2.RawHash != nil {
			t.Errorf("none id %q: RawHash is not nil", id)
		}
		// Quantized codes must match.
		if v1.Quantized == nil || v2.Quantized == nil {
			t.Fatalf("id %q: nil Quantized", id)
		}
		if string(v1.Quantized.MSE) != string(v2.Quantized.MSE) {
			t.Errorf("id %q: MSE mismatch between recompute and none", id)
		}
		if string(v1.Quantized.Signs) != string(v2.Quantized.Signs) {
			t.Errorf("id %q: Signs mismatch between recompute and none", id)
		}
	}
}

// TestRecomputePutMultiVectorRejected verifies §5.8: PutMultiVector on a
// recompute collection returns ErrRecomputeMultiVectorUnsupported before
// writing any version, and that raw/none collections are unaffected.
func TestRecomputePutMultiVectorRejected(t *testing.T) {
	spy := newSpyProvider(8)
	dir := filepath.Join(t.TempDir(), "mv.csdb")
	db, err := Open(dir, WithProvider(spy))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
	if c.err != nil {
		t.Fatalf("create: %v", c.err)
	}

	vec := make([]float32, 8)
	for i := range vec {
		vec[i] = float32(i+1) * 0.1
	}

	entry := MultiVectorEntry{
		Children: []MultiVectorChild{
			{ID: "c1", Vector: vec},
			{ID: "c2", Vector: vec},
		},
	}

	err = c.PutMultiVector("parent1", entry)
	if !errors.Is(err, ErrRecomputeMultiVectorUnsupported) {
		t.Errorf("PutMultiVector on recompute: got %v, want ErrRecomputeMultiVectorUnsupported", err)
	}

	// No version should have been written.
	hist, _ := c.History("parent1")
	if len(hist) != 0 {
		t.Errorf("history after rejected PutMultiVector: got %d versions, want 0", len(hist))
	}

	// Regression: raw collection PutMultiVector must be unaffected.
	cRaw := db.Collection("raw", WithBitWidth(2))
	if cRaw.err != nil {
		t.Fatalf("raw create: %v", cRaw.err)
	}
	if err := cRaw.PutMultiVector("parent2", entry); err != nil {
		t.Errorf("raw PutMultiVector failed: %v", err)
	}

	// Regression: recompute single-vector PutVector(WithText) must succeed.
	if err := c.PutVector("id1", vec, WithText("hello")); err != nil {
		t.Errorf("recompute PutVector(WithText) failed: %v", err)
	}
}
