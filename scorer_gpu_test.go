package corkscrewdb

import (
	"errors"
	"testing"

	"m31labs.dev/turboquant"
)

// TestNewGPUScorerStub runs on the default (non-cuda) build: newGPUScorer must
// report the GPU backend is unavailable, and a collection that fails to build a
// GPU scorer must degrade to the default scorer without panic.
func TestNewGPUScorerStub(t *testing.T) {
	qz := turboquant.NewIPWithSeed(8, 2, 1)
	corpus := make([]turboquant.IPQuantized, 4)
	for i := range corpus {
		v := make([]float32, 8)
		v[i] = 1
		corpus[i] = qz.Quantize(v)
	}
	s, err := newGPUScorer(qz, corpus)
	if !errors.Is(err, turboquant.ErrGPUBackendUnavailable) {
		t.Fatalf("newGPUScorer err = %v, want ErrGPUBackendUnavailable", err)
	}
	if s != nil {
		t.Fatalf("newGPUScorer returned non-nil scorer on stub build")
	}

	// A collection given a nil scorer (the failed GPU build) must still search via
	// the default scorer without panic.
	db, err := Open(t.TempDir(), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll := db.Collection("g", WithBitWidth(2))
	if err := coll.PutVector("a", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	res, err := coll.SearchVector([]float32{1, 0, 0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != "a" {
		t.Fatalf("default-scorer search after failed GPU build: %+v", res)
	}
}
