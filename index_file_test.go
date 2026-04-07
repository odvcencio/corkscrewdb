package corkscrewdb

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func TestIndexFileRoundTrip(t *testing.T) {
	idx := newIndex(32, 2, 42)
	rng := rand.New(rand.NewSource(99))
	vecs := make([][]float32, 3)
	for i := range vecs {
		vecs[i] = randVec(rng, 32)
		idx.Add(fmt.Sprintf("v%d", i), vecs[i], fmt.Sprintf("text-%d", i), map[string]string{"source": "test"}, uint64(i+1))
	}

	path := filepath.Join(t.TempDir(), "quantized.tqi")
	if err := saveIndexFile(path, idx, 3); err != nil {
		t.Fatal(err)
	}

	loaded, maxLamport, err := loadIndexFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if maxLamport != 3 {
		t.Fatalf("maxLamport = %d, want 3", maxLamport)
	}
	results := loaded.Search(vecs[0], 1, nil)
	if len(results) != 1 || results[0].ID != "v0" {
		t.Fatalf("results = %v, want v0", results)
	}
}
