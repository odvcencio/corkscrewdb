package corkscrewdb

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func TestHNSWFileRoundTrip(t *testing.T) {
	dim := 32
	n := 50
	rng := rand.New(rand.NewSource(42))
	hw := newHNSWIndex(dim, 2, 99, defaultHNSWParams())

	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = randVec(rng, dim)
		hw.Add(fmt.Sprintf("v%d", i), vecs[i], fmt.Sprintf("text-%d", i), map[string]string{"idx": fmt.Sprint(i)}, uint64(i+1))
	}

	dir := t.TempDir()
	indexPath := filepath.Join(dir, "quantized.tqi")
	hnswPath := filepath.Join(dir, "graph.hnsw")

	// Save the flat index portion.
	if err := saveIndexFile(indexPath, hw.flat, 99); err != nil {
		t.Fatal(err)
	}
	// Save the HNSW graph.
	if err := saveHNSWFile(hnswPath, hw); err != nil {
		t.Fatal(err)
	}

	// Load them back.
	flat, maxLamport, err := loadIndexFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if maxLamport != 99 {
		t.Fatalf("maxLamport = %d, want 99", maxLamport)
	}

	loaded, err := loadHNSWFile(hnswPath, flat, defaultHNSWParams())
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Len() != n {
		t.Fatalf("loaded.Len() = %d, want %d", loaded.Len(), n)
	}
	if loaded.maxLevel != hw.maxLevel {
		t.Fatalf("maxLevel = %d, want %d", loaded.maxLevel, hw.maxLevel)
	}
	if loaded.entryNode != hw.entryNode {
		t.Fatalf("entryNode = %d, want %d", loaded.entryNode, hw.entryNode)
	}
	if loaded.params.M != hw.params.M {
		t.Fatalf("M = %d, want %d", loaded.params.M, hw.params.M)
	}

	// Search should still work on the loaded index.
	results := loaded.Search(vecs[0], 1, nil)
	if len(results) != 1 || results[0].ID != "v0" {
		t.Fatalf("loaded search: got %v, want v0", results)
	}

	// Verify filtered search works.
	results = loaded.Search(vecs[0], 10, []FilterOption{Filter("idx", "0")})
	found := false
	for _, r := range results {
		if r.ID == "v0" {
			found = true
		}
	}
	if !found {
		t.Fatal("v0 not found in filtered search after load")
	}
}

func TestHNSWFileNilSave(t *testing.T) {
	if err := saveHNSWFile(filepath.Join(t.TempDir(), "test.hnsw"), nil); err != nil {
		t.Fatal(err)
	}
}
