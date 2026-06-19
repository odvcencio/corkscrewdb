package corkscrewdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
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
	if err := saveIndexFile(indexPath, hw.flat, 99, true, false); err != nil {
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

// TestHNSWFileV2NodeIDsRoundTrip verifies that v2 persists node IDs and that
// the count matches flat.Len() on load.
func TestHNSWFileV2NodeIDsRoundTrip(t *testing.T) {
	dim := 16
	n := 10
	rng := rand.New(rand.NewSource(55))
	hw := newHNSWIndex(dim, 2, 77, defaultHNSWParams())

	for i := 0; i < n; i++ {
		v := randVec(rng, dim)
		hw.Add(fmt.Sprintf("node%d", i), v, fmt.Sprintf("text%d", i), nil, uint64(i+1))
	}

	dir := t.TempDir()
	indexPath := filepath.Join(dir, "quantized.tqi")
	hnswPath := filepath.Join(dir, "graph.hnsw")

	if err := saveIndexFile(indexPath, hw.flat, 77, true, false); err != nil {
		t.Fatalf("saveIndexFile: %v", err)
	}
	if err := saveHNSWFile(hnswPath, hw); err != nil {
		t.Fatalf("saveHNSWFile: %v", err)
	}

	flat, _, err := loadIndexFile(indexPath)
	if err != nil {
		t.Fatalf("loadIndexFile: %v", err)
	}

	loaded, err := loadHNSWFile(hnswPath, flat, defaultHNSWParams())
	if err != nil {
		t.Fatalf("loadHNSWFile: %v", err)
	}

	if loaded.Len() != n {
		t.Fatalf("loaded.Len() = %d, want %d", loaded.Len(), n)
	}

	// Search should work correctly after load.
	results := loaded.Search(randVec(rng, dim), 3, nil)
	if len(results) == 0 {
		t.Fatal("expected non-empty search results after v2 round-trip")
	}
}

// TestHNSWFileV1RejectsForgedOldVersion checks that a file with version 1
// returns ErrFormatTooOld.
func TestHNSWFileV1RejectsForgedOldVersion(t *testing.T) {
	dim := 8
	hw := newHNSWIndex(dim, 2, 1, defaultHNSWParams())
	rng := rand.New(rand.NewSource(11))
	hw.Add("a", randVec(rng, dim), "ta", nil, 1)

	dir := t.TempDir()
	indexPath := filepath.Join(dir, "quantized.tqi")
	hnswPath := filepath.Join(dir, "forgedv1.hnsw")

	if err := saveIndexFile(indexPath, hw.flat, 1, false, false); err != nil {
		t.Fatalf("saveIndexFile: %v", err)
	}
	if err := saveHNSWFile(hnswPath, hw); err != nil {
		t.Fatalf("saveHNSWFile: %v", err)
	}

	// Overwrite version byte (offset 4, after 4-byte magic) with value 1.
	data, err := os.ReadFile(hnswPath)
	if err != nil {
		t.Fatal(err)
	}
	data[4] = 1
	// Corrupt CRC so it doesn't accidentally pass.
	binary.LittleEndian.PutUint32(data[len(data)-4:], 0)
	if err := os.WriteFile(hnswPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	flat, _, err := loadIndexFile(indexPath)
	if err != nil {
		t.Fatalf("loadIndexFile: %v", err)
	}

	_, err = loadHNSWFile(hnswPath, flat, defaultHNSWParams())
	if !errors.Is(err, ErrFormatTooOld) {
		t.Fatalf("expected ErrFormatTooOld, got: %v", err)
	}
}
