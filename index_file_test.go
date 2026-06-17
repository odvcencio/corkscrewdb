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

func TestIndexFileRoundTrip(t *testing.T) {
	idx := newIndex(32, 2, 42)
	rng := rand.New(rand.NewSource(99))
	vecs := make([][]float32, 3)
	for i := range vecs {
		vecs[i] = randVec(rng, 32)
		idx.Add(fmt.Sprintf("v%d", i), vecs[i], fmt.Sprintf("text-%d", i), map[string]string{"source": "test"}, uint64(i+1))
	}

	path := filepath.Join(t.TempDir(), "quantized.tqi")
	if err := saveIndexFile(path, idx, 3, true, false); err != nil {
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

// TestIndexFileV3HeaderFlagsRoundTrip verifies that rawStore and sparse flags
// survive a save/load round-trip in the v3 format.
func TestIndexFileV3HeaderFlagsRoundTrip(t *testing.T) {
	idx := newIndex(16, 2, 7)
	rng := rand.New(rand.NewSource(13))
	for i := 0; i < 2; i++ {
		v := randVec(rng, 16)
		idx.Add(fmt.Sprintf("e%d", i), v, fmt.Sprintf("t%d", i), nil, uint64(i+1))
	}

	path := filepath.Join(t.TempDir(), "v3flags.tqi")
	// Save with rawStore=true, sparse=true.
	if err := saveIndexFile(path, idx, 5, true, true); err != nil {
		t.Fatalf("saveIndexFile: %v", err)
	}

	loaded, maxLamport, rawStore, sparse, err := loadIndexFileV3(path)
	if err != nil {
		t.Fatalf("loadIndexFileV3: %v", err)
	}
	if maxLamport != 5 {
		t.Fatalf("maxLamport = %d, want 5", maxLamport)
	}
	if !rawStore {
		t.Fatal("rawStore flag should be true")
	}
	if !sparse {
		t.Fatal("sparse flag should be true")
	}
	results := loaded.Search(make([]float32, 16), 2, nil)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

// TestIndexFileV3FlagsOffRoundTrip verifies rawStore=false, sparse=false round-trip.
func TestIndexFileV3FlagsOffRoundTrip(t *testing.T) {
	idx := newIndex(8, 2, 3)
	rng := rand.New(rand.NewSource(17))
	v := randVec(rng, 8)
	idx.Add("a", v, "text-a", nil, 1)

	path := filepath.Join(t.TempDir(), "v3noflags.tqi")
	if err := saveIndexFile(path, idx, 1, false, false); err != nil {
		t.Fatalf("saveIndexFile: %v", err)
	}

	_, _, rawStore, sparse, err := loadIndexFileV3(path)
	if err != nil {
		t.Fatalf("loadIndexFileV3: %v", err)
	}
	if rawStore {
		t.Fatal("rawStore flag should be false")
	}
	if sparse {
		t.Fatal("sparse flag should be false")
	}
}

// TestIndexFileV2RejectsForgedOldVersion checks that a file with version 2
// (below the v3 floor) returns ErrFormatTooOld.
func TestIndexFileV2RejectsForgedOldVersion(t *testing.T) {
	idx := newIndex(8, 2, 1)
	rng := rand.New(rand.NewSource(5))
	v := randVec(rng, 8)
	idx.Add("x", v, "tx", nil, 1)

	path := filepath.Join(t.TempDir(), "forgedv2.tqi")
	if err := saveIndexFile(path, idx, 1, false, false); err != nil {
		t.Fatalf("saveIndexFile: %v", err)
	}

	// Overwrite the version byte (offset 4, after 4-byte magic) with value 2.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[4] = 2
	// Recompute CRC by writing without it and appending.
	// Simpler: just corrupt the version; loadIndexFile will reject before CRC.
	// But CRC covers the whole body including version. We just want to test the
	// floor guard fires BEFORE CRC. So we patch both the version byte and the
	// last 4 bytes (CRC) — set CRC to 0 which will also fail, but the version
	// error fires first only if we check version before CRC.
	// Actually the design checks magic, then version, then CRC at the end.
	// So patching only the version byte is sufficient: version check fires before
	// the CRC is read.
	binary.LittleEndian.PutUint32(data[len(data)-4:], 0) // corrupt CRC too for safety
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err = loadIndexFile(path)
	if !errors.Is(err, ErrFormatTooOld) {
		t.Fatalf("expected ErrFormatTooOld, got: %v", err)
	}
}
