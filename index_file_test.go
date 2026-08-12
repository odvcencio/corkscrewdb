package corkscrewdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"m31labs.dev/turboquant"
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

// TestIndexFileV3LegacyLoadsNormDefaultOne proves the true-MIPS migration's
// backward-compat contract: a v3 .tqi payload (predates IPQuantized.Norm)
// loads with Norm defaulted to 1 (unit-space/cosine semantics), leaving
// ResNorm/MSE/Signs untouched. It simulates a v3 payload by marshaling a
// current (v4) index file, splicing out the 4-byte Norm field that
// immediately follows ResNorm in the v4 wire layout, downgrading the version
// byte, and recomputing the CRC trailer over the shortened body.
func TestIndexFileV3LegacyLoadsNormDefaultOne(t *testing.T) {
	idx := newIndex(8, 2, 1)
	rng := rand.New(rand.NewSource(7))
	v := randVec(rng, 8)
	idx.Add("x", v, "tx", nil, 1)

	data, err := marshalIndexFile(idx, 1, false, false)
	if err != nil {
		t.Fatalf("marshalIndexFile: %v", err)
	}

	// The real Norm/ResNorm the quantizer computed for "x" tells us exactly
	// which bit pattern to locate and splice out.
	qv := idx.entries[0].qv
	var resNormBytes [4]byte
	binary.LittleEndian.PutUint32(resNormBytes[:], math.Float32bits(qv.ResNorm))
	pos := bytes.Index(data, resNormBytes[:])
	if pos < 0 {
		t.Fatal("could not locate ResNorm bit pattern in the v4-encoded index file")
	}
	var normBytes [4]byte
	binary.LittleEndian.PutUint32(normBytes[:], math.Float32bits(qv.Norm))
	if !bytes.Equal(data[pos+4:pos+8], normBytes[:]) {
		t.Fatal("Norm bytes are not immediately after ResNorm bytes as the v4 layout requires")
	}

	// Splice out the Norm field and downgrade the version byte (offset 4,
	// after the 4-byte magic) to simulate a genuine v3 payload.
	legacy := append([]byte{}, data[:pos+4]...)
	legacy = append(legacy, data[pos+8:]...)
	legacy[4] = indexMinVersion
	body := legacy[:len(legacy)-4]
	binary.LittleEndian.PutUint32(legacy[len(legacy)-4:], crc32.ChecksumIEEE(body))

	path := filepath.Join(t.TempDir(), "legacy-v3.tqi")
	if err := os.WriteFile(path, legacy, 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, _, _, _, err := loadIndexFileV3(path)
	if err != nil {
		t.Fatalf("loadIndexFileV3(legacy v3): %v", err)
	}
	if len(loaded.entries) != 1 {
		t.Fatalf("entries = %+v, want 1", loaded.entries)
	}
	got := loaded.entries[0].qv
	if got.Norm != 1 {
		t.Fatalf("legacy v3 load: Norm = %v, want 1 (unit-space default)", got.Norm)
	}
	if got.ResNorm != qv.ResNorm {
		t.Fatalf("legacy v3 load: ResNorm = %v, want %v (unaffected by the splice)", got.ResNorm, qv.ResNorm)
	}
}

// TestLoadIndexFileV3RejectsInvalidNorm proves m1's decode-time validation:
// a NaN/Inf/negative Norm is rejected, matching turboquant's own wire codec
// (wire.go:144). A negative norm silently inverts ScoreUpperBound into a
// lower bound and drops real top-k members, so untrusted/corrupt input must
// never decode into an IPQuantized carrying one.
func TestLoadIndexFileV3RejectsInvalidNorm(t *testing.T) {
	invalidNorms := []struct {
		name string
		norm float32
	}{
		{"NaN", float32(math.NaN())},
		{"+Inf", float32(math.Inf(1))},
		{"-Inf", float32(math.Inf(-1))},
		{"negative", -1.0},
	}
	for _, tc := range invalidNorms {
		t.Run(tc.name, func(t *testing.T) {
			idx := newIndex(8, 2, 1)
			idx.addQuantized("x", turboquant.IPQuantized{MSE: []byte{1}, Signs: []byte{2}, ResNorm: 0.5, Norm: tc.norm}, "tx", nil, 1)

			data, err := marshalIndexFile(idx, 1, false, false)
			if err != nil {
				t.Fatalf("marshalIndexFile: %v", err)
			}
			path := filepath.Join(t.TempDir(), "invalid-norm.tqi")
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, _, _, _, err := loadIndexFileV3(path); err == nil {
				t.Fatalf("loadIndexFileV3(norm=%v): want error, got nil", tc.norm)
			}
		})
	}
}
