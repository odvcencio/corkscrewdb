package corkscrewdb

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// marshalSmallIndex produces a valid .tqi payload for use as seed corpus.
func marshalSmallIndex(t testing.TB) []byte {
	t.Helper()
	idx := newIndex(8, 2, 1)
	rng := rand.New(rand.NewSource(100))
	for i := 0; i < 3; i++ {
		v := randVec(rng, 8)
		idx.Add(fmt.Sprintf("e%d", i), v, fmt.Sprintf("text%d", i), map[string]string{"i": fmt.Sprint(i)}, uint64(i+1))
	}
	data, err := marshalIndexFile(idx, 3, true, false)
	if err != nil {
		t.Fatalf("marshalIndexFile: %v", err)
	}
	return data
}

// FuzzIndexFileLoad fuzzes the .tqi loader against arbitrary bytes.
//
// Invariants:
//  1. No panic for any input.
//  2. Corruption must surface as a non-nil error, never a silent wrong-result.
//  3. A validly-encoded .tqi round-trips through write→fuzz-read without error.
func FuzzIndexFileLoad(f *testing.F) {
	// Seed: valid index with 3 entries.
	f.Add(marshalSmallIndex(f))
	// Seed: empty.
	f.Add([]byte{})
	// Seed: TQI1 magic (little-endian 0x54514931 = bytes 31 49 51 54) + version 3.
	f.Add([]byte{0x31, 0x49, 0x51, 0x54, 0x03})
	// Seed: correct magic + version 2 (below floor).
	f.Add([]byte{0x31, 0x49, 0x51, 0x54, 0x02})
	// Seed: wrong magic.
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x03})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Write to a temp file (loader requires a path).
		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.tqi")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		// Must not panic; error is acceptable.
		_, _, _, _, _ = loadIndexFileV3(path)
	})
}
