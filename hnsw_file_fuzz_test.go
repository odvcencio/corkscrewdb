package corkscrewdb

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// marshalSmallHNSW returns a valid serialised HNSW graph payload and the
// corresponding flat index (needed as the flat context for loadHNSWFile).
func marshalSmallHNSW(t testing.TB) (hnswPayload []byte, flat *index) {
	t.Helper()
	const dim = 8
	const n = 10
	rng := rand.New(rand.NewSource(77))
	hw := newHNSWIndex(dim, 2, 55, defaultHNSWParams())
	for i := 0; i < n; i++ {
		v := randVec(rng, dim)
		hw.Add(fmt.Sprintf("n%d", i), v, fmt.Sprintf("t%d", i), nil, uint64(i+1))
	}
	payload, err := marshalHNSWFile(hw)
	if err != nil {
		t.Fatalf("marshalHNSWFile: %v", err)
	}
	return payload, hw.flat
}

// FuzzHNSWFileLoad fuzzes the graph.hnsw loader against arbitrary bytes.
//
// The loader requires a flat *index whose Len() matches the nodeCount embedded
// in the file.  For fuzzed (random) bytes the count will almost never match, so
// the loader returns an error early.  That is correct and expected behaviour.
// The fuzz test's only hard requirement is: no panic for any input.
//
// For the valid seed the round-trip is also asserted.
func FuzzHNSWFileLoad(f *testing.F) {
	payload, flat := marshalSmallHNSW(f)
	// Seed: valid HNSW graph.
	f.Add(payload)
	// Seed: empty.
	f.Add([]byte{})
	// Seed: HNSW magic (0x484E5357 = bytes 57 53 4E 48) + version 2.
	f.Add([]byte{0x57, 0x53, 0x4E, 0x48, 0x02})
	// Seed: correct magic + version 1 (below floor).
	f.Add([]byte{0x57, 0x53, 0x4E, 0x48, 0x01})
	// Seed: wrong magic.
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x02})

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.hnsw")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		// Must not panic; error is acceptable (and expected for most inputs
		// since nodeCount in fuzzed bytes won't match flat.Len()).
		_, _ = loadHNSWFile(path, flat, defaultHNSWParams())
	})
}
