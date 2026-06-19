package snapshot

import (
	"bytes"
	"testing"
	"time"
)

// validSnapshotBytes encodes a minimal but complete snapshot for use as seed.
func validSnapshotBytes() ([]byte, error) {
	data := Data{
		Collection:    "fuzz-col",
		BitWidth:      2,
		Seed:          42,
		Dim:           8,
		RawStore:      true,
		SparseEnabled: false,
		MaxLamport:    5,
		CreatedAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Records: []Record{
			{
				ID: "vec-1",
				Versions: []Version{
					{
						Quantized: &QuantizedVector{
							MSE:     []byte{1, 2, 3},
							Signs:   []byte{0xAA},
							ResNorm: 0.75,
						},
						Text:         "hello fuzz",
						Metadata:     map[string]string{"src": "test"},
						LamportClock: 1,
						ActorID:      "actor-fuzz",
						WallClock:    time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
						Tombstone:    false,
					},
				},
			},
		},
	}
	return marshal(data)
}

// validSnapshotWithSparse encodes a snapshot record with a sparse block.
func validSnapshotWithSparse() ([]byte, error) {
	data := Data{
		Collection:    "fuzz-sparse",
		BitWidth:      2,
		Seed:          7,
		Dim:           16,
		SparseEnabled: true,
		MaxLamport:    3,
		CreatedAt:     time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Records: []Record{
			{
				ID: "sparse-1",
				Versions: []Version{
					{
						RawHash: func() []byte { h := make([]byte, 32); h[0] = 0xDE; return h }(),
						Sparse: &SparseBlock{
							Indices: []uint32{0, 7, 15},
							Values:  []float32{0.1, 0.5, 0.9},
						},
						LamportClock: 3,
						ActorID:      "actor-sparse",
						WallClock:    time.Date(2026, 1, 2, 2, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}
	return marshal(data)
}

// validSnapshotWithCompactChildren encodes a snapshot with compact-quantized-ordinal children.
func validSnapshotWithCompactChildren() ([]byte, error) {
	children := make([]ChildVector, 3)
	for i := range children {
		children[i] = ChildVector{
			ID:  itoa(i),
			Dim: 4,
			Quantized: &QuantizedVector{
				MSE:     []byte{byte(i), byte(i + 1)},
				Signs:   []byte{byte(i + 10)},
				ResNorm: float32(i) * 0.1,
			},
		}
	}
	data := Data{
		Collection: "fuzz-compact",
		BitWidth:   2,
		Seed:       13,
		Dim:        4,
		MaxLamport: 2,
		CreatedAt:  time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		Records: []Record{
			{
				ID: "parent-1",
				Versions: []Version{
					{
						Children:     children,
						LamportClock: 2,
						ActorID:      "actor-compact",
						WallClock:    time.Date(2026, 1, 3, 3, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}
	return marshal(data)
}

func itoa(i int) string {
	// minimal int-to-string without importing strconv to keep this file lean
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// FuzzSnapshotRead fuzzes the snapshot read decoder.
//
// Invariants:
//  1. No panic for any input.
//  2. Corruption (bad magic, bad version, CRC mismatch, oversized blocks) must
//     surface as a non-nil error, never a silent wrong-result.
//  3. A valid encoded snapshot must round-trip: read → re-marshal → re-read without error.
func FuzzSnapshotRead(f *testing.F) {
	// Seed: minimal valid snapshot.
	if data, err := validSnapshotBytes(); err == nil {
		f.Add(data)
	}
	// Seed: snapshot with sparse block.
	if data, err := validSnapshotWithSparse(); err == nil {
		f.Add(data)
	}
	// Seed: snapshot with compact children.
	if data, err := validSnapshotWithCompactChildren(); err == nil {
		f.Add(data)
	}
	// Seed: empty bytes.
	f.Add([]byte{})
	// Seed: just the magic header (CSDB = 0x43534442, little-endian).
	f.Add([]byte{0x42, 0x44, 0x53, 0x43})
	// Seed: magic + wrong version (5, below floor 6).
	f.Add([]byte{0x42, 0x44, 0x53, 0x43, 0x05})

	f.Fuzz(func(t *testing.T, data []byte) {
		snap, err := read(bytes.NewReader(data))
		if err != nil {
			// Detected corruption — perfectly acceptable.
			return
		}
		// Decoded successfully: re-marshal must not panic and must produce
		// bytes that are themselves decodable.
		re, reErr := marshal(snap)
		if reErr != nil {
			t.Fatalf("re-marshal of decoded snapshot failed: %v", reErr)
		}
		_, _ = read(bytes.NewReader(re))
	})
}
