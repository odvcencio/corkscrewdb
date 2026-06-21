package wal

import (
	"testing"
	"time"
)

// TestManagerRotateCreatesDurableSegments forces a rotation and asserts that a
// new segment file appears. Rotation opens the new segment via
// NewWriterWithSync, which fsyncs the parent directory so the rotated segment's
// directory entry is durable; this test exercises that path.
func TestManagerRotateCreatesDurableSegments(t *testing.T) {
	dir := t.TempDir()
	// Tiny max segment size so a couple of appends force rotation.
	m, err := NewManager(dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(id string, clock uint64) Entry {
		return Entry{
			Kind:         EntryPut,
			CollectionID: "docs",
			VectorID:     id,
			Quantized:    &QuantizedVector{MSE: []byte{1, 2, 3, 4}, Signs: []byte{1, 2, 3, 4}},
			Dim:          4,
			LamportClock: clock,
			ActorID:      "x",
			WallClock:    time.Now().UTC(),
		}
	}
	for i := 0; i < 6; i++ {
		if err := m.Append(mk("v", uint64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("expected rotation to produce multiple segments, got %d: %v", len(segs), segs)
	}
	// The data must replay across all rotated segments.
	var total int
	for _, seg := range segs {
		r, err := NewReader(seg)
		if err != nil {
			t.Fatal(err)
		}
		for r.Next() {
			total++
		}
		if r.Err() != nil {
			r.Close()
			t.Fatal(r.Err())
		}
		r.Close()
	}
	if total != 6 {
		t.Fatalf("replayed %d entries across segments, want 6", total)
	}
}
