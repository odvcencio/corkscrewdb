package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"testing"
	"time"
)

func TestEntryPutRoundTrip(t *testing.T) {
	entry := Entry{
		Kind:         EntryPut,
		CollectionID: "documents",
		VectorID:     "doc-1",
		Quantized:    &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.5},
		Dim:          3,
		Text:         "hello world",
		Metadata:     map[string]string{"source": "test"},
		LamportClock: 42,
		ActorID:      "actor-1",
		WallClock:    time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
	}
	var buf bytes.Buffer
	if err := entry.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEntry(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != EntryPut {
		t.Fatalf("Kind = %d, want %d", got.Kind, EntryPut)
	}
	if got.VectorID != "doc-1" {
		t.Fatalf("VectorID = %q, want %q", got.VectorID, "doc-1")
	}
	if got.LamportClock != 42 {
		t.Fatalf("LamportClock = %d, want 42", got.LamportClock)
	}
	if got.Metadata["source"] != "test" {
		t.Fatalf("Metadata[source] = %q, want %q", got.Metadata["source"], "test")
	}
	if got.Quantized == nil || got.Dim != 3 {
		t.Fatalf("quantized payload not preserved: %+v", got)
	}
}

func TestEntryTombstoneRoundTrip(t *testing.T) {
	entry := Entry{
		Kind:         EntryTombstone,
		CollectionID: "documents",
		VectorID:     "doc-1",
		LamportClock: 99,
		ActorID:      "actor-2",
		WallClock:    time.Date(2026, 4, 7, 13, 0, 0, 0, time.UTC),
	}
	var buf bytes.Buffer
	if err := entry.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEntry(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != EntryTombstone {
		t.Fatalf("Kind = %d, want %d", got.Kind, EntryTombstone)
	}
	if got.VectorID != "doc-1" {
		t.Fatalf("VectorID = %q", got.VectorID)
	}
}

func TestEntryPackedChildrenRoundTrip(t *testing.T) {
	entry := Entry{
		Kind:         EntryPut,
		CollectionID: "documents",
		VectorID:     "parent-1",
		Dim:          4,
		Children: []ChildVector{
			{
				ID:        "child-1",
				Quantized: &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.75},
				Dim:       4,
				Text:      "child text",
				Metadata:  map[string]string{"slot": "one"},
			},
			{
				ID:        "child-2",
				Quantized: &QuantizedVector{MSE: []byte{4, 5}, Signs: []byte{6}, ResNorm: 0.25},
				Dim:       4,
				Text:      "second child",
				Metadata:  map[string]string{"slot": "two"},
			},
		},
		Text:         "parent text",
		Metadata:     map[string]string{"tenant": "acme"},
		LamportClock: 123,
		ActorID:      "actor-1",
		WallClock:    time.Date(2026, 4, 7, 14, 0, 0, 0, time.UTC),
	}
	var buf bytes.Buffer
	if err := entry.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEntry(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.VectorID != "parent-1" || got.Dim != 4 || len(got.Children) != 2 {
		t.Fatalf("got = %+v, want parent with two children", got)
	}
	if got.Children[0].ID != "child-1" || got.Children[0].Quantized == nil || got.Children[0].Metadata["slot"] != "one" {
		t.Fatalf("quantized child = %+v", got.Children[0])
	}
	if got.Children[1].ID != "child-2" || got.Children[1].Quantized == nil || got.Children[1].Metadata["slot"] != "two" {
		t.Fatalf("second child = %+v", got.Children[1])
	}
}

func TestEntryV5RoundTrip(t *testing.T) {
	e := Entry{
		Kind:         EntryPut,
		CollectionID: "c",
		VectorID:     "v",
		Quantized:    &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 1.5},
		Dim:          4,
		RawHash:      make([]byte, 32),
		Sparse:       &SparseBlock{Indices: []uint32{1, 5}, Values: []float32{0.5, 0.25}},
		Text:         "hello",
		Metadata:     map[string]string{"k": "vv"},
		LamportClock: 99,
		ActorID:      "actor",
	}
	for i := range e.RawHash {
		e.RawHash[i] = byte(i)
	}
	data, err := e.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadEntry(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if got.Dim != 4 || got.Text != "hello" || got.Metadata["k"] != "vv" {
		t.Fatalf("scalar mismatch: %+v", got)
	}
	if len(got.RawHash) != 32 || got.RawHash[5] != 5 {
		t.Fatalf("rawhash mismatch: %x", got.RawHash)
	}
	if got.Sparse == nil || len(got.Sparse.Indices) != 2 || got.Sparse.Values[1] != 0.25 {
		t.Fatalf("sparse mismatch: %+v", got.Sparse)
	}
}

func TestEntryFloorGuardRejectsV4(t *testing.T) {
	// Forge a header with version 4.
	buf := []byte{0x57, 0x43, 0x04}
	_, err := ReadEntry(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected floor guard error for v4")
	}
}

// TestEntryV5LegacyLoadsNormDefaultOne proves the true-MIPS migration's
// backward-compat contract: a v5 payload (predates QuantizedVector.Norm)
// loads with Norm defaulted to 1 (unit-space/cosine semantics), leaving
// ResNorm/MSE/Signs untouched. It simulates a v5 payload by encoding a
// current (v6) entry, splicing out the 4-byte Norm field that immediately
// follows ResNorm in the v6 wire layout, downgrading the version byte, and
// recomputing the CRC trailer over the shortened body.
func TestEntryV5LegacyLoadsNormDefaultOne(t *testing.T) {
	e := Entry{
		Kind:         EntryPut,
		CollectionID: "c",
		VectorID:     "v",
		Quantized:    &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.75, Norm: 3.5},
		Dim:          4,
		ActorID:      "a",
		LamportClock: 1,
	}
	data, err := e.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Locate the 4-byte ResNorm bit pattern; the v6 layout writes Norm
	// immediately after it.
	var resNormBytes [4]byte
	binary.LittleEndian.PutUint32(resNormBytes[:], math.Float32bits(0.75))
	idx := bytes.Index(data, resNormBytes[:])
	if idx < 0 {
		t.Fatal("could not locate ResNorm bit pattern in the v6-encoded entry")
	}
	var normBytes [4]byte
	binary.LittleEndian.PutUint32(normBytes[:], math.Float32bits(3.5))
	if !bytes.Equal(data[idx+4:idx+8], normBytes[:]) {
		t.Fatal("Norm bytes are not immediately after ResNorm bytes as the v6 layout requires")
	}

	// Splice out the Norm field and downgrade the version byte (offset 2,
	// after the 2-byte magic) to simulate a genuine v5 payload.
	legacy := append([]byte{}, data[:idx+4]...)
	legacy = append(legacy, data[idx+8:]...)
	legacy[2] = walMinVersion
	body := legacy[:len(legacy)-4]
	binary.LittleEndian.PutUint32(legacy[len(legacy)-4:], crc32.ChecksumIEEE(body))

	got, err := ReadEntry(bytes.NewReader(legacy))
	if err != nil {
		t.Fatalf("ReadEntry(legacy v5): %v", err)
	}
	if got.Quantized == nil {
		t.Fatal("Quantized payload missing after legacy v5 load")
	}
	if got.Quantized.Norm != 1 {
		t.Fatalf("legacy v5 load: Norm = %v, want 1 (unit-space default)", got.Quantized.Norm)
	}
	if got.Quantized.ResNorm != 0.75 {
		t.Fatalf("legacy v5 load: ResNorm = %v, want 0.75 (unaffected by the splice)", got.Quantized.ResNorm)
	}
}

// TestReadEntryRejectsInvalidNorm proves m1's decode-time validation: a
// NaN/Inf/negative Norm is rejected, matching turboquant's own wire codec
// (wire.go:144). A negative norm silently inverts ScoreUpperBound into a
// lower bound and drops real top-k members, so untrusted/corrupt input must
// never decode into a QuantizedVector carrying one. Covers both the
// top-level Quantized field and a packed child's Quantized field.
func TestReadEntryRejectsInvalidNorm(t *testing.T) {
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
		t.Run("top-level/"+tc.name, func(t *testing.T) {
			e := Entry{
				Kind:         EntryPut,
				CollectionID: "c",
				VectorID:     "v",
				Quantized:    &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.5, Norm: tc.norm},
				Dim:          4,
				ActorID:      "a",
			}
			data, err := e.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary: %v", err)
			}
			if _, err := ReadEntry(bytes.NewReader(data)); err == nil {
				t.Fatalf("ReadEntry(norm=%v): want error, got nil", tc.norm)
			}
		})
		t.Run("child/"+tc.name, func(t *testing.T) {
			e := Entry{
				Kind:         EntryPut,
				CollectionID: "c",
				VectorID:     "parent",
				Dim:          4,
				Children: []ChildVector{
					{ID: "c0", Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{2}, ResNorm: 0.5, Norm: tc.norm}, Dim: 4},
				},
				ActorID: "a",
			}
			data, err := e.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary: %v", err)
			}
			if _, err := ReadEntry(bytes.NewReader(data)); err == nil {
				t.Fatalf("ReadEntry(child norm=%v): want error, got nil", tc.norm)
			}
		})
	}
}
