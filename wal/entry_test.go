package wal

import (
	"bytes"
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
