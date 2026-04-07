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
		Embedding:    []float32{0.1, 0.2, 0.3},
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
