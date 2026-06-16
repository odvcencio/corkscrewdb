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
				ID:        "child-raw-future",
				Embedding: []float32{1, 0, 0, 0},
				Dim:       4,
				Text:      "future raw child",
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
	if len(got.Children[1].Embedding) != 4 || got.Children[1].Metadata["slot"] != "two" {
		t.Fatalf("raw child = %+v", got.Children[1])
	}
}
