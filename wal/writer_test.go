package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriterAppendAndReaderReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{Kind: EntryPut, CollectionID: "docs", VectorID: "a", Embedding: []float32{1, 2, 3}, LamportClock: 1, ActorID: "x", WallClock: time.Now().UTC()},
		{Kind: EntryPut, CollectionID: "docs", VectorID: "b", Embedding: []float32{4, 5, 6}, LamportClock: 2, ActorID: "x", WallClock: time.Now().UTC()},
		{Kind: EntryTombstone, CollectionID: "docs", VectorID: "a", LamportClock: 3, ActorID: "x", WallClock: time.Now().UTC()},
	}
	for _, entry := range entries {
		if err := w.Append(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var replayed []Entry
	for r.Next() {
		replayed = append(replayed, r.Entry())
	}
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if len(replayed) != 3 {
		t.Fatalf("replayed %d entries, want 3", len(replayed))
	}
	if replayed[2].Kind != EntryTombstone {
		t.Fatalf("entry 2 kind = %d, want tombstone", replayed[2].Kind)
	}
}

func TestWriterCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("wal file not created: %v", err)
	}
}
