package wal

import (
	"path/filepath"
	"testing"
	"time"
)

func TestReaderReplayOrdering(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ordered.wal")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []Entry{
		{Kind: EntryPut, CollectionID: "docs", VectorID: "a", LamportClock: 1, ActorID: "x", WallClock: time.Now().UTC()},
		{Kind: EntryPut, CollectionID: "docs", VectorID: "b", LamportClock: 2, ActorID: "x", WallClock: time.Now().UTC()},
		{Kind: EntryTombstone, CollectionID: "docs", VectorID: "a", LamportClock: 3, ActorID: "x", WallClock: time.Now().UTC()},
	}
	for _, entry := range want {
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
	var got []Entry
	for r.Next() {
		got = append(got, r.Entry())
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].VectorID != want[i].VectorID || got[i].LamportClock != want[i].LamportClock {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
