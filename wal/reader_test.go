package wal

import (
	"errors"
	"os"
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

func TestReaderCleanTailEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")
	w, _ := NewWriter(path)
	_ = w.Append(Entry{Kind: EntryPut, CollectionID: "c", VectorID: "a", LamportClock: 1,
		Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{1}}, Dim: 2})
	_ = w.Close()
	r, _ := NewReader(path)
	count := 0
	for r.Next() {
		count++
	}
	if err := r.Err(); err != nil {
		t.Fatalf("clean tail must yield nil Err, got %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1 entry, got %d", count)
	}
}

func TestReaderTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")
	w, _ := NewWriter(path)
	_ = w.Append(Entry{Kind: EntryPut, CollectionID: "c", VectorID: "a", LamportClock: 1,
		Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{1}}, Dim: 2})
	_ = w.Close()
	raw, _ := os.ReadFile(path)
	// Append a partial (truncated) second frame: magic+version+kind only.
	_ = os.WriteFile(path, append(raw, 0x57, 0x43, 0x05, 0x01), 0o644)
	r, _ := NewReader(path)
	count := 0
	for r.Next() {
		count++
	}
	if err := r.Err(); err != nil {
		t.Fatalf("truncated tail must yield nil Err, got %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1 valid entry, got %d", count)
	}
}

func TestReaderInteriorCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")
	w, _ := NewWriter(path)
	_ = w.Append(Entry{Kind: EntryPut, CollectionID: "c", VectorID: "a", LamportClock: 1,
		Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{1}}, Dim: 2})
	_ = w.Append(Entry{Kind: EntryPut, CollectionID: "c", VectorID: "b", LamportClock: 2,
		Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{1}}, Dim: 2})
	_ = w.Close()
	raw, _ := os.ReadFile(path)
	raw[10] ^= 0xFF // flip a byte inside the first entry -> CRC fails, but bytes follow
	_ = os.WriteFile(path, raw, 0o644)
	r, _ := NewReader(path)
	for r.Next() {
	}
	var corrupt *ErrWALCorrupt
	if !errors.As(r.Err(), &corrupt) {
		t.Fatalf("interior corruption must yield *ErrWALCorrupt, got %v", r.Err())
	}
	if corrupt.Offset != 0 {
		t.Fatalf("want offset 0, got %d", corrupt.Offset)
	}
}
