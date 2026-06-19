package wal

import (
	"encoding/binary"
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

// TestReaderTruncatedTailPlausibleLength proves a crash-truncated tail with a
// PLAUSIBLE (<= maxEntryFieldBytes) length prefix whose data bytes are cut off
// by EOF is still classified as a truncated tail (silent, nil Err) -- normal
// crash recovery, not corruption. This is the constraint that prevents the
// length bound from over-reporting corruption.
func TestReaderTruncatedTailPlausibleLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")
	w, _ := NewWriter(path)
	_ = w.Append(Entry{Kind: EntryPut, CollectionID: "c", VectorID: "a", LamportClock: 1, ActorID: "x",
		Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{1}}, Dim: 2})
	_ = w.Close()
	raw, _ := os.ReadFile(path)

	// Forge a second frame whose ActorID length prefix declares 64 bytes (well
	// within the bound) but provide zero of those bytes -- the writer crashed
	// right after the prefix. Header: magic(2)+version(1)+kind(1)+lamport(8),
	// then ActorID length(4) = plausible, then EOF.
	tail := make([]byte, 0, 16)
	tail = append(tail, 0x57, 0x43, 0x05, 0x01) // magic, version, kind
	tail = append(tail, 0, 0, 0, 0, 0, 0, 0, 0) // lamport clock = 0
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], 64) // plausible ActorID length
	tail = append(tail, lenBuf[:]...)
	_ = os.WriteFile(path, append(raw, tail...), 0o644)

	r, _ := NewReader(path)
	count := 0
	for r.Next() {
		count++
	}
	if err := r.Err(); err != nil {
		t.Fatalf("plausible-length truncated tail must yield nil Err, got %v", err)
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

// TestReaderInteriorLengthPrefixCorruption corrupts the FIRST (interior) entry's
// ActorID length prefix to an inflated value. Without the max-plausible-length
// bound, io.ReadFull would consume the rest of the file and return
// io.ErrUnexpectedEOF, which the reader would silently classify as a truncated
// tail -- discarding the corrupt entry AND the valid second entry. The bound
// must surface this as *ErrWALCorrupt instead.
func TestReaderInteriorLengthPrefixCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")
	w, _ := NewWriter(path)
	first := Entry{Kind: EntryPut, CollectionID: "c", VectorID: "a", LamportClock: 1, ActorID: "x",
		Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{1}}, Dim: 2}
	if err := w.Append(first); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Kind: EntryPut, CollectionID: "c", VectorID: "b", LamportClock: 2, ActorID: "x",
		Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{1}}, Dim: 2}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	raw, _ := os.ReadFile(path)
	// ActorID length prefix sits at offset 12 of the first frame:
	// magic(2) + version(1) + kind(1) + lamport(8) = 12.
	const actorLenOff = 12
	// Inflate it well past maxEntryFieldBytes so the bound trips before ReadFull.
	binary.LittleEndian.PutUint32(raw[actorLenOff:], maxEntryFieldBytes+1)
	_ = os.WriteFile(path, raw, 0o644)

	r, _ := NewReader(path)
	for r.Next() {
	}
	var corrupt *ErrWALCorrupt
	if !errors.As(r.Err(), &corrupt) {
		t.Fatalf("interior length-prefix corruption must yield *ErrWALCorrupt, got %v", r.Err())
	}
	if corrupt.Offset != 0 {
		t.Fatalf("want offset 0, got %d", corrupt.Offset)
	}
}
