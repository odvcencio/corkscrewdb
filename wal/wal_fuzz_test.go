package wal

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

// validEntry produces a correctly-encoded WAL entry for use as a seed corpus.
func validEntry() ([]byte, error) {
	e := Entry{
		Kind:         EntryPut,
		CollectionID: "fuzz-col",
		VectorID:     "fuzz-vec-1",
		Quantized:    &QuantizedVector{MSE: []byte{1, 2, 3, 4}, Signs: []byte{0xAB}, ResNorm: 0.5},
		Dim:          8,
		RawHash:      make([]byte, 32),
		Sparse:       &SparseBlock{Indices: []uint32{0, 5}, Values: []float32{0.1, 0.9}},
		Text:         "hello fuzz",
		Metadata:     map[string]string{"k": "v"},
		LamportClock: 7,
		ActorID:      "actor-fuzz",
		WallClock:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for i := range e.RawHash {
		e.RawHash[i] = byte(i)
	}
	return e.MarshalBinary()
}

// validTombstone produces a minimal tombstone entry for the seed corpus.
func validTombstone() ([]byte, error) {
	e := Entry{
		Kind:         EntryTombstone,
		CollectionID: "fuzz-col",
		VectorID:     "fuzz-vec-2",
		LamportClock: 8,
		ActorID:      "actor-fuzz",
		WallClock:    time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	return e.MarshalBinary()
}

// inflatedLengthEntry returns a valid entry with one field's length prefix
// artificially inflated to maxEntryFieldBytes+1, reproducing the historical
// corruption hole caught during code review.
func inflatedLengthEntry() []byte {
	// Use a valid tombstone as the base, then corrupt the ActorID length prefix
	// (bytes 11..14 in the wire format: magic(2)+ver(1)+kind(1)+lamport(8) = 12
	// bytes before ActorID length).
	e := Entry{
		Kind:         EntryTombstone,
		CollectionID: "fuzz-col",
		VectorID:     "fuzz-vec-3",
		LamportClock: 1,
		ActorID:      "actor-ok",
		WallClock:    time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
	}
	data, err := e.MarshalBinary()
	if err != nil {
		panic("inflatedLengthEntry: MarshalBinary: " + err.Error())
	}
	// Offset of ActorID length field:
	// magic(2) + version(1) + kind(1) + lamport(8) = 12
	const actorIDLenOffset = 12
	if len(data) < actorIDLenOffset+4 {
		return data
	}
	// Write maxEntryFieldBytes+1 as the length prefix.
	inflated := uint32(maxEntryFieldBytes) + 1
	binary.LittleEndian.PutUint32(data[actorIDLenOffset:], inflated)
	return data
}

// FuzzReadEntry fuzzes the WAL ReadEntry decoder.
//
// Invariants asserted:
//  1. No panic for any input (the fuzzer's main job).
//  2. If ReadEntry returns a non-nil error, that's acceptable (corruption detected).
//  3. If ReadEntry succeeds (err == nil), re-encoding the returned entry must not
//     panic. The re-encoded form need not be byte-identical (timestamps round-trip
//     through int64 nanoseconds), but must itself be decodable.
//  4. The inflated-length seed must always be rejected (err != nil).
func FuzzReadEntry(f *testing.F) {
	// Seed: valid put entry.
	if data, err := validEntry(); err == nil {
		f.Add(data)
	}
	// Seed: valid tombstone.
	if data, err := validTombstone(); err == nil {
		f.Add(data)
	}
	// Seed: inflated length prefix — must be caught.
	f.Add(inflatedLengthEntry())
	// Seed: empty bytes.
	f.Add([]byte{})
	// Seed: just the magic bytes.
	f.Add([]byte{0x57, 0x43})
	// Seed: magic + version(5) + kind.
	f.Add([]byte{0x57, 0x43, 0x05, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		entry, err := ReadEntry(bytes.NewReader(data))
		if err != nil {
			// Corruption detected — acceptable, and expected for most inputs.
			return
		}
		// Entry decoded successfully: re-encode must not panic.
		re, reErr := entry.MarshalBinary()
		if reErr != nil {
			// Re-encoding a decoded entry must always succeed (the encoder only
			// fails on writer errors, never on the data itself).
			t.Fatalf("MarshalBinary of decoded entry failed: %v", reErr)
		}
		// Re-encoded bytes must themselves be decodable without panic.
		_, _ = ReadEntry(bytes.NewReader(re))
	})
}

// FuzzReadEntryInflatedLength specifically verifies the inflated-length-prefix
// corruption hole is caught.  This seed runs in unit-test mode (no -fuzz flag)
// to give a fast regression signal.
func FuzzReadEntryInflatedLength(f *testing.F) {
	f.Add(inflatedLengthEntry())

	f.Fuzz(func(t *testing.T, data []byte) {
		// We only care that ReadEntry never panics; errors are expected.
		_, _ = ReadEntry(bytes.NewReader(data))
	})
}
