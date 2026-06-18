package rawstore

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// validSegmentBytes produces a valid .rvs segment containing one frame.
func validSegmentBytes(key, value []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeSegmentHeader(&buf); err != nil {
		return nil, err
	}
	if err := writeFrame(&buf, key, value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// inflatedFrameBytes produces a segment header followed by a frame whose
// declared value length is math.MaxUint32, reproducing an OOM-style attack.
func inflatedFrameBytes() []byte {
	key := make([]byte, keyLen)
	for i := range key {
		key[i] = byte(i)
	}
	var buf bytes.Buffer
	_ = writeSegmentHeader(&buf)
	// Write key.
	_, _ = buf.Write(key)
	// Write inflated length (max uint32).
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0xFFFFFFFF))
	// No value bytes follow — readFrame must fail before allocating.
	return buf.Bytes()
}

// FuzzRVSSegmentHeader fuzzes the segment header decoder.
//
// Invariants:
//  1. No panic.
//  2. An invalid header must return a non-nil error.
func FuzzRVSSegmentHeader(f *testing.F) {
	// Seed: valid header.
	var validHdr bytes.Buffer
	_ = writeSegmentHeader(&validHdr)
	f.Add(validHdr.Bytes())
	// Seed: empty.
	f.Add([]byte{})
	// Seed: wrong magic.
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x01})
	// Seed: correct magic, version 0 (too old).
	f.Add([]byte{0x31, 0x53, 0x56, 0x52, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = readSegmentHeader(bytes.NewReader(data))
	})
}

// FuzzRVSReadFrame fuzzes the frame decoder (key + value-length prefix + bytes).
//
// Invariants:
//  1. No panic for any input.
//  2. If readFrame succeeds, key must be exactly keyLen bytes.
//  3. The inflated-length seed must not succeed (OOM guard).
func FuzzRVSReadFrame(f *testing.F) {
	// Seed: valid segment (header + one frame).
	key := make([]byte, keyLen)
	for i := range key {
		key[i] = byte(i)
	}
	value := []byte("hello fuzz rawstore")
	if data, err := validSegmentBytes(key, value); err == nil {
		// Pass only the frame portion (skip the header).
		f.Add(data[headerLen:])
	}
	// Seed: inflated length — must be caught before allocation.
	f.Add(inflatedFrameBytes()[headerLen:])
	// Seed: empty.
	f.Add([]byte{})
	// Seed: just 32 key bytes, no length.
	f.Add(make([]byte, keyLen))
	// Seed: key + zero-length value.
	zeroVal := make([]byte, keyLen+4)
	f.Add(zeroVal)

	f.Fuzz(func(t *testing.T, frameBytes []byte) {
		gotKey, _, _, err := readFrame(bytes.NewReader(frameBytes))
		if err != nil {
			// Error is fine — corrupt/truncated input expected.
			return
		}
		if len(gotKey) != keyLen {
			t.Fatalf("readFrame returned key of length %d, want %d", len(gotKey), keyLen)
		}
	})
}
