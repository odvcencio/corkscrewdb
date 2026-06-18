package rawstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	rvsMagic   = uint32(0x52565331) // "RVS1"
	rvsVersion = uint8(1)           // v0.3.0 floor
	keyLen     = 32                 // blake3-256
)

// ErrRVSFormat is returned for an unreadable or too-old .rvs segment header.
var ErrRVSFormat = errors.New("rawstore: invalid or too-old .rvs segment")

func writeSegmentHeader(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, rvsMagic); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, rvsVersion)
}

func readSegmentHeader(r io.Reader) error {
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		if errors.Is(err, io.EOF) {
			return ErrRVSFormat
		}
		return err
	}
	if magic != rvsMagic {
		return fmt.Errorf("%w: magic %x", ErrRVSFormat, magic)
	}
	var version uint8
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version < rvsVersion {
		return fmt.Errorf("%w: version %d", ErrRVSFormat, version)
	}
	if version != rvsVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrRVSFormat, version)
	}
	return nil
}

// writeFrame writes key(32) + valueLen(u32) + bytes.
func writeFrame(w io.Writer, key, value []byte) error {
	if len(key) != keyLen {
		return fmt.Errorf("rawstore: key must be %d bytes, got %d", keyLen, len(key))
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}

// maxRVSValueBytes caps the value allocation in readFrame.  No legitimate blob
// in a single .rvs segment can exceed the segment size ceiling (maxSegmentBytes,
// 64 MiB).  An inflated length prefix is corruption, not a plausible large blob.
const maxRVSValueBytes = maxSegmentBytes

// readFrame returns key, value, and the total bytes consumed by the frame.
func readFrame(r io.Reader) (key, value []byte, n int, err error) {
	key = make([]byte, keyLen)
	if _, err = io.ReadFull(r, key); err != nil {
		return nil, nil, 0, err
	}
	var valLen uint32
	if err = binary.Read(r, binary.LittleEndian, &valLen); err != nil {
		return nil, nil, 0, err
	}
	// Bound the declared length before allocating so an inflated length prefix
	// surfaces as a typed error rather than a multi-GiB allocation.
	if int64(valLen) > maxRVSValueBytes {
		return nil, nil, 0, fmt.Errorf("%w: frame value length too large %d (max %d)", ErrRVSFormat, valLen, maxRVSValueBytes)
	}
	value = make([]byte, valLen)
	if _, err = io.ReadFull(r, value); err != nil {
		return nil, nil, 0, err
	}
	return key, value, keyLen + 4 + int(valLen), nil
}
