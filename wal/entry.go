package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"
)

const (
	walMagic   = uint16(0x4357)
	walVersion = uint8(6) // v6 adds QuantizedVector.Norm (true-MIPS migration)

	// walMinVersion is the oldest wire version ReadEntry still accepts.
	// Versions below this floor predate v0.3.0 and are rejected outright.
	walMinVersion = uint8(5)
)

// maxEntryFieldBytes bounds any single length-prefixed field (ActorID,
// CollectionID, VectorID, MSE, Signs, Text, Metadata, children, sparse blocks)
// read out of a WAL entry. No legitimate field can exceed the WAL segment max
// size (defaultSegmentBytes, ~8 MiB), so a declared length above this ceiling
// is interior corruption (an inflated length prefix), not a plausible
// crash-truncated tail. This mirrors snapshot/loader.go's max-readable-bytes
// defense and lets the reader distinguish corruption from a truncated tail
// before io.ReadFull can swallow the rest of the file as ErrUnexpectedEOF.
const maxEntryFieldBytes = uint32(defaultSegmentBytes)

const (
	EntryPut       = uint8(1)
	EntryTombstone = uint8(2)
)

// ErrFormatTooOld is returned when a WAL segment predates the v0.3.0 floor.
var ErrFormatTooOld = errors.New("wal: format older than v0.3.0 floor")

// Entry is one durable collection mutation in the append-only WAL.
type Entry struct {
	Kind         uint8
	CollectionID string
	VectorID     string
	Quantized    *QuantizedVector
	Dim          int
	RawHash      []byte
	Sparse       *SparseBlock
	Children     []ChildVector
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
}

// QuantizedVector stores a TurboQuant inner-product payload.
type QuantizedVector struct {
	MSE     []byte
	Signs   []byte
	ResNorm float32
	Norm    float32 // L2 norm of the original input vector (turboquant.IPQuantized.Norm)
}

// SparseBlock is a sparse channel persisted in the WAL/snapshot.
type SparseBlock struct {
	Indices []uint32
	Values  []float32
}

// ChildVector stores one packed child vector inside a parent WAL entry.
type ChildVector struct {
	ID        string
	Quantized *QuantizedVector
	Dim       int
	Text      string
	Metadata  map[string]string
}

func (e Entry) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	if err := e.Encode(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e Entry) Encode(w io.Writer) error {
	h := crc32.NewIEEE()
	mw := io.MultiWriter(w, h)
	write := func(data any) error {
		return binary.Write(mw, binary.LittleEndian, data)
	}
	writeBytes := func(data []byte) error {
		if err := write(uint32(len(data))); err != nil {
			return err
		}
		_, err := mw.Write(data)
		return err
	}
	writeString := func(s string) error {
		return writeBytes([]byte(s))
	}

	if err := write(walMagic); err != nil {
		return err
	}
	if err := write(walVersion); err != nil {
		return err
	}
	if err := write(e.Kind); err != nil {
		return err
	}
	if err := write(e.LamportClock); err != nil {
		return err
	}
	if err := writeString(e.ActorID); err != nil {
		return err
	}
	if err := write(e.WallClock.UnixNano()); err != nil {
		return err
	}
	if err := writeString(e.CollectionID); err != nil {
		return err
	}
	if err := writeString(e.VectorID); err != nil {
		return err
	}

	if e.Quantized != nil {
		if err := write(uint8(1)); err != nil {
			return err
		}
		if err := writeBytes(e.Quantized.MSE); err != nil {
			return err
		}
		if err := writeBytes(e.Quantized.Signs); err != nil {
			return err
		}
		if err := write(math.Float32bits(e.Quantized.ResNorm)); err != nil {
			return err
		}
		if err := write(math.Float32bits(e.Quantized.Norm)); err != nil {
			return err
		}
	} else if err := write(uint8(0)); err != nil {
		return err
	}
	if err := write(uint32(e.Dim)); err != nil {
		return err
	}
	// RawHash block: presence byte + 32 bytes iff present.
	if len(e.RawHash) == 32 {
		if err := write(uint8(1)); err != nil {
			return err
		}
		if _, err := mw.Write(e.RawHash); err != nil {
			return err
		}
	} else if err := write(uint8(0)); err != nil {
		return err
	}
	// Sparse block: presence byte + count(u32) + indices + values.
	if e.Sparse != nil && len(e.Sparse.Indices) > 0 {
		if err := write(uint8(1)); err != nil {
			return err
		}
		if err := write(uint32(len(e.Sparse.Indices))); err != nil {
			return err
		}
		for _, idx := range e.Sparse.Indices {
			if err := write(idx); err != nil {
				return err
			}
		}
		for _, val := range e.Sparse.Values {
			if err := write(math.Float32bits(val)); err != nil {
				return err
			}
		}
	} else if err := write(uint8(0)); err != nil {
		return err
	}
	if err := write(uint32(len(e.Children))); err != nil {
		return err
	}
	for _, child := range e.Children {
		if err := writeString(child.ID); err != nil {
			return err
		}
		if child.Quantized != nil {
			if err := write(uint8(1)); err != nil {
				return err
			}
			if err := writeBytes(child.Quantized.MSE); err != nil {
				return err
			}
			if err := writeBytes(child.Quantized.Signs); err != nil {
				return err
			}
			if err := write(math.Float32bits(child.Quantized.ResNorm)); err != nil {
				return err
			}
			if err := write(math.Float32bits(child.Quantized.Norm)); err != nil {
				return err
			}
		} else if err := write(uint8(0)); err != nil {
			return err
		}
		if err := write(uint32(child.Dim)); err != nil {
			return err
		}
		if err := writeString(child.Text); err != nil {
			return err
		}
		childMetaJSON, err := json.Marshal(child.Metadata)
		if err != nil {
			return err
		}
		if err := writeBytes(childMetaJSON); err != nil {
			return err
		}
	}
	if err := writeString(e.Text); err != nil {
		return err
	}
	metaJSON, err := json.Marshal(e.Metadata)
	if err != nil {
		return err
	}
	if err := writeBytes(metaJSON); err != nil {
		return err
	}

	return binary.Write(w, binary.LittleEndian, h.Sum32())
}

func ReadEntry(r io.Reader) (Entry, error) {
	h := crc32.NewIEEE()
	mr := io.TeeReader(r, h)
	read := func(data any) error {
		return binary.Read(mr, binary.LittleEndian, data)
	}
	readBytes := func() ([]byte, error) {
		var length uint32
		if err := read(&length); err != nil {
			return nil, err
		}
		// Bound the declared length before allocating/ReadFull so an inflated
		// length prefix surfaces as corruption rather than being swallowed as a
		// truncated tail when io.ReadFull consumes the rest of the file.
		if length > maxEntryFieldBytes {
			return nil, fmt.Errorf("wal: field length too large %d (max %d)", length, maxEntryFieldBytes)
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(mr, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	readString := func() (string, error) {
		buf, err := readBytes()
		return string(buf), err
	}
	// readCount reads a u32 element count and bounds it against the segment
	// ceiling so an inflated count cannot trigger a giant allocation or be
	// swallowed as a truncated tail. Each element costs at least one byte, so no
	// legitimate count can exceed maxEntryFieldBytes.
	readCount := func(what string) (uint32, error) {
		var count uint32
		if err := read(&count); err != nil {
			return 0, err
		}
		if count > maxEntryFieldBytes {
			return 0, fmt.Errorf("wal: %s count too large %d (max %d)", what, count, maxEntryFieldBytes)
		}
		return count, nil
	}

	var entry Entry
	var magic uint16
	if err := read(&magic); err != nil {
		return entry, err
	}
	if magic != walMagic {
		return entry, fmt.Errorf("wal: invalid magic %x", magic)
	}

	var version uint8
	if err := read(&version); err != nil {
		return entry, err
	}
	if version < walMinVersion {
		return entry, fmt.Errorf("wal: %w: version %d", ErrFormatTooOld, version)
	}
	if version > walVersion {
		return entry, fmt.Errorf("wal: unsupported version %d (max known %d)", version, walVersion)
	}
	// v5 predates QuantizedVector.Norm; decoded v5 payloads default Norm to 1,
	// preserving their old unit-space (cosine) scoring until re-quantized.
	hasNorm := version >= 6
	if err := read(&entry.Kind); err != nil {
		return entry, err
	}
	if err := read(&entry.LamportClock); err != nil {
		return entry, err
	}

	var err error
	entry.ActorID, err = readString()
	if err != nil {
		return entry, err
	}
	var wallNanos int64
	if err := read(&wallNanos); err != nil {
		return entry, err
	}
	entry.WallClock = time.Unix(0, wallNanos).UTC()
	entry.CollectionID, err = readString()
	if err != nil {
		return entry, err
	}
	entry.VectorID, err = readString()
	if err != nil {
		return entry, err
	}

	var hasQuantized uint8
	if err := read(&hasQuantized); err != nil {
		return entry, err
	}
	if hasQuantized == 1 {
		mse, err := readBytes()
		if err != nil {
			return entry, err
		}
		signs, err := readBytes()
		if err != nil {
			return entry, err
		}
		var resNormBits uint32
		if err := read(&resNormBits); err != nil {
			return entry, err
		}
		norm := float32(1) // v5 default: unit norm (legacy cosine semantics)
		if hasNorm {
			var normBits uint32
			if err := read(&normBits); err != nil {
				return entry, err
			}
			norm = math.Float32frombits(normBits)
			// m1: reject NaN/Inf/negative, matching turboquant's own wire codec
			// (wire.go:144). A negative norm silently inverts ScoreUpperBound into
			// a lower bound and drops real top-k members.
			if math.IsNaN(float64(norm)) || math.IsInf(float64(norm), 0) || norm < 0 {
				return entry, fmt.Errorf("wal: invalid norm %v", norm)
			}
		}
		entry.Quantized = &QuantizedVector{
			MSE:     append([]byte(nil), mse...),
			Signs:   append([]byte(nil), signs...),
			ResNorm: math.Float32frombits(resNormBits),
			Norm:    norm,
		}
	}
	var dim uint32
	if err := read(&dim); err != nil {
		return entry, err
	}
	entry.Dim = int(dim)
	// RawHash block.
	var hasRawHash uint8
	if err := read(&hasRawHash); err != nil {
		return entry, err
	}
	if hasRawHash == 1 {
		rawHash := make([]byte, 32)
		if _, err := io.ReadFull(mr, rawHash); err != nil {
			return entry, err
		}
		entry.RawHash = rawHash
	}
	// Sparse block.
	var hasSparse uint8
	if err := read(&hasSparse); err != nil {
		return entry, err
	}
	if hasSparse == 1 {
		count, err := readCount("sparse")
		if err != nil {
			return entry, err
		}
		sparse := &SparseBlock{
			Indices: make([]uint32, count),
			Values:  make([]float32, count),
		}
		for i := range sparse.Indices {
			if err := read(&sparse.Indices[i]); err != nil {
				return entry, err
			}
		}
		for i := range sparse.Values {
			var bits uint32
			if err := read(&bits); err != nil {
				return entry, err
			}
			sparse.Values[i] = math.Float32frombits(bits)
		}
		entry.Sparse = sparse
	}
	childCount, err := readCount("children")
	if err != nil {
		return entry, err
	}
	entry.Children = make([]ChildVector, 0, childCount)
	for range childCount {
		child := ChildVector{}
		child.ID, err = readString()
		if err != nil {
			return entry, err
		}
		var hasQuantized uint8
		if err := read(&hasQuantized); err != nil {
			return entry, err
		}
		if hasQuantized == 1 {
			mse, err := readBytes()
			if err != nil {
				return entry, err
			}
			signs, err := readBytes()
			if err != nil {
				return entry, err
			}
			var resNormBits uint32
			if err := read(&resNormBits); err != nil {
				return entry, err
			}
			childNorm := float32(1) // v5 default: unit norm (legacy cosine semantics)
			if hasNorm {
				var normBits uint32
				if err := read(&normBits); err != nil {
					return entry, err
				}
				childNorm = math.Float32frombits(normBits)
				// m1: reject NaN/Inf/negative, matching turboquant's own wire codec
				// (wire.go:144). A negative norm silently inverts ScoreUpperBound
				// into a lower bound and drops real top-k members.
				if math.IsNaN(float64(childNorm)) || math.IsInf(float64(childNorm), 0) || childNorm < 0 {
					return entry, fmt.Errorf("wal: invalid child norm %v", childNorm)
				}
			}
			child.Quantized = &QuantizedVector{
				MSE:     append([]byte(nil), mse...),
				Signs:   append([]byte(nil), signs...),
				ResNorm: math.Float32frombits(resNormBits),
				Norm:    childNorm,
			}
		}
		var childDim uint32
		if err := read(&childDim); err != nil {
			return entry, err
		}
		child.Dim = int(childDim)
		child.Text, err = readString()
		if err != nil {
			return entry, err
		}
		metaJSON, err := readBytes()
		if err != nil {
			return entry, err
		}
		if len(metaJSON) > 0 {
			if err := json.Unmarshal(metaJSON, &child.Metadata); err != nil {
				return entry, err
			}
		}
		entry.Children = append(entry.Children, child)
	}
	entry.Text, err = readString()
	if err != nil {
		return entry, err
	}

	metaJSON, err := readBytes()
	if err != nil {
		return entry, err
	}
	if len(metaJSON) > 0 {
		if err := json.Unmarshal(metaJSON, &entry.Metadata); err != nil {
			return entry, err
		}
	}

	computed := h.Sum32()
	var stored uint32
	if err := binary.Read(r, binary.LittleEndian, &stored); err != nil {
		return entry, err
	}
	if computed != stored {
		return entry, fmt.Errorf("wal: crc mismatch: computed %x, stored %x", computed, stored)
	}
	return entry, nil
}

// countingReader counts the bytes consumed from the underlying reader so the
// WAL reader can distinguish a clean frame boundary from a truncated tail.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// readEntryCounting reads one entry and classifies a read failure:
//   - clean tail (io.EOF with nothing consumed) is mapped to io.EOF,
//   - a truncated trailing frame (short read after the first byte) is mapped to
//     io.ErrUnexpectedEOF,
//   - any other failure (CRC mismatch / bad magic / format) is interior
//     corruption and surfaces as-is.
func readEntryCounting(r io.Reader) (Entry, int64, error) {
	cr := &countingReader{r: r}
	entry, err := ReadEntry(cr)
	if err != nil {
		if errors.Is(err, io.EOF) && cr.n == 0 {
			return entry, 0, io.EOF // clean tail
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return entry, cr.n, io.ErrUnexpectedEOF // truncated tail
		}
		return entry, cr.n, err // CRC / magic / format = interior corruption
	}
	return entry, cr.n, nil
}
