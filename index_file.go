package corkscrewdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"m31labs.dev/turboquant"
)

const (
	indexMagic   = uint32(0x54514931) // TQI1
	indexVersion = uint8(3)

	// maxIndexFieldBytes caps any single length-prefixed field read from a .tqi
	// file.  No legitimate field can exceed a generous 64 MiB ceiling (matching
	// the rawstore segment cap); a higher declared length is corruption.
	maxIndexFieldBytes = uint64(64 << 20)
)

func saveIndexFile(path string, idx *index, maxLamport uint64, rawStore, sparse bool) error {
	if idx == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := marshalIndexFile(idx, maxLamport, rawStore, sparse)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func marshalIndexFile(idx *index, maxLamport uint64, rawStore, sparse bool) ([]byte, error) {
	var buf bytes.Buffer
	h := crc32.NewIEEE()
	mw := io.MultiWriter(&buf, h)
	write := func(value any) error {
		return binary.Write(mw, binary.LittleEndian, value)
	}
	writeBytes := func(raw []byte) error {
		if err := write(uint32(len(raw))); err != nil {
			return err
		}
		_, err := mw.Write(raw)
		return err
	}
	writeString := func(s string) error {
		return writeBytes([]byte(s))
	}

	entries := idx.snapshotEntries()
	if err := write(indexMagic); err != nil {
		return nil, err
	}
	if err := write(indexVersion); err != nil {
		return nil, err
	}
	if err := write(uint32(idx.Dim())); err != nil {
		return nil, err
	}
	if err := write(uint32(idx.BitWidth())); err != nil {
		return nil, err
	}
	if err := write(idx.quantizer.Seed()); err != nil {
		return nil, err
	}
	if err := write(maxLamport); err != nil {
		return nil, err
	}
	var rawStoreByte, sparseByte uint8
	if rawStore {
		rawStoreByte = 1
	}
	if sparse {
		sparseByte = 1
	}
	if err := write(rawStoreByte); err != nil {
		return nil, err
	}
	if err := write(sparseByte); err != nil {
		return nil, err
	}
	if err := write(uint32(len(entries))); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if err := writeString(entry.id); err != nil {
			return nil, err
		}
		if err := writeBytes(entry.qv.MSE); err != nil {
			return nil, err
		}
		if err := writeBytes(entry.qv.Signs); err != nil {
			return nil, err
		}
		if err := write(math.Float32bits(entry.qv.ResNorm)); err != nil {
			return nil, err
		}
		if err := writeString(entry.text); err != nil {
			return nil, err
		}
		metaJSON, err := json.Marshal(entry.metadata)
		if err != nil {
			return nil, err
		}
		if err := writeBytes(metaJSON); err != nil {
			return nil, err
		}
		if err := write(entry.version); err != nil {
			return nil, err
		}
	}
	if err := binary.Write(&buf, binary.LittleEndian, h.Sum32()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func loadIndexFile(path string) (*index, uint64, error) {
	idx, maxLamport, _, _, err := loadIndexFileV3(path)
	return idx, maxLamport, err
}

// loadIndexFileV3 loads a v3 .tqi file and also returns the rawStore and sparse header flags.
func loadIndexFileV3(path string) (*index, uint64, bool, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, false, false, err
	}
	defer file.Close()

	h := crc32.NewIEEE()
	mr := io.TeeReader(file, h)
	read := func(value any) error {
		return binary.Read(mr, binary.LittleEndian, value)
	}
	readBytes := func() ([]byte, error) {
		var length uint32
		if err := read(&length); err != nil {
			return nil, err
		}
		// Bound the declared length before allocating so an inflated length prefix
		// (corruption or crafted input) surfaces as a typed error rather than an
		// OOM-inducing multi-GiB allocation.
		if uint64(length) > maxIndexFieldBytes {
			return nil, fmt.Errorf("corkscrewdb: index field length too large %d (max %d)", length, maxIndexFieldBytes)
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

	var magic uint32
	if err := read(&magic); err != nil {
		return nil, 0, false, false, err
	}
	if magic != indexMagic {
		return nil, 0, false, false, fmt.Errorf("corkscrewdb: invalid index magic %x", magic)
	}
	var version uint8
	if err := read(&version); err != nil {
		return nil, 0, false, false, err
	}
	if version != 3 {
		return nil, 0, false, false, fmt.Errorf("%w: tqi version %d", ErrFormatTooOld, version)
	}
	var dim uint32
	if err := read(&dim); err != nil {
		return nil, 0, false, false, err
	}
	// Bound dim before calling newIndex.  turboquant's Gaussian projection matrix
	// is O(dim²): at dim=2048 initialisation takes ~200 ms; beyond that it
	// blocks for seconds.  No currently-supported embedding model exceeds 2048;
	// reject larger values as corruption to prevent DoS via crafted .tqi files.
	const maxIndexDim = uint32(2048)
	if dim == 0 || dim > maxIndexDim {
		return nil, 0, false, false, fmt.Errorf("corkscrewdb: index dim out of range %d (max %d)", dim, maxIndexDim)
	}
	var bitWidth uint32
	if err := read(&bitWidth); err != nil {
		return nil, 0, false, false, err
	}
	// turboquant.validateIPBitWidth enforces [2,8]; reject outside that range
	// before calling newIndex to prevent a panic inside turboquant.
	if bitWidth < 2 || bitWidth > 8 {
		return nil, 0, false, false, fmt.Errorf("corkscrewdb: index bitWidth out of range %d (must be 2-8)", bitWidth)
	}
	var seed int64
	if err := read(&seed); err != nil {
		return nil, 0, false, false, err
	}
	var maxLamport uint64
	if err := read(&maxLamport); err != nil {
		return nil, 0, false, false, err
	}
	var rawStoreByte, sparseByte uint8
	if err := read(&rawStoreByte); err != nil {
		return nil, 0, false, false, err
	}
	if err := read(&sparseByte); err != nil {
		return nil, 0, false, false, err
	}
	var count uint32
	if err := read(&count); err != nil {
		return nil, 0, false, false, err
	}

	idx := newIndex(int(dim), int(bitWidth), seed)
	for range count {
		id, err := readString()
		if err != nil {
			return nil, 0, false, false, err
		}
		mse, err := readBytes()
		if err != nil {
			return nil, 0, false, false, err
		}
		signs, err := readBytes()
		if err != nil {
			return nil, 0, false, false, err
		}
		var resNormBits uint32
		if err := read(&resNormBits); err != nil {
			return nil, 0, false, false, err
		}
		text, err := readString()
		if err != nil {
			return nil, 0, false, false, err
		}
		metaJSON, err := readBytes()
		if err != nil {
			return nil, 0, false, false, err
		}
		var version uint64
		if err := read(&version); err != nil {
			return nil, 0, false, false, err
		}
		var meta map[string]string
		if len(metaJSON) > 0 {
			if err := json.Unmarshal(metaJSON, &meta); err != nil {
				return nil, 0, false, false, err
			}
		}
		qv := turboquant.IPQuantized{
			MSE:     append([]byte(nil), mse...),
			Signs:   append([]byte(nil), signs...),
			ResNorm: math.Float32frombits(resNormBits),
		}
		idx.addQuantized(id, qv, text, meta, version)
	}

	computed := h.Sum32()
	var stored uint32
	if err := binary.Read(file, binary.LittleEndian, &stored); err != nil {
		return nil, 0, false, false, err
	}
	if computed != stored {
		return nil, 0, false, false, fmt.Errorf("corkscrewdb: index crc mismatch: computed %x, stored %x", computed, stored)
	}
	return idx, maxLamport, rawStoreByte != 0, sparseByte != 0, nil
}
