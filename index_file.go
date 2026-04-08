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

	"github.com/odvcencio/turboquant"
)

const (
	indexMagic   = uint32(0x54514931) // TQI1
	indexVersion = uint8(2)
)

func saveIndexFile(path string, idx *index, maxLamport uint64) error {
	if idx == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := marshalIndexFile(idx, maxLamport)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func marshalIndexFile(idx *index, maxLamport uint64) ([]byte, error) {
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
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
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
		return nil, 0, err
	}
	if magic != indexMagic {
		return nil, 0, fmt.Errorf("corkscrewdb: invalid index magic %x", magic)
	}
	var version uint8
	if err := read(&version); err != nil {
		return nil, 0, err
	}
	if version != 1 && version != 2 {
		return nil, 0, fmt.Errorf("corkscrewdb: unsupported index version %d", version)
	}
	var dim uint32
	if err := read(&dim); err != nil {
		return nil, 0, err
	}
	var bitWidth uint32
	if err := read(&bitWidth); err != nil {
		return nil, 0, err
	}
	var seed int64
	if err := read(&seed); err != nil {
		return nil, 0, err
	}
	var maxLamport uint64
	if err := read(&maxLamport); err != nil {
		return nil, 0, err
	}
	var count uint32
	if err := read(&count); err != nil {
		return nil, 0, err
	}

	idx := newIndex(int(dim), int(bitWidth), seed)
	for range count {
		id, err := readString()
		if err != nil {
			return nil, 0, err
		}
		mse, err := readBytes()
		if err != nil {
			return nil, 0, err
		}
		signs, err := readBytes()
		if err != nil {
			return nil, 0, err
		}
		var resNormBits uint32
		if err := read(&resNormBits); err != nil {
			return nil, 0, err
		}
		text, err := readString()
		if err != nil {
			return nil, 0, err
		}
		metaJSON, err := readBytes()
		if err != nil {
			return nil, 0, err
		}
		var version uint64
		if err := read(&version); err != nil {
			return nil, 0, err
		}
		var meta map[string]string
		if len(metaJSON) > 0 {
			if err := json.Unmarshal(metaJSON, &meta); err != nil {
				return nil, 0, err
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
		return nil, 0, err
	}
	if computed != stored {
		return nil, 0, fmt.Errorf("corkscrewdb: index crc mismatch: computed %x, stored %x", computed, stored)
	}
	return idx, maxLamport, nil
}
