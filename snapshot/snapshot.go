package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	snapshotMagic   = uint32(0x43534442)
	snapshotVersion = uint8(5)

	childEncodingLegacy                  = uint8(0)
	childEncodingCompactQuantizedOrdinal = uint8(1)
)

// Data is one collection snapshot.
type Data struct {
	Collection string
	BitWidth   int
	Seed       int64
	Dim        int
	Storage    string
	MaxLamport uint64
	CreatedAt  time.Time
	Records    []Record
}

// Record is one vector ID plus full version history.
type Record struct {
	ID       string
	Versions []Version
}

// Version is one immutable version stored in a snapshot.
type Version struct {
	Embedding    []float32
	Quantized    *QuantizedVector
	Children     []ChildVector
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
	Tombstone    bool
}

// QuantizedVector stores a TurboQuant inner-product payload.
type QuantizedVector struct {
	MSE     []byte
	Signs   []byte
	ResNorm float32
}

// ChildVector stores one packed child vector inside a parent snapshot version.
type ChildVector struct {
	ID        string
	Embedding []float32
	Quantized *QuantizedVector
	Dim       int
	Text      string
	Metadata  map[string]string
}

func WriteFile(path string, data Data) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	payload, err := marshal(data)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanupTmp = false
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := dirFile.Sync(); err != nil {
		_ = dirFile.Close()
		return err
	}
	return dirFile.Close()
}

func marshal(data Data) ([]byte, error) {
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

	if err := write(snapshotMagic); err != nil {
		return nil, err
	}
	if err := write(snapshotVersion); err != nil {
		return nil, err
	}
	if err := writeString(data.Collection); err != nil {
		return nil, err
	}
	if err := write(int32(data.BitWidth)); err != nil {
		return nil, err
	}
	if err := write(data.Seed); err != nil {
		return nil, err
	}
	if err := write(uint32(data.Dim)); err != nil {
		return nil, err
	}
	if err := writeString(data.Storage); err != nil {
		return nil, err
	}
	if err := write(data.MaxLamport); err != nil {
		return nil, err
	}
	if err := write(data.CreatedAt.UnixNano()); err != nil {
		return nil, err
	}
	if err := write(uint32(len(data.Records))); err != nil {
		return nil, err
	}
	for _, record := range data.Records {
		if err := writeString(record.ID); err != nil {
			return nil, err
		}
		if err := write(uint32(len(record.Versions))); err != nil {
			return nil, err
		}
		for _, version := range record.Versions {
			embedding := make([]byte, len(version.Embedding)*4)
			for i, value := range version.Embedding {
				binary.LittleEndian.PutUint32(embedding[i*4:], math.Float32bits(value))
			}
			if err := writeBytes(embedding); err != nil {
				return nil, err
			}
			if version.Quantized != nil {
				if err := write(uint8(1)); err != nil {
					return nil, err
				}
				if err := writeBytes(version.Quantized.MSE); err != nil {
					return nil, err
				}
				if err := writeBytes(version.Quantized.Signs); err != nil {
					return nil, err
				}
				if err := write(math.Float32bits(version.Quantized.ResNorm)); err != nil {
					return nil, err
				}
			} else if err := write(uint8(0)); err != nil {
				return nil, err
			}
			childEncoding := childEncodingLegacy
			if canUseCompactQuantizedOrdinalChildren(version.Children) {
				childEncoding = childEncodingCompactQuantizedOrdinal
			}
			if err := write(uint32(len(version.Children))); err != nil {
				return nil, err
			}
			if err := write(childEncoding); err != nil {
				return nil, err
			}
			if childEncoding == childEncodingCompactQuantizedOrdinal {
				if err := writeCompactQuantizedOrdinalChildren(mw, write, version.Children); err != nil {
					return nil, err
				}
			} else if err := writeLegacyChildren(write, writeBytes, writeString, version.Children); err != nil {
				return nil, err
			}
			if err := writeString(version.Text); err != nil {
				return nil, err
			}
			metaJSON, err := json.Marshal(version.Metadata)
			if err != nil {
				return nil, err
			}
			if err := writeBytes(metaJSON); err != nil {
				return nil, err
			}
			if err := write(version.LamportClock); err != nil {
				return nil, err
			}
			if err := writeString(version.ActorID); err != nil {
				return nil, err
			}
			if err := write(version.WallClock.UnixNano()); err != nil {
				return nil, err
			}
			var tombstone uint8
			if version.Tombstone {
				tombstone = 1
			}
			if err := write(tombstone); err != nil {
				return nil, err
			}
		}
	}
	if err := binary.Write(&buf, binary.LittleEndian, h.Sum32()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeLegacyChildren(
	write func(any) error,
	writeBytes func([]byte) error,
	writeString func(string) error,
	children []ChildVector,
) error {
	for _, child := range children {
		if err := writeString(child.ID); err != nil {
			return err
		}
		childEmbedding := make([]byte, len(child.Embedding)*4)
		for i, value := range child.Embedding {
			binary.LittleEndian.PutUint32(childEmbedding[i*4:], math.Float32bits(value))
		}
		if err := writeBytes(childEmbedding); err != nil {
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
	return nil
}

func canUseCompactQuantizedOrdinalChildren(children []ChildVector) bool {
	if len(children) == 0 {
		return false
	}
	first := children[0]
	if first.Quantized == nil || first.Dim <= 0 || len(first.Embedding) != 0 || first.Text != "" || len(first.Metadata) != 0 {
		return false
	}
	mseLen := len(first.Quantized.MSE)
	signsLen := len(first.Quantized.Signs)
	if mseLen == 0 || signsLen == 0 {
		return false
	}
	for i, child := range children {
		if child.ID != strconv.Itoa(i) || child.Quantized == nil || child.Dim != first.Dim {
			return false
		}
		if len(child.Embedding) != 0 || child.Text != "" || len(child.Metadata) != 0 {
			return false
		}
		if len(child.Quantized.MSE) != mseLen || len(child.Quantized.Signs) != signsLen {
			return false
		}
	}
	return true
}

func writeCompactQuantizedOrdinalChildren(
	w io.Writer,
	write func(any) error,
	children []ChildVector,
) error {
	dim := uint32(children[0].Dim)
	mseLen := uint32(len(children[0].Quantized.MSE))
	signsLen := uint32(len(children[0].Quantized.Signs))
	if err := write(dim); err != nil {
		return err
	}
	if err := write(mseLen); err != nil {
		return err
	}
	if err := write(signsLen); err != nil {
		return err
	}
	for _, child := range children {
		if _, err := w.Write(child.Quantized.MSE); err != nil {
			return err
		}
	}
	for _, child := range children {
		if _, err := w.Write(child.Quantized.Signs); err != nil {
			return err
		}
	}
	for _, child := range children {
		if err := write(math.Float32bits(child.Quantized.ResNorm)); err != nil {
			return err
		}
	}
	return nil
}
