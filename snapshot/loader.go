package snapshot

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func LoadFile(path string) (Data, error) {
	file, err := os.Open(path)
	if err != nil {
		return Data{}, err
	}
	defer file.Close()
	return read(file)
}

func FindLatestFile(dir string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "snapshot-*.csdb"))
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", nil
	}
	sort.Strings(matches)
	return matches[len(matches)-1], nil
}

func read(r io.Reader) (Data, error) {
	h := crc32.NewIEEE()
	mr := io.TeeReader(r, h)
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

	var data Data
	var magic uint32
	if err := read(&magic); err != nil {
		return data, err
	}
	if magic != snapshotMagic {
		return data, fmt.Errorf("snapshot: invalid magic %x", magic)
	}
	var version uint8
	if err := read(&version); err != nil {
		return data, err
	}
	if version != 1 && version != 2 {
		return data, fmt.Errorf("snapshot: unsupported version %d", version)
	}

	var err error
	data.Collection, err = readString()
	if err != nil {
		return data, err
	}
	var bitWidth int32
	if err := read(&bitWidth); err != nil {
		return data, err
	}
	data.BitWidth = int(bitWidth)
	if err := read(&data.Seed); err != nil {
		return data, err
	}
	var dim uint32
	if err := read(&dim); err != nil {
		return data, err
	}
	data.Dim = int(dim)
	if err := read(&data.MaxLamport); err != nil {
		return data, err
	}
	var createdAt int64
	if err := read(&createdAt); err != nil {
		return data, err
	}
	data.CreatedAt = time.Unix(0, createdAt).UTC()

	var recordCount uint32
	if err := read(&recordCount); err != nil {
		return data, err
	}
	data.Records = make([]Record, 0, recordCount)
	for range recordCount {
		id, err := readString()
		if err != nil {
			return data, err
		}
		var versionCount uint32
		if err := read(&versionCount); err != nil {
			return data, err
		}
		record := Record{ID: id, Versions: make([]Version, 0, versionCount)}
		for range versionCount {
			embeddingBytes, err := readBytes()
			if err != nil {
				return data, err
			}
			if len(embeddingBytes)%4 != 0 {
				return data, fmt.Errorf("snapshot: invalid embedding byte length %d", len(embeddingBytes))
			}
			version := Version{Embedding: make([]float32, len(embeddingBytes)/4)}
			for i := range version.Embedding {
				version.Embedding[i] = math.Float32frombits(binary.LittleEndian.Uint32(embeddingBytes[i*4:]))
			}
			version.Text, err = readString()
			if err != nil {
				return data, err
			}
			metaJSON, err := readBytes()
			if err != nil {
				return data, err
			}
			if len(metaJSON) > 0 {
				if err := json.Unmarshal(metaJSON, &version.Metadata); err != nil {
					return data, err
				}
			}
			if err := read(&version.LamportClock); err != nil {
				return data, err
			}
			version.ActorID, err = readString()
			if err != nil {
				return data, err
			}
			var wallClock int64
			if err := read(&wallClock); err != nil {
				return data, err
			}
			version.WallClock = time.Unix(0, wallClock).UTC()
			var tombstone uint8
			if err := read(&tombstone); err != nil {
				return data, err
			}
			version.Tombstone = tombstone == 1
			record.Versions = append(record.Versions, version)
		}
		data.Records = append(data.Records, record)
	}

	computed := h.Sum32()
	var stored uint32
	if err := binary.Read(r, binary.LittleEndian, &stored); err != nil {
		return data, err
	}
	if computed != stored {
		return data, fmt.Errorf("snapshot: crc mismatch: computed %x, stored %x", computed, stored)
	}
	return data, nil
}
