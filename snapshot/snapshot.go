package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	snapshotMagic   = uint32(0x43534442)
	snapshotVersion = uint8(1)
)

// Data is one collection snapshot.
type Data struct {
	Collection string
	BitWidth   int
	Seed       int64
	Dim        int
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
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
	Tombstone    bool
}

func WriteFile(path string, data Data) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := marshal(data)
	if err != nil {
		return err
	}
	return os.WriteFile(path, payload, 0o644)
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
				binary.LittleEndian.PutUint32(embedding[i*4:], mathFloat32bits(value))
			}
			if err := writeBytes(embedding); err != nil {
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
