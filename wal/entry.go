package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"
)

const (
	walMagic   = uint16(0x4357)
	walVersion = uint8(4)
)

const (
	EntryPut       = uint8(1)
	EntryTombstone = uint8(2)
)

// Entry is one durable collection mutation in the append-only WAL.
type Entry struct {
	Kind         uint8
	CollectionID string
	VectorID     string
	Embedding    []float32
	Quantized    *QuantizedVector
	Dim          int
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
}

// ChildVector stores one packed child vector inside a parent WAL entry.
type ChildVector struct {
	ID        string
	Embedding []float32
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

	embeddingBytes := make([]byte, len(e.Embedding)*4)
	for i, v := range e.Embedding {
		binary.LittleEndian.PutUint32(embeddingBytes[i*4:], math.Float32bits(v))
	}
	if err := writeBytes(embeddingBytes); err != nil {
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
	} else if err := write(uint8(0)); err != nil {
		return err
	}
	if err := write(uint32(e.Dim)); err != nil {
		return err
	}
	if err := write(uint32(len(e.Children))); err != nil {
		return err
	}
	for _, child := range e.Children {
		if err := writeString(child.ID); err != nil {
			return err
		}
		childEmbedding := make([]byte, len(child.Embedding)*4)
		for i, v := range child.Embedding {
			binary.LittleEndian.PutUint32(childEmbedding[i*4:], math.Float32bits(v))
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
	if version != 1 && version != 2 && version != 3 && version != 4 {
		return entry, fmt.Errorf("wal: unsupported version %d", version)
	}
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

	embeddingBytes, err := readBytes()
	if err != nil {
		return entry, err
	}
	if len(embeddingBytes)%4 != 0 {
		return entry, fmt.Errorf("wal: invalid embedding byte length %d", len(embeddingBytes))
	}
	entry.Embedding = make([]float32, len(embeddingBytes)/4)
	for i := range entry.Embedding {
		entry.Embedding[i] = math.Float32frombits(binary.LittleEndian.Uint32(embeddingBytes[i*4:]))
	}
	if version >= 3 {
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
			entry.Quantized = &QuantizedVector{
				MSE:     append([]byte(nil), mse...),
				Signs:   append([]byte(nil), signs...),
				ResNorm: math.Float32frombits(resNormBits),
			}
		}
		var dim uint32
		if err := read(&dim); err != nil {
			return entry, err
		}
		entry.Dim = int(dim)
	}
	if version >= 4 {
		var childCount uint32
		if err := read(&childCount); err != nil {
			return entry, err
		}
		entry.Children = make([]ChildVector, 0, childCount)
		for range childCount {
			child := ChildVector{}
			child.ID, err = readString()
			if err != nil {
				return entry, err
			}
			embeddingBytes, err := readBytes()
			if err != nil {
				return entry, err
			}
			if len(embeddingBytes)%4 != 0 {
				return entry, fmt.Errorf("wal: invalid child embedding byte length %d", len(embeddingBytes))
			}
			child.Embedding = make([]float32, len(embeddingBytes)/4)
			for i := range child.Embedding {
				child.Embedding[i] = math.Float32frombits(binary.LittleEndian.Uint32(embeddingBytes[i*4:]))
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
				child.Quantized = &QuantizedVector{
					MSE:     append([]byte(nil), mse...),
					Signs:   append([]byte(nil), signs...),
					ResNorm: math.Float32frombits(resNormBits),
				}
			}
			var dim uint32
			if err := read(&dim); err != nil {
				return entry, err
			}
			child.Dim = int(dim)
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
