package corkscrewdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
)

const (
	hnswMagic   = uint32(0x484E5357) // HNSW
	hnswVersion = uint8(1)
)

func saveHNSWFile(path string, h *hnswIndex) error {
	if h == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := marshalHNSWFile(h)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func marshalHNSWFile(h *hnswIndex) ([]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var buf bytes.Buffer
	cw := crc32.NewIEEE()
	mw := io.MultiWriter(&buf, cw)

	write := func(value any) error {
		return binary.Write(mw, binary.LittleEndian, value)
	}

	// Header.
	if err := write(hnswMagic); err != nil {
		return nil, err
	}
	if err := write(hnswVersion); err != nil {
		return nil, err
	}

	// Params.
	if err := write(int32(h.params.M)); err != nil {
		return nil, err
	}
	if err := write(int32(h.params.EfConstruction)); err != nil {
		return nil, err
	}
	if err := write(int32(h.params.EfSearch)); err != nil {
		return nil, err
	}

	// Graph metadata.
	if err := write(int32(h.maxLevel)); err != nil {
		return nil, err
	}
	if err := write(int32(h.entryNode)); err != nil {
		return nil, err
	}

	// Number of layers.
	if err := write(int32(len(h.layers))); err != nil {
		return nil, err
	}

	for _, layer := range h.layers {
		// Number of nodes in this layer.
		if err := write(int32(len(layer))); err != nil {
			return nil, err
		}
		for _, neighbors := range layer {
			// Number of neighbors for this node.
			if err := write(int32(len(neighbors))); err != nil {
				return nil, err
			}
			for _, ni := range neighbors {
				if err := write(ni); err != nil {
					return nil, err
				}
			}
		}
	}

	// CRC32 checksum (written directly to buf, not through cw).
	if err := binary.Write(&buf, binary.LittleEndian, cw.Sum32()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func loadHNSWFile(path string, flat *index, params hnswParams) (*hnswIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	cw := crc32.NewIEEE()
	mr := io.TeeReader(file, cw)
	read := func(value any) error {
		return binary.Read(mr, binary.LittleEndian, value)
	}

	var magic uint32
	if err := read(&magic); err != nil {
		return nil, err
	}
	if magic != hnswMagic {
		return nil, fmt.Errorf("corkscrewdb: invalid HNSW magic %x", magic)
	}

	var version uint8
	if err := read(&version); err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, fmt.Errorf("corkscrewdb: unsupported HNSW version %d", version)
	}

	var m, efConstruction, efSearch int32
	if err := read(&m); err != nil {
		return nil, err
	}
	if err := read(&efConstruction); err != nil {
		return nil, err
	}
	if err := read(&efSearch); err != nil {
		return nil, err
	}

	var maxLevel, entryNode int32
	if err := read(&maxLevel); err != nil {
		return nil, err
	}
	if err := read(&entryNode); err != nil {
		return nil, err
	}

	var numLayers int32
	if err := read(&numLayers); err != nil {
		return nil, err
	}

	layers := make([][][]int32, numLayers)
	for l := int32(0); l < numLayers; l++ {
		var numNodes int32
		if err := read(&numNodes); err != nil {
			return nil, err
		}
		layers[l] = make([][]int32, numNodes)
		for n := int32(0); n < numNodes; n++ {
			var numNeighbors int32
			if err := read(&numNeighbors); err != nil {
				return nil, err
			}
			if numNeighbors > 0 {
				neighbors := make([]int32, numNeighbors)
				for k := int32(0); k < numNeighbors; k++ {
					if err := read(&neighbors[k]); err != nil {
						return nil, err
					}
				}
				layers[l][n] = neighbors
			}
		}
	}

	computed := cw.Sum32()
	var stored uint32
	if err := binary.Read(file, binary.LittleEndian, &stored); err != nil {
		return nil, err
	}
	if computed != stored {
		return nil, fmt.Errorf("corkscrewdb: HNSW crc mismatch: computed %x, stored %x", computed, stored)
	}

	// Use stored params if they differ from defaults, preferring the file's values.
	loadedParams := hnswParams{
		M:              int(m),
		EfConstruction: int(efConstruction),
		EfSearch:       int(efSearch),
	}

	return &hnswIndex{
		flat:      flat,
		layers:    layers,
		maxLevel:  int(maxLevel),
		entryNode: int(entryNode),
		params:    loadedParams,
		rng:       rand.New(rand.NewSource(flat.quantizer.Seed())),
	}, nil
}
