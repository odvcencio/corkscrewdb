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
	hnswVersion = uint8(2)
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

	// Compact tombstoned slots out of the layers (D2). The remap is identical to
	// snapshotEntries() ordering, so the persisted positional layers line up with
	// the persisted node IDs. The runtime-only tombstone/free-list/inbound state is
	// NOT serialized (no format change).
	layers, entryNode, maxLevel, _ := h.compactedLayers()

	// Graph metadata.
	if err := write(int32(maxLevel)); err != nil {
		return nil, err
	}
	if err := write(int32(entryNode)); err != nil {
		return nil, err
	}

	// Node IDs: persist per-node IDs in the compacted live order.
	entries := h.flat.snapshotEntries()
	nodeCount := uint32(len(entries))
	if err := write(nodeCount); err != nil {
		return nil, err
	}
	for _, e := range entries {
		idBytes := []byte(e.id)
		if err := write(uint32(len(idBytes))); err != nil {
			return nil, err
		}
		if _, err := mw.Write(idBytes); err != nil {
			return nil, err
		}
	}

	// Number of layers.
	if err := write(int32(len(layers))); err != nil {
		return nil, err
	}

	for _, layer := range layers {
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

func loadHNSWFile(path string, flat *index, params HNSWParams) (*hnswIndex, error) {
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
	if version != 2 {
		return nil, fmt.Errorf("%w: hnsw version %d", ErrFormatTooOld, version)
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

	// Node IDs: read into the graph-owned identity map (D2). The count must match
	// the live flat index (which the cache loader rebuilds from live codes).
	var nodeCount uint32
	if err := read(&nodeCount); err != nil {
		return nil, err
	}
	if int(nodeCount) != flat.Len() {
		return nil, fmt.Errorf("corkscrewdb: hnsw node count mismatch: file has %d, flat index has %d", nodeCount, flat.Len())
	}
	nodeIDs := make([]string, nodeCount)
	for i := uint32(0); i < nodeCount; i++ {
		var idLen uint32
		if err := read(&idLen); err != nil {
			return nil, err
		}
		idBuf := make([]byte, idLen)
		if _, err := io.ReadFull(mr, idBuf); err != nil {
			return nil, err
		}
		nodeIDs[i] = string(idBuf)
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
	loadedParams := HNSWParams{
		M:              int(m),
		EfConstruction: int(efConstruction),
		EfSearch:       int(efSearch),
	}

	h := &hnswIndex{
		flat:      flat,
		layers:    layers,
		maxLevel:  int(maxLevel),
		entryNode: int(entryNode),
		params:    loadedParams,
		rng:       rand.New(rand.NewSource(flat.quantizer.Seed())),
		hubFactor: 1.5,
		idToNode:  make(map[string]int32, nodeCount),
	}

	// Rebuild graph-owned identity + runtime adjacency state (D2). All persisted
	// nodes are live (the save compacted tombstones out); free-list is empty.
	n := int(nodeCount)
	h.nodeToID = make([]string, n)
	h.tombstone = make([]bool, n)
	h.degree = make([]int32, n)
	h.inbound = make([][]int32, n)
	for i := 0; i < n; i++ {
		h.nodeToID[i] = nodeIDs[i]
		h.idToNode[nodeIDs[i]] = int32(i)
	}
	for level := range h.layers {
		for node := range h.layers[level] {
			for _, dst := range h.layers[level][node] {
				if int(dst) < n {
					h.inbound[dst] = append(h.inbound[dst], int32(node))
				}
			}
		}
	}
	if len(h.layers) > 0 {
		for node := range h.layers[0] {
			if node < n {
				h.degree[node] = int32(len(h.layers[0][node]))
			}
		}
	}
	return h, nil
}
