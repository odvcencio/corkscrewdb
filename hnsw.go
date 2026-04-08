package corkscrewdb

import (
	"container/heap"
	"math"
	"math/rand"
	"sync"

	"github.com/odvcencio/turboquant"
)

type hnswParams struct {
	M              int // max connections per layer
	EfConstruction int // build-time beam width
	EfSearch       int // query-time beam width
}

func defaultHNSWParams() hnswParams {
	return hnswParams{M: 16, EfConstruction: 200, EfSearch: 50}
}

// hnswIndex wraps a flat index with an HNSW graph for sub-linear search.
type hnswIndex struct {
	mu        sync.RWMutex
	flat      *index       // stores all vectors + quantized data
	layers    [][][]int32  // layers[level][node] = neighbor indices
	maxLevel  int
	entryNode int
	params    hnswParams
	rng       *rand.Rand
}

var _ indexer = (*hnswIndex)(nil)

func newHNSWIndex(dim, bitWidth int, seed int64, params hnswParams) *hnswIndex {
	if params.M <= 0 {
		params.M = 16
	}
	if params.EfConstruction <= 0 {
		params.EfConstruction = 200
	}
	if params.EfSearch <= 0 {
		params.EfSearch = 50
	}
	return &hnswIndex{
		flat:      newIndex(dim, bitWidth, seed),
		layers:    nil,
		maxLevel:  -1,
		entryNode: -1,
		params:    params,
		rng:       rand.New(rand.NewSource(seed)),
	}
}

func (h *hnswIndex) Dim() int      { return h.flat.Dim() }
func (h *hnswIndex) BitWidth() int { return h.flat.BitWidth() }

func (h *hnswIndex) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.flat.Len()
}

// randomLevel returns a random level for a new node using a geometric
// distribution with parameter 1/ln(M).
func (h *hnswIndex) randomLevel() int {
	ml := 1.0 / math.Log(float64(h.params.M))
	level := int(-math.Log(h.rng.Float64()+1e-18) * ml)
	if level < 0 {
		level = 0
	}
	return level
}

// maxNeighbors returns the max neighbor count for a given level.
// Level 0 gets 2*M connections; higher levels get M.
func (h *hnswIndex) maxNeighbors(level int) int {
	if level == 0 {
		return h.params.M * 2
	}
	return h.params.M
}

// ensureLayerSlots grows layers and per-layer slices so that layers[level][nodeIdx] is valid.
func (h *hnswIndex) ensureLayerSlots(level, nodeIdx int) {
	for len(h.layers) <= level {
		h.layers = append(h.layers, nil)
	}
	for len(h.layers[level]) <= nodeIdx {
		h.layers[level] = append(h.layers[level], nil)
	}
}

func (h *hnswIndex) Add(id string, vec []float32, text string, metadata map[string]string, version uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if this is an update to an existing entry.
	h.flat.mu.Lock()
	_, isUpdate := h.flat.idIndex[id]
	h.flat.mu.Unlock()

	if isUpdate {
		h.removeLocked(id)
	}

	// Add to flat index (handles quantization).
	h.flat.Add(id, vec, text, metadata, version)

	h.flat.mu.RLock()
	nodeIdx := h.flat.idIndex[id]
	h.flat.mu.RUnlock()

	nodeLevel := h.randomLevel()

	// Ensure all layers up to nodeLevel have a slot for this node.
	for level := 0; level <= nodeLevel; level++ {
		h.ensureLayerSlots(level, nodeIdx)
	}
	// Layer 0 always needs a slot.
	h.ensureLayerSlots(0, nodeIdx)

	if h.entryNode < 0 {
		h.entryNode = nodeIdx
		h.maxLevel = nodeLevel
		return
	}

	pq := h.flat.quantizer.PrepareQuery(vec)

	// Greedy traverse from top level down to nodeLevel+1.
	current := h.entryNode
	for level := h.maxLevel; level > nodeLevel; level-- {
		current = h.greedyClosest(current, pq, level)
	}

	// Insert at each level from min(nodeLevel, maxLevel) down to 0.
	topInsert := nodeLevel
	if topInsert > h.maxLevel {
		topInsert = h.maxLevel
	}
	for level := topInsert; level >= 0; level-- {
		neighbors := h.searchLayer(current, pq, h.params.EfConstruction, level)
		selected := h.selectNeighbors(neighbors, h.maxNeighbors(level))

		h.ensureLayerSlots(level, nodeIdx)
		h.layers[level][nodeIdx] = selected

		// Add bidirectional edges and prune if over limit.
		maxN := h.maxNeighbors(level)
		for _, neighbor := range selected {
			ni := int(neighbor)
			h.ensureLayerSlots(level, ni)
			h.layers[level][ni] = append(h.layers[level][ni], int32(nodeIdx))
			if len(h.layers[level][ni]) > maxN {
				h.layers[level][ni] = h.pruneNeighborsByNode(ni, h.layers[level][ni], maxN)
			}
		}

		if len(neighbors) > 0 {
			current = int(neighbors[0])
		}
	}

	if nodeLevel > h.maxLevel {
		h.maxLevel = nodeLevel
		h.entryNode = nodeIdx
	}
}

// greedyClosest walks level's graph from start, greedily moving to the
// neighbor with the highest inner product until no improvement.
func (h *hnswIndex) greedyClosest(start int, pq turboquant.PreparedQuery, level int) int {
	if level >= len(h.layers) || start >= len(h.layers[level]) {
		return start
	}
	h.flat.mu.RLock()
	defer h.flat.mu.RUnlock()

	current := start
	if current >= len(h.flat.entries) {
		return start
	}
	bestScore := h.flat.quantizer.InnerProductPrepared(h.flat.entries[current].qv, pq)
	for {
		improved := false
		for _, ni := range h.layers[level][current] {
			n := int(ni)
			if n >= len(h.flat.entries) {
				continue
			}
			score := h.flat.quantizer.InnerProductPrepared(h.flat.entries[n].qv, pq)
			if score > bestScore {
				bestScore = score
				current = n
				improved = true
			}
		}
		if !improved {
			break
		}
	}
	return current
}

// hnswCandidate is used internally by searchLayer and the candidate heaps.
type hnswCandidate struct {
	idx   int32
	score float32
}

// searchLayer runs a beam search on the given layer, returning up to ef
// candidate node indices sorted by descending similarity.
func (h *hnswIndex) searchLayer(entry int, pq turboquant.PreparedQuery, ef int, level int) []int32 {
	if level >= len(h.layers) {
		return nil
	}
	h.flat.mu.RLock()
	defer h.flat.mu.RUnlock()

	if entry >= len(h.flat.entries) {
		return nil
	}

	visited := make(map[int32]bool)
	visited[int32(entry)] = true

	entryScore := h.flat.quantizer.InnerProductPrepared(h.flat.entries[entry].qv, pq)

	// candidates: max-heap (best on top for expansion)
	candidates := &hnswMaxHeap{}
	heap.Push(candidates, hnswCandidate{int32(entry), entryScore})

	// results: min-heap (worst on top for pruning)
	results := &hnswMinHeap{}
	heap.Push(results, hnswCandidate{int32(entry), entryScore})

	for candidates.Len() > 0 {
		c := heap.Pop(candidates).(hnswCandidate)
		if results.Len() >= ef && c.score < (*results)[0].score {
			break
		}

		ni := int(c.idx)
		if ni >= len(h.layers[level]) {
			continue
		}
		for _, neighbor := range h.layers[level][ni] {
			if visited[neighbor] {
				continue
			}
			visited[neighbor] = true

			n := int(neighbor)
			if n >= len(h.flat.entries) {
				continue
			}
			score := h.flat.quantizer.InnerProductPrepared(h.flat.entries[n].qv, pq)

			if results.Len() < ef || score > (*results)[0].score {
				heap.Push(candidates, hnswCandidate{neighbor, score})
				heap.Push(results, hnswCandidate{neighbor, score})
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}

	out := make([]int32, results.Len())
	for i := len(out) - 1; i >= 0; i-- {
		c := heap.Pop(results).(hnswCandidate)
		out[i] = c.idx
	}
	return out
}

// selectNeighbors picks the top-maxN candidates (already sorted by descending score).
func (h *hnswIndex) selectNeighbors(candidates []int32, maxN int) []int32 {
	if len(candidates) <= maxN {
		result := make([]int32, len(candidates))
		copy(result, candidates)
		return result
	}
	result := make([]int32, maxN)
	copy(result, candidates[:maxN])
	return result
}

// pruneNeighborsByNode trims a neighbor list that has exceeded maxN back
// to maxN entries. We keep the most recent maxN connections because newer
// connections tend to link to newer parts of the graph, preserving global
// connectivity as the graph grows.
func (h *hnswIndex) pruneNeighborsByNode(nodeIdx int, neighbors []int32, maxN int) []int32 {
	if len(neighbors) <= maxN {
		return neighbors
	}
	// Keep the last maxN entries (the most recent connections).
	start := len(neighbors) - maxN
	result := make([]int32, maxN)
	copy(result, neighbors[start:])
	return result
}

func (h *hnswIndex) Remove(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.removeLocked(id)
}

func (h *hnswIndex) removeLocked(id string) bool {
	h.flat.mu.RLock()
	pos, ok := h.flat.idIndex[id]
	h.flat.mu.RUnlock()
	if !ok {
		return false
	}

	nodeIdx := int32(pos)

	// Remove all edges involving this node from all layers.
	for level := range h.layers {
		if int(nodeIdx) < len(h.layers[level]) {
			h.layers[level][nodeIdx] = nil
		}
		for i := range h.layers[level] {
			h.layers[level][i] = removeFromSlice(h.layers[level][i], nodeIdx)
		}
	}

	// The flat index uses swap-remove: the last entry moves into the removed
	// slot. We must remap graph references accordingly.
	h.flat.mu.RLock()
	lastIdx := len(h.flat.entries) - 1
	h.flat.mu.RUnlock()

	swappedFrom := int32(lastIdx)
	swappedTo := nodeIdx

	result := h.flat.Remove(id)

	if result && swappedFrom != swappedTo && lastIdx > 0 {
		for level := range h.layers {
			if int(swappedFrom) < len(h.layers[level]) {
				h.ensureLayerSlots(level, int(swappedTo))
				h.layers[level][swappedTo] = h.layers[level][swappedFrom]
				h.layers[level][swappedFrom] = nil
			}
			for i := range h.layers[level] {
				for j := range h.layers[level][i] {
					if h.layers[level][i][j] == swappedFrom {
						h.layers[level][i][j] = swappedTo
					}
				}
			}
		}
		if h.entryNode == int(swappedFrom) {
			h.entryNode = int(swappedTo)
		}
	}

	if h.entryNode == int(nodeIdx) && (swappedFrom == swappedTo || lastIdx == 0) {
		h.flat.mu.RLock()
		n := len(h.flat.entries)
		h.flat.mu.RUnlock()
		if n == 0 {
			h.entryNode = -1
			h.maxLevel = -1
		} else {
			h.entryNode = 0
		}
	}

	return result
}

func removeFromSlice(s []int32, val int32) []int32 {
	out := s[:0]
	for _, v := range s {
		if v != val {
			out = append(out, v)
		}
	}
	return out
}

func (h *hnswIndex) Search(query []float32, k int, filters []FilterOption) []SearchResult {
	if k <= 0 {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	h.flat.mu.RLock()
	n := len(h.flat.entries)
	h.flat.mu.RUnlock()

	if n == 0 || h.entryNode < 0 {
		return nil
	}

	// For very small datasets, fall back to flat scan.
	if n <= h.params.EfSearch {
		return h.flat.Search(query, k, filters)
	}

	pq := h.flat.quantizer.PrepareQuery(query)

	// Greedy descent from top level to level 1.
	current := h.entryNode
	for level := h.maxLevel; level >= 1; level-- {
		current = h.greedyClosest(current, pq, level)
	}

	// Beam search at level 0 with a wider ef to account for filtering.
	ef := h.params.EfSearch
	if len(filters) > 0 {
		ef *= 4
	}
	if ef < k {
		ef = k
	}

	candidates := h.searchLayer(current, pq, ef, 0)

	// Apply filters and collect top-k results.
	h.flat.mu.RLock()
	defer h.flat.mu.RUnlock()

	resultHeap := &searchHeap{}
	for _, ci := range candidates {
		idx := int(ci)
		if idx >= len(h.flat.entries) {
			continue
		}
		entry := h.flat.entries[idx]
		if !matchesFilters(entry.metadata, filters) {
			continue
		}
		score := h.flat.quantizer.InnerProductPrepared(entry.qv, pq)
		r := SearchResult{
			ID:       entry.id,
			Score:    score,
			Text:     entry.text,
			Metadata: cloneMetadata(entry.metadata),
			Version:  entry.version,
		}
		if resultHeap.Len() < k {
			heap.Push(resultHeap, r)
		} else if score > (*resultHeap)[0].Score {
			(*resultHeap)[0] = r
			heap.Fix(resultHeap, 0)
		}
	}

	results := make([]SearchResult, resultHeap.Len())
	copy(results, *resultHeap)
	sortSearchResults(results)
	return results
}

// hnswMinHeap is a min-heap on score (worst on top, for result set pruning).
type hnswMinHeap []hnswCandidate

func (h hnswMinHeap) Len() int           { return len(h) }
func (h hnswMinHeap) Less(i, j int) bool { return h[i].score < h[j].score }
func (h hnswMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *hnswMinHeap) Push(x any)        { *h = append(*h, x.(hnswCandidate)) }
func (h *hnswMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// hnswMaxHeap is a max-heap on score (best on top, for expanding candidates).
type hnswMaxHeap []hnswCandidate

func (h hnswMaxHeap) Len() int           { return len(h) }
func (h hnswMaxHeap) Less(i, j int) bool { return h[i].score > h[j].score }
func (h hnswMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *hnswMaxHeap) Push(x any)        { *h = append(*h, x.(hnswCandidate)) }
func (h *hnswMaxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
