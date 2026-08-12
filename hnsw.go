package corkscrewdb

import (
	"container/heap"
	"math"
	"math/rand"
	"sort"
	"sync"

	"m31labs.dev/turboquant"
)

func defaultHNSWParams() HNSWParams {
	return HNSWParams{M: 16, EfConstruction: 200, EfSearch: 50}
}

// hnswIndex wraps a flat index with an HNSW graph for sub-linear search.
type hnswIndex struct {
	mu        sync.RWMutex
	flat      *index      // stores all vectors + quantized data
	layers    [][][]int32 // layers[level][node] = neighbor indices
	maxLevel  int
	entryNode int
	params    HNSWParams
	rng       *rand.Rand
	degree    []int32 // layer-0 degree per node (LEANN hub detection)
	hubFactor float32 // a node is a hub when degree >= hubFactor*M

	// D2: tombstone delete + free-list + inbound-adjacency keyed on stable slots.
	freeList  []int32          // recycled (tombstoned) node slots, LIFO
	tombstone []bool           // parallel to nodes: logically removed
	idToNode  map[string]int32 // graph-owned identity (not borrowed from flat)
	nodeToID  []string         // node slot -> string ID
	inbound   [][]int32        // inbound[b] = nodes holding an out-edge to b (runtime only)
}

var _ indexer = (*hnswIndex)(nil)

func newHNSWIndex(dim, bitWidth int, seed int64, params HNSWParams) *hnswIndex {
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
		hubFactor: 1.5,
		idToNode:  make(map[string]int32),
	}
}

// ensureTombstoneSlot grows tombstone/nodeToID/inbound so index i is addressable.
func (h *hnswIndex) ensureTombstoneSlot(i int) {
	for len(h.tombstone) <= i {
		h.tombstone = append(h.tombstone, false)
	}
	for len(h.nodeToID) <= i {
		h.nodeToID = append(h.nodeToID, "")
	}
	for len(h.inbound) <= i {
		h.inbound = append(h.inbound, nil)
	}
}

func (h *hnswIndex) addInbound(b, src int32) {
	h.ensureTombstoneSlot(int(b))
	h.inbound[b] = append(h.inbound[b], src)
}

func (h *hnswIndex) dropInbound(b, src int32) {
	if int(b) >= len(h.inbound) {
		return
	}
	// Remove exactly ONE occurrence: the same (src,b) edge can exist at multiple
	// layers, so inbound[b] holds src once per layer-edge. Dropping all would wipe
	// a sibling-layer edge's back-ref and corrupt the inverse-of-out-edges invariant.
	h.inbound[b] = removeOneFromSlice(h.inbound[b], src)
}

// setNeighbors replaces node a's neighbor list at level, updating inbound back-refs
// for every added/removed edge so inbound stays the exact inverse of out-edges.
func (h *hnswIndex) setNeighbors(level int, a int32, list []int32) {
	h.ensureLayerSlots(level, int(a))
	old := h.layers[level][a]
	for _, dst := range old {
		h.dropInbound(dst, a)
	}
	h.layers[level][a] = list
	for _, dst := range list {
		h.addInbound(dst, a)
	}
	if level == 0 {
		h.recomputeDegree(int(a))
	}
}

// addEdge appends a single out-edge a->b at level and records the inbound back-ref.
func (h *hnswIndex) addEdge(level int, a, b int32) {
	h.ensureLayerSlots(level, int(a))
	h.layers[level][a] = append(h.layers[level][a], b)
	h.addInbound(b, a)
	if level == 0 {
		h.recomputeDegree(int(a))
	}
}

// pickLiveEntry scans from the top layer down for any non-tombstoned node present
// at that layer; returns -1 (and the caller resets maxLevel) if none.
func (h *hnswIndex) pickLiveEntry() int {
	for level := len(h.layers) - 1; level >= 0; level-- {
		for node := range h.layers[level] {
			if int(node) < len(h.tombstone) && h.tombstone[node] {
				continue
			}
			if _, live := h.idToNode[h.nodeID(int32(node))]; live {
				return node
			}
		}
	}
	return -1
}

func (h *hnswIndex) nodeID(node int32) string {
	if int(node) < len(h.nodeToID) {
		return h.nodeToID[node]
	}
	return ""
}

func (h *hnswIndex) isTombstoned(node int) bool {
	return node < len(h.tombstone) && h.tombstone[node]
}

// scrubInbound removes all stale inbound edges into node (O(in-degree)) before the
// slot becomes live again — prevents phantom edges the heuristic never built.
func (h *hnswIndex) scrubInbound(node int32) {
	if int(node) >= len(h.inbound) {
		h.ensureTombstoneSlot(int(node))
		return
	}
	for _, src := range h.inbound[node] {
		for level := range h.layers {
			if int(src) < len(h.layers[level]) {
				h.layers[level][src] = removeFromSlice(h.layers[level][src], node)
			}
		}
		if int(src) < len(h.degree) && len(h.layers) > 0 && int(src) < len(h.layers[0]) {
			h.degree[src] = int32(len(h.layers[0][src]))
		}
	}
	h.inbound[node] = h.inbound[node][:0]
	h.tombstone[node] = false
	h.nodeToID[node] = "" // reassigned by caller
}

// allocSlot returns a slot for a new node: a scrubbed recycled slot from the
// free-list (O(in-degree) scrub) or a brand-new appended slot.
func (h *hnswIndex) allocSlot() int32 {
	if len(h.freeList) > 0 {
		node := h.freeList[len(h.freeList)-1]
		h.freeList = h.freeList[:len(h.freeList)-1]
		h.scrubInbound(node)
		return node
	}
	h.flat.mu.RLock()
	node := int32(len(h.flat.entries))
	h.flat.mu.RUnlock()
	h.ensureTombstoneSlot(int(node))
	return node
}

// ensureDegreeSlot grows the degree slice so that degree[nodeIdx] is addressable.
func (h *hnswIndex) ensureDegreeSlot(nodeIdx int) {
	for len(h.degree) <= nodeIdx {
		h.degree = append(h.degree, 0)
	}
}

// recomputeDegree resets a node's layer-0 degree from its current neighbor list.
func (h *hnswIndex) recomputeDegree(nodeIdx int) {
	h.ensureDegreeSlot(nodeIdx)
	if len(h.layers) > 0 && nodeIdx < len(h.layers[0]) {
		h.degree[nodeIdx] = int32(len(h.layers[0][nodeIdx]))
	} else {
		h.degree[nodeIdx] = 0
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

	qv := h.flat.quantizer.Quantize(vec)
	h.addQuantizedLocked(id, qv, text, metadata, version, vec)
}

// AddQuantized inserts a node from stored codes only (no raw vector). The
// graph-positioning query is reconstructed from the codes via Dequantize, so
// construction never touches the raw store. Traversal already scores neighbors
// on codes, so graph quality is unchanged versus the raw path.
func (h *hnswIndex) AddQuantized(id string, qv turboquant.IPQuantized, text string, metadata map[string]string, version uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.addQuantizedLocked(id, qv, text, metadata, version, nil)
}

// addQuantizedLocked is the shared insert path. The GRAPH owns slot assignment:
// updates tombstone the old slot (so the update also flows through the scrub
// path), then a fresh/recycled slot is allocated, the flat row written at that
// slot via putAt, and the node wired into the graph. queryVec, when non-nil, is
// the raw positioning vector; otherwise it is reconstructed from the codes.
func (h *hnswIndex) addQuantizedLocked(id string, qv turboquant.IPQuantized, text string, metadata map[string]string, version uint64, queryVec []float32) {
	if _, isUpdate := h.idToNode[id]; isUpdate {
		h.removeLocked(id)
	}

	node := h.allocSlot()
	h.flat.putAt(int(node), id, qv, text, metadata, version)

	h.ensureTombstoneSlot(int(node))
	h.idToNode[id] = node
	h.nodeToID[node] = id
	h.tombstone[node] = false

	if queryVec == nil {
		queryVec = h.flat.quantizer.Dequantize(qv) // code-only positioning vector
	}
	h.insertNode(int(node), queryVec)
}

// insertNode wires nodeIdx into the graph using queryVec to position it.
func (h *hnswIndex) insertNode(nodeIdx int, queryVec []float32) {
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

	pq := h.flat.quantizer.PrepareQuery(queryVec)

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
		cands := h.searchLayerCandidates(current, pq, h.params.EfConstruction, level)
		selected := h.selectNeighborsHeuristic(cands, h.maxNeighbors(level))

		// Forward edges nodeIdx -> selected (setNeighbors maintains inbound+degree).
		h.setNeighbors(level, int32(nodeIdx), selected)

		// Add back-edges selected -> nodeIdx and prune if over limit.
		maxN := h.maxNeighbors(level)
		for _, neighbor := range selected {
			ni := int(neighbor)
			h.addEdge(level, neighbor, int32(nodeIdx))
			if len(h.layers[level][ni]) > maxN {
				pruned := h.pruneNeighborsHeuristic(ni, h.layers[level][ni], maxN)
				h.setNeighbors(level, neighbor, pruned)
			}
		}

		if len(cands) > 0 {
			current = int(cands[0].idx)
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
			if n >= len(h.flat.entries) || h.isTombstoned(n) {
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
			if n >= len(h.flat.entries) || h.isTombstoned(n) {
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

// searchLayerCandidates is searchLayer that returns (idx, score) candidates sorted
// by descending similarity, for use by the RNG selection heuristic.
func (h *hnswIndex) searchLayerCandidates(entry int, pq turboquant.PreparedQuery, ef int, level int) []hnswCandidate {
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

	candidates := &hnswMaxHeap{}
	heap.Push(candidates, hnswCandidate{int32(entry), entryScore})

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
			if n >= len(h.flat.entries) || h.isTombstoned(n) {
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

	out := make([]hnswCandidate, results.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(results).(hnswCandidate)
	}
	return out
}

// selectNeighborsHeuristic implements Malkov–Yashunin Algorithm 4: relative-
// neighborhood-graph pruning over candidates already sorted by descending sim
// to the inserted node q. A candidate c is dropped when some already-kept r is
// closer to c than q is to c (r "covers" c), keeping diverse/long-range edges.
// extendCandidates backfills from the discarded set to reach m so a node is never
// less connected than the old top-M for the same candidate set. Distances are
// code-only via InnerProductPrepared (no raw store).
//
// The selector reads h.flat.entries; callers do NOT hold h.flat.mu, so it takes
// the read lock internally.
func (h *hnswIndex) selectNeighborsHeuristic(candidates []hnswCandidate, m int) []int32 {
	if len(candidates) <= m {
		out := make([]int32, len(candidates))
		for i, c := range candidates {
			out[i] = c.idx
		}
		return out
	}
	h.flat.mu.RLock()
	defer h.flat.mu.RUnlock()

	type kept struct {
		idx int32
		pq  turboquant.PreparedQuery
	}
	keptList := make([]kept, 0, m)
	discarded := make([]int32, 0, len(candidates))
	queryCache := make(map[int32]turboquant.PreparedQuery, m)
	prepFor := func(idx int32) turboquant.PreparedQuery {
		if pq, ok := queryCache[idx]; ok {
			return pq
		}
		approx := h.flat.quantizer.Dequantize(h.flat.entries[int(idx)].qv)
		pq := h.flat.quantizer.PrepareQuery(approx)
		queryCache[idx] = pq
		return pq
	}
	for _, c := range candidates {
		if len(keptList) >= m {
			discarded = append(discarded, c.idx)
			continue
		}
		if int(c.idx) >= len(h.flat.entries) {
			continue
		}
		keep := true
		for _, r := range keptList {
			// sim(c, r): score c's codes against r's prepared query.
			simCR := h.flat.quantizer.InnerProductPrepared(h.flat.entries[int(c.idx)].qv, r.pq)
			if simCR > c.score { // r is closer to c than q is to c → c is redundant
				keep = false
				break
			}
		}
		if keep {
			keptList = append(keptList, kept{idx: c.idx, pq: prepFor(c.idx)})
		} else {
			discarded = append(discarded, c.idx)
		}
	}
	// extendCandidates: backfill from discarded (closest-first order preserved) to m.
	for _, idx := range discarded {
		if len(keptList) >= m {
			break
		}
		keptList = append(keptList, kept{idx: idx})
	}
	out := make([]int32, len(keptList))
	for i, k := range keptList {
		out[i] = k.idx
	}
	return out
}

// pruneNeighborsHeuristic re-prunes an overflowing layer-0 (or any-layer) neighbor
// list of nodeIdx back to maxN using the RNG heuristic against nodeIdx as the query,
// with LEANN hub protection: an edge to a hub neighbor is exempt from the RNG
// domination test and only dropped if the list still exceeds maxN after all non-hub
// edges are considered. Callers do NOT hold h.flat.mu.
//
// Note: hub protection is applied here (overflow re-prune) but NOT in
// selectNeighborsHeuristic (initial neighbor selection). This is a known
// recall/quality limitation: a hub that would be dominated at insert time may
// gain the edge only after the first overflow re-prune. Exact rescoring at
// query time compensates; graph recall is not materially affected.
func (h *hnswIndex) pruneNeighborsHeuristic(nodeIdx int, neighbors []int32, maxN int) []int32 {
	if len(neighbors) <= maxN {
		return neighbors
	}
	h.flat.mu.RLock()
	defer h.flat.mu.RUnlock()
	if nodeIdx >= len(h.flat.entries) {
		return neighbors
	}
	approx := h.flat.quantizer.Dequantize(h.flat.entries[nodeIdx].qv)
	pqNode := h.flat.quantizer.PrepareQuery(approx)
	// Score each neighbor vs nodeIdx, sort descending (closest first).
	cands := make([]hnswCandidate, 0, len(neighbors))
	for _, ni := range neighbors {
		if int(ni) >= len(h.flat.entries) {
			continue
		}
		s := h.flat.quantizer.InnerProductPrepared(h.flat.entries[int(ni)].qv, pqNode)
		cands = append(cands, hnswCandidate{idx: ni, score: s})
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].score != cands[j].score {
			return cands[i].score > cands[j].score
		}
		return cands[i].idx < cands[j].idx // deterministic tie-break
	})
	isHub := func(idx int32) bool {
		return int(idx) < len(h.degree) && float32(h.degree[idx]) >= h.hubFactor*float32(h.params.M)
	}
	type kept struct {
		idx int32
		pq  turboquant.PreparedQuery
	}
	keptList := make([]kept, 0, maxN)
	queryCache := make(map[int32]turboquant.PreparedQuery, maxN)
	prepFor := func(idx int32) turboquant.PreparedQuery {
		if pq, ok := queryCache[idx]; ok {
			return pq
		}
		a := h.flat.quantizer.Dequantize(h.flat.entries[int(idx)].qv)
		pq := h.flat.quantizer.PrepareQuery(a)
		queryCache[idx] = pq
		return pq
	}
	for _, c := range cands {
		if len(keptList) >= maxN {
			break
		}
		if isHub(c.idx) { // hub edges exempt from RNG domination
			keptList = append(keptList, kept{idx: c.idx, pq: prepFor(c.idx)})
			continue
		}
		keep := true
		for _, r := range keptList {
			simCR := h.flat.quantizer.InnerProductPrepared(h.flat.entries[int(c.idx)].qv, r.pq)
			if simCR > c.score {
				keep = false
				break
			}
		}
		if keep {
			keptList = append(keptList, kept{idx: c.idx, pq: prepFor(c.idx)})
		}
	}
	out := make([]int32, len(keptList))
	for i, k := range keptList {
		out[i] = k.idx
	}
	return out
}

func (h *hnswIndex) Remove(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.removeLocked(id)
}

// compactedLayers returns the graph layers with tombstoned slots dropped and all
// node indices remapped to the dense live ordering. The remap is identical to the
// one snapshotEntries() produces (iterate slots in order, skip dead), so the saved
// positional layers line up with the saved node IDs. Caller holds h.mu (read is
// fine for marshalling). hasTomb reports whether any compaction was needed.
func (h *hnswIndex) compactedLayers() (layers [][][]int32, entryNode int, maxLevel int, hasTomb bool) {
	// Build oldSlot -> newIndex over live slots in slot order.
	liveCount := 0
	maxSlot := 0
	if len(h.layers) > 0 {
		maxSlot = len(h.layers[0])
	}
	if len(h.tombstone) > maxSlot {
		maxSlot = len(h.tombstone)
	}
	remap := make([]int32, maxSlot)
	for i := range remap {
		remap[i] = -1
	}
	// Authority: a slot is live iff its flat row exists and is not dead. This
	// matches snapshotEntries() exactly so saved layers line up with saved IDs.
	h.flat.mu.RLock()
	flatLen := len(h.flat.entries)
	deadAt := func(slot int) bool {
		if slot >= flatLen {
			return true
		}
		return h.flat.entries[slot].dead
	}
	for slot := 0; slot < maxSlot; slot++ {
		if deadAt(slot) {
			hasTomb = true
			continue
		}
		remap[slot] = int32(liveCount)
		liveCount++
	}
	h.flat.mu.RUnlock()
	if !hasTomb {
		// No tombstones: return layers verbatim (no copy needed for marshalling).
		return h.layers, h.entryNode, h.maxLevel, false
	}

	out := make([][][]int32, len(h.layers))
	for level := range h.layers {
		// Determine the highest remapped index present at this level so the level
		// slice keeps its original (sparse, shorter-for-higher-levels) shape.
		maxIdx := -1
		for slot := range h.layers[level] {
			if ni := remap[slot]; ni >= 0 && int(ni) > maxIdx {
				maxIdx = int(ni)
			}
		}
		dense := make([][]int32, maxIdx+1)
		for slot := range h.layers[level] {
			ni := remap[slot]
			if ni < 0 {
				continue // tombstoned source dropped
			}
			var nbrs []int32
			for _, dst := range h.layers[level][slot] {
				if int(dst) < len(remap) && remap[dst] >= 0 {
					nbrs = append(nbrs, remap[dst])
				}
			}
			dense[ni] = nbrs
		}
		out[level] = dense
	}
	en := -1
	if h.entryNode >= 0 && h.entryNode < len(remap) {
		en = int(remap[h.entryNode])
	}
	ml := h.maxLevel
	if en < 0 || (ml >= 0 && (ml >= len(out) || en >= len(out[ml]))) {
		// Entry node was tombstoned or no longer present at maxLevel: pick the
		// topmost live node as the new entry, matching pickLiveEntry semantics in
		// the remapped space.
		en, ml = -1, -1
		for level := len(out) - 1; level >= 0 && en < 0; level-- {
			for ni := range out[level] {
				en = ni
				ml = level
				break
			}
		}
		if en < 0 {
			en, ml = 0, 0
		}
	}
	return out, en, ml, true
}

// removeLocked tombstones a node: O(out-degree) drop of its own out-edges and the
// matching inbound back-refs, then frees the slot. The flat row stays in place
// (skipped at traversal) and is overwritten when the slot is reused (scrubInbound
// removes the surviving stale inbound edges at that point). Inbound back-refs from
// other live sources to this slot are deliberately left until reuse-scrub.
func (h *hnswIndex) removeLocked(id string) bool {
	node, ok := h.idToNode[id]
	if !ok {
		return false
	}

	// O(out-degree): drop node's own out-edges and the matching inbound back-refs.
	for level := range h.layers {
		if int(node) >= len(h.layers[level]) {
			continue
		}
		for _, dst := range h.layers[level][node] {
			h.dropInbound(dst, node)
		}
		h.layers[level][node] = nil
	}
	if int(node) < len(h.degree) {
		h.degree[node] = 0
	}

	h.ensureTombstoneSlot(int(node))
	h.tombstone[node] = true
	delete(h.idToNode, id)
	h.flat.tombstoneAt(int(node))
	h.freeList = append(h.freeList, node)

	// Re-pick a live entry if we tombstoned the entry node.
	if h.entryNode == int(node) {
		h.entryNode = h.pickLiveEntry()
		if h.entryNode < 0 {
			h.maxLevel = -1
		}
	}
	return true
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

// removeOneFromSlice removes the first occurrence of val, preserving the rest
// (including any duplicate occurrences). Used for inbound multiplicity.
func removeOneFromSlice(s []int32, val int32) []int32 {
	for i, v := range s {
		if v == val {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

// searchCandidates runs the same HNSW traversal as Search but emits []candidate
// carrying each surviving hit's stored qv (BY-VALUE copy), text, and
// quantizedScore (the InnerProductPrepared it computes anyway), along with the
// prepared query source vector so rerankExact does not re-encode it.
//
// This helper exists because the public Search discards qv and text, which are
// needed for the drift-check exact rerank (Task 4 / §5). Keep Search itself
// UNCHANGED for the non-rerank path.
//
// The returned []candidate slice is self-contained: storedCode is copied by
// value under h.flat.mu.RLock() before the lock is released. The caller
// (collection.go searchVectorLocal HNSW branch) calls rerankExact with NO lock
// held — this is correct and safe.
func (h *hnswIndex) searchCandidates(query []float32, k int, filters []FilterOption) ([]candidate, []float32) {
	if k <= 0 {
		return nil, query
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	n := h.flat.Len()
	if n == 0 || h.entryNode < 0 {
		return nil, query
	}

	// For very small datasets, fall back to flat scan and build candidates from it.
	if n <= h.params.EfSearch {
		results := h.flat.Search(query, k, filters)
		return searchResultsToCandidates(h, results), query
	}

	pq := h.flat.quantizer.PrepareQuery(query)

	current := h.entryNode
	if h.isTombstoned(current) {
		current = h.pickLiveEntry()
		if current < 0 {
			return nil, query
		}
	}
	for level := h.maxLevel; level >= 1; level-- {
		current = h.greedyClosest(current, pq, level)
	}

	ef := h.params.EfSearch
	if len(filters) > 0 {
		ef *= 4
	}
	if ef < k {
		ef = k
	}

	rawCandidates := h.searchLayer(current, pq, ef, 0)

	h.flat.mu.RLock()
	defer h.flat.mu.RUnlock()

	resultHeap := &searchHeap{}
	for _, ci := range rawCandidates {
		idx := int(ci)
		if idx >= len(h.flat.entries) || h.isTombstoned(idx) {
			continue
		}
		entry := h.flat.entries[idx]
		if entry.child {
			continue
		}
		if !matchesFilters(entry.metadata, filters) {
			continue
		}
		if resultHeap.Len() == k {
			bound, prunable := pq.ScoreUpperBound(entry.qv.Norm, entry.qv.ResNorm)
			if prunable && bound <= (*resultHeap)[0].Score {
				continue
			}
		}
		score := h.flat.quantizer.InnerProductPrepared(entry.qv, pq)
		r := SearchResult{
			ID:      entry.id,
			Score:   score,
			Version: entry.version,
		}
		if resultHeap.Len() < k {
			heap.Push(resultHeap, r)
		} else if score > (*resultHeap)[0].Score {
			(*resultHeap)[0] = r
			heap.Fix(resultHeap, 0)
		}
	}

	// Build candidates from the surviving results, copying storedCode BY VALUE
	// while h.flat.mu.RLock() is held. After this loop the lock will be released
	// (via defer) and candidates are fully self-contained.
	cands := make([]candidate, 0, resultHeap.Len())
	for _, r := range *resultHeap {
		pos, ok := h.flat.idIndex[r.ID]
		if !ok {
			continue
		}
		entry := &h.flat.entries[pos]
		cands = append(cands, candidate{
			id:             entry.id,
			storedCode:     entry.qv, // by-value copy; safe after lock release
			text:           entry.text,
			quantizedScore: r.Score,
			version:        entry.version,
			metadata:       cloneMetadata(entry.metadata),
		})
	}
	return cands, query
}

// searchResultsToCandidates converts a []SearchResult (from flat scan fallback)
// into []candidate by copying qv BY VALUE from the flat index under its own
// RLock. It acquires h.flat.mu.RLock() internally, so it must be called WITHOUT
// h.flat.mu held (reentrant RLock can deadlock vs a pending writer). The
// returned candidates are self-contained; no lock needs to be held after return.
func searchResultsToCandidates(h *hnswIndex, results []SearchResult) []candidate {
	h.flat.mu.RLock()
	defer h.flat.mu.RUnlock()
	cands := make([]candidate, 0, len(results))
	for _, r := range results {
		pos, ok := h.flat.idIndex[r.ID]
		if !ok {
			continue
		}
		entry := &h.flat.entries[pos]
		cands = append(cands, candidate{
			id:             entry.id,
			storedCode:     entry.qv, // by-value copy; safe after lock release
			text:           entry.text,
			quantizedScore: r.Score,
			version:        entry.version,
			metadata:       cloneMetadata(entry.metadata),
		})
	}
	return cands
}

func (h *hnswIndex) Search(query []float32, k int, filters []FilterOption) []SearchResult {
	if k <= 0 {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	n := h.flat.Len() // live count (tombstones excluded)

	if n == 0 || h.entryNode < 0 {
		return nil
	}

	// For very small datasets, fall back to flat scan (skips dead rows internally).
	if n <= h.params.EfSearch {
		return h.flat.Search(query, k, filters)
	}

	pq := h.flat.quantizer.PrepareQuery(query)

	// Ensure the entry node is live (it may have been tombstoned).
	current := h.entryNode
	if h.isTombstoned(current) {
		current = h.pickLiveEntry()
		if current < 0 {
			return nil
		}
	}
	// Greedy descent from top level to level 1.
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
		if idx >= len(h.flat.entries) || h.isTombstoned(idx) {
			continue
		}
		entry := h.flat.entries[idx]
		if entry.child {
			continue
		}
		if !matchesFilters(entry.metadata, filters) {
			continue
		}
		if resultHeap.Len() == k { // heap full: proven upper-bound early-out (D3)
			bound, prunable := pq.ScoreUpperBound(entry.qv.Norm, entry.qv.ResNorm)
			if prunable && bound <= (*resultHeap)[0].Score {
				continue // exact <= bound <= kthBest: cannot displace the k-th member
			}
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
