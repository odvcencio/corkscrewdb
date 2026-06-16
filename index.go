package corkscrewdb

import (
	"container/heap"
	"errors"
	"sort"
	"sync"

	"m31labs.dev/turboquant"
)

// indexer is the common interface implemented by flat and HNSW indices.
type indexer interface {
	Add(id string, vec []float32, text string, metadata map[string]string, version uint64)
	Remove(id string) bool
	Search(query []float32, k int, filters []FilterOption) []SearchResult
	Len() int
	Dim() int
	BitWidth() int
}

var _ indexer = (*index)(nil)

type indexEntry struct {
	id             string
	qv             turboquant.IPQuantized
	text           string
	metadata       map[string]string
	version        uint64
	parentID       string
	parentText     string
	parentMetadata map[string]string
	childID        string
	childText      string
	childMetadata  map[string]string
	child          bool
}

type index struct {
	mu        sync.RWMutex
	quantizer *turboquant.IPQuantizer
	entries   []indexEntry
	idIndex   map[string]int
}

func newIndex(dim, bitWidth int, seed int64) *index {
	return &index{
		quantizer: turboquant.NewIPWithSeed(dim, bitWidth, seed),
		entries:   make([]indexEntry, 0),
		idIndex:   make(map[string]int),
	}
}

func (idx *index) Dim() int      { return idx.quantizer.Dim() }
func (idx *index) BitWidth() int { return idx.quantizer.BitWidth() }

func (idx *index) Add(id string, vec []float32, text string, metadata map[string]string, version uint64) {
	qv := idx.quantizer.Quantize(vec)
	idx.addQuantized(id, qv, text, metadata, version)
}

func (idx *index) AddBatch(ids []string, vecs [][]float32, texts []string, metas []map[string]string, versions []uint64) {
	qvs := make([]turboquant.IPQuantized, len(ids))
	for i := range ids {
		qvs[i] = idx.quantizer.Quantize(vecs[i])
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for i, id := range ids {
		entry := indexEntry{
			id:       id,
			qv:       qvs[i],
			text:     texts[i],
			metadata: cloneMetadata(metas[i]),
			version:  versions[i],
		}
		if pos, ok := idx.idIndex[id]; ok {
			idx.entries[pos] = entry
			continue
		}
		idx.idIndex[id] = len(idx.entries)
		idx.entries = append(idx.entries, entry)
	}
}

func (idx *index) Remove(id string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	pos, ok := idx.idIndex[id]
	if !ok {
		return false
	}
	last := len(idx.entries) - 1
	if pos != last {
		idx.entries[pos] = idx.entries[last]
		idx.idIndex[idx.entries[pos].id] = pos
	}
	idx.entries = idx.entries[:last]
	delete(idx.idIndex, id)
	return true
}

func (idx *index) removePackedChildren(parentID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for i := 0; i < len(idx.entries); {
		if idx.entries[i].child && idx.entries[i].parentID == parentID {
			idx.removeAtLocked(i)
			continue
		}
		i++
	}
}

func (idx *index) removeAtLocked(pos int) {
	last := len(idx.entries) - 1
	removedID := idx.entries[pos].id
	if pos != last {
		idx.entries[pos] = idx.entries[last]
		idx.idIndex[idx.entries[pos].id] = pos
	}
	idx.entries = idx.entries[:last]
	delete(idx.idIndex, removedID)
}

func (idx *index) addQuantized(id string, qv turboquant.IPQuantized, text string, metadata map[string]string, version uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entry := indexEntry{
		id:       id,
		qv:       cloneQuantized(qv),
		text:     text,
		metadata: cloneMetadata(metadata),
		version:  version,
	}
	if pos, ok := idx.idIndex[id]; ok {
		idx.entries[pos] = entry
		return
	}
	idx.idIndex[id] = len(idx.entries)
	idx.entries = append(idx.entries, entry)
}

func (idx *index) addPackedChild(parentID string, parent Version, child MultiVectorChildVersion) {
	if child.quantized == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	id := packedChildIndexID(parentID, child.ID)
	entry := indexEntry{
		id:             id,
		qv:             cloneQuantized(*child.quantized),
		text:           child.Text,
		metadata:       cloneMetadata(child.Metadata),
		version:        parent.LamportClock,
		parentID:       parentID,
		parentText:     parent.Text,
		parentMetadata: cloneMetadata(parent.Metadata),
		childID:        child.ID,
		childText:      child.Text,
		childMetadata:  cloneMetadata(child.Metadata),
		child:          true,
	}
	if pos, ok := idx.idIndex[id]; ok {
		idx.entries[pos] = entry
		return
	}
	idx.idIndex[id] = len(idx.entries)
	idx.entries = append(idx.entries, entry)
}

func packedChildIndexID(parentID, childID string) string {
	return parentID + "\x00" + childID
}

func (idx *index) snapshotEntries() []indexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make([]indexEntry, len(idx.entries))
	for i, entry := range idx.entries {
		out[i] = indexEntry{
			id:             entry.id,
			qv:             cloneQuantized(entry.qv),
			text:           entry.text,
			metadata:       cloneMetadata(entry.metadata),
			version:        entry.version,
			parentID:       entry.parentID,
			parentText:     entry.parentText,
			parentMetadata: cloneMetadata(entry.parentMetadata),
			childID:        entry.childID,
			childText:      entry.childText,
			childMetadata:  cloneMetadata(entry.childMetadata),
			child:          entry.child,
		}
	}
	return out
}

func cloneQuantized(qv turboquant.IPQuantized) turboquant.IPQuantized {
	return turboquant.IPQuantized{
		MSE:     append([]byte(nil), qv.MSE...),
		Signs:   append([]byte(nil), qv.Signs...),
		ResNorm: qv.ResNorm,
	}
}

func (idx *index) Search(query []float32, k int, filters []FilterOption) []SearchResult {
	if k <= 0 {
		return nil
	}
	idx.mu.RLock()
	n := len(idx.entries)
	if n == 0 {
		idx.mu.RUnlock()
		return nil
	}
	pq := idx.quantizer.PrepareQuery(query)
	h := &searchHeap{}
	for i := 0; i < n; i++ {
		entry := idx.entries[i]
		if entry.child {
			continue
		}
		if !matchesFilters(entry.metadata, filters) {
			continue
		}
		score := idx.quantizer.InnerProductPrepared(entry.qv, pq)
		result := SearchResult{
			ID:       entry.id,
			Score:    score,
			Text:     entry.text,
			Metadata: cloneMetadata(entry.metadata),
			Version:  entry.version,
		}
		if h.Len() < k {
			heap.Push(h, result)
			continue
		}
		if score > (*h)[0].Score {
			(*h)[0] = result
			heap.Fix(h, 0)
		}
	}
	idx.mu.RUnlock()

	results := make([]SearchResult, h.Len())
	copy(results, *h)
	sortSearchResults(results)
	return results
}

func (idx *index) SearchParents(query []float32, k int, cfg parentSearchConfig) ([]ParentSearchResult, error) {
	if k <= 0 {
		return nil, nil
	}
	if cfg.childOverfetch > 0 {
		return nil, errors.New("corkscrewdb: WithChildOverfetch is unsupported for exact flat parent search in v1")
	}
	idx.mu.RLock()
	n := len(idx.entries)
	if n == 0 {
		idx.mu.RUnlock()
		return nil, nil
	}
	pq := idx.quantizer.PrepareQuery(query)
	best := make(map[string]ParentSearchResult)
	for i := 0; i < n; i++ {
		entry := idx.entries[i]
		if !entry.child {
			continue
		}
		if !matchesFilters(entry.parentMetadata, cfg.parentFilters) || !matchesFilters(entry.childMetadata, cfg.childFilters) {
			continue
		}
		score := idx.quantizer.InnerProductPrepared(entry.qv, pq)
		result := ParentSearchResult{
			ID:            entry.parentID,
			Score:         score,
			Text:          entry.parentText,
			Metadata:      cloneMetadata(entry.parentMetadata),
			Version:       entry.version,
			ChildID:       entry.childID,
			ChildScore:    score,
			ChildText:     entry.childText,
			ChildMetadata: cloneMetadata(entry.childMetadata),
		}
		current, ok := best[entry.parentID]
		if !ok || parentResultLess(current, result) {
			best[entry.parentID] = result
		}
	}
	idx.mu.RUnlock()

	results := make([]ParentSearchResult, 0, len(best))
	for _, result := range best {
		results = append(results, result)
	}
	sortParentSearchResults(results)
	if len(results) > k {
		results = results[:k]
	}
	return results, nil
}

func sortParentSearchResults(results []ParentSearchResult) {
	sort.Slice(results, func(i, j int) bool {
		return parentResultLess(results[j], results[i])
	})
}

func parentResultLess(a, b ParentSearchResult) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	if a.ID != b.ID {
		return a.ID > b.ID
	}
	return a.ChildID > b.ChildID
}

func (idx *index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

type searchHeap []SearchResult

func (h searchHeap) Len() int           { return len(h) }
func (h searchHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h searchHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *searchHeap) Push(x any)        { *h = append(*h, x.(SearchResult)) }
func (h *searchHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
