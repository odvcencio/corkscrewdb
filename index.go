package corkscrewdb

import (
	"container/heap"
	"sync"

	"github.com/odvcencio/turboquant"
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
	id       string
	qv       turboquant.IPQuantized
	text     string
	metadata map[string]string
	version  uint64
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

func (idx *index) snapshotEntries() []indexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make([]indexEntry, len(idx.entries))
	for i, entry := range idx.entries {
		out[i] = indexEntry{
			id:       entry.id,
			qv:       cloneQuantized(entry.qv),
			text:     entry.text,
			metadata: cloneMetadata(entry.metadata),
			version:  entry.version,
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
