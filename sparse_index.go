package corkscrewdb

import "container/heap"

// sparseSearch runs an exact brute-force top-k inner-product scan over a sparse
// active set. accept is the filter predicate, evaluated for every candidate
// BEFORE scoring (filter pushdown, mirroring index.Search §7.4). hydrate fills
// Text/Metadata/Version for surviving candidates. Score is the raw sparse dot
// product (no normalization); ids whose overlap is empty (score == 0) are kept
// — dropping on zero is the fusion layer's call (§2/§4.6).
func sparseSearch(
	set map[string]*SparseVector,
	query *SparseVector,
	k int,
	accept func(id string) bool,
	hydrate func(id string) (text string, meta map[string]string, version uint64),
) []SearchResult {
	if k <= 0 {
		return nil
	}
	h := &searchHeap{}
	for id, sv := range set {
		if accept != nil && !accept(id) {
			continue
		}
		score := sparseDot(query, sv)
		result := SearchResult{ID: id, Score: score}
		if h.Len() < k {
			heap.Push(h, result)
			continue
		}
		if score > (*h)[0].Score {
			(*h)[0] = result
			heap.Fix(h, 0)
		}
	}

	results := make([]SearchResult, h.Len())
	copy(results, *h)
	if hydrate != nil {
		for i := range results {
			text, meta, version := hydrate(results[i].ID)
			results[i].Text = text
			results[i].Metadata = meta
			results[i].Version = version
		}
	}
	sortSearchResults(results)
	return results
}
