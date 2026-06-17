package corkscrewdb

import (
	"container/heap"

	"m31labs.dev/turboquant"
)

// ScoredHit is one (corpus index, score) result.
type ScoredHit struct {
	Index int
	Score float32
}

// Scorer scores prepared query(ies) against a collection's quantized corpus.
// accept(i) is the pushed-down filter predicate (§7.4); rows where accept==false
// are skipped before scoring.
type Scorer interface {
	ScoreTopK(pq turboquant.PreparedQuery, k int, accept func(i int) bool) []ScoredHit
	ScoreTopKMulti(pqs []turboquant.PreparedQuery, k int, accept func(i int) bool) [][]ScoredHit
	Close() error
}

// defaultScorer is the certified pure-Go scorer over InnerProductPrepared.
type defaultScorer struct {
	quantizer *turboquant.IPQuantizer
	corpus    []turboquant.IPQuantized
}

func newDefaultScorer(q *turboquant.IPQuantizer) *defaultScorer {
	return &defaultScorer{quantizer: q}
}

// SetCorpus installs the row-ordered quantized corpus the scorer scans.
func (s *defaultScorer) SetCorpus(corpus []turboquant.IPQuantized) {
	s.corpus = corpus
}

func (s *defaultScorer) ScoreTopK(pq turboquant.PreparedQuery, k int, accept func(i int) bool) []ScoredHit {
	if k <= 0 {
		return nil
	}
	h := &scoredHitHeap{}
	for i := range s.corpus {
		if accept != nil && !accept(i) { // filter pushdown BEFORE scoring
			continue
		}
		score := s.quantizer.InnerProductPrepared(s.corpus[i], pq)
		hit := ScoredHit{Index: i, Score: score}
		if h.Len() < k {
			heap.Push(h, hit)
		} else if score > (*h)[0].Score {
			(*h)[0] = hit
			heap.Fix(h, 0)
		}
	}
	out := make([]ScoredHit, h.Len())
	copy(out, *h)
	sortScoredHits(out)
	return out
}

func (s *defaultScorer) ScoreTopKMulti(pqs []turboquant.PreparedQuery, k int, accept func(i int) bool) [][]ScoredHit {
	// Phase 1: simple per-query loop (batched 1-4 kernel wiring deferred to Perf).
	out := make([][]ScoredHit, len(pqs))
	for qi := range pqs {
		out[qi] = s.ScoreTopK(pqs[qi], k, accept)
	}
	return out
}

func (s *defaultScorer) Close() error { return nil }

// NOTE: there is no write-time ScoreBound. The §7.3 two-tier prune is a
// query-time computation deferred to the Performance plan; it reuses the
// already-stored turboquant.IPQuantized.ResNorm and bounds the sign-stage
// contribution by q_factor*ResNorm at query time. No field is persisted here.

type scoredHitHeap []ScoredHit

func (h scoredHitHeap) Len() int            { return len(h) }
func (h scoredHitHeap) Less(i, j int) bool  { return h[i].Score < h[j].Score }
func (h scoredHitHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *scoredHitHeap) Push(x any)         { *h = append(*h, x.(ScoredHit)) }
func (h *scoredHitHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func sortScoredHits(hits []ScoredHit) {
	for i := 1; i < len(hits); i++ { // small k: insertion sort, stable by (score desc, index asc)
		for j := i; j > 0 && (hits[j].Score > hits[j-1].Score ||
			(hits[j].Score == hits[j-1].Score && hits[j].Index < hits[j-1].Index)); j-- {
			hits[j], hits[j-1] = hits[j-1], hits[j]
		}
	}
}
