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
	// pruneSkips counts candidates skipped by the query-time two-tier prune (D3).
	// Test-only instrumentation; zero in normal use.
	pruneSkips int
}

func newDefaultScorer(q *turboquant.IPQuantizer) *defaultScorer {
	return &defaultScorer{quantizer: q}
}

// SetCorpus installs the row-ordered quantized corpus the scorer scans.
func (s *defaultScorer) SetCorpus(corpus []turboquant.IPQuantized) {
	s.corpus = corpus
}

// pushHit inserts (idx, score) into a bounded top-k min-heap (worst on root).
func pushHit(h *scoredHitHeap, k int, idx int, score float32) {
	hit := ScoredHit{Index: idx, Score: score}
	if h.Len() < k {
		heap.Push(h, hit)
	} else if score > (*h)[0].Score {
		(*h)[0] = hit
		heap.Fix(h, 0)
	}
}

// rowCorpus abstracts the row-ordered quantized codes a scorer scans. Both the
// copied snapshot (injected-scorer path) and a zero-copy view over the index's
// LIVE entries (the default in-process path) satisfy it, so the certified scoring
// loop is shared and byte-identical regardless of the source. len() is the row
// count; codeAt(i) returns row i's turboquant.IPQuantized BY REFERENCE — the live
// view returns the entry's stored code without copying it, so the default path
// allocates nothing proportional to the corpus per query (§ Phase 3 Credibility).
type rowCorpus interface {
	len() int
	codeAt(i int) *turboquant.IPQuantized
}

// sliceCorpus is a rowCorpus over a materialized []turboquant.IPQuantized (the
// copied snapshot adopted by an injected scorer).
type sliceCorpus []turboquant.IPQuantized

func (c sliceCorpus) len() int                             { return len(c) }
func (c sliceCorpus) codeAt(i int) *turboquant.IPQuantized { return &c[i] }

func (s *defaultScorer) ScoreTopK(pq turboquant.PreparedQuery, k int, accept func(i int) bool) []ScoredHit {
	return s.scoreTopKOver(sliceCorpus(s.corpus), pq, k, accept)
}

// scoreTopKOver is the certified single-query top-k scan, shared by the
// copied-snapshot and zero-copy live-entry paths. It is byte-identical to the
// original inline scan: §7.4 filter pushdown before scoring, the D3 upper-bound
// early-out (LUT widths only), and the same bounded min-heap. codeAt is read by
// reference so the live-entry view never copies the corpus.
func (s *defaultScorer) scoreTopKOver(corpus rowCorpus, pq turboquant.PreparedQuery, k int, accept func(i int) bool) []ScoredHit {
	if k <= 0 {
		return nil
	}
	n := corpus.len()
	h := &scoredHitHeap{}
	for i := 0; i < n; i++ {
		if accept != nil && !accept(i) { // filter pushdown BEFORE scoring
			continue
		}
		code := corpus.codeAt(i)
		if h.Len() == k { // heap full: try the proven upper-bound early-out (D3)
			bound, prunable := pq.ScoreUpperBound(code.ResNorm)
			if prunable && bound <= (*h)[0].Score {
				// exact <= bound <= kthBest: cannot displace the k-th member, and
				// kthBest only rises, so it can never qualify later. Safe to skip.
				s.pruneSkips++
				continue
			}
			// prunable==false (non-LUT widths): never skip; exact full scoring kept.
		}
		score := s.quantizer.InnerProductPrepared(*code, pq)
		pushHit(h, k, i, score)
	}
	out := make([]ScoredHit, h.Len())
	copy(out, *h)
	sortScoredHits(out)
	return out
}

// ScoreTopKMulti scores all queries in a single corpus pass: one batched
// InnerProductPreparedBatchTo per row produces every query's score, and per-query
// heaps are updated from that. accept is evaluated once per row (§7.4). The safe
// (non-Trusted) batch variant byte-matches InnerProductPrepared, so each per-query
// result is identical to an independent ScoreTopK (the equivalence lock).
func (s *defaultScorer) ScoreTopKMulti(pqs []turboquant.PreparedQuery, k int, accept func(i int) bool) [][]ScoredHit {
	if k <= 0 || len(pqs) == 0 {
		return make([][]ScoredHit, len(pqs))
	}
	heaps := make([]*scoredHitHeap, len(pqs))
	for qi := range heaps {
		heaps[qi] = &scoredHitHeap{}
	}
	dst := make([]float32, len(pqs)) // reused per row
	for i := range s.corpus {
		if accept != nil && !accept(i) { // §7.4 pushdown, once per row
			continue
		}
		// Row prune (D3): skip the whole row only when EVERY query's own bound says
		// it cannot displace that query's k-th member. Any prunable==false query, or
		// any not-yet-full heap, forces the row to be scored — preserving each
		// query's exactness (a query is skipped on a row only when its OWN bound says so).
		skipRow := true
		for qi := range pqs {
			if heaps[qi].Len() < k {
				skipRow = false
				break
			}
			b, prunable := pqs[qi].ScoreUpperBound(s.corpus[i].ResNorm)
			if !prunable || b > (*heaps[qi])[0].Score {
				skipRow = false
				break
			}
		}
		if skipRow {
			s.pruneSkips++
			continue
		}
		s.quantizer.InnerProductPreparedBatchTo(dst, s.corpus[i], pqs)
		for qi := range pqs {
			pushHit(heaps[qi], k, i, dst[qi])
		}
	}
	out := make([][]ScoredHit, len(pqs))
	for qi := range pqs {
		res := make([]ScoredHit, heaps[qi].Len())
		copy(res, *heaps[qi])
		sortScoredHits(res)
		out[qi] = res
	}
	return out
}

func (s *defaultScorer) Close() error { return nil }

// NOTE: there is no write-time ScoreBound. The §7.3 two-tier prune is a
// query-time computation deferred to the Performance plan; it reuses the
// already-stored turboquant.IPQuantized.ResNorm and bounds the sign-stage
// contribution by q_factor*ResNorm at query time. No field is persisted here.

type scoredHitHeap []ScoredHit

func (h scoredHitHeap) Len() int           { return len(h) }
func (h scoredHitHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h scoredHitHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *scoredHitHeap) Push(x any)        { *h = append(*h, x.(ScoredHit)) }
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
