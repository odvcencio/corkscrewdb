//go:build linux && amd64 && cgo && cuda

package corkscrewdb

import (
	"math"
	"math/bits"

	"m31labs.dev/turboquant"
)

// gpuScorer is a build-tagged Scorer over turboquant.GPUPreparedScorer. The corpus
// is device-resident and static between SetCorpus calls; the search path re-packs
// (rebuilds) the device scorer when the live corpus changes via corpusSetter.
//
// Filtered queries are NOT served here (the CUDA kernel scores the whole corpus
// with no per-row accept); searchVectorLocal routes those to the default scorer.
type gpuScorer struct {
	quantizer      *turboquant.IPQuantizer
	scorer         *turboquant.GPUPreparedScorer
	corpusLen      int
	corpusFingerprint uint64
}

// newGPUScorer uploads the corpus to the device. It returns the same
// ErrGPUBackendUnavailable as the stub when no device/LUT-eligible config exists,
// so collection wiring can degrade to the default scorer uniformly.
func newGPUScorer(q *turboquant.IPQuantizer, corpus []turboquant.IPQuantized) (Scorer, error) {
	sc, err := q.NewGPUPreparedScorer(corpus) // rejects non-LUT bit widths
	if err != nil {
		return nil, err
	}
	return &gpuScorer{
		quantizer:         q,
		scorer:            sc,
		corpusLen:         len(corpus),
		corpusFingerprint: gpuCorpusFingerprint(corpus),
	}, nil
}

// gpuCorpusFingerprint computes a cheap rolling hash over the corpus so that
// in-place mutations (same cardinality, changed codes) are detected. It folds
// each entry's ResNorm bits and a sample of the Signs bytes into a 64-bit
// accumulator using rotate-xor — fast enough for a per-SetCorpus call on the
// GPU path (SetCorpus is not on the query hot path).
func gpuCorpusFingerprint(corpus []turboquant.IPQuantized) uint64 {
	h := uint64(len(corpus)) * 0x9e3779b97f4a7c15 // seed with cardinality
	for i := range corpus {
		c := &corpus[i]
		// Fold ResNorm (float32 bit pattern).
		h ^= bits.RotateLeft64(uint64(math.Float32bits(c.ResNorm)), 17)
		h *= 0x517cc1b727220a95
		// Sample Signs bytes: first, middle, last (if they exist).
		if n := len(c.Signs); n > 0 {
			h ^= bits.RotateLeft64(uint64(c.Signs[0]), 31)
			h ^= bits.RotateLeft64(uint64(c.Signs[n/2]), 23)
			h ^= bits.RotateLeft64(uint64(c.Signs[n-1]), 7)
			h *= 0xbf58476d1ce4e5b9
		}
	}
	return h
}

// SetCorpus re-packs the device-resident corpus when the live index changed. A
// re-pack failure leaves the previous device scorer in place (best-effort); the
// next search degrades to nil results -> default-scorer fallback at the call site.
//
// Staleness is detected via a content fingerprint (not just cardinality) so that
// in-place updates (same entry count, changed codes) and delete+add cycles that
// net the same cardinality both trigger a re-pack. See gpuCorpusFingerprint.
func (g *gpuScorer) SetCorpus(corpus []turboquant.IPQuantized) {
	fp := gpuCorpusFingerprint(corpus)
	if fp == g.corpusFingerprint {
		return // content unchanged; device scorer is still valid
	}
	sc, err := g.quantizer.NewGPUPreparedScorer(corpus)
	if err != nil {
		return
	}
	if g.scorer != nil {
		_ = g.scorer.Close()
	}
	g.scorer = sc
	g.corpusLen = len(corpus)
	g.corpusFingerprint = fp
}

func (g *gpuScorer) ScoreTopK(pq turboquant.PreparedQuery, k int, accept func(i int) bool) []ScoredHit {
	// No on-device accept; filtered queries are routed away in searchVectorLocal.
	if k <= 0 || k > turboquant.GPUPreparedTopKMax || k > g.corpusLen || g.scorer == nil {
		return nil // caller falls back to the default scorer
	}
	idx, sc, err := g.scorer.ScorePreparedQueryTopK(pq, k)
	if err != nil {
		return nil
	}
	out := make([]ScoredHit, len(idx))
	for i := range idx {
		out[i] = ScoredHit{Index: int(idx[i]), Score: sc[i]}
	}
	sortScoredHits(out)
	return out
}

func (g *gpuScorer) ScoreTopKMulti(pqs []turboquant.PreparedQuery, k int, accept func(i int) bool) [][]ScoredHit {
	out := make([][]ScoredHit, len(pqs))
	if k <= 0 || k > turboquant.GPUPreparedTopKMax || k > g.corpusLen || g.scorer == nil {
		return out
	}
	idx, sc, err := g.scorer.ScorePreparedQueriesTopK(pqs, k)
	if err != nil {
		return out
	}
	for qi := range pqs {
		hits := make([]ScoredHit, 0, k)
		for j := 0; j < k; j++ {
			p := qi*k + j
			if p >= len(idx) {
				break
			}
			hits = append(hits, ScoredHit{Index: int(idx[p]), Score: sc[p]})
		}
		sortScoredHits(hits)
		out[qi] = hits
	}
	return out
}

func (g *gpuScorer) Close() error {
	if g.scorer != nil {
		return g.scorer.Close()
	}
	return nil
}
