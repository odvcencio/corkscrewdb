package corkscrewdb

// FusionPolicy combines one or more positionally-ordered rankings
// (rankings[0] = dense, rankings[1] = sparse) into a single fused ranking.
// fuse is unexported: there is no exported fusion entry point; fusion is reached
// only through SearchMulti. The returned ranking is sorted score-desc, id-asc;
// an all-empty input yields nil.
type FusionPolicy interface {
	fuse(rankings ...[]SearchResult) []SearchResult
}

// RRFFusion implements Reciprocal Rank Fusion. For each ranking r (sorted
// best-first), the candidate at 0-based position p adds 1/(K + p + 1) to its
// id's fused score. K == 0 is treated as 60 (the conventional default).
type RRFFusion struct {
	K int
}

// WeightedFusion linearly combines per-ranking min-max-normalized scores.
// Positional weights: rankings[0] (dense) -> Dense, rankings[1] (sparse) ->
// Sparse. Weights need not sum to 1; 0 drops a channel; negatives are allowed.
type WeightedFusion struct {
	Dense  float32
	Sparse float32
}

// fusedAccumulator collects per-id fused scores plus the first-seen
// Text/Metadata/Version (first ranking, in argument order, that contained the
// id).
type fusedAccumulator struct {
	order []string // insertion order for stable, deterministic materialization
	score map[string]float64
	first map[string]SearchResult
}

func newFusedAccumulator() *fusedAccumulator {
	return &fusedAccumulator{
		score: make(map[string]float64),
		first: make(map[string]SearchResult),
	}
}

func (a *fusedAccumulator) add(r SearchResult, contribution float64) {
	if _, seen := a.score[r.ID]; !seen {
		a.order = append(a.order, r.ID)
		a.first[r.ID] = r
	}
	a.score[r.ID] += contribution
}

func (a *fusedAccumulator) materialize() []SearchResult {
	if len(a.order) == 0 {
		return nil
	}
	out := make([]SearchResult, 0, len(a.order))
	for _, id := range a.order {
		src := a.first[id]
		out = append(out, SearchResult{
			ID:       id,
			Score:    float32(a.score[id]),
			Text:     src.Text,
			Metadata: src.Metadata,
			Version:  src.Version,
		})
	}
	sortSearchResults(out)
	return out
}

func allEmpty(rankings [][]SearchResult) bool {
	for _, r := range rankings {
		if len(r) > 0 {
			return false
		}
	}
	return true
}

func (p RRFFusion) fuse(rankings ...[]SearchResult) []SearchResult {
	if allEmpty(rankings) {
		return nil
	}
	k := p.K
	if k == 0 {
		k = 60
	}
	acc := newFusedAccumulator()
	for _, ranking := range rankings {
		for pos, r := range ranking {
			acc.add(r, 1.0/float64(k+pos+1))
		}
	}
	return acc.materialize()
}

func (p WeightedFusion) fuse(rankings ...[]SearchResult) []SearchResult {
	if allEmpty(rankings) {
		return nil
	}
	weights := []float64{float64(p.Dense), float64(p.Sparse)}
	acc := newFusedAccumulator()
	for slot, ranking := range rankings {
		var w float64
		if slot < len(weights) {
			w = weights[slot]
		}
		norms := minMaxNormalize(ranking)
		for i, r := range ranking {
			acc.add(r, w*norms[i])
		}
	}
	return acc.materialize()
}

// minMaxNormalize returns per-element min-max-normalized scores for one ranking.
// If hi > lo: (s-lo)/(hi-lo). If hi == lo (non-empty): all 1 (never NaN).
func minMaxNormalize(ranking []SearchResult) []float64 {
	n := len(ranking)
	if n == 0 {
		return nil
	}
	lo := float64(ranking[0].Score)
	hi := lo
	for _, r := range ranking {
		s := float64(r.Score)
		if s < lo {
			lo = s
		}
		if s > hi {
			hi = s
		}
	}
	out := make([]float64, n)
	span := hi - lo
	for i, r := range ranking {
		if span > 0 {
			out[i] = (float64(r.Score) - lo) / span
		} else {
			out[i] = 1
		}
	}
	return out
}
