package corkscrewdb

import (
	"math/rand"
	"testing"

	"m31labs.dev/turboquant"
)

func absScore(v float32) float32 {
	if v < 0 {
		return -v
	}
	return v
}

func buildScorerCorpus(t testing.TB, dim, bits int, seed int64, n int) (*turboquant.IPQuantizer, []turboquant.IPQuantized) {
	t.Helper()
	qz := turboquant.NewIPWithSeed(dim, bits, seed)
	rng := rand.New(rand.NewSource(seed + 1))
	corpus := make([]turboquant.IPQuantized, n)
	for i := range corpus {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		corpus[i] = qz.Quantize(v)
	}
	return qz, corpus
}

func TestScoreTopKMultiEqualsSingle(t *testing.T) {
	dim := 16
	// Cover unrolled 1-4 and generic N>4 query batch sizes, plus a non-LUT width (3).
	for _, bits := range []int{2, 3, 4} {
		qz, corpus := buildScorerCorpus(t, dim, bits, int64(100+bits), 64)
		s := newDefaultScorer(qz)
		s.SetCorpus(corpus)
		rng := rand.New(rand.NewSource(int64(900 + bits)))
		for _, nq := range []int{1, 2, 3, 4, 5, 6} {
			pqs := make([]turboquant.PreparedQuery, nq)
			for qi := range pqs {
				q := make([]float32, dim)
				for j := range q {
					q[j] = rng.Float32()*2 - 1
				}
				pqs[qi] = qz.PrepareQuery(q)
			}
			for _, accept := range []func(int) bool{nil, func(i int) bool { return i%3 != 0 }} {
				k := 5
				multi := s.ScoreTopKMulti(pqs, k, accept)
				if len(multi) != nq {
					t.Fatalf("bits=%d nq=%d: multi len %d", bits, nq, len(multi))
				}
				for qi := range pqs {
					single := s.ScoreTopK(pqs[qi], k, accept)
					if len(multi[qi]) != len(single) {
						t.Fatalf("bits=%d nq=%d qi=%d: count multi=%d single=%d", bits, nq, qi, len(multi[qi]), len(single))
					}
					for i := range single {
						// Top-k membership and ORDERING must be byte-identical: the
						// batched kernel and scalar path select the same winners.
						if multi[qi][i].Index != single[i].Index {
							t.Fatalf("bits=%d nq=%d qi=%d pos=%d: index multi=%d single=%d", bits, nq, qi, i, multi[qi][i].Index, single[i].Index)
						}
						// Scores accumulate the same terms in a different order, so they
						// may differ by ~1 float32 ULP (the batch kernel vs scalar).
						a, b := multi[qi][i].Score, single[i].Score
						slack := float32(1e-5) * (absScore(b) + 1)
						if a-b > slack || b-a > slack {
							t.Fatalf("bits=%d nq=%d qi=%d pos=%d: score multi=%g single=%g (slack=%g)", bits, nq, qi, i, a, b, slack)
						}
					}
				}
			}
		}
	}
}

func BenchmarkScoreTopKMulti4(b *testing.B) {
	dim := 64
	qz, corpus := buildScorerCorpus(b, dim, 2, 7, 10000)
	s := newDefaultScorer(qz)
	s.SetCorpus(corpus)
	rng := rand.New(rand.NewSource(99))
	pqs := make([]turboquant.PreparedQuery, 4)
	for qi := range pqs {
		q := make([]float32, dim)
		for j := range q {
			q[j] = rng.Float32()*2 - 1
		}
		pqs[qi] = qz.PrepareQuery(q)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ScoreTopKMulti(pqs, 10, nil)
	}
}

func BenchmarkScoreTopK4xSingle(b *testing.B) {
	dim := 64
	qz, corpus := buildScorerCorpus(b, dim, 2, 7, 10000)
	s := newDefaultScorer(qz)
	s.SetCorpus(corpus)
	rng := rand.New(rand.NewSource(99))
	pqs := make([]turboquant.PreparedQuery, 4)
	for qi := range pqs {
		q := make([]float32, dim)
		for j := range q {
			q[j] = rng.Float32()*2 - 1
		}
		pqs[qi] = qz.PrepareQuery(q)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for qi := range pqs {
			_ = s.ScoreTopK(pqs[qi], 10, nil)
		}
	}
}

func TestDefaultScorerTopKAndPushdown(t *testing.T) {
	dim, bits, seed := 8, 2, int64(7)
	qz := turboquant.NewIPWithSeed(dim, bits, seed)
	corpus := make([]turboquant.IPQuantized, 5)
	for i := range corpus {
		v := make([]float32, dim)
		v[i%dim] = float32(i + 1)
		corpus[i] = qz.Quantize(v)
	}
	s := newDefaultScorer(qz)
	s.SetCorpus(corpus)
	q := make([]float32, dim)
	q[0] = 5
	pq := qz.PrepareQuery(q)

	scored := 0
	accept := func(i int) bool { scored++; return i != 2 } // exclude row 2
	hits := s.ScoreTopK(pq, 3, accept)
	if len(hits) != 3 {
		t.Fatalf("want 3 hits, got %d", len(hits))
	}
	for _, h := range hits {
		if h.Index == 2 {
			t.Fatal("filtered row 2 must never appear in results")
		}
	}
	// accept must be evaluated for every corpus row (pushdown happens before scoring).
	if scored != len(corpus) {
		t.Fatalf("accept evaluated %d times, want %d", scored, len(corpus))
	}
}

// referenceTopK is the unpruned full-scoring reference: it scores EVERY accepted
// corpus row with InnerProductPrepared (no upper-bound early-out), producing the
// certified top-k the prune must reproduce byte-for-byte.
func referenceTopK(qz *turboquant.IPQuantizer, corpus []turboquant.IPQuantized, pq turboquant.PreparedQuery, k int, accept func(i int) bool) []ScoredHit {
	if k <= 0 {
		return nil
	}
	h := &scoredHitHeap{}
	for i := range corpus {
		if accept != nil && !accept(i) {
			continue
		}
		score := qz.InnerProductPrepared(corpus[i], pq)
		pushHit(h, k, i, score)
	}
	out := make([]ScoredHit, h.Len())
	copy(out, *h)
	sortScoredHits(out)
	return out
}

func TestScoreUpperBoundPrunableContract(t *testing.T) {
	dim := 64
	// IP bit widths whose MSE stage (bitWidth-1) has an MSE LUT (1/2/4) are
	// prunable: 2->1, 3->2, 5->4. The non-LUT widths (4/6/7/8 -> MSE stage 3/5/6/7)
	// must report prunable=false. We cover {2,3,5} prunable and {4,6} non-prunable;
	// bits 7/8 are the same non-LUT code path but their turboquant quantizers cost
	// ~12-22s each to construct, so they are left to turboquant's own ip_test.go
	// (which asserts prunable=false for 4/6/7/8). (Mirrors turboquant ip_test.go.)
	prunableWidths := map[int]bool{2: true, 3: true, 5: true, 4: false, 6: false}
	for bits, wantPrunable := range prunableWidths {
		qz := turboquant.NewIPWithSeed(dim, bits, int64(1000+bits))
		pq := qz.PrepareQuery(make([]float32, dim))
		_, prunable := pq.ScoreUpperBound(1.0, 1.0)
		if prunable != wantPrunable {
			t.Fatalf("ipBitWidth=%d: prunable=%v want %v", bits, prunable, wantPrunable)
		}

		// For a non-prunable width the scorer must skip ZERO candidates. The prune
		// branch is gated on `prunable`, so the disable behavior is identical for all
		// non-LUT widths; we exercise the cheapest representative (bit 4) since
		// constructing bit 7/8 quantizers is prohibitively slow (turboquant codebook).
		if !wantPrunable && bits == 4 {
			_, corpus := buildScorerCorpus(t, dim, bits, int64(1000+bits), 50)
			s := newDefaultScorer(qz)
			s.SetCorpus(corpus)
			q := make([]float32, dim)
			q[0] = 1
			s.pruneSkips = 0
			_ = s.ScoreTopK(qz.PrepareQuery(q), 5, nil)
			if s.pruneSkips != 0 {
				t.Fatalf("ipBitWidth=%d (non-LUT): prune skipped %d candidates, want 0", bits, s.pruneSkips)
			}
		}
	}
}

func TestScorePruneExactness(t *testing.T) {
	dim := 48
	// Prunable LUT widths {2,3,5} (where the prune actively fires) plus bit 4 (a
	// non-LUT width where the prune disables itself). Pruned top-k must be
	// byte-identical to the unpruned reference. Bits 6/7/8 also disable the prune
	// (prunable=false), but constructing those turboquant quantizers is
	// prohibitively slow (~12-22s each) and adds no new code path beyond bit 4's
	// disable, so they are covered by the prunable-contract test, not here.
	for _, bits := range []int{2, 3, 4, 5} {
		qz, corpus := buildScorerCorpus(t, dim, bits, int64(50+bits), 200)
		s := newDefaultScorer(qz)
		s.SetCorpus(corpus)
		rng := rand.New(rand.NewSource(int64(7000 + bits)))
		for trial := 0; trial < 12; trial++ {
			q := make([]float32, dim)
			for j := range q {
				q[j] = rng.Float32()*2 - 1
			}
			pq := qz.PrepareQuery(q)
			for _, k := range []int{1, 5, 10} {
				for _, accept := range []func(int) bool{nil, func(i int) bool { return i%4 != 0 }} {
					ref := referenceTopK(qz, corpus, pq, k, accept)
					got := s.ScoreTopK(qz.PrepareQuery(q), k, accept)
					if len(got) != len(ref) {
						t.Fatalf("bits=%d trial=%d k=%d: count got=%d ref=%d", bits, trial, k, len(got), len(ref))
					}
					for i := range ref {
						// STRICT: the prune over-estimates in the safe direction, so
						// membership AND scores are byte-identical to unpruned.
						if got[i].Index != ref[i].Index || got[i].Score != ref[i].Score {
							t.Fatalf("bits=%d trial=%d k=%d pos=%d: got=%+v ref=%+v", bits, trial, k, i, got[i], ref[i])
						}
					}
				}
			}
		}
	}
}

func TestScorePruneBoundAndEffectiveness(t *testing.T) {
	dim := 64
	// LUT-prunable widths only (2/3/5).
	for _, bits := range []int{2, 3, 5} {
		qz, corpus := buildScorerCorpus(t, dim, bits, int64(300+bits), 500)
		rng := rand.New(rand.NewSource(int64(8000 + bits)))

		// Bound safety with float32 slack: exact <= bound + 1e-4*(|bound|+1).
		for trial := 0; trial < 20; trial++ {
			q := make([]float32, dim)
			for j := range q {
				q[j] = rng.Float32()*2 - 1
			}
			pq := qz.PrepareQuery(q)
			for i := range corpus {
				exact := qz.InnerProductPrepared(corpus[i], pq)
				bound, prunable := pq.ScoreUpperBound(corpus[i].Norm, corpus[i].ResNorm)
				if !prunable {
					t.Fatalf("bits=%d: expected prunable for LUT width", bits)
				}
				slack := float32(1e-4) * (absScore(bound) + 1)
				if exact > bound+slack {
					t.Fatalf("bits=%d trial=%d row=%d: exact %g exceeds bound %g (slack %g)", bits, trial, i, exact, bound, slack)
				}
			}
		}

		// Effectiveness: build a corpus with a clear top-k — many identical strong
		// vectors aligned with the query (high, tight kthBest) plus a sparse
		// low-resNorm bulk (low upper bound). The redundant aligned duplicates and
		// the low-bound bulk both fall at/under kthBest and are pruned.
		q := make([]float32, dim)
		q[0] = 1
		effCorpus := make([]turboquant.IPQuantized, 0, 500)
		for i := 0; i < 100; i++ {
			v := make([]float32, dim)
			v[0] = 5
			effCorpus = append(effCorpus, qz.Quantize(v))
		}
		for i := 0; i < 400; i++ {
			v := make([]float32, dim)
			v[1] = 0.001 // sparse tiny -> low resNorm -> low bound
			effCorpus = append(effCorpus, qz.Quantize(v))
		}
		s := newDefaultScorer(qz)
		s.SetCorpus(effCorpus)
		s.pruneSkips = 0
		got := s.ScoreTopK(qz.PrepareQuery(q), 5, nil)
		if len(got) != 5 {
			t.Fatalf("bits=%d: expected 5 results, got %d", bits, len(got))
		}
		t.Logf("bits=%d: prune skipped %d / %d candidates", bits, s.pruneSkips, len(effCorpus))
		// The bound's B floor (worst-case MSE) grows with bit width, so on realistic
		// scores the prune fires most strongly at the DEFAULT width 2 (where the
		// aligned bound is tight). We require effectiveness at the default width; at
		// 3/5 the bound is provably safe (asserted above) but typically looser.
		if bits == 2 && s.pruneSkips == 0 {
			t.Fatalf("bits=2 (default): prune skipped 0 candidates; expected > 0 on clear-top-k data")
		}
	}
}
