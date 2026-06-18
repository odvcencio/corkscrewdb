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
