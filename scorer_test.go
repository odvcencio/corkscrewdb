package corkscrewdb

import (
	"testing"

	"m31labs.dev/turboquant"
)

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
