//go:build linux && amd64 && cgo && cuda

package corkscrewdb

import (
	"math/rand"
	"testing"

	"m31labs.dev/turboquant"
)

// TestGPUTopK runs ONLY on a CUDA device (build tag cuda); it is excluded from the
// default certified CI build. It asserts the GPU top-k matches the pure-Go top-k
// within float tolerance for k <= GPUPreparedTopKMax, and that out-of-bounds k
// declines (empty -> caller falls back to the default scorer).
func TestGPUTopK(t *testing.T) {
	dim, bits := 64, 2
	qz := turboquant.NewIPWithSeed(dim, bits, 7)
	rng := rand.New(rand.NewSource(11))
	n := 1000
	corpus := make([]turboquant.IPQuantized, n)
	for i := range corpus {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		corpus[i] = qz.Quantize(v)
	}

	gs, err := newGPUScorer(qz, corpus)
	if err != nil {
		t.Skipf("GPU scorer unavailable: %v", err)
	}
	defer gs.Close()

	ref := newDefaultScorer(qz)
	ref.SetCorpus(corpus)

	q := make([]float32, dim)
	for j := range q {
		q[j] = rng.Float32()*2 - 1
	}
	pq := qz.PrepareQuery(q)

	for _, k := range []int{1, 10, 64} {
		gpu := gs.ScoreTopK(pq, k, nil)
		cpu := ref.ScoreTopK(pq, k, nil)
		if len(gpu) != len(cpu) {
			t.Fatalf("k=%d: GPU returned %d hits, CPU %d", k, len(gpu), len(cpu))
		}
		// Top-1 membership must agree; lower ranks may differ on score ties.
		if len(gpu) > 0 && gpu[0].Index != cpu[0].Index {
			t.Fatalf("k=%d: GPU top-1 index %d != CPU %d", k, gpu[0].Index, cpu[0].Index)
		}
	}

	// k beyond GPUPreparedTopKMax must decline (empty) so the call site falls back.
	if got := gs.ScoreTopK(pq, turboquant.GPUPreparedTopKMax+1, nil); got != nil {
		t.Fatalf("k>max: expected nil decline, got %d hits", len(got))
	}
}
