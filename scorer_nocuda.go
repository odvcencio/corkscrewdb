//go:build !(linux && amd64 && cgo && cuda)

package corkscrewdb

import "m31labs.dev/turboquant"

// newGPUScorer is the non-CUDA stub: it mirrors turboquant's gpu_stub behavior by
// returning ErrGPUBackendUnavailable so WithScorer selection compiles everywhere.
// The certified default artifact is the pure-Go build; the GPU scorer is opt-in
// per process and only present under the `cuda` build tag.
func newGPUScorer(q *turboquant.IPQuantizer, corpus []turboquant.IPQuantized) (Scorer, error) {
	return nil, turboquant.ErrGPUBackendUnavailable
}
