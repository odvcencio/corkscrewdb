package corkscrewdb

import "fmt"

// SparseVector is a sparse embedding channel.
// Indices MUST be sorted ascending and unique; Values is parallel to Indices.
type SparseVector struct {
	Indices []uint32
	Values  []float32
}

// validateSparseVector enforces the SparseVector invariants. A nil or empty
// vector is valid.
func validateSparseVector(sv *SparseVector) error {
	if sv == nil || len(sv.Indices) == 0 {
		if sv != nil && len(sv.Values) != 0 {
			return fmt.Errorf("%w: %d indices vs %d values", ErrInvalidSparseVector, len(sv.Indices), len(sv.Values))
		}
		return nil
	}
	if len(sv.Indices) != len(sv.Values) {
		return fmt.Errorf("%w: %d indices vs %d values", ErrInvalidSparseVector, len(sv.Indices), len(sv.Values))
	}
	for i := 1; i < len(sv.Indices); i++ {
		if sv.Indices[i] <= sv.Indices[i-1] {
			return fmt.Errorf("%w: indices must be strictly ascending at %d", ErrInvalidSparseVector, i)
		}
	}
	return nil
}

func cloneSparseVector(sv *SparseVector) *SparseVector {
	if sv == nil {
		return nil
	}
	out := &SparseVector{}
	if len(sv.Indices) > 0 {
		out.Indices = append([]uint32(nil), sv.Indices...)
		out.Values = append([]float32(nil), sv.Values...)
	}
	return out
}
