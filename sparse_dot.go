package corkscrewdb

// sparseDot computes the exact inner product of two sparse vectors via a
// two-pointer merge over their sorted-unique index sets. It trusts the
// ascending-unique invariant documented on SparseVector (and enforced by
// validateSparseVector at write time); it does NOT re-validate. A nil or empty
// operand yields 0. Zero-allocation.
func sparseDot(a, b *SparseVector) float32 {
	if a == nil || b == nil || len(a.Indices) == 0 || len(b.Indices) == 0 {
		return 0
	}
	ai := a.Indices
	av := a.Values
	bi := b.Indices
	bv := b.Values
	var sum float64
	i, j := 0, 0
	for i < len(ai) && j < len(bi) {
		switch {
		case ai[i] == bi[j]:
			sum += float64(av[i]) * float64(bv[j])
			i++
			j++
		case ai[i] < bi[j]:
			i++
		default:
			j++
		}
	}
	return float32(sum)
}
