package corkscrewdb

import (
	"math"
	"math/rand"
	"testing"
)

func TestSparseDot(t *testing.T) {
	tests := []struct {
		name string
		a    *SparseVector
		b    *SparseVector
		want float32
	}{
		{
			name: "disjoint",
			a:    &SparseVector{Indices: []uint32{0, 2, 4}, Values: []float32{1, 2, 3}},
			b:    &SparseVector{Indices: []uint32{1, 3, 5}, Values: []float32{4, 5, 6}},
			want: 0,
		},
		{
			name: "fullOverlap",
			a:    &SparseVector{Indices: []uint32{0, 1, 2}, Values: []float32{1, 2, 3}},
			b:    &SparseVector{Indices: []uint32{0, 1, 2}, Values: []float32{4, 5, 6}},
			// 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
			want: 32,
		},
		{
			name: "partialOverlap",
			a:    &SparseVector{Indices: []uint32{0, 2, 4, 7}, Values: []float32{1, 2, 3, 4}},
			b:    &SparseVector{Indices: []uint32{2, 3, 4, 8}, Values: []float32{5, 6, 7, 8}},
			// shared indices 2 and 4: 2*5 + 3*7 = 10 + 21 = 31
			want: 31,
		},
		{
			name: "nilA",
			a:    nil,
			b:    &SparseVector{Indices: []uint32{0}, Values: []float32{1}},
			want: 0,
		},
		{
			name: "nilB",
			a:    &SparseVector{Indices: []uint32{0}, Values: []float32{1}},
			b:    nil,
			want: 0,
		},
		{
			name: "emptyA",
			a:    &SparseVector{},
			b:    &SparseVector{Indices: []uint32{0}, Values: []float32{1}},
			want: 0,
		},
		{
			name: "emptyB",
			a:    &SparseVector{Indices: []uint32{0}, Values: []float32{1}},
			b:    &SparseVector{},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sparseDot(tt.a, tt.b); got != tt.want {
				t.Fatalf("sparseDot = %v, want %v", got, tt.want)
			}
		})
	}
}

func randomSparse(rng *rand.Rand, dim int) *SparseVector {
	present := make([]bool, dim)
	count := 1 + rng.Intn(dim)
	for i := 0; i < count; i++ {
		present[rng.Intn(dim)] = true
	}
	sv := &SparseVector{}
	for i := 0; i < dim; i++ {
		if present[i] {
			sv.Indices = append(sv.Indices, uint32(i))
			sv.Values = append(sv.Values, rng.Float32()*10-5)
		}
	}
	return sv
}

func densify(sv *SparseVector, dim int) []float32 {
	dense := make([]float32, dim)
	for i, idx := range sv.Indices {
		dense[idx] = sv.Values[i]
	}
	return dense
}

func TestSparseDotDensifiedEquivalence(t *testing.T) {
	const dim = 64
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 50; trial++ {
		a := randomSparse(rng, dim)
		b := randomSparse(rng, dim)
		da := densify(a, dim)
		db := densify(b, dim)
		var dense float64
		for i := 0; i < dim; i++ {
			dense += float64(da[i]) * float64(db[i])
		}
		got := sparseDot(a, b)
		if math.Abs(float64(got)-dense) > 1e-3 {
			t.Fatalf("trial %d: sparseDot = %v, dense IP = %v", trial, got, dense)
		}
	}
}

func TestSparseDotZeroAlloc(t *testing.T) {
	a := &SparseVector{Indices: []uint32{0, 2, 4, 6, 8}, Values: []float32{1, 2, 3, 4, 5}}
	b := &SparseVector{Indices: []uint32{1, 2, 4, 5, 8}, Values: []float32{6, 7, 8, 9, 10}}
	if n := testing.AllocsPerRun(100, func() { _ = sparseDot(a, b) }); n != 0 {
		t.Fatalf("sparseDot allocated %v times per run, want 0", n)
	}
}
