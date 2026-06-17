package corkscrewdb

import (
	"errors"
	"testing"
)

func TestValidateSparseVector(t *testing.T) {
	cases := []struct {
		name string
		sv   SparseVector
		ok   bool
	}{
		{"valid", SparseVector{Indices: []uint32{1, 3, 7}, Values: []float32{0.1, 0.2, 0.3}}, true},
		{"empty", SparseVector{}, true},
		{"lenMismatch", SparseVector{Indices: []uint32{1, 2}, Values: []float32{0.1}}, false},
		{"notAscending", SparseVector{Indices: []uint32{3, 1}, Values: []float32{0.1, 0.2}}, false},
		{"duplicate", SparseVector{Indices: []uint32{1, 1}, Values: []float32{0.1, 0.2}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSparseVector(&tc.sv)
			if tc.ok && err != nil {
				t.Fatalf("want ok, got %v", err)
			}
			if !tc.ok {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				if !errors.Is(err, ErrInvalidSparseVector) {
					t.Fatalf("want ErrInvalidSparseVector, got %v", err)
				}
			}
		})
	}
}
