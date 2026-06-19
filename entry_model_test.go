package corkscrewdb

import "testing"

func TestHNSWParamsDefaultsAndOptions(t *testing.T) {
	var cfg collectionConfig
	WithIndexType(IndexHNSW).applyCollection(&cfg)
	WithHNSWParams(HNSWParams{M: 8, EfConstruction: 100, EfSearch: 25}).applyCollection(&cfg)
	WithoutRawStore().applyCollection(&cfg)
	WithSparse().applyCollection(&cfg)
	if cfg.indexType != IndexHNSW {
		t.Fatalf("indexType not honored: %v", cfg.indexType)
	}
	if cfg.hnsw.M != 8 || cfg.hnsw.EfConstruction != 100 || cfg.hnsw.EfSearch != 25 {
		t.Fatalf("hnsw params not honored: %+v", cfg.hnsw)
	}
	if cfg.rawStore { // WithoutRawStore sets rawStore=false
		t.Fatal("WithoutRawStore did not disable raw store")
	}
	if !cfg.sparseEnabled {
		t.Fatal("WithSparse did not enable sparse")
	}
}

func TestChildVectorAndVersionShape(t *testing.T) {
	v := Version{
		RawHash:  []byte{1, 2, 3},
		Children: []ChildVector{{ID: "0"}},
		Sparse:   &SparseVector{Indices: []uint32{1}, Values: []float32{0.5}},
	}
	if len(v.RawHash) != 3 || len(v.Children) != 1 || v.Sparse == nil {
		t.Fatal("Version shape mismatch")
	}
}
