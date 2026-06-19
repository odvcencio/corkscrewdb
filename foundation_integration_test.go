package corkscrewdb

import (
	"path/filepath"
	"testing"
)

func TestFoundationRoundTripWithRawStore(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithProvider(&mockProvider{dim: 16}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs", WithBitWidth(2))
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	vec := make([]float32, 16)
	for i := range vec {
		vec[i] = float32(i) - 8
	}
	if err := coll.PutVector("a", vec); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A .rvs segment must exist (raw retained by default).
	matches, _ := filepath.Glob(filepath.Join(dir, "collections", "docs", "raw", "raw-*.rvs"))
	if len(matches) == 0 {
		t.Fatal("expected a raw store segment")
	}
	// Reopen: history + index rebuilt from snapshot + WAL + raw store.
	db2, err := Open(dir, WithProvider(&mockProvider{dim: 16}))
	if err != nil {
		t.Fatal(err)
	}
	coll2 := db2.Collection("docs")
	res, err := coll2.SearchVector(vec, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != "a" {
		t.Fatalf("reopen search failed: %+v", res)
	}
	_ = db2.Close()
}

func TestWithoutRawStoreSkipsRVS(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, WithProvider(&mockProvider{dim: 8}))
	coll := db.Collection("q", WithBitWidth(2), WithoutRawStore())
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	vec := make([]float32, 8)
	vec[0] = 1
	if err := coll.PutVector("x", vec); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	matches, _ := filepath.Glob(filepath.Join(dir, "collections", "q", "raw", "raw-*.rvs"))
	if len(matches) != 0 {
		t.Fatalf("WithoutRawStore must not write .rvs, found %v", matches)
	}
}
