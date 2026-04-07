package snapshot

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAndFindLatest(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "snapshot-00001.csdb")
	second := filepath.Join(dir, "snapshot-00002.csdb")
	base := Data{
		Collection: "docs",
		BitWidth:   2,
		Seed:       1,
		Dim:        2,
		CreatedAt:  time.Now().UTC(),
		Records:    []Record{{ID: "a"}},
	}
	data1 := base
	data1.MaxLamport = 1
	data2 := base
	data2.MaxLamport = 2
	if err := WriteFile(first, data1); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(second, data2); err != nil {
		t.Fatal(err)
	}
	latest, err := FindLatestFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if latest != second {
		t.Fatalf("latest = %q, want %q", latest, second)
	}
	loaded, err := LoadFile(latest)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.MaxLamport != 2 {
		t.Fatalf("MaxLamport = %d, want 2", loaded.MaxLamport)
	}
}
