package corkscrewdb

import "testing"

func TestCollectionPutSearchHistoryDeleteAndAt(t *testing.T) {
	db, err := Open(t.TempDir(), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "alpha", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	results, err := coll.Search("alpha", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("results = %v, want doc-1", results)
	}
	history, err := coll.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	firstClock := history[0].LamportClock

	if err := coll.Put("doc-1", Entry{Text: "beta", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	history, err = coll.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("history len = %d, want 2", len(history))
	}

	view := coll.At(firstClock)
	viewResults, err := view.Search("alpha", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(viewResults) != 1 || viewResults[0].ID != "doc-1" {
		t.Fatalf("view results = %v, want doc-1", viewResults)
	}
	currentResults, err := coll.Search("beta", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(currentResults) != 1 || currentResults[0].Text != "beta" {
		t.Fatalf("current results = %v, want beta", currentResults)
	}

	if err := coll.Delete("doc-1"); err != nil {
		t.Fatal(err)
	}
	results, err = coll.Search("beta", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("deleted entry still searchable: %v", results)
	}
	history, err = coll.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 || !history[2].Tombstone {
		t.Fatalf("history = %+v, want tombstone third version", history)
	}
}

func TestCollectionPutVectorAndFilter(t *testing.T) {
	db, err := Open(t.TempDir(), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("vecs")
	vec := []float32{1, 0, 0, 0}
	if err := coll.PutVector("v1", vec, WithText("unit-x"), WithMetadata(map[string]string{"source": "code"})); err != nil {
		t.Fatal(err)
	}
	if err := coll.PutVector("v2", []float32{0, 1, 0, 0}, WithMetadata(map[string]string{"source": "notes"})); err != nil {
		t.Fatal(err)
	}
	results, err := coll.SearchVector(vec, 5, Filter("source", "code"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "v1" {
		t.Fatalf("results = %v, want v1", results)
	}
}

func TestCollectionWithQuantizerSeedPersistsAndValidates(t *testing.T) {
	path := t.TempDir()
	const seed int64 = 12345

	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("vecs", WithBitWidth(4), WithQuantizerSeed(seed))
	if err := coll.PutVector("v1", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := coll.PutVector("v2", []float32{0, 1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if got := db.manifest.Collections["vecs"].Seed; got != seed {
		t.Fatalf("manifest seed = %d, want %d", got, seed)
	}
	before, err := coll.SearchVector([]float32{1, 0, 0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll = db.Collection("vecs", WithQuantizerSeed(seed))
	if got := db.manifest.Collections["vecs"].Seed; got != seed {
		t.Fatalf("reopened manifest seed = %d, want %d", got, seed)
	}
	after, err := coll.SearchVector([]float32{1, 0, 0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !sameResultIDs(before, after) {
		t.Fatalf("search results changed after reopen: before=%v after=%v", before, after)
	}

	mismatched := db.Collection("vecs", WithQuantizerSeed(seed+1))
	if err := mismatched.usable(); err == nil {
		t.Fatal("expected mismatched quantizer seed to error")
	}

	negative := db.Collection("negative", WithQuantizerSeed(-1))
	if err := negative.usable(); err == nil {
		t.Fatal("expected negative quantizer seed to error")
	}
}

func sameResultIDs(a, b []SearchResult) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			return false
		}
	}
	return true
}
