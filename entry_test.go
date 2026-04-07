package corkscrewdb

import "testing"

func TestEntryTextSet(t *testing.T) {
	e := Entry{
		Text:     "hello world",
		Metadata: map[string]string{"source": "test"},
	}
	if e.Text != "hello world" {
		t.Fatalf("Text = %q, want %q", e.Text, "hello world")
	}
	if e.Metadata["source"] != "test" {
		t.Fatalf("Metadata[source] = %q, want %q", e.Metadata["source"], "test")
	}
}

func TestSearchResultOrdering(t *testing.T) {
	results := []SearchResult{
		{ID: "a", Score: 0.9},
		{ID: "b", Score: 0.5},
		{ID: "c", Score: 0.7},
	}
	sortSearchResults(results)
	if results[0].ID != "a" || results[1].ID != "c" || results[2].ID != "b" {
		t.Fatalf("sort order wrong: %v", results)
	}
}

func TestFilterMatch(t *testing.T) {
	meta := map[string]string{"source": "review", "agent": "cedar"}
	f := Filter("source", "review")
	if !f.matches(meta) {
		t.Fatal("filter should match")
	}
	f2 := Filter("source", "code")
	if f2.matches(meta) {
		t.Fatal("filter should not match")
	}
}

func TestCloneMetadataIsolated(t *testing.T) {
	meta := map[string]string{"source": "test"}
	cloned := cloneMetadata(meta)
	cloned["source"] = "changed"
	if meta["source"] != "test" {
		t.Fatalf("source metadata mutated: %q", meta["source"])
	}
}
