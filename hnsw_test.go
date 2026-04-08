package corkscrewdb

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestHNSWInsertAndSearch(t *testing.T) {
	dim := 32
	n := 100
	rng := rand.New(rand.NewSource(42))
	hw := newHNSWIndex(dim, 2, 99, defaultHNSWParams())

	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = randVec(rng, dim)
		hw.Add(fmt.Sprintf("v%d", i), vecs[i], "", nil, uint64(i+1))
	}

	if hw.Len() != n {
		t.Fatalf("Len = %d, want %d", hw.Len(), n)
	}

	results := hw.Search(vecs[0], 1, nil)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != "v0" {
		t.Fatalf("top result = %s, want v0", results[0].ID)
	}
}

func TestHNSWRemove(t *testing.T) {
	dim := 32
	rng := rand.New(rand.NewSource(42))
	hw := newHNSWIndex(dim, 2, 99, defaultHNSWParams())

	vecs := make([][]float32, 5)
	for i := range vecs {
		vecs[i] = randVec(rng, dim)
		hw.Add(fmt.Sprintf("v%d", i), vecs[i], "", nil, uint64(i+1))
	}

	if !hw.Remove("v2") {
		t.Fatal("Remove(v2) returned false")
	}
	if hw.Len() != 4 {
		t.Fatalf("Len = %d, want 4", hw.Len())
	}

	// Search for v2's vector; v2 must not appear in results.
	results := hw.Search(vecs[2], 10, nil)
	for _, r := range results {
		if r.ID == "v2" {
			t.Fatal("removed entry v2 still returned in search")
		}
	}
}

func TestHNSWRecall(t *testing.T) {
	dim := 64
	n := 500
	k := 10
	rng := rand.New(rand.NewSource(123))

	hw := newHNSWIndex(dim, 4, 77, hnswParams{
		M:              16,
		EfConstruction: 200,
		EfSearch:       100,
	})

	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = randVec(rng, dim)
		hw.Add(fmt.Sprintf("v%d", i), vecs[i], "", nil, uint64(i+1))
	}

	// Also build a flat index for ground truth.
	flat := newIndex(dim, 4, 77)
	for i := range vecs {
		flat.Add(fmt.Sprintf("v%d", i), vecs[i], "", nil, uint64(i+1))
	}

	nQueries := 50
	totalRecall := 0.0
	for q := 0; q < nQueries; q++ {
		query := randVec(rng, dim)
		flatResults := flat.Search(query, k, nil)
		trueTopK := make(map[string]bool, k)
		for _, r := range flatResults {
			trueTopK[r.ID] = true
		}

		hnswResults := hw.Search(query, k, nil)
		hits := 0
		for _, r := range hnswResults {
			if trueTopK[r.ID] {
				hits++
			}
		}
		totalRecall += float64(hits) / float64(k)
	}

	avgRecall := totalRecall / float64(nQueries)
	t.Logf("HNSW Recall@%d = %.3f (n=%d, dim=%d)", k, avgRecall, n, dim)
	if avgRecall < 0.85 {
		t.Errorf("Recall@%d = %.2f, want >= 0.85", k, avgRecall)
	}
}

func TestHNSWFilter(t *testing.T) {
	dim := 16
	rng := rand.New(rand.NewSource(42))
	hw := newHNSWIndex(dim, 2, 99, defaultHNSWParams())

	query := randVec(rng, dim)
	hw.Add("a", query, "a", map[string]string{"source": "review"}, 1)
	hw.Add("b", query, "b", map[string]string{"source": "code"}, 2)
	hw.Add("c", randVec(rng, dim), "c", map[string]string{"source": "review"}, 3)

	results := hw.Search(query, 10, []FilterOption{Filter("source", "review")})
	for _, r := range results {
		if r.Metadata["source"] != "review" {
			t.Errorf("result %s has source=%s, want review", r.ID, r.Metadata["source"])
		}
	}
	if len(results) == 0 {
		t.Fatal("expected at least 1 filtered result")
	}
	// The query itself was inserted as "a" with source=review, so it must be top.
	if results[0].ID != "a" {
		t.Errorf("top filtered result = %s, want a", results[0].ID)
	}
}

func TestHNSWEmptySearch(t *testing.T) {
	hw := newHNSWIndex(16, 2, 42, defaultHNSWParams())
	results := hw.Search(randVec(rand.New(rand.NewSource(1)), 16), 5, nil)
	if len(results) != 0 {
		t.Fatalf("expected 0 results on empty index, got %d", len(results))
	}
}

func TestHNSWUpdateEntry(t *testing.T) {
	dim := 16
	rng := rand.New(rand.NewSource(42))
	hw := newHNSWIndex(dim, 2, 99, defaultHNSWParams())

	v1 := randVec(rng, dim)
	v2 := randVec(rng, dim)

	hw.Add("x", v1, "old", nil, 1)
	hw.Add("x", v2, "new", nil, 2)

	if hw.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after update", hw.Len())
	}

	results := hw.Search(v2, 1, nil)
	if len(results) != 1 || results[0].ID != "x" {
		t.Fatalf("unexpected results after update: %v", results)
	}
	if results[0].Text != "new" {
		t.Fatalf("text = %q, want %q", results[0].Text, "new")
	}
}

func TestHNSWRecallBruteForceComparison(t *testing.T) {
	// This test directly compares HNSW results against exact brute-force
	// inner-product computation (float64 precision).
	dim := 64
	n := 500
	k := 10
	rng := rand.New(rand.NewSource(555))

	hw := newHNSWIndex(dim, 4, 77, hnswParams{
		M:              16,
		EfConstruction: 200,
		EfSearch:       100,
	})

	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = randVec(rng, dim)
		hw.Add(fmt.Sprintf("v%d", i), vecs[i], "", nil, uint64(i+1))
	}

	nQueries := 50
	totalRecall := 0.0
	for q := 0; q < nQueries; q++ {
		query := randVec(rng, dim)

		// Exact brute-force top-k.
		type scored struct {
			id    string
			score float64
		}
		exact := make([]scored, n)
		for i, v := range vecs {
			var dot float64
			for j := range v {
				dot += float64(v[j]) * float64(query[j])
			}
			exact[i] = scored{fmt.Sprintf("v%d", i), dot}
		}
		sort.Slice(exact, func(a, b int) bool { return exact[a].score > exact[b].score })
		trueTopK := make(map[string]bool, k)
		for i := 0; i < k; i++ {
			trueTopK[exact[i].id] = true
		}

		hnswResults := hw.Search(query, k, nil)
		hits := 0
		for _, r := range hnswResults {
			if trueTopK[r.ID] {
				hits++
			}
		}
		totalRecall += float64(hits) / float64(k)
	}

	avgRecall := totalRecall / float64(nQueries)
	t.Logf("HNSW vs exact Recall@%d = %.3f", k, avgRecall)
	// Against exact brute-force, recall may be lower due to quantization.
	// We just want it to be reasonable.
	if avgRecall < 0.70 {
		t.Errorf("Recall@%d vs exact = %.2f, want >= 0.70", k, avgRecall)
	}
}
