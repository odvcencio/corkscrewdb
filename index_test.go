package corkscrewdb

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func randVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

func TestIndexAddAndSearch(t *testing.T) {
	idx := newIndex(32, 2, 42)
	rng := rand.New(rand.NewSource(99))
	vecs := make([][]float32, 10)
	for i := range vecs {
		vecs[i] = randVec(rng, 32)
		idx.Add(fmt.Sprintf("v%d", i), vecs[i], "", nil, uint64(i+1))
	}
	if idx.Len() != 10 {
		t.Fatalf("Len = %d, want 10", idx.Len())
	}
	results := idx.Search(vecs[0], 1, nil)
	if len(results) != 1 || results[0].ID != "v0" {
		t.Fatalf("top result = %v, want v0", results)
	}
}

func TestIndexRemove(t *testing.T) {
	idx := newIndex(32, 2, 42)
	rng := rand.New(rand.NewSource(99))
	idx.Add("a", randVec(rng, 32), "", nil, 1)
	idx.Add("b", randVec(rng, 32), "", nil, 2)
	idx.Add("c", randVec(rng, 32), "", nil, 3)
	if !idx.Remove("b") {
		t.Fatal("Remove(b) returned false")
	}
	if idx.Len() != 2 {
		t.Fatalf("Len = %d, want 2", idx.Len())
	}
}

func TestIndexFilter(t *testing.T) {
	idx := newIndex(16, 2, 42)
	rng := rand.New(rand.NewSource(123))
	query := randVec(rng, 16)
	idx.Add("a", query, "a", map[string]string{"source": "review"}, 1)
	idx.Add("b", query, "b", map[string]string{"source": "code"}, 2)
	results := idx.Search(query, 10, []FilterOption{Filter("source", "review")})
	if len(results) != 1 || results[0].ID != "a" {
		t.Fatalf("filtered results = %v, want only a", results)
	}
}

func TestIndexRecall(t *testing.T) {
	dim := 64
	n := 200
	k := 10
	rng := rand.New(rand.NewSource(123))
	idx := newIndex(dim, 4, 77)
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = randVec(rng, dim)
		idx.Add(fmt.Sprintf("v%d", i), vecs[i], "", nil, uint64(i+1))
	}
	nQueries := 100
	totalRecall := 0.0
	for q := 0; q < nQueries; q++ {
		query := randVec(rng, dim)
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
		trueTopK := map[string]bool{}
		for i := 0; i < k; i++ {
			trueTopK[exact[i].id] = true
		}
		results := idx.Search(query, k, nil)
		hits := 0
		for _, r := range results {
			if trueTopK[r.ID] {
				hits++
			}
		}
		totalRecall += float64(hits) / float64(k)
	}
	avgRecall := totalRecall / float64(nQueries)
	if avgRecall < 0.78 {
		t.Errorf("Recall@%d = %.2f, want >= 0.78", k, avgRecall)
	}
	t.Logf("Recall@%d = %.3f", k, avgRecall)
}
