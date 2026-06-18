package corkscrewdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"m31labs.dev/turboquant"
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

	hw := newHNSWIndex(dim, 4, 77, HNSWParams{
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

	hw := newHNSWIndex(dim, 4, 77, HNSWParams{
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

// addVec is a small helper that quantizes and inserts a vector into the flat
// store of an hnswIndex WITHOUT graph wiring, returning the node slot. Tests use
// it to build a controlled candidate set for the selection heuristics.
func addFlatVec(h *hnswIndex, id string, v []float32) int32 {
	qv := h.flat.quantizer.Quantize(v)
	h.flat.addQuantized(id, qv, "", nil, 1)
	h.flat.mu.RLock()
	pos := h.flat.idIndex[id]
	h.flat.mu.RUnlock()
	return int32(pos)
}

func TestSelectNeighborsHeuristicRNGDrops(t *testing.T) {
	dim := 8
	// bit width 4: fine enough quantization that the candidate ordering by sim-to-q
	// matches the float layout (r before its near-duplicate c1), so the RNG
	// domination (sim(c1,r) > sim(c1,q)) is exercised deterministically.
	h := newHNSWIndex(dim, 4, 1, defaultHNSWParams())

	// q is the inserted node's query position.
	q := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	// r is already a strong candidate near q.
	r := []float32{0.9, 0.1, 0, 0, 0, 0, 0, 0}
	// c1 is a near-duplicate of r (so sim(c1,r) > sim(c1,q)) -> RNG-dominated by r.
	c1 := []float32{0.88, 0.12, 0, 0, 0, 0, 0, 0}
	// c2 is diverse / far from r but still has some sim to q -> kept.
	c2 := []float32{0.2, 0, 0.9, 0, 0, 0, 0, 0}

	rIdx := addFlatVec(h, "r", r)
	c1Idx := addFlatVec(h, "c1", c1)
	c2Idx := addFlatVec(h, "c2", c2)

	pq := h.flat.quantizer.PrepareQuery(q)
	score := func(idx int32) float32 {
		return h.flat.quantizer.InnerProductPrepared(h.flat.entries[int(idx)].qv, pq)
	}
	// Candidates sorted by descending sim to q: r (closest), then c1, then c2.
	cands := []hnswCandidate{
		{idx: rIdx, score: score(rIdx)},
		{idx: c1Idx, score: score(c1Idx)},
		{idx: c2Idx, score: score(c2Idx)},
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].score > cands[j].score })

	selected := h.selectNeighborsHeuristic(cands, 2)
	got := map[int32]bool{}
	for _, s := range selected {
		got[s] = true
	}
	if !got[rIdx] {
		t.Fatalf("RNG heuristic dropped strong candidate r")
	}
	if got[c1Idx] {
		t.Fatalf("RNG heuristic kept near-duplicate c1 (should be dominated by r)")
	}
	if !got[c2Idx] {
		t.Fatalf("RNG heuristic dropped diverse candidate c2")
	}
}

func TestPruneNeighborsHeuristicKeepsHub(t *testing.T) {
	dim := 8
	h := newHNSWIndex(dim, 4, 7, defaultHNSWParams())

	// node n at the center.
	n := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	nIdx := addFlatVec(h, "n", n)

	// A tight OFF-AXIS cluster of non-hub neighbors: members are nearly identical
	// to each other (high mutual sim) but only moderately similar to n, so each
	// member RNG-dominates the others (sim(ci,cj) > sim(ci,n)).
	cluster := [][]float32{
		{0.60, 0.60, 0, 0, 0, 0, 0, 0},
		{0.61, 0.59, 0, 0, 0, 0, 0, 0},
		{0.59, 0.61, 0, 0, 0, 0, 0, 0},
	}
	var clusterIdx []int32
	for i, v := range cluster {
		clusterIdx = append(clusterIdx, addFlatVec(h, fmt.Sprintf("c%d", i), v))
	}
	// A hub: a further neighbor (lower sim to n -> sorts after the cluster) with a
	// high layer-0 degree. Despite sorting last, hub exemption keeps it.
	hub := []float32{0.4, 0, 0.4, 0, 0, 0, 0, 0}
	hubIdx := addFlatVec(h, "hub", hub)

	// Mark hub as a hub by giving it a high layer-0 degree.
	h.ensureDegreeSlot(int(hubIdx))
	h.degree[hubIdx] = int32(float32(h.params.M)*h.hubFactor) + 1

	neighbors := append([]int32{}, clusterIdx...)
	neighbors = append(neighbors, hubIdx)

	// maxN=2: heuristic must keep the hub (exempt) plus the closest cluster member,
	// dropping a dominated cluster member.
	kept := h.pruneNeighborsHeuristic(int(nIdx), neighbors, 2)
	keptSet := map[int32]bool{}
	for _, k := range kept {
		keptSet[k] = true
	}
	if !keptSet[hubIdx] {
		t.Fatalf("prune dropped the hub neighbor (should be exempt from RNG domination)")
	}
	if len(kept) > 2 {
		t.Fatalf("prune kept %d > maxN=2 neighbors", len(kept))
	}
	// At least one dominated cluster member must have been dropped.
	dropped := 0
	for _, c := range clusterIdx {
		if !keptSet[c] {
			dropped++
		}
	}
	if dropped == 0 {
		t.Fatalf("prune kept all dominated cluster members; expected RNG domination to drop some")
	}
}

func TestHNSWDeterministicBuildAndDegree(t *testing.T) {
	dim := 16
	build := func() *hnswIndex {
		h := newHNSWIndex(dim, 2, 4242, HNSWParams{M: 8, EfConstruction: 100, EfSearch: 50})
		rng := rand.New(rand.NewSource(2026)) // fixed seed -> deterministic dataset
		// 5 clusters x 60 points.
		for cl := 0; cl < 5; cl++ {
			center := randVec(rng, dim)
			for p := 0; p < 60; p++ {
				v := make([]float32, dim)
				for j := range v {
					v[j] = center[j] + (rng.Float32()*0.2 - 0.1)
				}
				h.Add(fmt.Sprintf("c%d-p%d", cl, p), v, "", nil, uint64(cl*60+p+1))
			}
		}
		return h
	}

	h1 := build()
	h2 := build()
	b1, err := marshalHNSWFile(h1)
	if err != nil {
		t.Fatalf("marshal h1: %v", err)
	}
	b2, err := marshalHNSWFile(h2)
	if err != nil {
		t.Fatalf("marshal h2: %v", err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("deterministic build produced non-identical marshalled bytes (%d vs %d bytes)", len(b1), len(b2))
	}

	// Average layer-0 degree must never exceed the 2*M cap (the RNG heuristic +
	// hub-protected re-prune both bound the list at maxNeighbors(0)=2*M). On this
	// tightly-clustered dataset the extendCandidates backfill keeps the inserted
	// node's forward edges at the cap, so the OBSERVED average is 2*M (16 at M=8) —
	// the heuristic changes *which* edges are kept (diversity/RNG domination,
	// witnessed by the unit tests above) without dropping below the top-M cap here.
	// The load-bearing assertion of this test is byte-identical determinism above;
	// the degree bound is the "no worse than top-M" sanity guard.
	var total, count int
	for _, neighbors := range h1.layers[0] {
		if neighbors == nil {
			continue
		}
		total += len(neighbors)
		count++
	}
	if count == 0 {
		t.Fatal("no layer-0 nodes")
	}
	avg := float64(total) / float64(count)
	cap0 := float64(2 * h1.params.M)
	t.Logf("avg layer-0 degree = %.3f (M=%d, 2*M cap=%d)", avg, h1.params.M, 2*h1.params.M)
	if avg > cap0 {
		t.Errorf("avg layer-0 degree %.3f exceeds 2*M cap %.1f", avg, cap0)
	}
}

func TestHNSWAddQuantizedBuildsFromCodes(t *testing.T) {
	dim, bits, seed := 8, 2, int64(42)
	qz := turboquant.NewIPWithSeed(dim, bits, seed)
	h := newHNSWIndex(dim, bits, seed, defaultHNSWParams())
	vecs := map[string][]float32{}
	for i := 0; i < 200; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32((i*7+j)%13) - 6
		}
		id := fmt.Sprintf("id-%d", i)
		vecs[id] = v
		qv := qz.Quantize(v)
		h.AddQuantized(id, qv, "", nil, uint64(i+1)) // NO raw vector supplied
	}
	if h.Len() != 200 {
		t.Fatalf("want 200 nodes, got %d", h.Len())
	}
	// This synthetic dataset (v[j] = (i*7+j)%13 - 6) is periodic, so ~15 ids share
	// id-100's exact vector and tie at the maximum score. With the RNG/LEANN
	// heuristic the *which* of the tied class surfaces in a tiny top-5 is arbitrary
	// (and even the flat ground truth would prefer lower-index ties via the heap),
	// so we assert the genuine correctness property: the self-vector is reachable
	// (returned within a k that spans the tie class) and the top result achieves the
	// self-vector's exact maximal score.
	res := h.Search(vecs["id-100"], 20, nil)
	if len(res) == 0 {
		t.Fatalf("empty search result for self-vector")
	}
	selfScore := h.flat.quantizer.InnerProductPrepared(
		h.flat.entries[h.flat.idIndex["id-100"]].qv,
		h.flat.quantizer.PrepareQuery(vecs["id-100"]),
	)
	if res[0].Score < selfScore {
		t.Fatalf("top result score %.6f below self-vector score %.6f", res[0].Score, selfScore)
	}
	found := false
	for _, r := range res {
		if r.ID == "id-100" {
			found = true
		}
	}
	if !found {
		t.Fatalf("self-vector not reachable in top-20: %+v", res)
	}
}
