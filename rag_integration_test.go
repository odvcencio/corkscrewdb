package corkscrewdb

import (
	"path/filepath"
	"sync"
	"testing"
)

// rank returns the 0-based position of id in results, or -1 if absent.
func rank(results []SearchResult, id string) int {
	for i, r := range results {
		if r.ID == id {
			return i
		}
	}
	return -1
}

// TestHybridSearchCanonicalWin proves the canonical hybrid win: a doc that is
// merely mid-ranked in BOTH channels (top in neither) is promoted above the
// per-channel leader of at least one channel once the channels are fused,
// because cross-channel agreement accumulates.
func TestHybridSearchCanonicalWin(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "rag.csdb"), WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Fixed quantizer seed + bitWidth 4 + well-separated dense magnitudes make the
	// dense ranking deterministic and robust to quantization noise.
	coll := db.Collection("docs", WithBitWidth(4), WithQuantizerSeed(42), WithSparse())

	// Dense query points along axis 0. Sparse query weights index 0.
	dq := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	sq := &SparseVector{Indices: []uint32{0}, Values: []float32{1}}

	// Corpus: 8 docs.
	//  - dTop:  dense leader (strongest along axis 0), absent in sparse.
	//  - sTop:  sparse leader (largest weight at index 0), weak in dense.
	//  - X:     mid in dense AND mid in sparse (the cross-channel agreer).
	//  - filler docs spread across both rankings.
	type doc struct {
		id     string
		dense  []float32
		sparse *SparseVector
	}
	// Dense vectors are unit vectors at well-separated angles from axis 0 so the
	// dense ranking (cosine to dq) is robust to bitWidth-4 quantization noise.
	// Channel design:
	//   - denseOnly: dense leader (angle 0), ABSENT from the sparse query index.
	//   - sparseOnly: sparse leader (huge weight at index 0), near-orthogonal in
	//     dense (angle ~90°).
	//   - X: high-mid in BOTH (dense rank 1, sparse rank 1; top in neither) — its
	//     min-max norms in both channels are high, so it overtakes each
	//     channel-exclusive leader once the channels are fused.
	//   - f*: low-scoring fillers in both channels (pull each min down so X's
	//     normalized scores stay high).
	corpus := []doc{
		{"denseOnly", []float32{1.00, 0.00, 0, 0, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{9}, Values: []float32{1}}},
		{"X", []float32{0.70, 0.71, 0, 0, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{8}}},
		{"sparseOnly", []float32{0.05, 1.00, 0, 0, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{9}}},
		{"f1", []float32{0.00, 1.00, 0, 0, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}}},
		{"f2", []float32{0.00, 0.00, 1, 0, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}}},
		{"f3", []float32{0.00, 0.00, 0, 1, 0, 0, 0, 0}, &SparseVector{Indices: []uint32{7}, Values: []float32{1}}},
		{"f4", []float32{0.00, 0.00, 0, 0, 1, 0, 0, 0}, &SparseVector{Indices: []uint32{8}, Values: []float32{1}}},
		{"f5", []float32{0.00, 0.00, 0, 0, 0, 1, 0, 0}, &SparseVector{Indices: []uint32{6}, Values: []float32{1}}},
	}
	for _, d := range corpus {
		if err := coll.Put(d.id, Entry{Vector: d.dense, Sparse: d.sparse}); err != nil {
			t.Fatal(err)
		}
	}

	// Single-channel baselines.
	denseBaseline, err := coll.SearchVector(dq, 8)
	if err != nil {
		t.Fatal(err)
	}
	sparseBaseline := sparseSearch(coll.sparseSet, sq, 8, func(string) bool { return true },
		func(id string) (string, map[string]string, uint64) { return "", nil, 0 })

	dDenseRank := rank(denseBaseline, "X")
	dSparseRank := rank(sparseBaseline, "X")
	t.Logf("dense baseline order: %v (X at %d)", ids(denseBaseline), dDenseRank)
	t.Logf("sparse baseline order: %v (X at %d)", ids(sparseBaseline), dSparseRank)

	// X must be top in NEITHER channel (the premise of the canonical win).
	if dDenseRank <= 0 {
		t.Fatalf("X is dense rank %d; test premise requires X not be dense top", dDenseRank)
	}
	if dSparseRank <= 0 {
		t.Fatalf("X is sparse rank %d; test premise requires X not be sparse top", dSparseRank)
	}

	check := func(name string, fused []SearchResult) {
		xRank := rank(fused, "X")
		if xRank < 0 {
			t.Fatalf("%s: X absent from fused result %v", name, ids(fused))
		}
		dTopRank := rank(fused, "denseOnly")
		sTopRank := rank(fused, "sparseOnly")
		t.Logf("%s fused order: %v (X=%d denseOnly=%d sparseOnly=%d)", name, ids(fused), xRank, dTopRank, sTopRank)
		// The canonical hybrid win: X (mid in both channels, top in neither) is
		// promoted above at least one per-channel leader once channels are fused.
		if !(xRank < dTopRank || xRank < sTopRank) {
			t.Fatalf("%s: hybrid failed to promote X above either channel leader (X=%d denseOnly=%d sparseOnly=%d)",
				name, xRank, dTopRank, sTopRank)
		}
	}

	rrf, err := coll.SearchMulti(MultiQuery{Dense: dq, Sparse: sq}, 8)
	if err != nil {
		t.Fatal(err)
	}
	check("RRF", rrf)

	weighted, err := coll.SearchMulti(MultiQuery{Dense: dq, Sparse: sq, Fusion: WeightedFusion{Dense: 1, Sparse: 1}}, 8)
	if err != nil {
		t.Fatal(err)
	}
	check("Weighted", weighted)

	// Tighten: assert the exact RRF fused order so a no-fuse regression (returning a
	// raw single-channel ranking) would be caught.
	wantRRF := RRFFusion{K: 60}.fuse(denseBaseline, sparseBaseline)
	if len(rrf) != len(wantRRF) {
		t.Fatalf("RRF len %d, want %d", len(rrf), len(wantRRF))
	}
	for i := range rrf {
		if rrf[i].ID != wantRRF[i].ID || rrf[i].Score != wantRRF[i].Score {
			t.Fatalf("RRF[%d] = %+v, want %+v", i, rrf[i], wantRRF[i])
		}
	}
}

// TestHybridSearchRaceConcurrentPutAndSearchMulti exercises the snapshot
// discipline of SearchMulti under -race: concurrent Put (dense+sparse) and
// SearchMulti must not race because SearchMulti snapshots idx/sparseSet/hydrate
// under c.mu.RLock before scoring.
func TestHybridSearchRaceConcurrentPutAndSearchMulti(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "race.csdb"), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll := db.Collection("docs", WithBitWidth(2), WithSparse())

	// Seed so the index exists before searches start.
	if err := coll.Put("seed", Entry{Vector: []float32{1, 0, 0, 0}, Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}); err != nil {
		t.Fatal(err)
	}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			id := "doc-" + string(rune('A'+(i%26))) + string(rune('0'+(i%10)))
			_ = coll.Put(id, Entry{
				Vector: []float32{float32(i%4) + 1, 0, 0, 0},
				Sparse: &SparseVector{Indices: []uint32{uint32(i % 4)}, Values: []float32{1}},
			})
		}
	}()

	go func() {
		defer wg.Done()
		dq := []float32{1, 0, 0, 0}
		sq := &SparseVector{Indices: []uint32{0}, Values: []float32{1}}
		for i := 0; i < n; i++ {
			if _, err := coll.SearchMulti(MultiQuery{Dense: dq, Sparse: sq}, 5); err != nil {
				t.Errorf("SearchMulti during race: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}
