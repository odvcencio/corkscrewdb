package corkscrewdb

import (
	"math"
	"testing"
)

func resultByID(results []SearchResult, id string) (SearchResult, bool) {
	for _, r := range results {
		if r.ID == id {
			return r, true
		}
	}
	return SearchResult{}, false
}

func TestRRFFusion(t *testing.T) {
	dense := []SearchResult{
		{ID: "a", Score: 10},
		{ID: "b", Score: 9},
		{ID: "c", Score: 8},
	}
	sparse := []SearchResult{
		{ID: "b", Score: 5},
		{ID: "c", Score: 4},
		{ID: "d", Score: 3},
	}
	got := RRFFusion{K: 60}.fuse(dense, sparse)

	// Hand-computed fused scores.
	want := map[string]float64{
		"a": 1.0 / (60 + 1),               // dense rank 1
		"b": 1.0/(60+2) + 1.0/(60+1),      // dense rank 2 + sparse rank 1
		"c": 1.0/(60+3) + 1.0/(60+2),      // dense rank 3 + sparse rank 2
		"d": 1.0 / (60 + 3),               // sparse rank 3
	}
	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d", len(got), len(want))
	}
	for id, w := range want {
		r, ok := resultByID(got, id)
		if !ok {
			t.Fatalf("missing id %q", id)
		}
		if math.Abs(float64(r.Score)-w) > 1e-6 {
			t.Fatalf("id %q score = %v, want %v", id, r.Score, w)
		}
	}
	// Order: b (0.0325), c (0.0319), a (0.0164), d (0.0159) — score desc, id asc.
	wantOrder := []string{"b", "c", "a", "d"}
	for i, id := range wantOrder {
		if got[i].ID != id {
			t.Fatalf("order[%d] = %q, want %q (%v)", i, got[i].ID, id, got)
		}
	}
}

func TestRRFFusionDedup(t *testing.T) {
	dense := []SearchResult{{ID: "x", Score: 1}}
	sparse := []SearchResult{{ID: "x", Score: 1}}
	got := RRFFusion{K: 60}.fuse(dense, sparse)
	if len(got) != 1 {
		t.Fatalf("got %d results, want 1 (deduped)", len(got))
	}
	want := 1.0/(60+1) + 1.0/(60+1)
	if math.Abs(float64(got[0].Score)-want) > 1e-6 {
		t.Fatalf("dedup score = %v, want %v", got[0].Score, want)
	}
}

func TestRRFDefaultK(t *testing.T) {
	dense := []SearchResult{{ID: "a", Score: 3}, {ID: "b", Score: 2}}
	sparse := []SearchResult{{ID: "b", Score: 9}, {ID: "c", Score: 1}}
	withZero := RRFFusion{}.fuse(dense, sparse)
	with60 := RRFFusion{K: 60}.fuse(dense, sparse)
	if len(withZero) != len(with60) {
		t.Fatalf("len mismatch: %d vs %d", len(withZero), len(with60))
	}
	for i := range withZero {
		if withZero[i].ID != with60[i].ID || withZero[i].Score != with60[i].Score {
			t.Fatalf("RRFFusion{} != RRFFusion{K:60} at %d: %+v vs %+v", i, withZero[i], with60[i])
		}
	}
}

func TestRRFTextMetadataFromFirstRanking(t *testing.T) {
	dense := []SearchResult{{ID: "a", Score: 1, Text: "dense-a", Metadata: map[string]string{"src": "dense"}, Version: 1}}
	sparse := []SearchResult{{ID: "a", Score: 1, Text: "sparse-a", Metadata: map[string]string{"src": "sparse"}, Version: 2}}
	got := RRFFusion{}.fuse(dense, sparse)
	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if got[0].Text != "dense-a" || got[0].Metadata["src"] != "dense" || got[0].Version != 1 {
		t.Fatalf("Text/Metadata/Version not taken from first ranking: %+v", got[0])
	}
}

func TestWeightedFusionMinMax(t *testing.T) {
	dense := []SearchResult{
		{ID: "a", Score: 10}, // norm = (10-2)/(10-2) = 1
		{ID: "b", Score: 6},  // norm = (6-2)/8 = 0.5
		{ID: "c", Score: 2},  // norm = 0
	}
	sparse := []SearchResult{
		{ID: "b", Score: 4}, // norm = (4-0)/(4-0) = 1
		{ID: "c", Score: 0}, // norm = 0
	}
	got := WeightedFusion{Dense: 1, Sparse: 1}.fuse(dense, sparse)
	want := map[string]float64{
		"a": 1*1.0 + 1*0.0,  // dense norm 1, not in sparse
		"b": 1*0.5 + 1*1.0,  // dense norm 0.5, sparse norm 1
		"c": 1*0.0 + 1*0.0,  // dense norm 0, sparse norm 0
	}
	if len(got) != 3 {
		t.Fatalf("got %d, want 3", len(got))
	}
	for id, w := range want {
		r, ok := resultByID(got, id)
		if !ok {
			t.Fatalf("missing id %q", id)
		}
		if math.Abs(float64(r.Score)-w) > 1e-6 {
			t.Fatalf("id %q score = %v, want %v", id, r.Score, w)
		}
	}
}

func TestWeightedFusionHiEqLo(t *testing.T) {
	// All-equal scores -> norm == 1 for every member, never NaN.
	dense := []SearchResult{
		{ID: "a", Score: 5},
		{ID: "b", Score: 5},
	}
	got := WeightedFusion{Dense: 2}.fuse(dense)
	for _, r := range got {
		if math.IsNaN(float64(r.Score)) {
			t.Fatalf("NaN score for %q", r.ID)
		}
		if math.Abs(float64(r.Score)-2.0) > 1e-6 {
			t.Fatalf("id %q score = %v, want 2 (weight*1)", r.ID, r.Score)
		}
	}
	// Single-element ranking: norm == 1.
	single := WeightedFusion{Dense: 3}.fuse([]SearchResult{{ID: "z", Score: 99}})
	if len(single) != 1 || math.Abs(float64(single[0].Score)-3.0) > 1e-6 {
		t.Fatalf("single-element fuse = %+v, want score 3", single)
	}
}

func TestWeightedFusionWeightZero(t *testing.T) {
	dense := []SearchResult{{ID: "a", Score: 10}, {ID: "b", Score: 1}}
	sparse := []SearchResult{{ID: "c", Score: 100}}
	got := WeightedFusion{Dense: 1, Sparse: 0}.fuse(dense, sparse)
	r, ok := resultByID(got, "c")
	if !ok {
		t.Fatal("missing id c")
	}
	if r.Score != 0 {
		t.Fatalf("sparse-only id c score = %v, want 0 (weight zero)", r.Score)
	}
}

func TestWeightedFusionSingleListNonIdentity(t *testing.T) {
	one := []SearchResult{
		{ID: "a", Score: 100},
		{ID: "b", Score: 50},
		{ID: "c", Score: 0},
	}
	got := WeightedFusion{Dense: 1}.fuse(one)
	// Min-max normalized, NOT raw scores: top -> 1*Dense, bottom -> 0.
	top, _ := resultByID(got, "a")
	mid, _ := resultByID(got, "b")
	bot, _ := resultByID(got, "c")
	if math.Abs(float64(top.Score)-1.0) > 1e-6 {
		t.Fatalf("top score = %v, want 1 (not raw 100)", top.Score)
	}
	if math.Abs(float64(mid.Score)-0.5) > 1e-6 {
		t.Fatalf("mid score = %v, want 0.5", mid.Score)
	}
	if math.Abs(float64(bot.Score)-0.0) > 1e-6 {
		t.Fatalf("bottom score = %v, want 0", bot.Score)
	}
}

func TestFusionDeterminism(t *testing.T) {
	// Build rankings where several ids fuse to equal scores; shuffle insertion order.
	mk := func() ([]SearchResult, []SearchResult) {
		dense := []SearchResult{
			{ID: "b", Score: 1},
			{ID: "a", Score: 1},
			{ID: "c", Score: 1},
		}
		sparse := []SearchResult{
			{ID: "c", Score: 1},
			{ID: "a", Score: 1},
			{ID: "b", Score: 1},
		}
		return dense, sparse
	}
	for _, policy := range []FusionPolicy{RRFFusion{}, WeightedFusion{Dense: 1, Sparse: 1}} {
		var prev []SearchResult
		for trial := 0; trial < 5; trial++ {
			d, s := mk()
			got := policy.fuse(d, s)
			if prev != nil {
				if len(got) != len(prev) {
					t.Fatalf("non-deterministic length")
				}
				for i := range got {
					if got[i].ID != prev[i].ID {
						t.Fatalf("non-deterministic order at %d: %q vs %q", i, got[i].ID, prev[i].ID)
					}
				}
			}
			prev = got
		}
	}
}

func TestFusionEmptyInputs(t *testing.T) {
	for _, policy := range []FusionPolicy{RRFFusion{}, WeightedFusion{Dense: 1, Sparse: 1}} {
		if got := policy.fuse(); got != nil {
			t.Fatalf("%T fuse() = %v, want nil", policy, got)
		}
		if got := policy.fuse(nil); got != nil {
			t.Fatalf("%T fuse(nil) = %v, want nil", policy, got)
		}
		if got := policy.fuse(nil, nil); got != nil {
			t.Fatalf("%T fuse(nil,nil) = %v, want nil", policy, got)
		}
	}
	// Single non-empty ranking -> rescored (RRF) / normalized (weighted), non-empty.
	one := []SearchResult{{ID: "a", Score: 7}, {ID: "b", Score: 3}}
	if got := (RRFFusion{}).fuse(one); len(got) != 2 {
		t.Fatalf("RRF single fuse len = %d, want 2", len(got))
	}
	if got := (WeightedFusion{Dense: 1}).fuse(one); len(got) != 2 {
		t.Fatalf("weighted single fuse len = %d, want 2", len(got))
	}
}
