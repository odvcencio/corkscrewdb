package corkscrewdb

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// hybridColl opens a fresh dense+sparse collection and returns it plus the db.
func hybridColl(t *testing.T, dim int) (*DB, *Collection) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "hy.csdb"), WithProvider(&mockProvider{dim: dim}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs", WithBitWidth(2), WithSparse())
	if coll.err != nil {
		t.Fatal(coll.err)
	}
	return db, coll
}

func ids(results []SearchResult) []string {
	out := make([]string, len(results))
	for i, r := range results {
		out[i] = r.ID
	}
	return out
}

func TestSearchMultiDenseOnly(t *testing.T) {
	db, coll := hybridColl(t, 8)
	defer db.Close()
	corpus := map[string][]float32{
		"a": {1, 0, 0, 0, 0, 0, 0, 0},
		"b": {0, 1, 0, 0, 0, 0, 0, 0},
		"c": {0.5, 0.5, 0, 0, 0, 0, 0, 0},
	}
	for id, v := range corpus {
		if err := coll.Put(id, Entry{Vector: v, Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}); err != nil {
			t.Fatal(err)
		}
	}
	q := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	baseline, err := coll.SearchVector(q, 3)
	if err != nil {
		t.Fatal(err)
	}
	fused, err := coll.SearchMulti(MultiQuery{Dense: q}, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Order matches the dense baseline.
	if strings.Join(ids(fused), ",") != strings.Join(ids(baseline), ",") {
		t.Fatalf("fused order %v != dense baseline %v", ids(fused), ids(baseline))
	}
	// Score is the fused RRF score, NOT the raw IP.
	want := RRFFusion{K: 60}.fuse(baseline)
	for i := range fused {
		if fused[i].Score != want[i].Score {
			t.Fatalf("result[%d] score = %v, want fused %v (not raw)", i, fused[i].Score, want[i].Score)
		}
	}
}

func TestSearchMultiSparseOnlyQuery(t *testing.T) {
	db, coll := hybridColl(t, 4)
	defer db.Close()
	put := func(id string, dense []float32, sv *SparseVector) {
		if err := coll.Put(id, Entry{Vector: dense, Sparse: sv}); err != nil {
			t.Fatal(err)
		}
	}
	put("a", []float32{1, 0, 0, 0}, &SparseVector{Indices: []uint32{0, 1}, Values: []float32{1, 1}})
	put("b", []float32{0, 1, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}})
	put("c", []float32{0, 0, 1, 0}, &SparseVector{Indices: []uint32{5}, Values: []float32{1}})

	sq := &SparseVector{Indices: []uint32{0, 1}, Values: []float32{1, 1}}
	fused, err := coll.SearchMulti(MultiQuery{Sparse: sq}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(fused) == 0 || fused[0].ID != "a" {
		t.Fatalf("sparse-only top = %v, want a", ids(fused))
	}
	// Score equals RRF over the sparse ranking, not the raw dot.
	if fused[0].Score == 2 { // raw dot of a would be 2
		t.Fatalf("score %v looks like raw dot, want fused", fused[0].Score)
	}
}

func TestSearchMultiDenseAndSparse(t *testing.T) {
	db, coll := hybridColl(t, 4)
	defer db.Close()
	put := func(id string, dense []float32, sv *SparseVector) {
		if err := coll.Put(id, Entry{Vector: dense, Sparse: sv}); err != nil {
			t.Fatal(err)
		}
	}
	// "b" is strong in BOTH channels; "a" strong in dense only; "c" strong in sparse only.
	put("a", []float32{1, 0, 0, 0}, &SparseVector{Indices: []uint32{9}, Values: []float32{1}})
	put("b", []float32{0.9, 0.1, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}})
	put("c", []float32{0, 0, 1, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}})

	dq := []float32{1, 0, 0, 0}
	sq := &SparseVector{Indices: []uint32{0}, Values: []float32{1}}
	fused, err := coll.SearchMulti(MultiQuery{Dense: dq, Sparse: sq}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(fused) == 0 || fused[0].ID != "b" {
		t.Fatalf("hybrid top = %v, want b (cross-channel agreement)", ids(fused))
	}
}

func TestSearchMultiTextResolvesDense(t *testing.T) {
	db, coll := hybridColl(t, 8)
	defer db.Close()
	if err := coll.Put("hello", Entry{Text: "hello", Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("world", Entry{Text: "world", Sparse: &SparseVector{Indices: []uint32{1}, Values: []float32{1}}}); err != nil {
		t.Fatal(err)
	}
	sq := &SparseVector{Indices: []uint32{0}, Values: []float32{1}}
	fused, err := coll.SearchMulti(MultiQuery{Text: "hello", Sparse: sq}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(fused) == 0 || fused[0].ID != "hello" {
		t.Fatalf("text-resolved hybrid top = %v, want hello", ids(fused))
	}
}

func TestSearchMultiFilterPushdownBothChannels(t *testing.T) {
	db, coll := hybridColl(t, 4)
	defer db.Close()
	put := func(id string, dense []float32, sv *SparseVector, meta map[string]string) {
		if err := coll.Put(id, Entry{Vector: dense, Sparse: sv, Metadata: meta}); err != nil {
			t.Fatal(err)
		}
	}
	put("keep", []float32{1, 0, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}}, map[string]string{"lang": "en"})
	put("drop", []float32{1, 0, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}}, map[string]string{"lang": "fr"})

	dq := []float32{1, 0, 0, 0}
	sq := &SparseVector{Indices: []uint32{0}, Values: []float32{1}}
	fused, err := coll.SearchMulti(MultiQuery{Dense: dq, Sparse: sq, Filters: []FilterOption{Filter("lang", "en")}}, 5)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range fused {
		if r.ID == "drop" {
			t.Fatalf("filtered id drop appeared in fused result %v", ids(fused))
		}
	}
	if _, ok := resultByID(fused, "keep"); !ok {
		t.Fatalf("kept id missing from %v", ids(fused))
	}
}

func TestSearchMultiErrors(t *testing.T) {
	db, coll := hybridColl(t, 4)
	defer db.Close()
	if err := coll.Put("a", Entry{Vector: []float32{1, 0, 0, 0}, Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}); err != nil {
		t.Fatal(err)
	}

	// Malformed sparse query.
	bad := &SparseVector{Indices: []uint32{1, 1}, Values: []float32{1, 1}}
	if _, err := coll.SearchMulti(MultiQuery{Sparse: bad}, 1); !errors.Is(err, ErrInvalidSparseVector) {
		t.Fatalf("malformed sparse err = %v, want ErrInvalidSparseVector", err)
	}

	// Sparse query on a non-WithSparse collection.
	db2, err := Open(filepath.Join(t.TempDir(), "nos.csdb"), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	plain := db2.Collection("plain", WithBitWidth(2))
	if err := plain.Put("a", Entry{Vector: []float32{1, 0, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.SearchMulti(MultiQuery{Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}, 1); err == nil {
		t.Fatal("expected error for sparse query on non-WithSparse collection")
	}

	// Dense dimension mismatch on a written collection.
	if _, err := coll.SearchMulti(MultiQuery{Dense: []float32{1, 2, 3}}, 1); err == nil {
		t.Fatal("expected dimension error")
	}

	// Zero present channels.
	if _, err := coll.SearchMulti(MultiQuery{}, 1); err == nil {
		t.Fatal("expected error for zero present channels")
	}
}

func TestSearchMultiEmptyDenseCollectionEdge(t *testing.T) {
	db, coll := hybridColl(t, 4)
	defer db.Close()
	// No writes: c.index == nil, c.dim == 0. Pure-dense query returns nil (mirror
	// searchVectorLocal), not a dimension error.
	got, err := coll.SearchMulti(MultiQuery{Dense: []float32{1, 0, 0, 0}}, 3)
	if err != nil {
		t.Fatalf("empty-dense-collection err = %v, want nil", err)
	}
	if got != nil {
		t.Fatalf("empty-dense-collection result = %v, want nil", got)
	}
}

func TestSearchMultiRemoteUnsupported(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "fed.csdb"), WithProvider(nil),
		WithShards(twoNodeShardLayout(LocalShardOwner, "127.0.0.1:1")...))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll := db.Collection("docs", WithBitWidth(2), WithSparse())
	if _, err := coll.SearchMulti(MultiQuery{Dense: []float32{1, 0}}, 1); !errors.Is(err, errMultiVectorRemoteUnsupported) {
		t.Fatalf("federated SearchMulti err = %v, want errMultiVectorRemoteUnsupported", err)
	}
}

func TestSearchMultiKLargerThanCandidates(t *testing.T) {
	db, coll := hybridColl(t, 4)
	defer db.Close()
	for _, id := range []string{"a", "b"} {
		if err := coll.Put(id, Entry{Vector: []float32{1, 0, 0, 0}, Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}); err != nil {
			t.Fatal(err)
		}
	}
	fused, err := coll.SearchMulti(MultiQuery{Dense: []float32{1, 0, 0, 0}, Sparse: &SparseVector{Indices: []uint32{0}, Values: []float32{1}}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(fused) != 2 {
		t.Fatalf("k>candidates returned %d, want 2", len(fused))
	}
}

func TestSearchMultiWeightedScoreSemantics(t *testing.T) {
	db, coll := hybridColl(t, 4)
	defer db.Close()
	put := func(id string, dense []float32, sv *SparseVector) {
		if err := coll.Put(id, Entry{Vector: dense, Sparse: sv}); err != nil {
			t.Fatal(err)
		}
	}
	put("a", []float32{1, 0, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{2}})
	put("b", []float32{0, 1, 0, 0}, &SparseVector{Indices: []uint32{0}, Values: []float32{1}})

	dq := []float32{1, 0, 0, 0}
	sq := &SparseVector{Indices: []uint32{0}, Values: []float32{1}}
	policy := WeightedFusion{Dense: 1, Sparse: 1}
	fused, err := coll.SearchMulti(MultiQuery{Dense: dq, Sparse: sq, Fusion: policy}, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Build the reference by re-running the channels and fusing with the same policy.
	denseRanking, err := coll.SearchVector(dq, 100)
	if err != nil {
		t.Fatal(err)
	}
	sparseRanking := sparseSearch(coll.sparseSet, sq, 100, func(string) bool { return true },
		func(id string) (string, map[string]string, uint64) { return "", nil, 0 })
	want := policy.fuse(denseRanking, sparseRanking)
	if len(fused) != len(want) {
		t.Fatalf("len %d, want %d", len(fused), len(want))
	}
	for i := range fused {
		if fused[i].ID != want[i].ID {
			t.Fatalf("order[%d] = %q, want %q", i, fused[i].ID, want[i].ID)
		}
		if fused[i].Score != want[i].Score {
			t.Fatalf("score[%d] = %v, want weighted-fused %v", i, fused[i].Score, want[i].Score)
		}
	}
}

func TestCollectionViewSearchMulti(t *testing.T) {
	db, coll := hybridColl(t, 4)
	defer db.Close()

	// v1 of "a".
	svA1 := &SparseVector{Indices: []uint32{0}, Values: []float32{1}}
	if err := coll.Put("a", Entry{Vector: []float32{1, 0, 0, 0}, Sparse: svA1}); err != nil {
		t.Fatal(err)
	}
	bound := coll.clock.Now() // capture Lamport bound after a v1
	coll.clock.Witness(bound)

	// v2 of "a" and a new id "b", both after the bound.
	svA2 := &SparseVector{Indices: []uint32{1}, Values: []float32{1}}
	if err := coll.Put("a", Entry{Vector: []float32{0, 1, 0, 0}, Sparse: svA2}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("b", Entry{Vector: []float32{0, 0, 1, 0}, Sparse: &SparseVector{Indices: []uint32{2}, Values: []float32{1}}}); err != nil {
		t.Fatal(err)
	}

	view := coll.At(bound)
	if view.err != nil {
		t.Fatal(view.err)
	}
	// As of the bound, "a" is at v1 and "b" is not yet written.
	if !sparseEqual(view.sparseSet["a"], svA1) {
		t.Fatalf("view.sparseSet[a] = %v, want v1 %v", view.sparseSet["a"], svA1)
	}
	if _, ok := view.sparseSet["b"]; ok {
		t.Fatal("view.sparseSet[b] should be absent as of bound")
	}

	dq := []float32{1, 0, 0, 0}
	sq := &SparseVector{Indices: []uint32{0}, Values: []float32{1}}
	fused, err := view.SearchMulti(MultiQuery{Dense: dq, Sparse: sq}, 5)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range fused {
		if r.ID == "b" {
			t.Fatalf("b must not appear in view as of bound: %v", ids(fused))
		}
	}
	if len(fused) == 0 || fused[0].ID != "a" {
		t.Fatalf("view hybrid top = %v, want a", ids(fused))
	}
	// Score is the fused convention.
	denseRanking, err := view.SearchVector(dq, 100)
	if err != nil {
		t.Fatal(err)
	}
	sparseRanking := sparseSearch(view.sparseSet, sq, 100, func(string) bool { return true },
		func(id string) (string, map[string]string, uint64) { return "", nil, 0 })
	want := RRFFusion{K: 60}.fuse(denseRanking, sparseRanking)
	if fused[0].Score != want[0].Score {
		t.Fatalf("view fused score = %v, want %v (fused convention)", fused[0].Score, want[0].Score)
	}
}
