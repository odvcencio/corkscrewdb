package corkscrewdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	snap "m31labs.dev/corkscrewdb/snapshot"
	walpkg "m31labs.dev/corkscrewdb/wal"
	"m31labs.dev/turboquant"
)

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

func TestWithoutRawStorePutSearchReopenHistoryAndPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no-raw.csdb")
	seed := int64(5581486560434873699)
	raw := []float32{0.1234567, -0.2345678, 0.3456789, -0.456789}

	db, err := Open(path, WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("vecs", WithBitWidth(4), WithQuantizerSeed(seed), WithoutRawStore())
	if err := coll.PutVector("v1", raw, WithText("unit-x"), WithMetadata(map[string]string{"source": "code"})); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("v2", Entry{Text: "unit-y", Metadata: map[string]string{"source": "text"}}); err != nil {
		t.Fatal(err)
	}
	history, err := coll.History("v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	if len(history[0].RawHash) != 0 {
		t.Fatalf("WithoutRawStore history RawHash len = %d, want 0", len(history[0].RawHash))
	}
	if history[0].Quantized == nil {
		t.Fatalf("history version missing quantized payload: %+v", history[0])
	}
	if history[0].Text != "unit-x" || history[0].Metadata["source"] != "code" {
		t.Fatalf("history metadata/text not preserved: %+v", history[0])
	}
	firstClock := history[0].LamportClock

	results, err := coll.SearchVector(raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(results, "v1") {
		t.Fatalf("results = %v, want v1", results)
	}
	atResults, err := coll.At(firstClock).SearchVector(raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(atResults, "v1") {
		t.Fatalf("at results = %v, want v1", atResults)
	}
	// HNSW now builds from codes on any collection, raw store or not.
	if err := coll.RebuildIndex(IndexHNSW); err != nil {
		t.Fatalf("RebuildIndex(IndexHNSW) err = %v, want success", err)
	}
	if got := db.manifest.Collections["vecs"].RawStore; got {
		t.Fatalf("manifest raw store = %v, want false", got)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	rvsMatches, _ := filepath.Glob(filepath.Join(path, "collections", "vecs", "raw", "raw-*.rvs"))
	if len(rvsMatches) != 0 {
		t.Fatalf("WithoutRawStore wrote .rvs segments: %v", rvsMatches)
	}
	assertFilesDoNotContainRawVector(t, filepath.Join(path, "collections", "vecs"), raw)
	walBytes := totalWALBytes(t, filepath.Join(path, "collections", "vecs", "wal"))
	if walBytes > 128 {
		t.Fatalf("WAL bytes after snapshot prune = %d, want compact empty tail", walBytes)
	}

	db, err = Open(path, WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll = db.Collection("vecs", WithoutRawStore())
	reopened, err := coll.SearchVector(raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !sameResultIDs(results, reopened) {
		t.Fatalf("search results changed after reopen: before=%v after=%v", results, reopened)
	}
	reopenedHistory, err := coll.History("v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(reopenedHistory) != 1 || len(reopenedHistory[0].RawHash) != 0 || reopenedHistory[0].Text != "unit-x" {
		t.Fatalf("reopened history = %+v, want text with no raw hash", reopenedHistory)
	}
}

func TestQuantizedOnlyPackedParentMultiVectorLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "packed-parent.csdb")
	seed := int64(5581486560434873699)
	oldQuery := []float32{0, 1, 0, 0}
	newQuery := []float32{0, 0, 0, 1}

	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("vecs", WithBitWidth(4), WithQuantizerSeed(seed), WithoutRawStore())
	if err := coll.PutVector("single", oldQuery, WithText("single vector"), WithMetadata(map[string]string{"kind": "single"})); err != nil {
		t.Fatal(err)
	}
	if err := coll.PutMultiVector("parent-1", MultiVectorEntry{
		Text:     "parent text",
		Metadata: map[string]string{"tenant": "acme", "type": "article"},
		Children: []MultiVectorChild{
			{ID: "c-old", Vector: oldQuery, Text: "old child", Metadata: map[string]string{"slot": "old", "kind": "code"}},
			{ID: "c-other", Vector: []float32{0, 0, 1, 0}, Text: "other child", Metadata: map[string]string{"slot": "other", "kind": "note"}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	history, err := coll.History("parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || len(history[0].Children) != 2 {
		t.Fatalf("history = %+v, want one packed parent version with children", history)
	}
	if history[0].Children[0].Quantized == nil {
		t.Fatalf("child history = %+v, want quantized child payload", history[0].Children[0])
	}
	firstClock := history[0].LamportClock

	parentResults, err := coll.SearchParentsVector(oldQuery, 5, WithParentFilters(Filter("tenant", "acme")), WithChildFilters(Filter("kind", "code")))
	if err != nil {
		t.Fatal(err)
	}
	if len(parentResults) != 1 || parentResults[0].ID != "parent-1" || parentResults[0].ChildID != "c-old" {
		t.Fatalf("parent results = %+v, want parent-1/c-old", parentResults)
	}
	if parentResults[0].Text != "parent text" || parentResults[0].ChildText != "old child" || parentResults[0].ChildMetadata["slot"] != "old" {
		t.Fatalf("parent result fields not preserved: %+v", parentResults[0])
	}
	if _, err := coll.SearchParentsVector(oldQuery, 5, WithChildOverfetch(2)); err == nil || !strings.Contains(err.Error(), "WithChildOverfetch") {
		t.Fatalf("WithChildOverfetch err = %v, want unsupported", err)
	}

	vectorResults, err := coll.SearchVector(oldQuery, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vectorResults) != 1 || vectorResults[0].ID != "single" {
		t.Fatalf("SearchVector results = %+v, want only single-vector entries", vectorResults)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertFilesDoNotContainRawVector(t, filepath.Join(path, "collections", "vecs"), oldQuery)

	db, err = Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll = db.Collection("vecs", WithoutRawStore())
	reopened, err := coll.SearchParentsVector(oldQuery, 5, WithChildFilters(Filter("slot", "old")))
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened) != 1 || reopened[0].ID != "parent-1" || reopened[0].ChildID != "c-old" {
		t.Fatalf("reopened parent results = %+v, want parent-1/c-old", reopened)
	}
	reopenedHistory, err := coll.History("parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(reopenedHistory) != 1 || len(reopenedHistory[0].Children) != 2 {
		t.Fatalf("reopened history = %+v, want packed children", reopenedHistory)
	}

	if err := coll.PutMultiVector("parent-1", MultiVectorEntry{
		Text:     "replacement parent",
		Metadata: map[string]string{"tenant": "acme", "type": "article"},
		Children: []MultiVectorChild{
			{ID: "c-new", Vector: newQuery, Text: "new child", Metadata: map[string]string{"slot": "new", "kind": "code"}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	stale, err := coll.SearchParentsVector(oldQuery, 5, WithChildFilters(Filter("slot", "old")))
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 {
		t.Fatalf("stale child survived replacement: %+v", stale)
	}
	current, err := coll.SearchParentsVector(newQuery, 5, WithChildFilters(Filter("slot", "new")))
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 || current[0].ChildID != "c-new" || current[0].Text != "replacement parent" {
		t.Fatalf("current results = %+v, want c-new replacement", current)
	}
	atOld, err := coll.At(firstClock).SearchParentsVector(oldQuery, 5, WithChildFilters(Filter("slot", "old")))
	if err != nil {
		t.Fatal(err)
	}
	if len(atOld) != 1 || atOld[0].ChildID != "c-old" {
		t.Fatalf("At(firstClock) parent results = %+v, want c-old", atOld)
	}

	if err := coll.Delete("parent-1"); err != nil {
		t.Fatal(err)
	}
	deleted, err := coll.SearchParentsVector(newQuery, 5, WithChildFilters(Filter("slot", "new")))
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 0 {
		t.Fatalf("deleted parent still searchable: %+v", deleted)
	}
	history, err = coll.History("parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 || !history[2].Tombstone {
		t.Fatalf("history = %+v, want tombstone third version", history)
	}
}

func TestQuantizedOnlyPackedParentCompactOrdinalChildrenReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "packed-parent-ordinal.csdb")
	seed := int64(5581486560434873699)
	query := []float32{0, 1, 0, 0}
	children := []MultiVectorChild{
		{ID: "0", Vector: query},
		{ID: "1", Vector: []float32{1, 0, 0, 0}},
		{ID: "2", Vector: []float32{0, 0, 1, 0}},
	}

	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("vecs", WithBitWidth(4), WithQuantizerSeed(seed), WithoutRawStore())
	if err := coll.PutMultiVector("parent-ordinal", MultiVectorEntry{Children: children}); err != nil {
		t.Fatal(err)
	}
	before, err := coll.SearchParentsVector(query, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) == 0 || before[0].ID != "parent-ordinal" || before[0].ChildID != "0" {
		t.Fatalf("parent results before reopen = %+v, want parent-ordinal/0 first", before)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertFilesDoNotContainRawVector(t, filepath.Join(path, "collections", "vecs"), query)

	db, err = Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll = db.Collection("vecs", WithoutRawStore())
	after, err := coll.SearchParentsVector(query, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) == 0 || after[0].ID != "parent-ordinal" || after[0].ChildID != "0" {
		t.Fatalf("parent results after reopen = %+v, want parent-ordinal/0 first", after)
	}
	history, err := coll.History("parent-ordinal")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || len(history[0].Children) != len(children) {
		t.Fatalf("history = %+v, want one compact packed parent version", history)
	}
	for i, child := range history[0].Children {
		if child.ID != strconv.Itoa(i) || child.Text != "" || len(child.Metadata) != 0 {
			t.Fatalf("child[%d] fields = %+v, want compact ordinal child", i, child)
		}
		if child.Quantized == nil || len(child.Quantized.MSE) == 0 || len(child.Quantized.Signs) == 0 {
			t.Fatalf("child[%d] quantized payload = %+v, want non-empty", i, child.Quantized)
		}
	}
}

func TestPackedParentMultiVectorRejectsUnsupportedSurfacesAndInvalidInput(t *testing.T) {
	db, err := Open(t.TempDir(), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// PutMultiVector now works on any local flat collection (raw store on or off).
	rawColl := db.Collection("raw")
	if err := rawColl.PutMultiVector("parent", MultiVectorEntry{Children: []MultiVectorChild{{ID: "c1", Vector: []float32{1, 0}}}}); err != nil {
		t.Fatalf("raw PutMultiVector err = %v, want success", err)
	}
	if _, err := rawColl.SearchParentsVector([]float32{1, 0}, 1); err != nil {
		t.Fatalf("raw SearchParentsVector err = %v, want success", err)
	}

	coll := db.Collection("vecs", WithoutRawStore())
	tests := []struct {
		name    string
		id      string
		entry   MultiVectorEntry
		wantErr string
	}{
		{name: "empty parent", id: "", entry: MultiVectorEntry{Children: []MultiVectorChild{{ID: "c1", Vector: []float32{1, 0}}}}, wantErr: "parent id"},
		{name: "no children", id: "parent", entry: MultiVectorEntry{}, wantErr: "at least one child"},
		{name: "empty child", id: "parent", entry: MultiVectorEntry{Children: []MultiVectorChild{{Vector: []float32{1, 0}}}}, wantErr: "child id"},
		{name: "duplicate child", id: "parent", entry: MultiVectorEntry{Children: []MultiVectorChild{{ID: "c1", Vector: []float32{1, 0}}, {ID: "c1", Vector: []float32{0, 1}}}}, wantErr: "duplicate child id"},
		{name: "empty vector", id: "parent", entry: MultiVectorEntry{Children: []MultiVectorChild{{ID: "c1"}}}, wantErr: "empty embedding"},
		{name: "dimension mismatch", id: "parent", entry: MultiVectorEntry{Children: []MultiVectorChild{{ID: "c1", Vector: []float32{1, 0}}, {ID: "c2", Vector: []float32{1, 0, 0}}}}, wantErr: "dimension"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := coll.PutMultiVector(tt.id, tt.entry)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("PutMultiVector err = %v, want %q", err, tt.wantErr)
			}
		})
	}

	federated, err := Open(filepath.Join(t.TempDir(), "federated.csdb"), WithProvider(nil), WithShards(
		ShardAssignment{ID: "local", Owner: LocalShardOwner, Start: 0, End: 10},
		ShardAssignment{ID: "remote", Owner: "127.0.0.1:1", Start: 11, End: ^uint64(0)},
	))
	if err != nil {
		t.Fatal(err)
	}
	defer federated.Close()
	fedColl := federated.Collection("vecs", WithoutRawStore())
	err = fedColl.PutMultiVector("parent", MultiVectorEntry{Children: []MultiVectorChild{{ID: "c1", Vector: []float32{1, 0}}}})
	if err == nil || !strings.Contains(err.Error(), "federated") {
		t.Fatalf("federated PutMultiVector err = %v, want unsupported", err)
	}
	if _, err := fedColl.SearchParentsVector([]float32{1, 0}, 1); err == nil || !strings.Contains(err.Error(), "federated") {
		t.Fatalf("federated SearchParentsVector err = %v, want unsupported", err)
	}
}

func TestWithoutRawStoreHNSWNowWorksAndReplicationExportDisabled(t *testing.T) {
	db, err := Open(t.TempDir(), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// HNSW now works on every collection, raw store or not.
	hnsw := db.Collection("hnsw", WithoutRawStore(), WithIndexType(IndexHNSW))
	if err := hnsw.usable(); err != nil {
		t.Fatalf("WithoutRawStore HNSW creation err = %v, want success", err)
	}

	coll := db.Collection("vecs", WithoutRawStore())
	if err := coll.PutVector("v1", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	server := &transportServer{db: db}
	// Replication export of a WithoutRawStore collection is disabled pending Distribution.
	if _, err := server.pullEntries(RPCPullEntriesRequest{Collection: "vecs"}); !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("pullEntries err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}
	var snapResp RPCPullSnapshotResponse
	if err := server.PullSnapshot(RPCPullSnapshotRequest{Collection: "vecs"}, &snapResp); !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("PullSnapshot err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}
}

func TestRemotePathsDisabledPendingDistribution(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "gated.csdb"), WithProvider(nil),
		WithShards(twoNodeShardLayout(LocalShardOwner, "127.0.0.1:1")...))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, remoteID := pickPeerOwnedIDs(t, db, "vecs", db.localMemberID(), "127.0.0.1:1")
	coll := db.Collection("vecs")
	if err := coll.PutVector(remoteID, []float32{1, 0, 0, 0}); !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("federated PutVector err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}
}

// TestDefaultCollectionReplicationDisabledPendingDistribution asserts that the
// multi-node server paths return errRemoteUnsupportedPendingDistribution for a
// DEFAULT (raw-store-enabled) collection, not just for WithoutRawStore()
// collections. This guards against the silent data-loss bug where the
// collectionUsesQuantizedOnly gate only fired for quantized-only collections
// and let default collections fall through to lossy (Embedding: nil) streams.
func TestDefaultCollectionReplicationDisabledPendingDistribution(t *testing.T) {
	db, err := Open(t.TempDir(), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Default collection — raw store ON.
	coll := db.Collection("docs")
	if err := coll.PutVector("v1", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}

	server := &transportServer{db: db}

	// pullEntries must return the sentinel, not a lossy (Embedding: nil) stream.
	if _, err := server.pullEntries(RPCPullEntriesRequest{Collection: "docs"}); !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("default-coll pullEntries err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}

	// PullSnapshot must return the sentinel, not a vectorless snapshot.
	var snapResp RPCPullSnapshotResponse
	if err := server.PullSnapshot(RPCPullSnapshotRequest{Collection: "docs"}, &snapResp); !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("default-coll PullSnapshot err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}

	// PutVector (write path) must return the sentinel.
	if err := server.PutVector(RPCPutVectorRequest{Collection: "docs", ID: "v2", Vector: []float32{0, 1, 0, 0}}, &RPCEmpty{}); !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("default-coll PutVector err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}

	// Put (text-entry write path) must return the sentinel.
	if err := server.Put(RPCPutRequest{Collection: "docs", ID: "v3", Entry: Entry{Text: "hello"}}, &RPCEmpty{}); !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("default-coll Put err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}

	// Delete must return the sentinel.
	if err := server.Delete(RPCDeleteRequest{Collection: "docs", ID: "v1"}, &RPCEmpty{}); !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("default-coll Delete err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}
}

func TestQuantizedOnlySnapshotDoesNotPruneConcurrentWALWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quantized-race.csdb")
	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("vecs", WithoutRawStore())
	if err := coll.PutVector("v1", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}

	originalWriteSnapshotFile := writeSnapshotFile
	defer func() { writeSnapshotFile = originalWriteSnapshotFile }()
	injected := false
	writeSnapshotFile = func(path string, data snap.Data) error {
		if err := originalWriteSnapshotFile(path, data); err != nil {
			return err
		}
		if injected {
			return nil
		}
		injected = true
		return coll.PutVector("v2", []float32{0, 1, 0, 0})
	}

	if err := coll.persistSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := coll.sync(); err != nil {
		t.Fatal(err)
	}
	if !injected {
		t.Fatal("test did not inject post-snapshot write")
	}
	if !walContainsVectorID(t, filepath.Join(path, "collections", "vecs", "wal"), "v2") {
		t.Fatal("post-snapshot write was pruned from WAL")
	}
}

func TestQuantizedOnlyDropCollectionDirtyDoesNotDeadlock(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "quantized-drop.csdb"), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("vecs", WithoutRawStore())
	if err := coll.PutVector("v1", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- db.DropCollection("vecs")
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DropCollection deadlocked for dirty quantized_only collection")
	}
}

func TestFederatedRemoteWriteDisabledPendingDistribution(t *testing.T) {
	serverDB, addr := startRemoteTestServer(t, WithProvider(nil))
	_ = serverDB

	db, err := Open(filepath.Join(t.TempDir(), "local.csdb"), WithProvider(nil), WithShards(twoNodeShardLayout(LocalShardOwner, addr)...))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, remoteID := pickPeerOwnedIDs(t, db, "vecs", db.localMemberID(), addr)
	coll := db.Collection("vecs")
	err = coll.PutVector(remoteID, []float32{1, 0, 0, 0})
	if !errors.Is(err, errRemoteUnsupportedPendingDistribution) {
		t.Fatalf("federated PutVector err = %v, want errRemoteUnsupportedPendingDistribution", err)
	}

	serverDB.mu.RLock()
	_, created := serverDB.manifest.Collections["vecs"]
	serverDB.mu.RUnlock()
	if created {
		t.Fatal("federated write created a peer collection")
	}
}

func TestRemoteExistingWriteDisabledPendingDistribution(t *testing.T) {
	t.Skip("restored in v0.3.0 Distribution phase: code-carrying replication over codes + raw pull-by-hash")
}

func TestQuantizedOnlyRejectsMalformedSnapshotPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed-quantized.csdb")
	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("vecs", WithBitWidth(4), WithoutRawStore())
	seed := coll.seed
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	snapshotPath := filepath.Join(path, "collections", "vecs", "snapshot-00000000000000000001.csdb")
	err = snap.WriteFile(snapshotPath, snap.Data{
		Collection: "vecs",
		BitWidth:   4,
		Seed:       seed,
		Dim:        4,
		MaxLamport: 1,
		CreatedAt:  time.Now().UTC(),
		Records: []snap.Record{
			{
				ID: "bad",
				Versions: []snap.Version{
					{
						Quantized:    &snap.QuantizedVector{MSE: []byte{1}, Signs: []byte{1}, ResNorm: 1},
						Text:         "bad payload",
						LamportClock: 1,
						ActorID:      "test",
						WallClock:    time.Now().UTC(),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, WithProvider(nil))
	if err == nil {
		_ = reopened.Close()
		t.Fatal("Open succeeded with malformed quantized snapshot payload")
	}
	if !strings.Contains(err.Error(), "invalid quantized payload") {
		t.Fatalf("Open err = %v, want invalid quantized payload", err)
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

func assertFilesDoNotContainRawVector(t *testing.T, dir string, vector []float32) {
	t.Helper()
	raw := rawVectorBytes(vector)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, raw) {
			t.Fatalf("%s contains raw vector bytes", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func rawVectorBytes(vector []float32) []byte {
	out := make([]byte, len(vector)*4)
	for i, value := range vector {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(value))
	}
	return out
}

func totalWALBytes(t *testing.T, dir string) int64 {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}

func walContainsVectorID(t *testing.T, dir, id string) bool {
	t.Helper()
	segments, err := walpkg.ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, segment := range segments {
		reader, err := walpkg.NewReader(segment)
		if err != nil {
			t.Fatal(err)
		}
		for reader.Next() {
			if reader.Entry().VectorID == id {
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				return true
			}
		}
		if err := reader.Err(); err != nil {
			_ = reader.Close()
			t.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return false
}

// spyScorer wraps a defaultScorer and counts ScoreTopK calls to witness that the
// flat search path routes through the injected Scorer seam (Task 3 / §10). It
// lazily builds its inner defaultScorer from the quantizer carried by the first
// corpus set, so it can be injected before the collection's quantizer exists.
type spyScorer struct {
	quantizer  *turboquant.IPQuantizer
	inner      *defaultScorer
	corpus     []turboquant.IPQuantized
	topKCalls  int
	multiCalls int
}

func (s *spyScorer) ensureInner() {
	if s.inner == nil {
		s.inner = newDefaultScorer(s.quantizer)
	}
}

func (s *spyScorer) SetCorpus(corpus []turboquant.IPQuantized) {
	s.corpus = corpus
	s.ensureInner()
	s.inner.SetCorpus(corpus)
}

func (s *spyScorer) ScoreTopK(pq turboquant.PreparedQuery, k int, accept func(i int) bool) []ScoredHit {
	s.topKCalls++
	s.ensureInner()
	return s.inner.ScoreTopK(pq, k, accept)
}

func (s *spyScorer) ScoreTopKMulti(pqs []turboquant.PreparedQuery, k int, accept func(i int) bool) [][]ScoredHit {
	s.multiCalls++
	s.ensureInner()
	return s.inner.ScoreTopKMulti(pqs, k, accept)
}

func (s *spyScorer) Close() error {
	if s.inner != nil {
		return s.inner.Close()
	}
	return nil
}

func TestCollectionUsesInjectedScorer(t *testing.T) {
	db, err := Open(t.TempDir(), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const seed = int64(424242)
	spy := &spyScorer{}
	coll := db.Collection("vecs", WithBitWidth(2), WithQuantizerSeed(seed), WithScorer(spy))
	if coll.scorer != spy {
		t.Fatalf("collection scorer not set to spy")
	}

	vecs := map[string][]float32{
		"a": {1, 0, 0, 0},
		"b": {0, 1, 0, 0},
		"c": {0.9, 0.1, 0, 0},
	}
	for id, v := range vecs {
		if err := coll.PutVector(id, v); err != nil {
			t.Fatal(err)
		}
	}
	// Give the spy the collection's quantizer (available once the flat index exists).
	coll.mu.RLock()
	spy.quantizer = coll.index.(*index).quantizer
	coll.mu.RUnlock()

	// The spy's inner scorer is built lazily per-search by scorerForSearch; the spy
	// itself is the injected wrapper, so its counter must increment.
	res, err := coll.SearchVector([]float32{1, 0, 0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if spy.topKCalls == 0 {
		t.Fatalf("injected scorer ScoreTopK never called (count=%d)", spy.topKCalls)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 results via scorer path, got %+v", res)
	}

	// Equivalence with a plain (no-scorer) collection at the SAME seed.
	plain := db.Collection("plain", WithBitWidth(2), WithQuantizerSeed(seed))
	for id, v := range vecs {
		if err := plain.PutVector(id, v); err != nil {
			t.Fatal(err)
		}
	}
	plainRes, err := plain.SearchVector([]float32{1, 0, 0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(plainRes) != len(res) {
		t.Fatalf("scorer-path result count %d != plain %d", len(res), len(plainRes))
	}
	for i := range res {
		if res[i].ID != plainRes[i].ID || res[i].Score != plainRes[i].Score {
			t.Fatalf("scorer path differs from plain at %d: %+v vs %+v", i, res[i], plainRes[i])
		}
	}
}

func TestScorerPathMatchesInline(t *testing.T) {
	db, err := Open(t.TempDir(), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rng := rand.New(rand.NewSource(2026))
	dim := 4
	coll := db.Collection("vecs", WithBitWidth(2))
	type rec struct {
		id  string
		vec []float32
		src string
	}
	var recs []rec
	for i := 0; i < 30; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		id := "v" + strconv.Itoa(i)
		src := "code"
		if i%2 == 0 {
			src = "review"
		}
		recs = append(recs, rec{id: id, vec: v, src: src})
		if err := coll.PutVector(id, v, WithMetadata(map[string]string{"source": src})); err != nil {
			t.Fatal(err)
		}
	}

	// Golden: direct InnerProductPrepared over the live corpus (certified reference).
	c := coll
	c.mu.RLock()
	flat := c.index.(*index)
	c.mu.RUnlock()

	golden := func(query []float32, k int, src string) []string {
		flat.mu.RLock()
		pq := flat.quantizer.PrepareQuery(query)
		type sh struct {
			id    string
			score float32
		}
		var hits []sh
		for i := range flat.entries {
			e := flat.entries[i]
			if e.child || e.dead {
				continue
			}
			if src != "" && e.metadata["source"] != src {
				continue
			}
			hits = append(hits, sh{e.id, flat.quantizer.InnerProductPrepared(e.qv, pq)})
		}
		flat.mu.RUnlock()
		sort.Slice(hits, func(a, b int) bool {
			if hits[a].score != hits[b].score {
				return hits[a].score > hits[b].score
			}
			return hits[a].id < hits[b].id
		})
		if len(hits) > k {
			hits = hits[:k]
		}
		ids := make([]string, len(hits))
		for i := range hits {
			ids[i] = hits[i].id
		}
		return ids
	}

	for q := 0; q < 15; q++ {
		query := make([]float32, dim)
		for j := range query {
			query[j] = rng.Float32()*2 - 1
		}
		for _, k := range []int{1, 3, 5} {
			for _, src := range []string{"", "code", "review"} {
				var filters []FilterOption
				if src != "" {
					filters = append(filters, Filter("source", src))
				}
				res, err := coll.SearchVector(query, k, filters...)
				if err != nil {
					t.Fatal(err)
				}
				want := golden(query, k, src)
				if len(res) != len(want) {
					t.Fatalf("q=%d k=%d src=%q result count %d != golden %d", q, k, src, len(res), len(want))
				}
				for i := range res {
					if res[i].ID != want[i] {
						t.Fatalf("q=%d k=%d src=%q pos %d: scorer %s != golden %s", q, k, src, i, res[i].ID, want[i])
					}
				}
			}
		}
	}
}
