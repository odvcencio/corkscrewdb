package corkscrewdb

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	snap "m31labs.dev/corkscrewdb/snapshot"
	walpkg "m31labs.dev/corkscrewdb/wal"
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

func TestQuantizedOnlyPutSearchReopenHistoryAndPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "quantized-only.csdb")
	seed := int64(5581486560434873699)
	raw := []float32{0.1234567, -0.2345678, 0.3456789, -0.456789}

	db, err := Open(path, WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("vecs", WithBitWidth(4), WithQuantizerSeed(seed), WithQuantizedOnlyPersistence())
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
	if len(history[0].Embedding) != 0 {
		t.Fatalf("quantized_only history embedding len = %d, want 0", len(history[0].Embedding))
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
	if err := coll.RebuildIndex(IndexHNSW); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("RebuildIndex(IndexHNSW) err = %v, want unsupported", err)
	}
	if got := db.manifest.Collections["vecs"].VectorStorage; got != VectorStorageQuantizedOnly {
		t.Fatalf("manifest vector storage = %q, want quantized_only", got)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	indexPath := filepath.Join(path, "collections", "vecs", "index", "quantized.tqi")
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		t.Fatalf("quantized_only should not write .tqi index file, stat err=%v", err)
	}
	assertFilesDoNotContainRawVector(t, filepath.Join(path, "collections", "vecs"), raw)
	walBytes := totalWALBytes(t, filepath.Join(path, "collections", "vecs", "wal"))
	if walBytes > 128 {
		t.Fatalf("WAL bytes after quantized_only snapshot prune = %d, want compact empty tail", walBytes)
	}

	db, err = Open(path, WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	coll = db.Collection("vecs", WithQuantizedOnlyPersistence())
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
	if len(reopenedHistory) != 1 || len(reopenedHistory[0].Embedding) != 0 || reopenedHistory[0].Text != "unit-x" {
		t.Fatalf("reopened history = %+v, want text with no raw embedding", reopenedHistory)
	}
}

func TestQuantizedOnlyRejectsHNSWCreationAndReplicationExport(t *testing.T) {
	db, err := Open(t.TempDir(), WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	bad := db.Collection("bad", WithQuantizedOnlyPersistence(), WithIndexType(IndexHNSW))
	if err := bad.usable(); err == nil || !strings.Contains(err.Error(), "flat local indexes") {
		t.Fatalf("quantized_only HNSW creation err = %v, want flat local indexes", err)
	}

	coll := db.Collection("vecs", WithQuantizedOnlyPersistence())
	if err := coll.PutVector("v1", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	server := &transportServer{db: db}
	if _, err := server.pullEntries(RPCPullEntriesRequest{Collection: "vecs"}); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("pullEntries err = %v, want unsupported", err)
	}
	var snapResp RPCPullSnapshotResponse
	if err := server.PullSnapshot(RPCPullSnapshotRequest{Collection: "vecs"}, &snapResp); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("PullSnapshot err = %v, want unsupported", err)
	}
}

func TestQuantizedOnlySnapshotDoesNotPruneConcurrentWALWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quantized-race.csdb")
	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("vecs", WithQuantizedOnlyPersistence())
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

	coll := db.Collection("vecs", WithQuantizedOnlyPersistence())
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

func TestQuantizedOnlyRejectsFederatedRemoteWrite(t *testing.T) {
	serverDB, addr := startRemoteTestServer(t, WithProvider(nil))
	_ = serverDB

	db, err := Open(filepath.Join(t.TempDir(), "local.csdb"), WithProvider(nil), WithShards(twoNodeShardLayout(LocalShardOwner, addr)...))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, remoteID := pickPeerOwnedIDs(t, db, "vecs", db.localMemberID(), addr)
	coll := db.Collection("vecs", WithQuantizedOnlyPersistence())
	err = coll.PutVector(remoteID, []float32{1, 0, 0, 0})
	if err == nil || !strings.Contains(err.Error(), "quantized_only") || !strings.Contains(err.Error(), "federated or remote writes") {
		t.Fatalf("federated quantized_only PutVector err = %v, want unsupported", err)
	}

	serverDB.mu.RLock()
	_, created := serverDB.manifest.Collections["vecs"]
	serverDB.mu.RUnlock()
	if created {
		t.Fatal("federated quantized_only write created a peer collection")
	}
}

func TestQuantizedOnlyRejectsRemoteExistingWrite(t *testing.T) {
	serverDB, addr := startRemoteTestServer(t, WithProvider(nil))
	coll := serverDB.Collection("vecs", WithQuantizedOnlyPersistence())
	if err := coll.PutVector("local", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}

	client, err := Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	remoteColl := client.Collection("vecs")
	err = remoteColl.PutVector("remote", []float32{0, 1, 0, 0})
	if err == nil || !strings.Contains(err.Error(), "quantized_only") || !strings.Contains(err.Error(), "remote writes") {
		t.Fatalf("remote quantized_only PutVector err = %v, want unsupported", err)
	}

	withOption := client.Collection("vecs", WithQuantizedOnlyPersistence())
	if err := withOption.usable(); err == nil || !strings.Contains(err.Error(), "unsupported over remote collections") {
		t.Fatalf("remote quantized_only option err = %v, want unsupported", err)
	}
}

func TestQuantizedOnlyRejectsMalformedSnapshotPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed-quantized.csdb")
	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("vecs", WithBitWidth(4), WithQuantizedOnlyPersistence())
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
		Storage:    string(VectorStorageQuantizedOnly),
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
