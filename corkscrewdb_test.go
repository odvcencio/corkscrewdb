package corkscrewdb

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	walpkg "m31labs.dev/corkscrewdb/wal"
)

func TestOpenSurfacesWALInteriorCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.csdb")
	db, err := Open(path, WithProvider(&mockProvider{dim: 4}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs", WithBitWidth(2))
	if err := coll.PutVector("a", []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := coll.PutVector("b", []float32{0, 1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := coll.sync(); err != nil {
		t.Fatal(err)
	}

	// Corrupt an interior byte of the first WAL segment (valid bytes follow),
	// without closing (close would snapshot + prune the WAL).
	walDir := filepath.Join(path, "collections", "docs", "wal")
	segments, err := walpkg.ListSegments(walDir)
	if err != nil || len(segments) == 0 {
		t.Fatalf("list segments err=%v segments=%v", err, segments)
	}
	raw, err := os.ReadFile(segments[0])
	if err != nil {
		t.Fatal(err)
	}
	raw[10] ^= 0xFF
	if err := os.WriteFile(segments[0], raw, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, WithProvider(&mockProvider{dim: 4})); err == nil {
		t.Fatal("Open succeeded over interior-corrupted WAL")
	} else {
		var corrupt *walpkg.ErrWALCorrupt
		if !errors.As(err, &corrupt) {
			t.Fatalf("Open err = %v, want *walpkg.ErrWALCorrupt", err)
		}
	}
}

func TestOpenCloseRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csdb")
	provider := &mockProvider{dim: 32}

	db, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs")
	if err := coll.Put("doc-1", Entry{Text: "hello world", Metadata: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	coll2 := db2.Collection("docs")
	results, err := coll2.Search("hello", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("after reopen: results = %v, want doc-1", results)
	}
}

func TestOpenCreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "test.csdb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

func TestOpenDefaultProviderSupportsTextSearch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "default.csdb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("docs")
	if err := coll.Put("doc-1", Entry{Text: "the auth module uses passkeys", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	results, err := coll.Search("passkeys", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("results = %v, want doc-1", results)
	}
}

func TestOpenRejectsEmbeddingConfigMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mismatch.csdb")

	db, err := Open(path, WithProvider(&mockProvider{dim: 32}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, WithProvider(&mockProvider{dim: 16})); err == nil {
		t.Fatal("expected embedding config mismatch error")
	}
}

func TestOpenPersistsPeerConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.csdb")
	db, err := Open(path, WithPeers("node-a:4040", "node-b:4040"), WithToken("secret-token"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(path, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) == "" || !containsAll(string(data), "node-a:4040", "node-b:4040") {
		t.Fatalf("manifest missing peers: %s", string(data))
	}
	if containsAll(string(data), "secret-token") {
		t.Fatalf("token should not be persisted in manifest: %s", string(data))
	}
}

func TestOpenPersistsShardConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shards.csdb")
	db, err := Open(path, WithPeers("node-b:4040"), WithShards(
		ShardAssignment{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: (^uint64(0)) / 2},
		ShardAssignment{ID: "shard-b", Owner: "node-b:4040", Start: (^uint64(0))/2 + 1, End: ^uint64(0)},
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(path, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !containsAll(text, "\"shards\"", "\"shard-a\"", "\"shard-b\"", "\"owner\": \"self\"", "\"owner\": \"node-b:4040\"") {
		t.Fatalf("manifest missing shard config: %s", text)
	}
}

func TestOpenRejectsInvalidShardLayout(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid-shards.csdb")
	_, err := Open(path, WithShards(
		ShardAssignment{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: 10},
		ShardAssignment{ID: "shard-b", Owner: "node-b:4040", Start: 10, End: ^uint64(0)},
	))
	if err == nil || !strings.Contains(err.Error(), "starts at 10, want 11") {
		t.Fatalf("err = %v, want contiguous shard layout error", err)
	}
}

func TestRecoveryFromSnapshotAndWALTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tail.csdb")
	provider := &mockProvider{dim: 16}

	db, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("docs")
	if err := coll.Put("doc-1", Entry{Text: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := coll.persistSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := coll.sync(); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("doc-2", Entry{Text: "bravo"}); err != nil {
		t.Fatal(err)
	}
	if err := coll.sync(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	results, err := db2.Collection("docs").Search("bravo", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(results, "doc-2") {
		t.Fatalf("results = %v, want doc-2 from WAL tail replay", results)
	}
}

func TestCloseWritesDurableSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index-file.csdb")
	db, err := Open(path, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs")
	if err := coll.Put("doc-1", Entry{Text: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The snapshot is the durable full-state copy (codes + raw hash refs);
	// .tqi/graph.hnsw are regenerable caches no longer written on close.
	matches, _ := filepath.Glob(filepath.Join(path, "collections", "docs", "snapshot-*.csdb"))
	if len(matches) == 0 {
		t.Fatal("expected a durable snapshot file")
	}

	db2, err := Open(path, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	results, err := db2.Collection("docs").Search("alpha", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("reopen search = %v, want doc-1", results)
	}
}

func TestEmbeddedLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lifecycle.csdb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs")

	if err := coll.Put("doc-1", Entry{Text: "webauthn passkeys are enabled", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("doc-2", Entry{Text: "database migrations are append only", Metadata: map[string]string{"source": "design"}}); err != nil {
		t.Fatal(err)
	}
	filtered, err := coll.Search("passkeys", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 1 || filtered[0].ID != "doc-1" {
		t.Fatalf("filtered = %v, want doc-1", filtered)
	}
	history, err := coll.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	if err := coll.Delete("doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	results, err := db2.Collection("docs").Search("passkeys", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("deleted review entry survived reopen: %v", results)
	}
	history, err = db2.Collection("docs").History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || !history[1].Tombstone {
		t.Fatalf("history after reopen = %+v", history)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

func TestConnectRemoteLifecycle(t *testing.T) {
	serverDB, addr := startRemoteTestServer(t, WithProvider(&mockProvider{dim: 16}), WithToken("secret"))
	_ = serverDB

	client, err := Connect(addr, WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	coll := client.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "alpha remote", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	history, err := coll.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	firstClock := history[0].LamportClock

	if err := coll.Put("doc-1", Entry{Text: "beta remote", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	results, err := coll.Search("beta", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("results = %v, want doc-1", results)
	}

	view := coll.At(firstClock)
	viewResults, err := view.Search("alpha", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(viewResults) != 1 || viewResults[0].Text != "alpha remote" {
		t.Fatalf("viewResults = %v, want alpha version", viewResults)
	}

	if err := coll.Delete("doc-1"); err != nil {
		t.Fatal(err)
	}
	results, err = coll.Search("beta", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("deleted remote result survived: %v", results)
	}
}

func TestConnectRequiresToken(t *testing.T) {
	_, addr := startRemoteTestServer(t, WithProvider(&mockProvider{dim: 8}), WithToken("secret"))
	client, err := Connect(addr, WithToken("wrong"))
	if err == nil || err.Error() != ErrUnauthorized.Error() {
		if client != nil {
			_ = client.Close()
		}
		t.Fatalf("err = %v, want %v", err, ErrUnauthorized)
	}
}

func TestEmbeddedPeersRouteWritesAndFanoutSearch(t *testing.T) {
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerA.Close()
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerB.Close()

	addrA := listenerA.Addr().String()
	addrB := listenerB.Addr().String()

	dbA, err := Open(filepath.Join(t.TempDir(), "a.csdb"), WithProvider(&mockProvider{dim: 16}), WithToken("secret"), WithPeers(addrB))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbB, err := Open(filepath.Join(t.TempDir(), "b.csdb"), WithProvider(&mockProvider{dim: 16}), WithToken("secret"), WithPeers(addrA))
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	// Register serve addresses before starting goroutines so the hash ring
	// is stable when pickPeerOwnedIDs runs.
	dbA.registerServeAddr(addrA)
	dbB.registerServeAddr(addrB)

	doneA := make(chan error, 1)
	go func() { doneA <- dbA.Serve(listenerA) }()
	doneB := make(chan error, 1)
	go func() { doneB <- dbB.Serve(listenerB) }()
	t.Cleanup(func() {
		select {
		case err := <-doneA:
			if err != nil {
				t.Errorf("serve A: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve A did not exit")
		}
		select {
		case err := <-doneB:
			if err != nil {
				t.Errorf("serve B: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve B did not exit")
		}
	})

	localID, remoteID := pickPeerOwnedIDs(t, dbA, "docs", addrA, addrB)
	collA := dbA.Collection("docs", WithBitWidth(2))

	if err := collA.Put(localID, Entry{Text: "alpha federation", Metadata: map[string]string{"source": "local"}}); err != nil {
		t.Fatal(err)
	}
	if err := collA.Put(remoteID, Entry{Text: "beta federation", Metadata: map[string]string{"source": "remote"}}); err != nil {
		t.Fatal(err)
	}

	results, err := collA.Search("alpha", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(results, localID) {
		t.Fatalf("expected local result %q in %v", localID, results)
	}
	results, err = collA.Search("beta", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(results, remoteID) {
		t.Fatalf("expected remote result %q in %v", remoteID, results)
	}

	owner := dbA.ownerFor("docs", remoteID)
	var ownerDB *DB
	if owner == addrA {
		ownerDB = dbA
	} else {
		ownerDB = dbB
	}
	history, err := ownerDB.Collection("docs").historyFor(remoteID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("owner history len = %d, want 1", len(history))
	}

	if err := collA.Delete(remoteID); err != nil {
		t.Fatal(err)
	}
	results, err = dbB.Collection("docs").Search("beta", 10, Filter("source", "remote"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("deleted federated entry still searchable: %v", results)
	}
}

func TestExplicitShardsRouteWritesAndFanoutSearch(t *testing.T) {
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerA.Close()
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerB.Close()

	addrA := listenerA.Addr().String()
	addrB := listenerB.Addr().String()

	dbA, err := Open(
		filepath.Join(t.TempDir(), "a-explicit.csdb"),
		WithProvider(&mockProvider{dim: 16}),
		WithToken("secret"),
		WithPeers(addrB),
		WithShards(twoNodeShardLayout(LocalShardOwner, addrB)...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbB, err := Open(
		filepath.Join(t.TempDir(), "b-explicit.csdb"),
		WithProvider(&mockProvider{dim: 16}),
		WithToken("secret"),
		WithPeers(addrA),
		WithShards(twoNodeShardLayout(addrA, LocalShardOwner)...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	dbA.registerServeAddr(addrA)
	dbB.registerServeAddr(addrB)

	doneA := make(chan error, 1)
	go func() { doneA <- dbA.Serve(listenerA) }()
	doneB := make(chan error, 1)
	go func() { doneB <- dbB.Serve(listenerB) }()
	t.Cleanup(func() {
		select {
		case err := <-doneA:
			if err != nil {
				t.Errorf("serve A: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve A did not exit")
		}
		select {
		case err := <-doneB:
			if err != nil {
				t.Errorf("serve B: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve B did not exit")
		}
	})

	localID, remoteID := pickPeerOwnedIDs(t, dbA, "docs", addrA, addrB)
	collA := dbA.Collection("docs", WithBitWidth(2))

	if err := collA.Put(localID, Entry{Text: "alpha explicit", Metadata: map[string]string{"source": "local"}}); err != nil {
		t.Fatal(err)
	}
	if err := collA.Put(remoteID, Entry{Text: "beta explicit", Metadata: map[string]string{"source": "remote"}}); err != nil {
		t.Fatal(err)
	}

	results, err := collA.Search("alpha", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(results, localID) {
		t.Fatalf("expected local result %q in %v", localID, results)
	}
	results, err = collA.Search("beta", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResult(results, remoteID) {
		t.Fatalf("expected remote result %q in %v", remoteID, results)
	}

	if err := collA.Delete(remoteID); err != nil {
		t.Fatal(err)
	}
	results, err = dbB.Collection("docs").Search("beta", 10, Filter("source", "remote"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("deleted explicit-shard entry still searchable: %v", results)
	}
}

func TestRebalanceShardsMigratesOwnershipAndPrunesOldOwner(t *testing.T) {
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerA.Close()
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerB.Close()

	addrA := listenerA.Addr().String()
	addrB := listenerB.Addr().String()
	maxKey := ^uint64(0)
	oldCut := maxKey / 2
	newCut := (maxKey / 4) * 3

	oldLayoutA := []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: addrB, Start: oldCut + 1, End: maxKey},
	}
	oldLayoutB := []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: LocalShardOwner, Start: oldCut + 1, End: maxKey},
	}
	newLayoutA := []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: newCut},
		{ID: "shard-b", Owner: addrB, Start: newCut + 1, End: maxKey},
	}
	newLayoutB := []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: newCut},
		{ID: "shard-b", Owner: LocalShardOwner, Start: newCut + 1, End: maxKey},
	}

	dbA, err := Open(
		filepath.Join(t.TempDir(), "a-rebalance.csdb"),
		WithProvider(&mockProvider{dim: 16}),
		WithToken("secret"),
		WithPeers(addrB),
		WithShards(oldLayoutA...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbB, err := Open(
		filepath.Join(t.TempDir(), "b-rebalance.csdb"),
		WithProvider(&mockProvider{dim: 16}),
		WithToken("secret"),
		WithPeers(addrA),
		WithShards(oldLayoutB...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	dbA.registerServeAddr(addrA)
	dbB.registerServeAddr(addrB)

	doneA := make(chan error, 1)
	go func() { doneA <- dbA.Serve(listenerA) }()
	doneB := make(chan error, 1)
	go func() { doneB <- dbB.Serve(listenerB) }()
	t.Cleanup(func() {
		select {
		case err := <-doneA:
			if err != nil {
				t.Errorf("serve A: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve A did not exit")
		}
		select {
		case err := <-doneB:
			if err != nil {
				t.Errorf("serve B: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve B did not exit")
		}
	})

	movingID, stayingRemoteID := pickIDsForRebalance(t, dbA, "docs", oldLayoutA, newLayoutA, addrA, addrB)
	collA := dbA.Collection("docs", WithBitWidth(2))
	if err := collA.Put(movingID, Entry{Text: "moving before handoff"}); err != nil {
		t.Fatal(err)
	}
	if err := collA.Put(stayingRemoteID, Entry{Text: "stays remote"}); err != nil {
		t.Fatal(err)
	}

	before, err := dbB.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 {
		t.Fatalf("old owner history len = %d, want 1", len(before))
	}

	if err := dbA.RebalanceShards(newLayoutA...); err != nil {
		t.Fatal(err)
	}
	afterPull, err := dbA.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterPull) != 1 || afterPull[0].Text != "moving before handoff" {
		t.Fatalf("new owner history after pull = %+v", afterPull)
	}

	if err := dbB.RebalanceShards(newLayoutB...); err != nil {
		t.Fatal(err)
	}
	pruned, err := dbB.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(pruned) != 0 {
		t.Fatalf("old owner retained handed-off id: %+v", pruned)
	}
	stillRemote, err := dbB.Collection("docs").historyFor(stayingRemoteID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(stillRemote) != 1 {
		t.Fatalf("old owner lost stable remote id: %+v", stillRemote)
	}

	if err := dbB.Collection("docs").Put(movingID, Entry{Text: "moving after handoff"}); err != nil {
		t.Fatal(err)
	}
	finalHistory, err := dbA.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(finalHistory) != 2 || finalHistory[1].Text != "moving after handoff" {
		t.Fatalf("new owner history after routed write = %+v", finalHistory)
	}
}

func TestOrchestrateRebalanceCoordinatesClusterCutover(t *testing.T) {
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerA.Close()
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerB.Close()

	addrA := listenerA.Addr().String()
	addrB := listenerB.Addr().String()
	maxKey := ^uint64(0)
	oldCut := maxKey / 2
	newCut := (maxKey / 4) * 3

	oldLayoutA := []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: addrB, Start: oldCut + 1, End: maxKey},
	}
	oldLayoutB := []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: LocalShardOwner, Start: oldCut + 1, End: maxKey},
	}
	newLayout := []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: newCut},
		{ID: "shard-b", Owner: addrB, Start: newCut + 1, End: maxKey},
	}

	dbA, err := Open(
		filepath.Join(t.TempDir(), "a-orchestrated.csdb"),
		WithProvider(&mockProvider{dim: 16}),
		WithToken("secret"),
		WithPeers(addrB),
		WithShards(oldLayoutA...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbB, err := Open(
		filepath.Join(t.TempDir(), "b-orchestrated.csdb"),
		WithProvider(&mockProvider{dim: 16}),
		WithToken("secret"),
		WithPeers(addrA),
		WithShards(oldLayoutB...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	dbA.registerServeAddr(addrA)
	dbB.registerServeAddr(addrB)

	doneA := make(chan error, 1)
	go func() { doneA <- dbA.Serve(listenerA) }()
	doneB := make(chan error, 1)
	go func() { doneB <- dbB.Serve(listenerB) }()
	t.Cleanup(func() {
		select {
		case err := <-doneA:
			if err != nil {
				t.Errorf("serve A: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve A did not exit")
		}
		select {
		case err := <-doneB:
			if err != nil {
				t.Errorf("serve B: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve B did not exit")
		}
	})

	movingID, stayingRemoteID := pickIDsForRebalance(t, dbA, "docs", oldLayoutA, newLayout, addrA, addrB)
	collA := dbA.Collection("docs", WithBitWidth(2))
	if err := collA.Put(movingID, Entry{Text: "moving before orchestrated cutover"}); err != nil {
		t.Fatal(err)
	}
	if err := collA.Put(stayingRemoteID, Entry{Text: "stays remote after orchestrated cutover"}); err != nil {
		t.Fatal(err)
	}

	if err := dbA.OrchestrateRebalance(newLayout...); err != nil {
		t.Fatal(err)
	}

	newOwnerHistory, err := dbA.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(newOwnerHistory) != 1 || newOwnerHistory[0].Text != "moving before orchestrated cutover" {
		t.Fatalf("new owner history after orchestration = %+v", newOwnerHistory)
	}
	oldOwnerHistory, err := dbB.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldOwnerHistory) != 0 {
		t.Fatalf("old owner retained handed-off id after orchestration: %+v", oldOwnerHistory)
	}
	stillRemote, err := dbB.Collection("docs").historyFor(stayingRemoteID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(stillRemote) != 1 {
		t.Fatalf("stable remote id missing after orchestration: %+v", stillRemote)
	}

	if err := dbB.Collection("docs").Put(movingID, Entry{Text: "moving after orchestrated cutover"}); err != nil {
		t.Fatal(err)
	}
	finalHistory, err := dbA.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(finalHistory) != 2 || finalHistory[1].Text != "moving after orchestrated cutover" {
		t.Fatalf("new owner history after orchestrated routed write = %+v", finalHistory)
	}
}

func startRemoteTestServer(t *testing.T, opts ...Option) (*DB, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "remote.csdb")
	db, err := Open(path, opts...)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- db.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		_ = db.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve did not exit")
		}
	})
	return db, listener.Addr().String()
}

func pickPeerOwnedIDs(t *testing.T, db *DB, collection, localOwner, remoteOwner string) (string, string) {
	t.Helper()
	var localID, remoteID string
	for i := 0; i < 1000 && (localID == "" || remoteID == ""); i++ {
		candidate := "doc-peer-" + strconv.Itoa(i)
		switch db.ownerFor(collection, candidate) {
		case localOwner:
			if localID == "" {
				localID = candidate
			}
		case remoteOwner:
			if remoteID == "" {
				remoteID = candidate
			}
		}
	}
	if localID == "" || remoteID == "" {
		t.Fatalf("could not find both local and remote ids: local=%q remote=%q", localID, remoteID)
	}
	return localID, remoteID
}

func twoNodeShardLayout(localOwner, remoteOwner string) []ShardAssignment {
	mid := (^uint64(0)) / 2
	return []ShardAssignment{
		{ID: "shard-a", Owner: localOwner, Start: 0, End: mid},
		{ID: "shard-b", Owner: remoteOwner, Start: mid + 1, End: ^uint64(0)},
	}
}

func pickIDsForRebalance(t *testing.T, db *DB, collection string, oldLayout, newLayout []ShardAssignment, localOwner, remoteOwner string) (string, string) {
	t.Helper()
	var movingID, stayingRemoteID string
	oldMembers := db.legacyShardMembers()
	for i := 0; i < 10000 && (movingID == "" || stayingRemoteID == ""); i++ {
		candidate := "doc-rebalance-" + strconv.Itoa(i)
		oldOwner := db.ownerForLayout(collection, candidate, oldLayout, oldMembers)
		newOwner := db.ownerForLayout(collection, candidate, newLayout, nil)
		switch {
		case movingID == "" && oldOwner == remoteOwner && newOwner == localOwner:
			movingID = candidate
		case stayingRemoteID == "" && oldOwner == remoteOwner && newOwner == remoteOwner:
			stayingRemoteID = candidate
		}
	}
	if movingID == "" || stayingRemoteID == "" {
		t.Fatalf("could not find rebalance ids: moving=%q stayingRemote=%q", movingID, stayingRemoteID)
	}
	return movingID, stayingRemoteID
}

func hasResult(results []SearchResult, id string) bool {
	for _, result := range results {
		if result.ID == id {
			return true
		}
	}
	return false
}

func putHNSWVecs(t *testing.T, coll *Collection, n, dim int, seed int64) [][]float32 {
	t.Helper()
	rng := rng32(seed)
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng()*2 - 1
		}
		vecs[i] = v
		if err := coll.PutVector("v"+strconv.Itoa(i), v); err != nil {
			t.Fatal(err)
		}
	}
	return vecs
}

// rng32 returns a deterministic float32 generator (no external rand import needed
// here; corkscrewdb_test.go avoids math/rand to keep its import set minimal).
func rng32(seed int64) func() float32 {
	state := uint64(seed)*2862933555777941757 + 3037000493
	return func() float32 {
		state = state*6364136223846793005 + 1442695040888963407
		return float32((state>>40)&0xFFFFFF) / float32(0x1000000)
	}
}

func TestReopenFreshCacheNoRebuild(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.csdb")
	dim := 16

	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("hnsw", WithBitWidth(2), WithQuantizerSeed(123), WithIndexType(IndexHNSW))
	vecs := putHNSWVecs(t, coll, 120, dim, 7)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with hooks; the fresh cache must be ADOPTED (no rebuild).
	adopted, rebuilt := 0, 0
	cacheAdoptHook = func() { adopted++ }
	cacheRebuildHook = func() { rebuilt++ }
	defer func() { cacheAdoptHook, cacheRebuildHook = nil, nil }()

	db2, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	if adopted != 1 || rebuilt != 0 {
		t.Fatalf("fresh-cache reopen: adopted=%d rebuilt=%d, want adopted=1 rebuilt=0", adopted, rebuilt)
	}
	adoptResults, err := db2.Collection("hnsw").SearchVector(vecs[5], 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	// Force a rebuild by deleting the cache files, reopen, compare results.
	if err := os.Remove(filepath.Join(path, "collections", "hnsw", "index", "graph.hnsw")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(path, "collections", "hnsw", "index", "quantized.tqi")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	adopted, rebuilt = 0, 0
	db3, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	if rebuilt != 1 || adopted != 0 {
		t.Fatalf("deleted-cache reopen: adopted=%d rebuilt=%d, want adopted=0 rebuilt=1", adopted, rebuilt)
	}
	rebuildResults, err := db3.Collection("hnsw").SearchVector(vecs[5], 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(adoptResults) != len(rebuildResults) {
		t.Fatalf("adopt vs rebuild result count differ: %d vs %d", len(adoptResults), len(rebuildResults))
	}
	for i := range adoptResults {
		if adoptResults[i].ID != rebuildResults[i].ID {
			t.Fatalf("adopt vs rebuild differ at %d: %s vs %s", i, adoptResults[i].ID, rebuildResults[i].ID)
		}
	}
}

func TestCleanCloseAdoptsPriorCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.csdb")
	dim := 16

	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("hnsw", WithBitWidth(2), WithQuantizerSeed(9), WithIndexType(IndexHNSW))
	vecs := putHNSWVecs(t, coll, 100, dim, 11)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen, do NO writes, close (clean: dirty == false -> no snapshot, no cache write).
	db2, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen again: the PRIOR close's cache must still be adopted.
	adopted, rebuilt := 0, 0
	cacheAdoptHook = func() { adopted++ }
	cacheRebuildHook = func() { rebuilt++ }
	defer func() { cacheAdoptHook, cacheRebuildHook = nil, nil }()
	db3, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	if adopted != 1 || rebuilt != 0 {
		t.Fatalf("clean-close reopen: adopted=%d rebuilt=%d, want adopted=1 rebuilt=0", adopted, rebuilt)
	}
	res, err := db3.Collection("hnsw").SearchVector(vecs[3], 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 results after clean-close adopt, got %d", len(res))
	}

	// A never-written HNSW collection: reopen rebuilds cleanly (empty, no error).
	empty := db3.Collection("empty-hnsw", WithBitWidth(2), WithIndexType(IndexHNSW))
	_ = empty
}

func TestStaleAndCorruptCacheRebuild(t *testing.T) {
	dim := 16

	// (a) Stale .tqi lamport -> fallback to rebuild, correct results.
	t.Run("stale_tqi", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "db.csdb")
		db, err := Open(path, WithProvider(nil))
		if err != nil {
			t.Fatal(err)
		}
		coll := db.Collection("hnsw", WithBitWidth(2), WithQuantizerSeed(5), WithIndexType(IndexHNSW))
		vecs := putHNSWVecs(t, coll, 100, dim, 3)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		// Corrupt the .tqi so it fails to load -> graph cache treated stale.
		tqi := filepath.Join(path, "collections", "hnsw", "index", "quantized.tqi")
		if err := os.WriteFile(tqi, []byte("garbage"), 0o644); err != nil {
			t.Fatal(err)
		}
		adopted, rebuilt := 0, 0
		cacheAdoptHook = func() { adopted++ }
		cacheRebuildHook = func() { rebuilt++ }
		defer func() { cacheAdoptHook, cacheRebuildHook = nil, nil }()
		db2, err := Open(path, WithProvider(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()
		if rebuilt != 1 || adopted != 0 {
			t.Fatalf("stale .tqi: adopted=%d rebuilt=%d, want rebuilt=1", adopted, rebuilt)
		}
		res, err := db2.Collection("hnsw").SearchVector(vecs[1], 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 3 {
			t.Fatalf("expected 3 results after stale-cache rebuild, got %d", len(res))
		}
	})

	// (b) Truncated graph.hnsw -> CRC rejection -> rebuild, no error surfaced.
	t.Run("corrupt_graph", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "db.csdb")
		db, err := Open(path, WithProvider(nil))
		if err != nil {
			t.Fatal(err)
		}
		coll := db.Collection("hnsw", WithBitWidth(2), WithQuantizerSeed(6), WithIndexType(IndexHNSW))
		vecs := putHNSWVecs(t, coll, 100, dim, 4)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		graph := filepath.Join(path, "collections", "hnsw", "index", "graph.hnsw")
		data, err := os.ReadFile(graph)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(graph, data[:len(data)/2], 0o644); err != nil { // truncate
			t.Fatal(err)
		}
		adopted, rebuilt := 0, 0
		cacheAdoptHook = func() { adopted++ }
		cacheRebuildHook = func() { rebuilt++ }
		defer func() { cacheAdoptHook, cacheRebuildHook = nil, nil }()
		db2, err := Open(path, WithProvider(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()
		if rebuilt != 1 || adopted != 0 {
			t.Fatalf("corrupt graph: adopted=%d rebuilt=%d, want rebuilt=1", adopted, rebuilt)
		}
		res, err := db2.Collection("hnsw").SearchVector(vecs[2], 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 3 {
			t.Fatalf("expected 3 results after corrupt-graph rebuild, got %d", len(res))
		}
	})
}

// TestWriteManifestDurable verifies the fsync-hardened writeManifest leaves no
// temp file beside the manifest and that the on-disk content round-trips
// (§5.8). A true crash test is infeasible in-process; the observable contract
// is "atomic rename leaves no temp file and the content is present."
func TestWriteManifestDurable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")

	m := manifest{
		FormatVersion:   2,
		FormatFloor:     "v0.3.0",
		ModuleVersion:   PackageVersion,
		ActorID:         "actor-durable",
		DefaultBitWidth: 2,
		Peers:           []string{"127.0.0.1:9"},
		Collections:     map[string]collectionMeta{"docs": {BitWidth: 2, Dim: 8}},
		CreatedAt:       time.Now().UTC(),
	}

	if err := writeManifest(path, m); err != nil {
		t.Fatalf("writeManifest err = %v", err)
	}

	// No .tmp leftover beside the manifest.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp") {
			t.Fatalf("writeManifest left a temp file: %q", e.Name())
		}
	}

	// Content round-trips: reopen + unmarshal equals what was written.
	reopened, err := loadOrCreateManifest(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.ActorID != m.ActorID {
		t.Fatalf("ActorID = %q, want %q", reopened.ActorID, m.ActorID)
	}
	if reopened.DefaultBitWidth != m.DefaultBitWidth {
		t.Fatalf("DefaultBitWidth = %d, want %d", reopened.DefaultBitWidth, m.DefaultBitWidth)
	}
	if len(reopened.Peers) != 1 || reopened.Peers[0] != "127.0.0.1:9" {
		t.Fatalf("Peers = %v, want [127.0.0.1:9]", reopened.Peers)
	}
	if meta, ok := reopened.Collections["docs"]; !ok || meta.BitWidth != 2 || meta.Dim != 8 {
		t.Fatalf("Collections[docs] = %+v, want {BitWidth:2 Dim:8}", meta)
	}
}
