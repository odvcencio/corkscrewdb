package corkscrewdb

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/odvcencio/corkscrewdb/replica"
	walpkg "github.com/odvcencio/corkscrewdb/wal"
)

// rpcPullerAdapter adapts the rpcClient to the replica.Puller interface.
type rpcPullerAdapter struct {
	client *rpcClient
}

func (a *rpcPullerAdapter) PullEntries(req replica.PullRequest) (replica.PullResponse, error) {
	resp, err := a.client.PullEntries(RPCPullEntriesRequest{
		Collection: req.Collection,
		SinceClock: req.SinceClock,
		MaxEntries: req.MaxEntries,
	})
	if err != nil {
		return replica.PullResponse{}, err
	}
	entries := make([]replica.Entry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = replica.Entry{Entry: walpkg.Entry{
			Kind:         e.Kind,
			CollectionID: e.CollectionID,
			VectorID:     e.VectorID,
			Embedding:    e.Embedding,
			Text:         e.Text,
			Metadata:     e.Metadata,
			LamportClock: e.LamportClock,
			ActorID:      e.ActorID,
			WallClock:    e.WallClock,
		}}
	}
	return replica.PullResponse{
		Entries:     entries,
		LatestClock: resp.LatestClock,
		HasMore:     resp.HasMore,
	}, nil
}

func (a *rpcPullerAdapter) PullSnapshot(req replica.SnapshotRequest) (replica.SnapshotResponse, error) {
	resp, err := a.client.PullSnapshot(RPCPullSnapshotRequest{
		Collection: req.Collection,
	})
	if err != nil {
		return replica.SnapshotResponse{}, err
	}
	records := make([]replica.VersionRecord, len(resp.Records))
	for i, r := range resp.Records {
		versions := make([]replica.VersionEntry, len(r.Versions))
		for j, v := range r.Versions {
			versions[j] = replica.VersionEntry{
				Embedding:    v.Embedding,
				Text:         v.Text,
				Metadata:     v.Metadata,
				LamportClock: v.LamportClock,
				ActorID:      v.ActorID,
				WallClock:    v.WallClock,
				Tombstone:    v.Tombstone,
			}
		}
		records[i] = replica.VersionRecord{ID: r.ID, Versions: versions}
	}
	return replica.SnapshotResponse{
		Data: replica.SnapshotData{
			Collection: resp.Collection,
			BitWidth:   resp.BitWidth,
			Seed:       resp.Seed,
			Dim:        resp.Dim,
			MaxLamport: resp.MaxLamport,
			Entries:    records,
		},
	}, nil
}

// dbApplier adapts a DB to the replica.Applier interface.
type dbApplier struct {
	db *DB
}

func (a *dbApplier) ApplyReplicatedEntry(collection string, entry replica.Entry) error {
	coll := a.db.Collection(collection)
	return coll.loadVersion(entry.VectorID, Version{
		Embedding:    cloneVector(entry.Embedding),
		Text:         entry.Text,
		Metadata:     cloneMetadata(entry.Metadata),
		LamportClock: entry.LamportClock,
		ActorID:      entry.ActorID,
		WallClock:    entry.WallClock,
		Tombstone:    entry.Kind == walpkg.EntryTombstone,
	})
}

func (a *dbApplier) ApplySnapshot(data replica.SnapshotData) error {
	coll := a.db.Collection(data.Collection, WithBitWidth(data.BitWidth))
	for _, record := range data.Entries {
		for _, v := range record.Versions {
			if err := coll.loadVersion(record.ID, Version{
				Embedding:    cloneVector(v.Embedding),
				Text:         v.Text,
				Metadata:     cloneMetadata(v.Metadata),
				LamportClock: v.LamportClock,
				ActorID:      v.ActorID,
				WallClock:    v.WallClock,
				Tombstone:    v.Tombstone,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestReplicationPrimaryToFollower(t *testing.T) {
	// Start primary.
	primaryPath := filepath.Join(t.TempDir(), "primary.csdb")
	primaryDB, err := Open(primaryPath, WithProvider(&mockProvider{dim: 16}), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer primaryDB.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	doneCh := make(chan error, 1)
	go func() { doneCh <- primaryDB.Serve(listener) }()

	// Write data on primary.
	coll := primaryDB.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "hello replication"}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("doc-2", Entry{Text: "world replication"}); err != nil {
		t.Fatal(err)
	}

	// Connect a follower DB.
	followerPath := filepath.Join(t.TempDir(), "follower.csdb")
	followerDB, err := Open(followerPath, WithProvider(&mockProvider{dim: 16}))
	if err != nil {
		t.Fatal(err)
	}
	defer followerDB.Close()

	// Connect to primary for pulling.
	client, err := Connect(listener.Addr().String(), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	puller := &rpcPullerAdapter{client: client.remote}
	applier := &dbApplier{db: followerDB}

	follower, err := replica.NewFollower(replica.FollowerConfig{
		Collection: "docs",
		Applier:    applier,
		Puller:     puller,
		Interval:   50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	follower.Start()
	time.Sleep(300 * time.Millisecond)
	follower.Stop()

	// Verify follower has both entries.
	followerColl := followerDB.Collection("docs")
	history1, err := followerColl.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history1) != 1 || history1[0].Text != "hello replication" {
		t.Fatalf("follower doc-1 history = %+v", history1)
	}
	history2, err := followerColl.History("doc-2")
	if err != nil {
		t.Fatal(err)
	}
	if len(history2) != 1 || history2[0].Text != "world replication" {
		t.Fatalf("follower doc-2 history = %+v", history2)
	}

	// Verify follower can search the replicated data.
	results, err := followerColl.Search("hello", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("follower search returned no results")
	}
	if !hasResult(results, "doc-1") {
		t.Fatalf("follower search results = %v, want doc-1 present", results)
	}
}

func TestReplicationCatchUp(t *testing.T) {
	// Start primary with existing data.
	primaryPath := filepath.Join(t.TempDir(), "primary.csdb")
	primaryDB, err := Open(primaryPath, WithProvider(&mockProvider{dim: 8}), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer primaryDB.Close()

	coll := primaryDB.Collection("docs", WithBitWidth(2))
	if err := coll.Put("existing-1", Entry{Text: "pre-existing data"}); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	doneCh := make(chan error, 1)
	go func() { doneCh <- primaryDB.Serve(listener) }()

	// Write more after serve starts (these go into streamer).
	if err := coll.Put("new-1", Entry{Text: "post-serve data"}); err != nil {
		t.Fatal(err)
	}

	// Follower catches up via snapshot + WAL tail.
	followerPath := filepath.Join(t.TempDir(), "follower.csdb")
	followerDB, err := Open(followerPath, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	defer followerDB.Close()

	client, err := Connect(listener.Addr().String(), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	puller := &rpcPullerAdapter{client: client.remote}
	applier := &dbApplier{db: followerDB}

	follower, err := replica.NewFollower(replica.FollowerConfig{
		Collection: "docs",
		Applier:    applier,
		Puller:     puller,
		Interval:   time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := follower.CatchUp("secret"); err != nil {
		t.Fatal(err)
	}

	// Verify follower has both pre-existing and post-serve entries.
	followerColl := followerDB.Collection("docs")
	h1, err := followerColl.History("existing-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(h1) != 1 {
		t.Fatalf("follower existing-1 history len = %d, want 1", len(h1))
	}
	h2, err := followerColl.History("new-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(h2) != 1 {
		t.Fatalf("follower new-1 history len = %d, want 1", len(h2))
	}
}
