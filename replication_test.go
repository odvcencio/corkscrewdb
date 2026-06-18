package corkscrewdb

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"m31labs.dev/corkscrewdb/replica"
)

func TestReplicationPrimaryToFollower(t *testing.T) {
	t.Skip("restored in v0.3.0 Distribution phase: code-carrying replication over codes + raw pull-by-hash")
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

	puller, err := NewRPCPuller(client)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewDBApplier(followerDB)
	if err != nil {
		t.Fatal(err)
	}

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
	t.Skip("restored in v0.3.0 Distribution phase: code-carrying replication over codes + raw pull-by-hash")
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

	puller, err := NewRPCPuller(client)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewDBApplier(followerDB)
	if err != nil {
		t.Fatal(err)
	}

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

func TestReplicationStreamingFollowerReceivesLiveWrites(t *testing.T) {
	t.Skip("restored in v0.3.0 Distribution phase: code-carrying replication over codes + raw pull-by-hash")
	primaryPath := filepath.Join(t.TempDir(), "primary-stream.csdb")
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

	followerPath := filepath.Join(t.TempDir(), "follower-stream.csdb")
	followerDB, err := Open(followerPath, WithProvider(&mockProvider{dim: 16}))
	if err != nil {
		t.Fatal(err)
	}
	defer followerDB.Close()

	client, err := Connect(listener.Addr().String(), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	puller, err := NewRPCPuller(client)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewDBApplier(followerDB)
	if err != nil {
		t.Fatal(err)
	}

	follower, err := replica.NewFollower(replica.FollowerConfig{
		Collection: "docs",
		Applier:    applier,
		Puller:     puller,
		Interval:   time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	follower.Start()
	defer follower.Stop()

	coll := primaryDB.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-live", Entry{Text: "live stream replication"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		history, err := followerDB.Collection("docs").History("doc-live")
		if err != nil {
			t.Fatal(err)
		}
		if len(history) == 1 && history[0].Text == "live stream replication" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	history, err := followerDB.Collection("docs").History("doc-live")
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("stream follower did not receive live write: %+v", history)
}
