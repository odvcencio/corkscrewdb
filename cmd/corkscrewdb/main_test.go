package main

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/odvcencio/corkscrewdb"
)

func TestReplicationFlagValidation(t *testing.T) {
	tests := []struct {
		name          string
		replicateFrom string
		replicateCols string
		wantErr       string
	}{
		{"both empty is ok (primary mode)", "", "", ""},
		{"from without cols fails", "corkscrewdb-0.corkscrewdb.m31labs.svc.cluster.local:4040", "", "-replicate-collections is required when -replicate-from is set"},
		{"cols without from fails", "", "agent-memory,mirage-runs", "-replicate-from is required when -replicate-collections is set"},
		{"both set ok", "corkscrewdb-0:4040", "agent-memory,mirage-runs", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateReplicationFlags(tc.replicateFrom, tc.replicateCols)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("got %v, want error %q", err, tc.wantErr)
			}
		})
	}
}

// TestReplicationEndToEnd asserts that running corkscrewdb with
// -replicate-from and -replicate-collections wires up a per-collection
// Follower backed by a shared DBApplier and eventually pulls data written
// to the primary before the follower was started.
func TestReplicationEndToEnd(t *testing.T) {
	const token = "e2e-token"
	const collection = "e2e-test"

	// 1) Primary: open, listen on a random local port, serve.
	primaryDir := filepath.Join(t.TempDir(), "primary")
	primary, err := corkscrewdb.Open(primaryDir, corkscrewdb.WithToken(token))
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	defer primary.Close()

	primaryListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen primary: %v", err)
	}
	defer primaryListener.Close()

	primaryDone := make(chan error, 1)
	go func() { primaryDone <- primary.Serve(primaryListener) }()

	// 2) Seed one entry BEFORE the follower starts, so CatchUp is exercised.
	if err := primary.Collection(collection).Put("doc-1", corkscrewdb.Entry{Text: "e2e"}); err != nil {
		t.Fatalf("seed primary: %v", err)
	}

	// 3) Boot a follower via runServer in a goroutine.
	followerDir := filepath.Join(t.TempDir(), "follower")
	readyCh := make(chan net.Addr, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- runServer(ctx, serverOpts{
			DataDir:              followerDir,
			Addr:                 "127.0.0.1:0",
			Token:                token,
			ReplicateFrom:        primaryListener.Addr().String(),
			ReplicateCollections: collection,
			ReadyCh:              readyCh,
		})
	}()

	var followerAddr net.Addr
	select {
	case followerAddr = <-readyCh:
	case err := <-runDone:
		t.Fatalf("runServer exited before ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("runServer never reported ready")
	}

	// 4) Connect a client to the follower and poll its collection for the
	// entry replicated from the primary. The follower's local DB is only
	// reachable through its advertised gRPC endpoint while runServer holds
	// it open, so we query it the same way any external caller would.
	follower, err := corkscrewdb.Connect(followerAddr.String(), corkscrewdb.WithToken(token))
	if err != nil {
		t.Fatalf("connect follower: %v", err)
	}
	defer follower.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		history, err := follower.Collection(collection).History("doc-1")
		if err == nil && len(history) >= 1 && history[0].Text == "e2e" {
			// Happy path: replication delivered the entry.
			cancel()
			select {
			case <-runDone:
			case <-time.After(2 * time.Second):
				t.Fatal("runServer did not exit after cancel")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	history, _ := follower.Collection(collection).History("doc-1")
	t.Fatalf("follower never replicated doc-1; got history=%+v", history)
}
