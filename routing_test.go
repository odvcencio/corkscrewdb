package corkscrewdb

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// TestInternalWriteAppliesLocally asserts that a server receiving an Internal
// write for a key it does NOT locally own applies it locally via
// put(..., federated=false): it trusts the client's routing, does no owner
// re-check, and does not re-fan-out (§6.1).
func TestInternalWriteAppliesLocally(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "internal.csdb"), WithProvider(nil),
		WithShards(twoNodeShardLayout(LocalShardOwner, "127.0.0.1:1")...))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, remoteID := pickPeerOwnedIDs(t, db, "vecs", db.localMemberID(), "127.0.0.1:1")
	server := &transportServer{db: db}

	// Internal write for a peer-owned key: applies locally, no rejection.
	if err := server.PutVector(RPCPutVectorRequest{
		Internal:   true,
		Collection: "vecs",
		ID:         remoteID,
		Vector:     []float32{1, 0, 0, 0},
	}, &RPCEmpty{}); err != nil {
		t.Fatalf("internal PutVector err = %v, want nil", err)
	}

	history, err := db.Collection("vecs").historyFor(remoteID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("internal write did not land locally: history len = %d, want 1", len(history))
	}
}

// TestNonInternalWrongOwnerRejected asserts that a server receiving a
// non-Internal direct-client write for a key it does NOT own rejects it with
// ErrWrongOwner and does not fan out (§6.1).
func TestNonInternalWrongOwnerRejected(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "wrongowner.csdb"), WithProvider(nil),
		WithShards(twoNodeShardLayout(LocalShardOwner, "127.0.0.1:1")...))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, remoteID := pickPeerOwnedIDs(t, db, "vecs", db.localMemberID(), "127.0.0.1:1")
	server := &transportServer{db: db}

	err = server.PutVector(RPCPutVectorRequest{
		Collection: "vecs",
		ID:         remoteID,
		Vector:     []float32{1, 0, 0, 0},
	}, &RPCEmpty{})
	if !errors.Is(err, ErrWrongOwner) {
		t.Fatalf("non-internal wrong-owner PutVector err = %v, want ErrWrongOwner", err)
	}

	history, err := db.Collection("vecs").historyFor(remoteID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("rejected write still landed locally: history len = %d", len(history))
	}
}

// TestFederatedPutFansOutToOwner asserts that a federating DB Put to a
// remote-owned key forwards it to the owner with internal=true and lands there
// (§6.1).
func TestFederatedPutFansOutToOwner(t *testing.T) {
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

	dbA, err := Open(filepath.Join(t.TempDir(), "fed-a.csdb"), WithProvider(&mockProvider{dim: 16}),
		WithToken("secret"), WithPeers(addrB), WithShards(twoNodeShardLayout(LocalShardOwner, addrB)...))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbB, err := Open(filepath.Join(t.TempDir(), "fed-b.csdb"), WithProvider(&mockProvider{dim: 16}),
		WithToken("secret"), WithPeers(addrA), WithShards(twoNodeShardLayout(addrA, LocalShardOwner)...))
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
		_ = listenerA.Close()
		_ = listenerB.Close()
		select {
		case <-doneA:
		case <-time.After(2 * time.Second):
			t.Errorf("serve A did not exit")
		}
		select {
		case <-doneB:
		case <-time.After(2 * time.Second):
			t.Errorf("serve B did not exit")
		}
	})

	_, remoteID := pickPeerOwnedIDs(t, dbA, "docs", addrA, addrB)
	collA := dbA.Collection("docs", WithBitWidth(2))
	if err := collA.Put(remoteID, Entry{Text: "fanned out to owner"}); err != nil {
		t.Fatalf("federated Put err = %v, want nil", err)
	}

	// The write landed on dbB (the owner), not dbA.
	ownerHistory, err := dbB.Collection("docs").historyFor(remoteID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ownerHistory) != 1 || ownerHistory[0].Text != "fanned out to owner" {
		t.Fatalf("federated Put did not land on owner: %+v", ownerHistory)
	}
}

// TestSingleNodeRemoteWrite asserts the basic Connect+Put-to-one-server path
// works after un-gating (no shard/federation involved).
func TestSingleNodeRemoteWrite(t *testing.T) {
	serverDB, addr := startRemoteTestServer(t, WithProvider(&mockProvider{dim: 16}), WithToken("secret"))
	_ = serverDB

	client, err := Connect(addr, WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	coll := client.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "single node remote"}); err != nil {
		t.Fatalf("remote Put err = %v, want nil", err)
	}
	history, err := coll.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("remote write history len = %d, want 1", len(history))
	}
}
