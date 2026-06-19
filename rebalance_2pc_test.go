package corkscrewdb

import (
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestImportOwnedReconstructsCodes drives a real peer snapshot+entries handoff
// through importOwnedSnapshot/importOwnedEntries and asserts only keys owned
// under L1 land on the gainer, with codes intact and searchable. Raw vectors
// are fetched by hash for raw_store collections (never inlined).
func TestImportOwnedReconstructsCodes(t *testing.T) {
	// Peer (source) node: serves a collection with two keys.
	peerDB, peerAddr := startRemoteTestServer(t, WithProvider(&mockProvider{dim: 16}))
	peerDB.registerServeAddr(peerAddr)

	peerColl := peerDB.Collection("docs", WithBitWidth(2))
	// Pick two ids: one we will hand to the local node (L1 owner = local), one
	// that stays with the peer.
	maxKey := ^uint64(0)
	mid := maxKey / 2
	l0 := []ShardAssignment{
		{ID: "s-local", Owner: "local-node", Start: 0, End: mid},
		{ID: "s-peer", Owner: peerAddr, Start: mid + 1, End: maxKey},
	}
	l1 := []ShardAssignment{
		{ID: "s-local", Owner: "local-node", Start: 0, End: maxKey}, // local gains everything
	}

	// Find one id owned by the peer under L0 (so the peer holds it) that moves
	// to local under L1, and one that ... under L1 local owns everything, so
	// every peer key moves. Use two distinct peer-owned ids.
	var k1, k2 string
	for i := 0; i < 100000 && (k1 == "" || k2 == ""); i++ {
		id := "doc-imp-" + strconv.Itoa(i)
		key := shardKey("docs", id)
		if key > mid {
			if k1 == "" {
				k1 = id
			} else if k2 == "" {
				k2 = id
			}
		}
	}
	if k1 == "" || k2 == "" {
		t.Fatal("could not find two peer-owned ids")
	}
	if err := peerColl.PutVector(k1, []float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := peerColl.PutVector(k2, []float32{0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}

	// Local (gainer) node, registered with a stable id and a peer client.
	localDB, err := Open(filepath.Join(t.TempDir(), "gainer.csdb"),
		WithProvider(&mockProvider{dim: 16}), WithPeers(peerAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer localDB.Close()
	localDB.serveAddr = "local-node"

	client, err := localDB.peerClient(peerAddr)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.PullSnapshot(RPCPullSnapshotRequest{Collection: "docs"})
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.RawStore {
		t.Fatal("peer snapshot RawStore = false, want true (default raw-store collection)")
	}

	oldMembers := []string{"local-node", peerAddr}
	if err := localDB.importOwnedSnapshot(peerAddr, RPCCollectionInfo{Name: "docs", BitWidth: snapshot.BitWidth}, snapshot, l0, oldMembers, l1); err != nil {
		t.Fatalf("importOwnedSnapshot err = %v", err)
	}

	// Both keys are now owned by local under L1 and must have landed with codes.
	for _, id := range []string{k1, k2} {
		hist, err := localDB.Collection("docs").historyFor(id, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) != 1 {
			t.Fatalf("imported key %q history len = %d, want 1", id, len(hist))
		}
		if hist[0].Quantized == nil {
			t.Fatalf("imported key %q lost its codes", id)
		}
		if len(hist[0].RawHash) != 32 {
			t.Fatalf("imported key %q lost its raw hash", id)
		}
		// Raw fetched by hash and stored locally.
		raw, err := localDB.Collection("docs").getRaw(hist[0].RawHash)
		if err != nil {
			t.Fatalf("imported raw for %q not local: %v", id, err)
		}
		if len(raw) == 0 {
			t.Fatalf("imported raw for %q is empty", id)
		}
	}
}

// TestLegacyMemberFencedRebalanceRejected asserts that OrchestrateRebalance
// rejects a fenced cluster rebalance whose L0/L1 diff would fall through to
// legacy peer-hash-mod for a moving key (no explicit shard ranges).
func TestLegacyMemberFencedRebalanceRejected(t *testing.T) {
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerA.Close()
	addrA := listenerA.Addr().String()

	// No explicit shards: routing is entirely legacy peer-hash-mod.
	dbA, err := Open(filepath.Join(t.TempDir(), "legacy.csdb"),
		WithProvider(&mockProvider{dim: 16}), WithPeers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbA.registerServeAddr(addrA)

	done := make(chan error, 1)
	go func() { done <- dbA.Serve(listenerA) }()
	t.Cleanup(func() {
		_ = listenerA.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("serve A did not exit")
		}
	})

	// The CURRENT layout is empty (legacy peer-hash-mod), so any moving key's
	// L0 owner is resolved via legacy members — the symmetric diff cannot be
	// expressed as explicit ranges. A full-range explicit L1 is still rejected.
	maxKey := ^uint64(0)
	mid := maxKey / 2
	newLayout := []ShardAssignment{
		{ID: "s-a", Owner: addrA, Start: 0, End: mid},
		{ID: "s-b", Owner: "127.0.0.1:1", Start: mid + 1, End: maxKey},
	}
	err = dbA.OrchestrateRebalance(newLayout...)
	if !errors.Is(err, ErrLegacyRebalanceUnsafe) {
		t.Fatalf("OrchestrateRebalance with legacy-fallthrough diff err = %v, want ErrLegacyRebalanceUnsafe", err)
	}
}

// servedNode is a served test DB plus its address and a cleanup-registered
// listener.
type servedNode struct {
	db   *DB
	addr string
}

// startServedNode opens a served DB at addr-bound listener with the given
// options and registers cleanup.
func startServedNode(t *testing.T, name string, opts ...Option) *servedNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	db, err := Open(filepath.Join(t.TempDir(), name+".csdb"), opts...)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	db.registerServeAddr(addr)
	done := make(chan error, 1)
	go func() { done <- db.Serve(listener) }()
	t.Cleanup(func() {
		_ = listener.Close()
		_ = db.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("serve %s did not exit", name)
		}
	})
	return &servedNode{db: db, addr: addr}
}

// recordingClient wraps a remoteClient and appends a label to *log on each 2PC
// control-plane call, so a test can assert call ordering (e.g. all Freeze ACKs
// precede any Pull).
type recordingClient struct {
	remoteClient
	mu  *sync.Mutex
	log *[]string
}

func (r *recordingClient) record(label string) {
	r.mu.Lock()
	*r.log = append(*r.log, label)
	r.mu.Unlock()
}

func (r *recordingClient) FreezeRebalance(shards []ShardAssignment, epoch uint64, coordinator string) error {
	r.record("freeze")
	return r.remoteClient.FreezeRebalance(shards, epoch, coordinator)
}

func (r *recordingClient) PullSnapshot(req RPCPullSnapshotRequest) (RPCPullSnapshotResponse, error) {
	r.record("pull-snapshot")
	return r.remoteClient.PullSnapshot(req)
}

func (r *recordingClient) PullEntries(req RPCPullEntriesRequest) (RPCPullEntriesResponse, error) {
	r.record("pull-entries")
	return r.remoteClient.PullEntries(req)
}

func (r *recordingClient) CommitRebalance(shards []ShardAssignment, epoch uint64) error {
	r.record("commit")
	return r.remoteClient.CommitRebalance(shards, epoch)
}

func (r *recordingClient) AbortRebalance(epoch uint64) error {
	r.record("abort")
	return r.remoteClient.AbortRebalance(epoch)
}

// wrapPeerClient replaces the cached peer client for addr with a recording
// wrapper that logs into *log. The peer must already be dialable.
func wrapPeerClient(t *testing.T, db *DB, addr string, mu *sync.Mutex, log *[]string) {
	t.Helper()
	inner, err := db.peerClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	db.peerClients[addr] = &recordingClient{remoteClient: inner, mu: mu, log: log}
	db.mu.Unlock()
}

// orchestrateLayouts builds a 2-node old/new layout that moves the cut so a
// band of keys hands off from B to A (A gains, B loses).
func orchestrateLayouts(addrA, addrB string) (oldA, newAll []ShardAssignment) {
	maxKey := ^uint64(0)
	oldCut := maxKey / 2
	newCut := (maxKey / 4) * 3 // A's range grows from [0,oldCut] to [0,newCut]
	oldA = []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: addrB, Start: oldCut + 1, End: maxKey},
	}
	newAll = []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: newCut},
		{ID: "shard-b", Owner: addrB, Start: newCut + 1, End: maxKey},
	}
	return oldA, newAll
}

func TestFreezeBeforePullOrdersBarrier(t *testing.T) {
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addrBPre := listenerB.Addr().String()
	_ = listenerB.Close()

	nodeA := startServedNode(t, "barrier-a", WithProvider(&mockProvider{dim: 16}), WithToken("s"), WithPeers(addrBPre))
	addrA := nodeA.addr
	oldA, newAll := orchestrateLayouts(addrA, addrBPre)
	// Reconfigure A's manifest shards to oldA now that we know addrA.
	if err := nodeA.db.applyShardLayout(oldA, 0); err != nil {
		t.Fatal(err)
	}

	// Node B bound to the pre-reserved port.
	lB, err := net.Listen("tcp", addrBPre)
	if err != nil {
		t.Fatal(err)
	}
	addrB := lB.Addr().String()
	dbB, err := Open(filepath.Join(t.TempDir(), "barrier-b.csdb"), WithProvider(&mockProvider{dim: 16}), WithToken("s"), WithPeers(addrA))
	if err != nil {
		t.Fatal(err)
	}
	dbB.registerServeAddr(addrB)
	oldB := []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: oldA[0].Start, End: oldA[0].End},
		{ID: "shard-b", Owner: LocalShardOwner, Start: oldA[1].Start, End: oldA[1].End},
	}
	if err := dbB.applyShardLayout(oldB, 0); err != nil {
		t.Fatal(err)
	}
	doneB := make(chan error, 1)
	go func() { doneB <- dbB.Serve(lB) }()
	t.Cleanup(func() {
		_ = lB.Close()
		_ = dbB.Close()
		select {
		case <-doneB:
		case <-time.After(2 * time.Second):
			t.Errorf("serve B did not exit")
		}
	})

	// Seed a moving key on B (B owns it under L0, A gains it under L1).
	var movingID string
	for i := 0; i < 100000; i++ {
		id := "doc-bar-" + strconv.Itoa(i)
		key := shardKey("docs", id)
		if key > oldA[0].End && key <= newAll[0].End {
			movingID = id
			break
		}
	}
	if movingID == "" {
		t.Fatal("no moving key found")
	}
	if err := dbB.Collection("docs", WithBitWidth(2)).PutVector(movingID, unitVec(16, 0)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var log []string
	wrapPeerClient(t, nodeA.db, addrB, &mu, &log)

	if err := nodeA.db.OrchestrateRebalance(newAll...); err != nil {
		t.Fatalf("OrchestrateRebalance err = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Every freeze ACK must precede any pull.
	firstPull := -1
	lastFreeze := -1
	for i, ev := range log {
		switch ev {
		case "freeze":
			lastFreeze = i
		case "pull-snapshot", "pull-entries":
			if firstPull == -1 {
				firstPull = i
			}
		}
	}
	if firstPull == -1 {
		t.Fatalf("no pull recorded; log = %v", log)
	}
	if lastFreeze == -1 {
		t.Fatalf("no freeze recorded; log = %v", log)
	}
	if lastFreeze > firstPull {
		t.Fatalf("freeze (idx %d) ordered after first pull (idx %d); log = %v", lastFreeze, firstPull, log)
	}
}

// unitVec returns a dim-length vector with 1 at index hot.
func unitVec(dim, hot int) []float32 {
	v := make([]float32, dim)
	if hot < dim {
		v[hot] = 1
	}
	return v
}

// twoNode2PC stands up two served nodes A and B with an explicit L0 layout
// (A owns [0,oldCut], B owns the rest) and returns them plus the L1 layout that
// hands a band of keys from B to A, and a moving key already written on B.
func twoNode2PC(t *testing.T, optsExtra ...Option) (a, b *servedNode, l1 []ShardAssignment, movingID string) {
	t.Helper()
	// Reserve two ports first.
	la, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addrA := la.Addr().String()
	lb, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = la.Close()
		t.Fatal(err)
	}
	addrB := lb.Addr().String()

	maxKey := ^uint64(0)
	oldCut := maxKey / 2
	newCut := (maxKey / 4) * 3
	l0A := []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: addrB, Start: oldCut + 1, End: maxKey},
	}
	l0B := []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: LocalShardOwner, Start: oldCut + 1, End: maxKey},
	}
	l1 = []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: newCut},
		{ID: "shard-b", Owner: addrB, Start: newCut + 1, End: maxKey},
	}

	baseOpts := append([]Option{WithProvider(&mockProvider{dim: 16}), WithToken("s")}, optsExtra...)
	dbA, err := Open(filepath.Join(t.TempDir(), "2pc-a.csdb"), append(append([]Option{}, baseOpts...), WithPeers(addrB), WithShards(l0A...))...)
	if err != nil {
		t.Fatal(err)
	}
	dbB, err := Open(filepath.Join(t.TempDir(), "2pc-b.csdb"), append(append([]Option{}, baseOpts...), WithPeers(addrA), WithShards(l0B...))...)
	if err != nil {
		t.Fatal(err)
	}
	dbA.registerServeAddr(addrA)
	dbB.registerServeAddr(addrB)

	doneA := make(chan error, 1)
	go func() { doneA <- dbA.Serve(la) }()
	doneB := make(chan error, 1)
	go func() { doneB <- dbB.Serve(lb) }()
	t.Cleanup(func() {
		_ = la.Close()
		_ = lb.Close()
		_ = dbA.Close()
		_ = dbB.Close()
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

	// A moving key: owned by B under L0 (key > oldCut), gained by A under L1
	// (key <= newCut).
	for i := 0; i < 200000; i++ {
		id := "doc-2pc-" + strconv.Itoa(i)
		key := shardKey("docs", id)
		if key > oldCut && key <= newCut {
			movingID = id
			break
		}
	}
	if movingID == "" {
		t.Fatal("no moving key found")
	}
	if err := dbB.Collection("docs", WithBitWidth(2)).PutVector(movingID, unitVec(16, 1)); err != nil {
		t.Fatal(err)
	}
	return &servedNode{db: dbA, addr: addrA}, &servedNode{db: dbB, addr: addrB}, l1, movingID
}

func TestOrchestrate2PCHappyPath(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	if err := a.db.OrchestrateRebalance(l1...); err != nil {
		t.Fatalf("OrchestrateRebalance err = %v", err)
	}

	// The moving key handed off: present on A (gainer), pruned on B (loser).
	gained, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(gained) != 1 {
		t.Fatalf("gainer history = %+v, want 1", gained)
	}
	lost, err := b.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 0 {
		t.Fatalf("loser retained handed-off key: %+v", lost)
	}

	// A post-cutover routed write to the moving key lands on the new owner (A).
	if err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 2)}); err != nil {
		t.Fatalf("post-cutover routed write err = %v", err)
	}
	final, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != 2 {
		t.Fatalf("new owner history after routed write = %+v, want 2", final)
	}
}

func TestPrepareRaceNoLostWrite(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// Fire a client write to the moving key during the gainer's Pull sub-phase
	// (the seam between the freeze barrier and the pull). The write goes through
	// B's federation, which now routes the moving key to its L0 owner (B) — but
	// B is frozen, so the write must be rejected with ErrRebalanceInProgress,
	// never silently dropped.
	a.db.rebalanceHooks = &rebalanceHooks{
		beforePull: func(target string) {
			err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 3)})
			if err == nil {
				t.Errorf("racing write during freeze was accepted; want ErrRebalanceInProgress")
			} else if !errors.Is(err, ErrRebalanceInProgress) {
				t.Errorf("racing write err = %v, want ErrRebalanceInProgress", err)
			}
		},
	}

	if err := a.db.OrchestrateRebalance(l1...); err != nil {
		t.Fatalf("OrchestrateRebalance err = %v", err)
	}
	a.db.rebalanceHooks = nil

	// After cutover the moving key is present on A with exactly the pre-handoff
	// version (the racing write was rejected, so no extra version, no loss).
	gained, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(gained) != 1 {
		t.Fatalf("gainer history after race = %+v, want 1 (racing write rejected, original present)", gained)
	}
}

// failingClient wraps a remoteClient and injects a failure on a named method.
// failOn is the method label; after failAfter successful passthroughs the next
// call returns injErr. A nil injErr means "succeed" (used to flip behavior).
type failingClient struct {
	remoteClient
	mu        sync.Mutex
	failOn    string
	failAfter int
	calls     map[string]int
	injErr    error
}

func newFailingClient(inner remoteClient, failOn string, failAfter int, injErr error) *failingClient {
	return &failingClient{remoteClient: inner, failOn: failOn, failAfter: failAfter, calls: map[string]int{}, injErr: injErr}
}

func (f *failingClient) shouldFail(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if method != f.failOn {
		return nil
	}
	n := f.calls[method]
	f.calls[method] = n + 1
	if n >= f.failAfter {
		return f.injErr
	}
	return nil
}

func (f *failingClient) FreezeRebalance(shards []ShardAssignment, epoch uint64, coordinator string) error {
	if err := f.shouldFail("freeze"); err != nil {
		return err
	}
	return f.remoteClient.FreezeRebalance(shards, epoch, coordinator)
}

func (f *failingClient) PrepareRebalance(shards []ShardAssignment, epoch uint64, coordinator string) error {
	if err := f.shouldFail("prepare"); err != nil {
		return err
	}
	return f.remoteClient.PrepareRebalance(shards, epoch, coordinator)
}

func (f *failingClient) CommitRebalance(shards []ShardAssignment, epoch uint64) error {
	if err := f.shouldFail("commit"); err != nil {
		return err
	}
	return f.remoteClient.CommitRebalance(shards, epoch)
}

func injectFailingPeer(t *testing.T, db *DB, addr, failOn string, failAfter int, injErr error) *failingClient {
	t.Helper()
	inner, err := db.peerClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	fc := newFailingClient(inner, failOn, failAfter, injErr)
	db.mu.Lock()
	db.peerClients[addr] = fc
	db.mu.Unlock()
	return fc
}

func TestFreezeFailAborts(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// B (the loser) fails its Freeze immediately.
	injectFailingPeer(t, a.db, b.addr, "freeze", 0, errors.New("injected freeze failure"))

	err := a.db.OrchestrateRebalance(l1...)
	if err == nil {
		t.Fatal("OrchestrateRebalance returned nil despite injected freeze failure")
	}

	// B never froze (its freeze failed), so the moving key is writable on B,
	// which still owns it under the unchanged L0.
	if err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 4)}); err != nil {
		t.Fatalf("moving-key write on old owner after abort err = %v, want nil", err)
	}

	// The coordinator A did not commit: it still owns its L0 range only and is IDLE.
	if a.db.rebalanceSnapshot().phase != phaseIdle {
		t.Fatalf("coordinator not IDLE after freeze-fail abort: %v", a.db.rebalanceSnapshot().phase)
	}
}

func TestPullFailAbortsReachesFrozenLoser(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// B (loser) freezes fine, but its Prepare (pull request handling) ... no:
	// the GAINER is A (local). To make the gainer's pull fail, fail B's
	// PullSnapshot. A pulls from B; injecting a failure on B's PullSnapshot makes
	// A's local pullRebalanceShards fail after B has frozen.
	injectPullFailure(t, a.db, b.addr)

	err := a.db.OrchestrateRebalance(l1...)
	if err == nil {
		t.Fatal("OrchestrateRebalance returned nil despite injected pull failure")
	}

	// The frozen loser B must have received Abort and unfrozen: the moving key
	// is writable again on B (§5.4 row 2).
	if b.db.rebalanceSnapshot().phase != phaseIdle {
		t.Fatalf("frozen loser not unfrozen after pull-fail abort: %v", b.db.rebalanceSnapshot().phase)
	}
	if err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 5)}); err != nil {
		t.Fatalf("moving-key write on frozen loser after abort err = %v, want nil", err)
	}
}

// pullFailClient fails PullSnapshot to simulate a gainer pull failure after the
// loser has frozen.
type pullFailClient struct {
	remoteClient
}

func (p *pullFailClient) PullSnapshot(req RPCPullSnapshotRequest) (RPCPullSnapshotResponse, error) {
	return RPCPullSnapshotResponse{}, errors.New("injected pull-snapshot failure")
}

func injectPullFailure(t *testing.T, db *DB, addr string) {
	t.Helper()
	inner, err := db.peerClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	db.peerClients[addr] = &pullFailClient{remoteClient: inner}
	db.mu.Unlock()
}

func TestCommitPartialFailureNoSplitBrain(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// B's FIRST CommitRebalance fails; the second (retry) succeeds.
	fc := injectFailingPeer(t, a.db, b.addr, "commit", 0, errors.New("injected commit failure"))

	err := a.db.OrchestrateRebalance(l1...)
	if err == nil {
		t.Fatal("OrchestrateRebalance returned nil despite injected commit failure")
	}

	// The coordinator A already durably decided commit (it bumped its epoch and
	// applied L1 locally before broadcasting Commit). The laggard B is still on
	// L0 and its moving keys are fenced (frozen) — no conflicting write.
	if cerr := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 6)}); !errors.Is(cerr, ErrRebalanceInProgress) {
		t.Fatalf("laggard moving-key write err = %v, want ErrRebalanceInProgress (fenced)", cerr)
	}

	// Stop failing; retry the commit/prune to converge to L1.
	fc.mu.Lock()
	fc.failOn = ""
	fc.mu.Unlock()
	if err := a.db.OrchestrateRebalance(l1...); err != nil {
		// A is already committed (idempotent); B commits on retry.
		t.Fatalf("retry OrchestrateRebalance err = %v", err)
	}

	// Cluster converged: A owns the moving key, B pruned it.
	gained, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(gained) != 1 {
		t.Fatalf("gainer history after converge = %+v, want 1", gained)
	}
	lost, err := b.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 0 {
		t.Fatalf("loser retained moving key after converge: %+v", lost)
	}
}

// driveToPrepared puts a participant DB into a persisted PREPARED state for
// epoch with the given coordinator address and pending layout, then closes it
// and returns the path so the caller can reopen and exercise recovery.
func driveToPrepared(t *testing.T, path, coordAddr string, epoch uint64, pending []ShardAssignment) {
	t.Helper()
	p, err := Open(path, WithProvider(nil), WithShards(twoNodeShardLayout("other", LocalShardOwner)...))
	if err != nil {
		t.Fatal(err)
	}
	p.installFreeze(epoch, coordAddr, pending)
	p.markPrepared()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedParticipantQueriesCoordinatorOnRestart(t *testing.T) {
	maxKey := ^uint64(0)
	pending := []ShardAssignment{{ID: "s", Owner: "other", Start: 0, End: maxKey}}

	t.Run("coordinator committed -> commit-forward", func(t *testing.T) {
		// Coordinator with a durable commit record for epoch 5.
		coord := startServedNode(t, "coord-commit", WithProvider(nil), WithToken("s"))
		coord.db.mu.Lock()
		coord.db.manifest.RebalanceEpoch = 5
		coord.db.mu.Unlock()
		if err := coord.db.saveManifest(); err != nil {
			t.Fatal(err)
		}

		path := filepath.Join(t.TempDir(), "p-commit.csdb")
		driveToPrepared(t, path, coord.addr, 5, pending)

		p, err := Open(path, WithProvider(nil), WithToken("s"))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		// Recovery committed-forward: phase IDLE and the layout is L1 (pending).
		if p.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("participant not IDLE after commit-forward: %v", p.rebalanceSnapshot().phase)
		}
		if len(p.shardAssignments()) != 1 {
			t.Fatalf("participant did not apply L1: shards = %+v", p.shardAssignments())
		}
	})

	t.Run("coordinator aborted -> revert to L0", func(t *testing.T) {
		// Coordinator with NO commit record for epoch 5 (floor at 0 => abort).
		coord := startServedNode(t, "coord-abort", WithProvider(nil), WithToken("s"))

		path := filepath.Join(t.TempDir(), "p-abort.csdb")
		driveToPrepared(t, path, coord.addr, 5, pending)

		p, err := Open(path, WithProvider(nil), WithToken("s"))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if p.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("participant not IDLE after abort: %v", p.rebalanceSnapshot().phase)
		}
		// Reverted to L0 (the original two-shard layout, not the single-shard L1).
		if len(p.shardAssignments()) != 2 {
			t.Fatalf("participant did not revert to L0: shards = %+v", p.shardAssignments())
		}
	})

	t.Run("coordinator gave away all shards but is still reachable", func(t *testing.T) {
		// The coordinator owns NO shards (it gave them all away), so it is in no
		// participant's shard-derived peer set. Recovery must STILL resolve by
		// dialing the persisted coordinator address directly.
		coord := startServedNode(t, "coord-empty", WithProvider(nil), WithToken("s"))
		coord.db.mu.Lock()
		coord.db.manifest.RebalanceEpoch = 9 // committed => commit
		// Coordinator owns nothing under the current layout.
		coord.db.mu.Unlock()
		if err := coord.db.saveManifest(); err != nil {
			t.Fatal(err)
		}

		path := filepath.Join(t.TempDir(), "p-empty.csdb")
		// The participant's shard layout does NOT mention the coordinator at all.
		p0, err := Open(path, WithProvider(nil), WithToken("s"))
		if err != nil {
			t.Fatal(err)
		}
		p0.installFreeze(9, coord.addr, pending)
		p0.markPrepared()
		if err := p0.Close(); err != nil {
			t.Fatal(err)
		}

		p, err := Open(path, WithProvider(nil), WithToken("s"))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if p.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("participant not IDLE after dialing coordinator-with-no-shards: %v", p.rebalanceSnapshot().phase)
		}
	})
}

// TestForceAbortedEpochRecoversAsAbort is the Bug 1 force-abort regression: a
// participant reaches PREPARED at epoch N (floor still 0), then the operator
// runs ForceAbortRebalance(N) on the coordinator — bumping the coordinator's
// floor PAST N — WITHOUT the abort broadcast ever reaching the (down)
// participant. On restart the participant queries the coordinator for epoch N;
// the coordinator's per-epoch outcome must be ABORT (the floor moved past N
// without committing N), so the participant reverts to L0 and unfreezes. The
// old `committed >= epoch ⇒ COMMIT` rule would mis-resolve this as COMMIT and
// silently apply a force-aborted layout.
func TestForceAbortedEpochRecoversAsAbort(t *testing.T) {
	maxKey := ^uint64(0)
	pending := []ShardAssignment{{ID: "s", Owner: "other", Start: 0, End: maxKey}}

	const epoch = 5
	// Coordinator starts with no commit record for epoch 5 (floor 0).
	coord := startServedNode(t, "coord-forceabort", WithProvider(nil), WithToken("s"))

	// Participant reaches PREPARED at epoch 5 with coord as its coordinator, then
	// goes down (Close) — it will NOT receive the abort broadcast.
	path := filepath.Join(t.TempDir(), "p-forceabort.csdb")
	driveToPrepared(t, path, coord.addr, epoch, pending)

	// Operator force-aborts epoch 5 on the coordinator. This bumps the coordinator
	// floor strictly past 5; the participant is down so it never sees the abort.
	if err := coord.db.ForceAbortRebalance(epoch); err != nil {
		t.Fatal(err)
	}
	if coord.db.committedEpoch() <= epoch {
		t.Fatalf("force-abort floor = %d, want > %d", coord.db.committedEpoch(), epoch)
	}
	// Sanity: the coordinator's per-epoch outcome for epoch 5 is ABORT, NOT commit.
	if d, _ := coord.db.resolveRebalanceDecision(epoch); d != decisionAbort {
		t.Fatalf("resolveRebalanceDecision(%d) = %v, want decisionAbort (force-aborted)", epoch, d)
	}

	// Participant restarts and recovers by querying the coordinator.
	p, err := Open(path, WithProvider(nil), WithToken("s"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.rebalanceSnapshot().phase != phaseIdle {
		t.Fatalf("participant not IDLE after force-abort recovery: %v", p.rebalanceSnapshot().phase)
	}
	// It must have reverted to L0 (the original two-shard layout), NOT committed
	// the single-shard force-aborted L1.
	if got := len(p.shardAssignments()); got != 2 {
		t.Fatalf("participant did not revert to L0 after force-abort: shards = %+v (want 2)", p.shardAssignments())
	}
}

// TestSupersededEpochRecoversAsAbort is the Bug 1 supersession regression: a
// DIFFERENT layout commits at epoch N+1 on the coordinator, then a stale
// epoch-N PREPARED participant recovers. The coordinator's per-epoch outcome
// for N must be ABORT (a newer rebalance committed, the floor is now N+1), so
// the participant reverts to L0 instead of applying its stale L1_N and creating
// a split-brain layout. The old `committed >= epoch ⇒ COMMIT` rule would
// mis-resolve this as COMMIT.
func TestSupersededEpochRecoversAsAbort(t *testing.T) {
	maxKey := ^uint64(0)
	staleL1 := []ShardAssignment{{ID: "s", Owner: "other", Start: 0, End: maxKey}}

	const staleEpoch = 5
	// Coordinator commits a DIFFERENT layout at epoch 6 (the floor becomes 6).
	coord := startServedNode(t, "coord-superseded", WithProvider(nil), WithToken("s"))
	differentL1 := []ShardAssignment{
		{ID: "s-a", Owner: coord.addr, Start: 0, End: maxKey / 2},
		{ID: "s-b", Owner: "other", Start: maxKey/2 + 1, End: maxKey},
	}
	if err := coord.db.applyShardLayout(differentL1, staleEpoch+1); err != nil {
		t.Fatal(err)
	}
	if coord.db.committedEpoch() != staleEpoch+1 {
		t.Fatalf("coordinator floor = %d, want %d", coord.db.committedEpoch(), staleEpoch+1)
	}
	// The coordinator's per-epoch outcome for the stale epoch is ABORT.
	if d, _ := coord.db.resolveRebalanceDecision(staleEpoch); d != decisionAbort {
		t.Fatalf("resolveRebalanceDecision(%d) = %v, want decisionAbort (superseded)", staleEpoch, d)
	}

	// A stale participant reaches PREPARED at epoch 5 (it missed the newer epoch).
	path := filepath.Join(t.TempDir(), "p-superseded.csdb")
	driveToPrepared(t, path, coord.addr, staleEpoch, staleL1)

	p, err := Open(path, WithProvider(nil), WithToken("s"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.rebalanceSnapshot().phase != phaseIdle {
		t.Fatalf("stale participant not IDLE after supersession recovery: %v", p.rebalanceSnapshot().phase)
	}
	// It reverts to L0 (two shards), NOT the stale single-shard L1_5.
	if got := len(p.shardAssignments()); got != 2 {
		t.Fatalf("stale participant applied superseded L1_5 instead of reverting to L0: shards = %+v (want 2)", p.shardAssignments())
	}
}

func TestCoordinatorCrashRecovery(t *testing.T) {
	t.Run("after durable decide -> commit-forward", func(t *testing.T) {
		a, b, l1, movingID := twoNode2PC(t)
		// Stop right after the durable decision write (commit record present).
		a.db.rebalanceHooks = &rebalanceHooks{
			afterDurableDecide: func(epoch uint64) {
				panic("stop-after-durable-decide")
			},
		}
		func() {
			defer func() { _ = recover() }()
			_ = a.db.OrchestrateRebalance(l1...)
		}()
		a.db.rebalanceHooks = nil
		// The coordinator A holds a durable commit record (COMMITTING, floor>=epoch).
		st := a.db.rebalanceSnapshot()
		if st.phase != phaseCommitting {
			t.Fatalf("coordinator phase after durable decide = %v, want COMMITTING", st.phase)
		}
		// Recovery (re-running the recover step) commits-forward locally.
		if err := a.db.recoverPendingRebalance(); err != nil {
			t.Fatalf("recoverPendingRebalance err = %v", err)
		}
		if a.db.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("coordinator not IDLE after commit-forward recovery")
		}
		gained, err := a.db.Collection("docs").historyFor(movingID, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(gained) != 1 {
			t.Fatalf("gainer missing moving key after commit-forward: %+v", gained)
		}
		_ = b
	})

	t.Run("before durable decide -> abort", func(t *testing.T) {
		a, b, l1, movingID := twoNode2PC(t)
		// Stop right before the durable decision (no commit record): panic in the
		// freeze barrier seam.
		a.db.rebalanceHooks = &rebalanceHooks{
			afterFreezeBarrier: func(epoch uint64) {
				panic("stop-before-durable-decide")
			},
		}
		func() {
			defer func() { _ = recover() }()
			_ = a.db.OrchestrateRebalance(l1...)
		}()
		a.db.rebalanceHooks = nil
		// No commit record: floor still below the in-flight epoch.
		if a.db.committedEpoch() >= a.db.rebalanceSnapshot().epoch {
			t.Fatalf("unexpected commit record before durable decide")
		}
		// Recovery aborts: coordinator self-aborts and broadcasts Abort.
		if err := a.db.recoverPendingRebalance(); err != nil {
			t.Fatalf("recoverPendingRebalance err = %v", err)
		}
		if a.db.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("coordinator not IDLE after abort recovery")
		}
		// The frozen loser B unfroze; the moving key is writable on B again.
		if err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 7)}); err != nil {
			t.Fatalf("moving-key write on loser after coordinator-abort recovery err = %v", err)
		}
	})
}

func TestForceAbortHighestEpochAndStaleCommitAborts(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "forceabort.csdb"), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// ForceAbortRebalance bumps the committed floor strictly above the stuck epoch.
	before := db.committedEpoch()
	if err := db.ForceAbortRebalance(3); err != nil {
		t.Fatal(err)
	}
	after := db.committedEpoch()
	if after <= 3 || after <= before {
		t.Fatalf("force-abort floor = %d, want strictly > max(committed=%d, stuck=3)", after, before)
	}
}

func TestForceAbortPersistsAndStaleCommitRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stale.csdb")
	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}

	// Force-abort epoch 4 => floor becomes 5.
	if err := db.ForceAbortRebalance(4); err != nil {
		t.Fatal(err)
	}
	floor := db.committedEpoch()
	if floor <= 4 {
		t.Fatalf("force-abort floor = %d, want > 4", floor)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.committedEpoch() != floor {
		t.Fatalf("floor after reopen = %d, want %d (persisted)", reopened.committedEpoch(), floor)
	}

	// A stale-epoch commit (epoch <= floor) is rejected with ErrStaleEpoch.
	if err := reopened.applyShardLayout(twoNodeShardLayout("a", "b"), 4); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale commit err = %v, want ErrStaleEpoch", err)
	}
}

func TestStaleEpochFreezeRejected(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stalefreeze.csdb"), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.mu.Lock()
	db.manifest.RebalanceEpoch = 5
	db.mu.Unlock()
	if err := db.saveManifest(); err != nil {
		t.Fatal(err)
	}

	// A Freeze/Prepare with epoch <= committed floor is rejected.
	if err := db.freezeRebalanceShards(5, "coord", twoNodeShardLayout("a", "b")); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale freeze err = %v, want ErrStaleEpoch", err)
	}
	if err := db.pullRebalanceShards(3, "coord", twoNodeShardLayout("a", "b")); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale prepare err = %v, want ErrStaleEpoch", err)
	}
}

// TestCoordinatorStaleCommitTreatsAsAbort verifies that when a participant
// rejects the coordinator's CommitRebalance with ErrStaleEpoch (a force-abort
// bumped the floor past this epoch), the coordinator treats the whole rebalance
// as aborted: it does NOT proceed to Prune and reverts its own pending state.
func TestCoordinatorStaleCommitTreatsAsAbort(t *testing.T) {
	a, b, l1, _ := twoNode2PC(t)

	// Inject a peer that rejects Commit with ErrStaleEpoch (simulating a peer
	// whose floor was bumped past this epoch by a concurrent force-abort).
	injectFailingPeer(t, a.db, b.addr, "commit", 0, ErrStaleEpoch)

	err := a.db.OrchestrateRebalance(l1...)
	if !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("OrchestrateRebalance err = %v, want ErrStaleEpoch", err)
	}
	// The coordinator reverted its own pending state (it does not stay COMMITTING).
	if a.db.rebalanceSnapshot().phase != phaseIdle {
		t.Fatalf("coordinator phase after stale-commit = %v, want IDLE", a.db.rebalanceSnapshot().phase)
	}
}

// TestGainerImportSurvivesRestart is the data-durability regression for the 2PC
// rebalance handoff: the gainer imports a moving range, the rebalance COMMITs
// (so the loser PRUNES its copy and the gainer is now the authoritative owner),
// then the gainer is CLOSED and REOPENED with NO intervening write to the gained
// key. The imported rows MUST survive the round-trip from the gainer's own
// WAL+snapshot, because the source's copy is gone.
//
// Before the fix the import path applied versions in-memory only
// (loadVersion -> applyVersionLocked markDirty=false): the imported rows were
// neither WAL-appended nor did they mark the collection dirty, so a clean
// Close() snapshotted nothing and the reopened gainer served an EMPTY range while
// its manifest claimed ownership — silent data loss.
func TestGainerImportSurvivesRestart(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	if err := a.db.OrchestrateRebalance(l1...); err != nil {
		t.Fatalf("OrchestrateRebalance err = %v", err)
	}

	// Sanity: the moving key handed off — present on A (gainer), pruned on B.
	gained, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(gained) != 1 {
		t.Fatalf("gainer history after commit = %+v, want 1", gained)
	}
	lost, err := b.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 0 {
		t.Fatalf("loser retained pruned key after commit: %+v", lost)
	}
	wantQuant := gained[0].Quantized

	// Restart the gainer WITHOUT any write to the gained key. A clean Close()
	// must have persisted the imported rows (snapshot and/or WAL); the source's
	// copy is pruned, so the reopened gainer is the ONLY holder.
	pathA := a.db.path
	if err := a.db.Close(); err != nil {
		t.Fatalf("close gainer err = %v", err)
	}

	reopened, err := Open(pathA, WithProvider(&mockProvider{dim: 16}), WithToken("s"))
	if err != nil {
		t.Fatalf("reopen gainer err = %v", err)
	}
	defer reopened.Close()
	reopened.registerServeAddr(a.addr)

	// The gained key must still be retrievable on the reopened gainer.
	survived, err := reopened.Collection("docs", WithBitWidth(2)).historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(survived) != 1 {
		t.Fatalf("gained key vanished after gainer restart: history = %+v, want 1 (silent data loss)", survived)
	}
	if survived[0].Quantized == nil {
		t.Fatalf("gained key lost its codes after restart")
	}
	if wantQuant != nil && survived[0].Quantized != nil &&
		len(survived[0].Quantized.MSE) != len(wantQuant.MSE) {
		t.Fatalf("gained key codes corrupted after restart: got %d MSE bytes, want %d",
			len(survived[0].Quantized.MSE), len(wantQuant.MSE))
	}

	// And it must be searchable (the index was rebuilt from the durable rows).
	results, err := reopened.Collection("docs", WithBitWidth(2)).SearchVector(unitVec(16, 1), 5)
	if err != nil {
		t.Fatalf("search on reopened gainer err = %v", err)
	}
	found := false
	for _, r := range results {
		if r.ID == movingID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("gained key not searchable on reopened gainer; results = %+v", results)
	}
}

// TestAbortedGainerDropsImportedRows is the symmetric-cleanup regression for the
// durable import fix: a gainer that PULLS rows durably (WAL-appended) and then
// ABORTS must NOT retain those rows for keys it does not own under L0 — not in
// memory, and not durably across a restart. Otherwise the durable-import fix
// would trade silent data loss for orphaned, never-owned data on disk.
func TestAbortedGainerDropsImportedRows(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// Freeze the loser B and run A's gainer pull so the moving key is imported
	// durably into A (this is the PREPARED-but-not-committed state).
	epoch := a.db.beginRebalanceEpoch()
	if err := b.db.freezeRebalanceShards(epoch, a.addr, l1); err != nil {
		t.Fatalf("freeze loser err = %v", err)
	}
	if err := a.db.pullRebalanceShards(epoch, a.addr, l1); err != nil {
		t.Fatalf("gainer pull err = %v", err)
	}

	// The moving key is now present on A (durably imported during pull).
	imported, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(imported) != 1 {
		t.Fatalf("gainer did not import moving key during pull: %+v", imported)
	}

	// Abort the gainer locally (no durable commit record): it reverts to L0 and
	// must prune the imported-but-unowned rows.
	if err := a.db.abortRebalanceLocal(epoch); err != nil {
		t.Fatalf("abortRebalanceLocal err = %v", err)
	}
	afterAbort, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterAbort) != 0 {
		t.Fatalf("aborted gainer retained imported key in memory: %+v", afterAbort)
	}

	// And the prune must be durable: a clean restart of A must not resurrect the
	// orphaned rows from the WAL.
	pathA := a.db.path
	if err := a.db.Close(); err != nil {
		t.Fatalf("close gainer err = %v", err)
	}
	reopened, err := Open(pathA, WithProvider(&mockProvider{dim: 16}), WithToken("s"))
	if err != nil {
		t.Fatalf("reopen gainer err = %v", err)
	}
	defer reopened.Close()
	resurrected, err := reopened.Collection("docs", WithBitWidth(2)).historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(resurrected) != 0 {
		t.Fatalf("aborted gainer resurrected orphaned key after restart: %+v (durable orphan)", resurrected)
	}

	// Cleanup: unfreeze B so its cleanup-close is clean.
	_ = b.db.abortRebalanceLocal(epoch)
}

// reservePort returns a free 127.0.0.1 address and closes its listener so the
// caller can bind it later.
func reservePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// serveAt opens a DB at path, binds it to addr, serves it, and registers
// cleanup. Returns the DB.
func serveAt(t *testing.T, path, addr string, opts ...Option) *DB {
	t.Helper()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, opts...)
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	db.registerServeAddr(addr)
	done := make(chan error, 1)
	go func() { done <- db.Serve(l) }()
	t.Cleanup(func() {
		_ = l.Close()
		_ = db.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("serve %s did not exit", addr)
		}
	})
	return db
}

// TestPureGainerPersistsCoordinatorAndRecovers is the Bug 2 regression: a pure
// gainer (gains keys, loses none, is NOT the coordinator) never receives Freeze,
// so before the fix it persisted an EMPTY coordinator address on PREPARED. If it
// then crashes and the coordinator owns no shards (in no peer's shard set and
// not in the gainer's static WithPeers), recovery's peer-iteration fallback
// cannot find the coordinator and the gainer is fenced forever. With the fix,
// PrepareRebalance carries the coordinator address so the gainer persists it and
// recovery dials the coordinator directly.
//
// Topology:
//   - C (coordinator) owns NO shards under L0 or L1 — it only drives.
//   - H (holder/loser) owns everything under L0; loses the [0,mid] band to G.
//   - G (pure gainer) gains [0,mid] from H under L1, loses nothing -> never freezes.
//
// G's static peers list ONLY H (never C), and C is in neither L0 nor L1's shard
// owners, so the only way G can reach C on recovery is the persisted coordinator.
func TestPureGainerPersistsCoordinatorAndRecovers(t *testing.T) {
	addrC := reservePort(t)
	addrH := reservePort(t)
	addrG := reservePort(t)

	maxKey := ^uint64(0)
	mid := maxKey / 2

	// L0: H owns everything. C and G own nothing.
	l0 := []ShardAssignment{{ID: "all", Owner: addrH, Start: 0, End: maxKey}}
	// L1: G gains [0,mid] from H; H keeps the rest. C still owns nothing.
	l1 := []ShardAssignment{
		{ID: "g", Owner: addrG, Start: 0, End: mid},
		{ID: "h", Owner: addrH, Start: mid + 1, End: maxKey},
	}

	prov := func() Option { return WithProvider(&mockProvider{dim: 16}) }

	// Coordinator C: knows H and G as peers (it must reach them to drive), owns
	// nothing. It uses L0 so its rebalanceRoles diff is well-defined.
	dbC := serveAt(t, filepath.Join(t.TempDir(), "pg-c.csdb"), addrC,
		prov(), WithToken("s"), WithPeers(addrH, addrG), WithShards(l0...))
	// Holder/loser H: owns everything; peers know C and G.
	dbH := serveAt(t, filepath.Join(t.TempDir(), "pg-h.csdb"), addrH,
		prov(), WithToken("s"), WithPeers(addrC, addrG), WithShards(l0...))

	// Pure gainer G: peers list ONLY H (NOT C). Owns nothing under L0.
	pathG := filepath.Join(t.TempDir(), "pg-g.csdb")
	lG, err := net.Listen("tcp", addrG)
	if err != nil {
		t.Fatal(err)
	}
	dbG, err := Open(pathG, prov(), WithToken("s"), WithPeers(addrH), WithShards(l0...))
	if err != nil {
		_ = lG.Close()
		t.Fatal(err)
	}
	dbG.registerServeAddr(addrG)
	doneG := make(chan error, 1)
	go func() { doneG <- dbG.Serve(lG) }()

	// Seed a moving key on H that G gains under L1 (key in [0,mid]).
	var movingID string
	for i := 0; i < 200000; i++ {
		id := "doc-pg-" + strconv.Itoa(i)
		if k := shardKey("docs", id); k <= mid {
			movingID = id
			break
		}
	}
	if movingID == "" {
		t.Fatal("no moving key found")
	}
	if err := dbH.Collection("docs", WithBitWidth(2)).PutVector(movingID, unitVec(16, 1)); err != nil {
		t.Fatal(err)
	}

	// Fail G's CommitRebalance so G stays PREPARED after the durable decide. The
	// coordinator C holds a durable commit record (floor == epoch) so a later
	// recovery query resolves COMMIT.
	injectFailingPeer(t, dbC, addrG, "commit", 0, errors.New("injected commit failure to strand gainer"))

	err = dbC.OrchestrateRebalance(l1...)
	if err == nil {
		t.Fatal("OrchestrateRebalance returned nil despite injected commit failure")
	}

	// Bug 2 core assertion: the pure gainer persisted the REAL coordinator
	// address on PREPARED (not an empty string).
	stG := dbG.rebalanceSnapshot()
	if stG.phase != phasePrepared {
		t.Fatalf("gainer phase = %v, want PREPARED", stG.phase)
	}
	if stG.coordinator != addrC {
		t.Fatalf("gainer persisted coordinator = %q, want %q (Bug 2: empty coordinator)", stG.coordinator, addrC)
	}

	// Crash G: close it (stop serving). Its persisted manifest holds PREPARED +
	// the coordinator address.
	_ = lG.Close()
	if err := dbG.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-doneG:
	case <-time.After(2 * time.Second):
		t.Fatal("serve G did not exit")
	}

	// Reopen G WITHOUT C in its peers (peers = [H]) and with C absent from its
	// shard set (still L0 = H owns all). Peer-iteration over G's shard set yields
	// only H, which cannot answer C's decision — recovery MUST dial the persisted
	// coordinator C directly. Recovery runs during Open.
	dbG2, err := Open(pathG, prov(), WithToken("s"), WithPeers(addrH))
	if err != nil {
		t.Fatalf("reopen gainer err = %v", err)
	}
	defer dbG2.Close()
	// Restore G's served identity so a routed write to its now-owned range is
	// recognized as local (the recovered layout names addrG as the owner).
	dbG2.registerServeAddr(addrG)

	// Converged: G resolved COMMIT via the persisted coordinator (NOT peer
	// iteration over its shard set, which lists only H), applied L1, and unfroze.
	// Before the fix, G's persisted coordinator was "" and peer-iteration over its
	// shards (only H, which has no decision for C's epoch) could not resolve it —
	// G would stay fenced forever in PREPARED.
	if ph := dbG2.rebalanceSnapshot().phase; ph != phaseIdle {
		t.Fatalf("gainer not IDLE after recovery via persisted coordinator: %v (fenced forever?)", ph)
	}
	if got := len(dbG2.shardAssignments()); got != 2 {
		t.Fatalf("gainer did not apply L1 on recovery: shards = %+v (want 2)", dbG2.shardAssignments())
	}
	// The moving key is no longer fenced on G (it owns [0,mid] under L1 now); a
	// routed write to it succeeds rather than being rejected with
	// ErrRebalanceInProgress.
	if err := dbG2.Collection("docs", WithBitWidth(2)).Put(movingID, Entry{Vector: unitVec(16, 2)}); err != nil {
		t.Fatalf("post-recovery write to gained key err = %v, want nil (unfrozen)", err)
	}
	_ = movingID
}
