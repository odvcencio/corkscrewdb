package corkscrewdb

import (
	"errors"
	"sort"
	"strings"
	"time"

	walpkg "m31labs.dev/corkscrewdb/wal"
)

// RebalanceShards applies a new explicit shard layout to the local node.
// The gaining node should run this before the losing node so snapshot+WAL
// handoff can pull the old owner's data before it gets pruned.
func (db *DB) RebalanceShards(shards ...ShardAssignment) error {
	normalized, err := db.normalizeLocalRebalanceShards(shards)
	if err != nil {
		return err
	}
	if err := db.prepareRebalanceShards(normalized); err != nil {
		return err
	}
	if err := db.applyShardLayout(normalized, 0); err != nil {
		return err
	}
	return db.pruneUnownedData(normalized)
}

// OrchestrateRebalance coordinates a cluster-wide rebalance with the 2PC
// protocol (§5.2-§5.4):
//
//	Freeze (all losers install a range freeze + ACK)
//	  -> coordinator BARRIERS on all freeze ACKs
//	  -> Pull (gainers pull moving data from losers via the rebuilt RPCs)
//	  -> durable-decide (coordinator writes its commit record BEFORE applying L1
//	     to itself — it is in its own Commit set)
//	  -> Commit (all apply L1 + unfreeze, idempotent)
//	  -> Prune (all drop unowned data, idempotent)
//
// A failure in Freeze or Pull aborts: every FROZEN/PREPARED participant
// (including the frozen loser) receives Abort and reverts to L0. After the
// durable-decide the rebalance is committed; Commit/Prune are retried, never
// rolled back.
func (db *DB) OrchestrateRebalance(shards ...ShardAssignment) error {
	if db == nil {
		return errors.New("corkscrewdb: nil database")
	}
	if db.remote != nil {
		return errors.New("corkscrewdb: remote clients cannot orchestrate rebalances")
	}
	if db.isClosed() {
		return errors.New("corkscrewdb: database is closed")
	}

	l1, err := db.normalizeClusterRebalanceShards(shards)
	if err != nil {
		return err
	}
	if len(l1) == 0 {
		return errors.New("corkscrewdb: shard assignments are required")
	}
	// A fenced cluster rebalance's symmetric-diff range freeze is only
	// well-defined when BOTH the current (L0) and target (L1) layouts route
	// every moving key via explicit shard ranges. If the current layout is
	// legacy peer-hash-mod (no explicit shards), reject — require WithShards
	// (§5.5).
	if !db.fencedRebalanceSafe(l1) {
		return ErrLegacyRebalanceUnsafe
	}

	l0 := db.shardAssignments()
	coord := db.localMemberID()
	epoch := db.beginRebalanceEpoch()

	// Durably record that this node is driving epoch for L1 (before any peer
	// RPC) so a crash mid-flight is recoverable as a coordinator-abort.
	db.installCoordinatorState(epoch, l1)

	losers, gainers := db.rebalanceRoles(l0, l1)

	// --- Freeze sub-phase: every loser installs the range freeze and ACKs. ---
	frozen := make([]string, 0, len(losers))
	for _, target := range losers {
		if err := db.freezeTarget(target, epoch, l1, coord); err != nil {
			// Roll back every loser that already froze (including the coordinator
			// if it froze locally), then surface the error.
			db.abortAll(frozen, epoch)
			return err
		}
		frozen = append(frozen, target)
	}

	// --- Barrier: every freeze ACK is in before any gainer pulls (§5.2.1). ---
	if db.rebalanceHooks != nil && db.rebalanceHooks.afterFreezeBarrier != nil {
		db.rebalanceHooks.afterFreezeBarrier(epoch)
	}

	// --- Pull sub-phase: every gainer pulls moving data from the frozen losers. ---
	prepared := make([]string, 0, len(gainers))
	for _, target := range gainers {
		if db.rebalanceHooks != nil && db.rebalanceHooks.beforePull != nil {
			db.rebalanceHooks.beforePull(target)
		}
		if err := db.pullTarget(target, epoch, l1, coord); err != nil {
			// Abort every FROZEN loser AND every PREPARED gainer (the frozen loser
			// MUST be unfrozen, §5.4 row 2).
			db.abortAll(unionTargets(frozen, prepared), epoch)
			return err
		}
		prepared = append(prepared, target)
	}

	// --- Durable decide: write the commit record BEFORE applying L1 to self. ---
	// The coordinator is in its own Commit set, so it MUST persist the decision
	// (RebalanceEpoch=epoch, PendingShards=L1, fsync) first — this is the
	// linearization point (§5.3).
	if err := db.commitEpoch(epoch); err != nil {
		// A concurrent force-abort bumped the committed floor past this epoch;
		// treat the whole rebalance as aborted.
		db.abortAll(unionTargets(frozen, prepared), epoch)
		return err
	}
	if db.rebalanceHooks != nil && db.rebalanceHooks.afterDurableDecide != nil {
		db.rebalanceHooks.afterDurableDecide(epoch)
	}

	// --- Commit: every participant applies L1 + unfreezes (idempotent, retry). ---
	targets := db.rebalanceTargets(l1)
	if err := db.commitAll(targets, epoch, l1); err != nil {
		return err
	}

	// --- Prune: every participant drops unowned data (idempotent). ---
	db.pruneAll(targets, epoch, l1)
	db.clearRebalance()
	return nil
}

// rebalanceRoles computes, from L0 and L1, the set of losing participants (own
// a key range under L0 that a DIFFERENT owner takes under L1) and gaining
// participants (take a key range under L1 a DIFFERENT owner held under L0). A
// participant may be both. Owners are resolved (LocalShardOwner -> local id).
func (db *DB) rebalanceRoles(l0, l1 []ShardAssignment) (losers, gainers []string) {
	loserSet := make(map[string]struct{})
	gainerSet := make(map[string]struct{})
	for _, oldShard := range l0 {
		oldOwner := db.resolveShardOwner(oldShard.Owner)
		for _, newShard := range l1 {
			if !shardRangesIntersect(oldShard, newShard) {
				continue
			}
			newOwner := db.resolveShardOwner(newShard.Owner)
			if oldOwner == newOwner {
				continue
			}
			if oldOwner != "" {
				loserSet[oldOwner] = struct{}{}
			}
			if newOwner != "" {
				gainerSet[newOwner] = struct{}{}
			}
		}
	}
	losers = sortedKeys(loserSet)
	gainers = sortedKeys(gainerSet)
	return losers, gainers
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func unionTargets(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, list := range [][]string{a, b} {
		for _, t := range list {
			if _, ok := seen[t]; ok {
				continue
			}
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}
	return out
}

// freezeTarget installs the range freeze for epoch over L1 on target (local or
// remote). The frozen participant persists RebalanceCoordinator=coord.
func (db *DB) freezeTarget(target string, epoch uint64, l1 []ShardAssignment, coord string) error {
	if target == db.localMemberID() {
		return db.freezeRebalanceShards(epoch, coord, l1)
	}
	client, err := db.peerClient(target)
	if err != nil {
		return err
	}
	return client.FreezeRebalance(l1, epoch, coord)
}

// pullTarget runs the gainer's pull sub-phase for epoch on target (local or
// remote). The gainer persists RebalanceCoordinator=coord when it goes PREPARED.
func (db *DB) pullTarget(target string, epoch uint64, l1 []ShardAssignment, coord string) error {
	if target == db.localMemberID() {
		return db.pullRebalanceShards(epoch, coord, l1)
	}
	client, err := db.peerClient(target)
	if err != nil {
		return err
	}
	return client.PrepareRebalance(l1, epoch, coord)
}

// commitAll sends Commit to every target, applying L1 + unfreezing. Commit is
// idempotent and retried; a committed participant is never rolled back. A
// stale-epoch rejection means a force-abort superseded this rebalance — the
// coordinator treats the whole rebalance as aborted (§5.6).
func (db *DB) commitAll(targets []string, epoch uint64, l1 []ShardAssignment) error {
	for _, target := range targets {
		if target == db.localMemberID() {
			if err := db.applyShardLayout(l1, epoch); err != nil {
				return err
			}
			continue
		}
		client, err := db.peerClient(target)
		if err != nil {
			return err
		}
		if err := client.CommitRebalance(l1, epoch); err != nil {
			if errors.Is(err, ErrStaleEpoch) {
				// A higher epoch (force-abort) won on this participant. Per the
				// monotonic-epoch invariant, force-abort wins: the coordinator
				// stops orchestrating, returns to IDLE (clearing the in-flight
				// COMMITTING state), and surfaces the error. It does NOT proceed
				// to Prune. Recovery — not this in-flight loop — is the authority
				// on the final layout, deferring to whichever epoch is highest in
				// the durable floor (§5.6).
				db.clearRebalance()
				return err
			}
			return err
		}
	}
	return nil
}

// pruneAll sends Prune to every target (idempotent; failures are logged via the
// returned best-effort and do not fail the committed rebalance).
func (db *DB) pruneAll(targets []string, epoch uint64, l1 []ShardAssignment) {
	for _, target := range targets {
		if target == db.localMemberID() {
			_ = db.pruneUnownedData(l1)
			continue
		}
		client, err := db.peerClient(target)
		if err != nil {
			continue
		}
		_ = client.PruneRebalance(l1, epoch)
	}
}

// abortAll broadcasts Abort to the given targets (FROZEN/PREPARED participants),
// reverting them to L0. A frozen loser MUST be reached so it unfreezes (§5.4).
// The coordinator's own in-flight state is ALWAYS reverted, even if it never
// appeared in the targets list (it installs coordinator state up front).
func (db *DB) abortAll(targets []string, epoch uint64) {
	local := db.localMemberID()
	sawLocal := false
	for _, target := range targets {
		if target == local {
			sawLocal = true
			_ = db.abortRebalanceLocal(epoch)
			continue
		}
		client, err := db.peerClient(target)
		if err != nil {
			continue
		}
		_ = client.AbortRebalance(epoch)
	}
	if !sawLocal {
		// Always revert the coordinator's own installed state on abort.
		_ = db.abortRebalanceLocal(epoch)
	}
}

// ForceAbortRebalance is the operator escape hatch (§5.6): it allocates a
// strictly-higher epoch than the stuck one, persists it as the committed floor
// (fencing the stuck coordinator out), and broadcasts Abort to every known
// participant so frozen losers unfreeze.
func (db *DB) ForceAbortRebalance(epoch uint64) error {
	if db == nil {
		return errors.New("corkscrewdb: nil database")
	}
	if db.remote != nil {
		return errors.New("corkscrewdb: remote clients cannot force-abort rebalances")
	}
	committed := db.committedEpoch()
	bump := committed
	if epoch > bump {
		bump = epoch
	}
	next := bump + 1
	// Persist the higher floor first (fsync) so the stuck coordinator's in-flight
	// Commit is rejected with ErrStaleEpoch on its next attempt.
	db.mu.Lock()
	if next > db.manifest.RebalanceEpoch {
		db.manifest.RebalanceEpoch = next
	}
	db.mu.Unlock()
	if err := db.saveManifest(); err != nil {
		return err
	}
	// Locally revert any in-flight pending rebalance.
	db.abortRebalanceLocal(epoch)
	// Broadcast Abort to all known participants for the stuck epoch.
	for _, target := range db.rebalanceTargets(db.shardAssignments()) {
		if target == db.localMemberID() {
			continue
		}
		client, err := db.peerClient(target)
		if err != nil {
			continue
		}
		_ = client.AbortRebalance(epoch)
	}
	return nil
}

func (db *DB) normalizeLocalRebalanceShards(shards []ShardAssignment) ([]ShardAssignment, error) {
	if db == nil {
		return nil, errors.New("corkscrewdb: nil database")
	}
	if db.remote != nil {
		return nil, errors.New("corkscrewdb: remote clients cannot rebalance shards")
	}
	if db.isClosed() {
		return nil, errors.New("corkscrewdb: database is closed")
	}
	return normalizeShardAssignments(shards)
}

func (db *DB) normalizeClusterRebalanceShards(shards []ShardAssignment) ([]ShardAssignment, error) {
	normalized, err := db.normalizeLocalRebalanceShards(shards)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return nil, nil
	}
	local := db.localMemberID()
	if strings.HasPrefix(local, "local://") {
		return nil, errors.New("corkscrewdb: cluster rebalance requires a served local address or explicit owner addresses")
	}
	out := cloneShardAssignments(normalized)
	for i := range out {
		if out[i].Owner == LocalShardOwner {
			out[i].Owner = local
		}
	}
	return normalizeShardAssignments(out)
}

// prepareRebalanceShards is the single-node legacy best-effort prepare used by
// RebalanceShards (un-fenced): it pulls migrated data from the old owners. The
// 2PC cluster path uses freezeRebalanceShards + pullRebalanceShards instead.
func (db *DB) prepareRebalanceShards(shards []ShardAssignment) error {
	normalized, err := db.normalizeLocalRebalanceShards(shards)
	if err != nil {
		return err
	}
	oldShards := db.shardAssignments()
	oldMembers := db.legacyShardMembers()
	for _, peer := range db.rebalanceSources(oldShards, normalized) {
		if err := db.pullMigratedDataFromPeer(peer, oldShards, oldMembers, normalized); err != nil {
			return err
		}
	}
	return nil
}

// freezeRebalanceShards installs the local range freeze for the 2PC freeze
// sub-phase: it records the coordinator address and pending L1 layout durably
// (phase=FROZEN) so writes to moving keys are rejected and recovery can resolve
// the coordinator. Idempotent for a re-sent Freeze at the same epoch.
func (db *DB) freezeRebalanceShards(epoch uint64, coordinator string, next []ShardAssignment) error {
	if err := db.checkEpoch(epoch); err != nil {
		return err
	}
	normalized, err := db.normalizeLocalRebalanceShards(next)
	if err != nil {
		return err
	}
	db.installFreeze(epoch, coordinator, normalized)
	return nil
}

// pullRebalanceShards runs the local gainer pull sub-phase: it pulls every
// moving key it gains under L1 from the old owners (via the rebuilt
// PullSnapshot/PullEntries + GetRaw), then records phase=PREPARED durably,
// persisting the coordinator address so recovery can query its decision.
func (db *DB) pullRebalanceShards(epoch uint64, coordinator string, next []ShardAssignment) error {
	if err := db.checkEpoch(epoch); err != nil {
		return err
	}
	normalized, err := db.normalizeLocalRebalanceShards(next)
	if err != nil {
		return err
	}
	// A gainer that did not also freeze still needs the coordinator recorded and
	// the pending layout installed so isFrozenKey is well-defined during pull.
	st := db.rebalanceSnapshot()
	if st.phase == phaseIdle || st.epoch != epoch {
		db.installFreeze(epoch, coordinator, normalized)
	}
	oldShards := db.shardAssignments()
	oldMembers := db.legacyShardMembers()
	for _, peer := range db.rebalanceSources(oldShards, normalized) {
		if err := db.pullMigratedDataFromPeer(peer, oldShards, oldMembers, normalized); err != nil {
			return err
		}
	}
	db.markPrepared()
	return nil
}

// resolveRebalanceDecision is the coordinator's answer to a recovered
// participant's ResolveRebalance(epoch) query (§5.3). The committed floor
// (RebalanceEpoch) records the LAST epoch whose layout was actually committed,
// so a per-epoch outcome is exact: COMMIT iff THIS epoch is the committed one
// (committedEpoch == epoch); ABORT when the floor moved PAST epoch without
// committing it — either an operator force-abort bumped the floor (committed >
// epoch, no layout for epoch) or a DIFFERENT, newer rebalance committed
// (supersession). A `>=`-on-one-floor test would conflate these and wrongly
// COMMIT a force-aborted or superseded epoch. The committed floor is returned
// so a stale caller can see the supersession explicitly.
func (db *DB) resolveRebalanceDecision(epoch uint64) (rebalanceDecision, uint64) {
	committed := db.committedEpoch()
	if committed == epoch {
		return decisionCommit, committed
	}
	return decisionAbort, committed
}

// recoverPendingRebalance resolves a non-IDLE rebalance left behind by a crash
// (§5.3). Three cases:
//
//  1. This node holds a durable commit record for the in-flight epoch
//     (phase=COMMITTING, RebalanceEpoch == epoch): finish commit-forward. A
//     floor PAST epoch means this record was superseded (force-abort / newer
//     commit) — revert to L0 instead.
//  2. This node was a participant (FROZEN/PREPARED) with a recorded coordinator
//     address: dial the coordinator and call ResolveRebalance(epoch). COMMIT
//     (committedEpoch == epoch) => apply L1 + unfreeze; ABORT (force-abort or
//     supersession, committedEpoch != epoch) => revert to L0.
//  3. This node was the coordinator (it drove the epoch) with NO commit record:
//     self-abort and broadcast Abort to FROZEN/PREPARED participants.
func (db *DB) recoverPendingRebalance() error {
	st := db.rebalanceSnapshot()
	if st.phase == phaseIdle || len(st.pending) == 0 {
		return nil
	}
	epoch := st.epoch
	committed := db.committedEpoch()

	// Case 1: we durably decided commit (COMMITTING with the floor EXACTLY at the
	// in-flight epoch — commitEpoch set RebalanceEpoch == epoch) — finish applying
	// L1 locally and unfreeze. A floor strictly past epoch means a force-abort or
	// a newer rebalance superseded this one; fall through to abort-revert.
	if st.phase == phaseCommitting && committed == epoch {
		return db.applyShardLayout(st.pending, epoch)
	}
	if st.phase == phaseCommitting && committed > epoch {
		// Our durable commit record was superseded (force-abort / newer commit
		// bumped the floor past epoch). Never apply the stale L1; revert to L0.
		db.clearRebalance()
		return nil
	}

	local := db.localMemberID()

	if st.isCoordinator {
		// Case 3: coordinator with no commit record => the decision was never
		// durable, so abort. Broadcast Abort so any FROZEN/PREPARED participant
		// unfreezes, then revert locally.
		for _, target := range db.rebalanceTargets(db.shardAssignments()) {
			if target == local {
				continue
			}
			if client, err := db.peerClient(target); err == nil {
				_ = client.AbortRebalance(epoch)
			}
		}
		db.clearRebalance()
		return nil
	}

	// Case 2: participant — dial the persisted coordinator directly and ask for
	// the durable decision. This closes the fence-forever hole: the coordinator
	// is reachable even if it gave away ALL of its shards (it is in no
	// shard-derived peer set, but its address is persisted here).
	decision, committedEpoch, err := db.queryCoordinatorDecision(st.coordinator, epoch)
	if err != nil {
		// Could not reach the coordinator. Leave the freeze in place (the node
		// stays fenced for the moving keys) rather than guessing — a later Open
		// or an operator ForceAbortRebalance resolves it. This is the classic
		// blocking behavior (§5.6); it is safe (no split-brain), not live.
		return nil
	}
	// Apply L1 only when the coordinator reports THIS epoch as the committed one.
	// A COMMIT with a floor past epoch would be a supersession (a force-abort or a
	// different, newer rebalance committed) — defensively treat it as ABORT.
	if decision == decisionCommit && committedEpoch == epoch {
		return db.applyShardLayout(st.pending, epoch)
	}
	// ABORT, or the coordinator moved on to a higher epoch (force-abort /
	// supersession) => revert to L0 and unfreeze.
	db.clearRebalance()
	return nil
}

// queryCoordinatorDecision dials the coordinator's persisted address and asks
// for its durable decision on epoch. Candidate-iteration over known peers is
// the fallback ONLY when the coordinator address is absent (legacy/partial
// manifests).
func (db *DB) queryCoordinatorDecision(coordinator string, epoch uint64) (rebalanceDecision, uint64, error) {
	local := db.localMemberID()
	if strings.TrimSpace(coordinator) != "" && coordinator != local {
		client, err := db.peerClient(coordinator)
		if err != nil {
			return decisionUnknown, 0, err
		}
		resp, err := client.ResolveRebalance(epoch)
		if err != nil {
			return decisionUnknown, 0, err
		}
		return resp.Decision, resp.CommittedEpoch, nil
	}

	// Fallback: iterate known peers (legacy/partial manifest with no recorded
	// coordinator). The first peer that answers a definite decision wins.
	var lastErr error
	for _, target := range db.rebalanceTargets(db.shardAssignments()) {
		if target == local {
			continue
		}
		client, err := db.peerClient(target)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := client.ResolveRebalance(epoch)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Decision != decisionUnknown {
			return resp.Decision, resp.CommittedEpoch, nil
		}
	}
	if lastErr != nil {
		return decisionUnknown, 0, lastErr
	}
	return decisionUnknown, 0, errors.New("corkscrewdb: no coordinator reachable for rebalance recovery")
}

// abortRebalanceLocal reverts an in-flight (FROZEN/PREPARED) rebalance to L0 by
// clearing the freeze. It is a no-op if already committed past the epoch (a
// committed participant must never roll back, §5.4). Returns ErrAlreadyCommitted
// when the epoch is at/below the committed floor and a layout was applied.
func (db *DB) abortRebalanceLocal(epoch uint64) error {
	st := db.rebalanceSnapshot()
	if st.phase == phaseCommitting && epoch <= db.committedEpoch() {
		return ErrAlreadyCommitted
	}
	db.clearRebalance()
	return nil
}

func (db *DB) rebalanceTargets(newShards []ShardAssignment) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(target string) {
		target = strings.TrimSpace(target)
		if target == "" {
			return
		}
		if _, ok := seen[target]; ok {
			return
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	add(db.localMemberID())
	for _, peer := range db.peers {
		add(peer)
	}
	for _, shard := range db.shardAssignments() {
		add(db.resolveShardOwner(shard.Owner))
	}
	for _, shard := range newShards {
		add(db.resolveShardOwner(shard.Owner))
	}
	sort.Strings(out)
	return out
}

func (db *DB) rebalanceSources(oldShards, newShards []ShardAssignment) []string {
	local := db.localMemberID()
	if len(oldShards) == 0 {
		return sanitizePeers(append(append([]string(nil), db.peers...), peersFromShardAssignments(newShards)...))
	}

	var localRanges []ShardAssignment
	for _, shard := range newShards {
		if db.resolveShardOwner(shard.Owner) == local {
			localRanges = append(localRanges, shard)
		}
	}
	if len(localRanges) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(oldShards))
	out := make([]string, 0, len(oldShards))
	for _, old := range oldShards {
		owner := db.resolveShardOwner(old.Owner)
		if owner == "" || owner == local {
			continue
		}
		for _, localRange := range localRanges {
			if !shardRangesIntersect(old, localRange) {
				continue
			}
			if _, ok := seen[owner]; ok {
				break
			}
			seen[owner] = struct{}{}
			out = append(out, owner)
			break
		}
	}
	sort.Strings(out)
	return out
}

func shardRangesIntersect(a, b ShardAssignment) bool {
	return a.Start <= b.End && b.Start <= a.End
}

func (db *DB) pullMigratedDataFromPeer(peer string, oldShards []ShardAssignment, oldMembers []string, newShards []ShardAssignment) error {
	local := db.localMemberID()
	if peer == "" || peer == local {
		return nil
	}
	client, err := db.peerClient(peer)
	if err != nil {
		return err
	}
	info, err := client.Info()
	if err != nil {
		return err
	}
	for _, collInfo := range info.Collections {
		snapshot, err := client.PullSnapshot(RPCPullSnapshotRequest{Collection: collInfo.Name})
		if err != nil {
			return err
		}
		if err := db.importOwnedSnapshot(peer, collInfo, snapshot, oldShards, oldMembers, newShards); err != nil {
			return err
		}
		var since uint64 = snapshot.MaxLamport
		for {
			resp, err := client.PullEntries(RPCPullEntriesRequest{
				Collection: collInfo.Name,
				SinceClock: since,
				MaxEntries: 1000,
			})
			if err != nil {
				return err
			}
			if err := db.importOwnedEntries(peer, collInfo, resp.Entries, oldShards, oldMembers, newShards); err != nil {
				return err
			}
			if len(resp.Entries) > 0 {
				since = resp.LatestClock
			}
			if !resp.HasMore {
				break
			}
		}
	}
	return nil
}

func (db *DB) importOwnedSnapshot(peer string, info RPCCollectionInfo, snapshot RPCPullSnapshotResponse, oldShards []ShardAssignment, oldMembers []string, newShards []ShardAssignment) error {
	opts := []CollectionOption{WithBitWidth(snapshot.BitWidth)}
	if !snapshot.RawStore {
		opts = append(opts, WithoutRawStore())
	}
	if snapshot.SparseEnabled {
		opts = append(opts, WithSparse())
	}
	coll := db.Collection(info.Name, opts...)
	if coll.err != nil {
		return coll.err
	}
	if snapshot.Seed != 0 {
		coll.pinQuantizerSeed(snapshot.Seed)
	}
	for _, record := range snapshot.Records {
		if db.ownerForLayout(info.Name, record.ID, oldShards, oldMembers) != peer {
			continue
		}
		if db.ownerForLayout(info.Name, record.ID, newShards, nil) != db.localMemberID() {
			continue
		}
		for _, version := range record.Versions {
			if err := db.importVersion(coll, record.ID, snapshot.RawStore, importedVersion{
				Quantized:    version.Quantized,
				Dim:          version.Dim,
				RawHash:      version.RawHash,
				Sparse:       version.Sparse,
				Children:     version.Children,
				Text:         version.Text,
				Metadata:     version.Metadata,
				LamportClock: version.LamportClock,
				ActorID:      version.ActorID,
				WallClock:    version.WallClock,
				Tombstone:    version.Tombstone,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) importOwnedEntries(peer string, info RPCCollectionInfo, entries []RPCReplicaEntry, oldShards []ShardAssignment, oldMembers []string, newShards []ShardAssignment) error {
	if len(entries) == 0 {
		return nil
	}
	coll := db.Collection(info.Name, WithBitWidth(info.BitWidth))
	if coll.err != nil {
		return coll.err
	}
	rawStore := coll.rawStoreEnabled
	for _, entry := range entries {
		if db.ownerForLayout(info.Name, entry.VectorID, oldShards, oldMembers) != peer {
			continue
		}
		if db.ownerForLayout(info.Name, entry.VectorID, newShards, nil) != db.localMemberID() {
			continue
		}
		if err := db.importVersion(coll, entry.VectorID, rawStore, importedVersion{
			Quantized:    entry.Quantized,
			Dim:          entry.Dim,
			RawHash:      entry.RawHash,
			Sparse:       entry.Sparse,
			Children:     entry.Children,
			Text:         entry.Text,
			Metadata:     entry.Metadata,
			LamportClock: entry.LamportClock,
			ActorID:      entry.ActorID,
			WallClock:    entry.WallClock,
			Tombstone:    entry.Kind == walpkg.EntryTombstone,
		}); err != nil {
			return err
		}
	}
	return nil
}

// importedVersion is the WAL v5 code payload pulled from a peer during a
// rebalance handoff. Raw vectors are NOT carried inline; for raw_store
// collections the gainer fetches them by hash via GetRaw before indexing.
type importedVersion struct {
	Quantized    *walpkg.QuantizedVector
	Dim          int
	RawHash      []byte
	Sparse       *walpkg.SparseBlock
	Children     []walpkg.ChildVector
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
	Tombstone    bool
}

// importVersion reconstructs a Version from transmitted codes and loads it. For
// raw_store collections it fetches the raw blob by hash and persists it before
// indexing (the gainer must hold raw before WAL/index, §7.3).
func (db *DB) importVersion(coll *Collection, id string, rawStore bool, v importedVersion) error {
	if rawStore && !v.Tombstone && len(v.RawHash) == 32 {
		if _, err := db.fetchRawByHash(coll.name, v.RawHash); err != nil {
			return err
		}
	}
	return coll.loadVersion(id, Version{
		Quantized:    fromWALQuantized(v.Quantized),
		dim:          v.Dim,
		RawHash:      append([]byte(nil), v.RawHash...),
		Sparse:       fromWALSparse(v.Sparse),
		Children:     fromWALChildren(v.Children),
		Text:         v.Text,
		Metadata:     cloneMetadata(v.Metadata),
		LamportClock: v.LamportClock,
		ActorID:      v.ActorID,
		WallClock:    v.WallClock,
		Tombstone:    v.Tombstone,
	})
}

// fencedRebalanceSafe reports whether a fenced cluster rebalance to next (L1)
// is safe to range-freeze: both the current explicit layout (L0) and the target
// (L1) must route every key via explicit shard ranges. A non-empty explicit
// layout is guaranteed by normalizeShardAssignments to cover [0, maxUint64], so
// the only legacy-fallthrough case is an empty current layout (legacy
// peer-hash-mod) or an empty target. next is already normalized (non-empty,
// full-coverage) when this is called.
func (db *DB) fencedRebalanceSafe(next []ShardAssignment) bool {
	if len(next) == 0 {
		return false
	}
	current := db.shardAssignments()
	return len(current) != 0
}

func (db *DB) ownerForLayout(collection, id string, shards []ShardAssignment, legacyMembers []string) string {
	key := shardKey(collection, id)
	if owner, ok := db.explicitOwnerForKeyFromAssignments(key, shards); ok {
		return owner
	}
	if len(legacyMembers) == 0 {
		return ""
	}
	return legacyMembers[key%uint64(len(legacyMembers))]
}

// applyShardLayout commits a new shard layout (L1) locally. When epoch > 0 it is
// the 2PC commit: it is epoch-fenced (a stale epoch is rejected), bumps the
// durable RebalanceEpoch floor, clears the pending layout, and unfreezes. epoch
// 0 is the legacy single-node RebalanceShards path (no fence). Idempotent: a
// re-applied commit for an already-committed epoch is a no-op success.
func (db *DB) applyShardLayout(shards []ShardAssignment, epoch uint64) error {
	local := db.localMemberID()
	db.mu.Lock()
	defer db.mu.Unlock()

	if epoch > 0 {
		if epoch < db.manifest.RebalanceEpoch {
			// A higher epoch already committed; this stale commit is rejected.
			return ErrStaleEpoch
		}
		if epoch == db.manifest.RebalanceEpoch && db.rebalance.phase == phaseIdle {
			// Already committed this epoch and unfrozen — idempotent no-op.
			return nil
		}
		db.manifest.RebalanceEpoch = epoch
	}

	db.manifest.Shards = cloneShardAssignments(shards)
	peers := sanitizePeers(append(append([]string(nil), db.peers...), peersFromShardAssignments(shards)...))
	filtered := peers[:0]
	for _, peer := range peers {
		if peer == local {
			continue
		}
		filtered = append(filtered, peer)
	}
	db.manifest.Peers = append([]string(nil), filtered...)
	db.peers = append([]string(nil), db.manifest.Peers...)

	if epoch > 0 {
		// Unfreeze: the layout is now L1, the moving keys are settled.
		db.rebalance.phase = phaseIdle
		db.rebalance.coordinator = ""
		db.rebalance.isCoordinator = false
		db.rebalance.pending = nil
		db.manifest.RebalancePhase = phaseIdle.String()
		db.manifest.RebalanceCoordinator = ""
		db.manifest.RebalanceIsCoordinator = false
		db.manifest.PendingShards = nil
		db.manifest.RebalanceInFlightEpoch = 0
	}
	return db.saveManifestLocked()
}

func (db *DB) pruneUnownedData(shards []ShardAssignment) error {
	db.mu.RLock()
	collections := make([]*Collection, 0, len(db.collections))
	for _, coll := range db.collections {
		collections = append(collections, coll)
	}
	db.mu.RUnlock()

	local := db.localMemberID()
	for _, coll := range collections {
		coll.mu.RLock()
		var removeIDs []string
		for id := range coll.history {
			if db.ownerForLayout(coll.name, id, shards, nil) == local {
				continue
			}
			removeIDs = append(removeIDs, id)
		}
		coll.mu.RUnlock()
		if len(removeIDs) == 0 {
			continue
		}
		if err := coll.removeLocalIDs(removeIDs); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collection) removeLocalIDs(ids []string) error {
	if err := c.usable(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	changed := false
	for _, id := range ids {
		if _, ok := c.history[id]; !ok {
			continue
		}
		delete(c.history, id)
		if c.index != nil {
			c.index.Remove(id)
		}
		changed = true
	}
	if !changed {
		return nil
	}
	if len(c.history) == 0 {
		c.index = nil
		c.dim = 0
	}
	c.dirty = true
	return nil
}
