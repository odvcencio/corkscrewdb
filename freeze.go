package corkscrewdb

// freeze.go implements the rebalance freeze/epoch/phase state machine (§5.2,
// §5.5). A rebalance fences the keys whose owner changes between the current
// layout (L0) and the pending layout (L1) — writes to those moving keys are
// rejected with ErrRebalanceInProgress while FROZEN or PREPARED. A persisted
// monotonic epoch fences out stale coordinators; a persisted coordinator
// address lets a recovering participant always reach the coordinator.

// rebalancePhase is the durable state of a participant's local rebalance.
type rebalancePhase int

const (
	phaseIdle rebalancePhase = iota
	phaseFrozen
	phasePrepared
	phaseCommitting
)

func (p rebalancePhase) String() string {
	switch p {
	case phaseFrozen:
		return "FROZEN"
	case phasePrepared:
		return "PREPARED"
	case phaseCommitting:
		return "COMMITTING"
	default:
		return "IDLE"
	}
}

func parseRebalancePhase(s string) rebalancePhase {
	switch s {
	case "FROZEN":
		return phaseFrozen
	case "PREPARED":
		return phasePrepared
	case "COMMITTING":
		return phaseCommitting
	default:
		return phaseIdle
	}
}

// rebalanceState is the in-memory 2PC state for a node, guarded by db.mu.
type rebalanceState struct {
	epoch       uint64            // the in-flight epoch (0 = none in flight)
	coordinator string            // coordinator serve address (persisted on FROZEN/PREPARED)
	pending     []ShardAssignment // the pending L1 layout
	phase       rebalancePhase
}

// rebalanceStateFromManifest reconstructs the in-memory rebalance state from a
// persisted manifest at Open.
func rebalanceStateFromManifest(m manifest) rebalanceState {
	phase := parseRebalancePhase(m.RebalancePhase)
	st := rebalanceState{
		coordinator: m.RebalanceCoordinator,
		pending:     cloneShardAssignments(m.PendingShards),
		phase:       phase,
	}
	// RebalanceEpoch is the COMMITTED floor; RebalanceInFlightEpoch is the
	// epoch currently being driven (recorded while non-IDLE so a recovering
	// participant can query its decision).
	if phase != phaseIdle {
		st.epoch = m.RebalanceInFlightEpoch
		if st.epoch == 0 {
			// Legacy/partial manifests without the separate field: fall back to
			// the committed floor's successor.
			st.epoch = m.RebalanceEpoch + 1
		}
	}
	return st
}

// committedEpoch returns the highest durably-committed rebalance epoch.
func (db *DB) committedEpoch() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.manifest.RebalanceEpoch
}

// beginRebalanceEpoch allocates a strictly-increasing epoch above the committed
// floor and above any in-flight epoch.
func (db *DB) beginRebalanceEpoch() uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	next := db.manifest.RebalanceEpoch + 1
	if db.rebalance.epoch >= next {
		next = db.rebalance.epoch + 1
	}
	db.rebalance.epoch = next
	return next
}

// checkEpoch rejects an epoch that is not strictly greater than the committed
// floor (a stale coordinator fenced out by a force-abort or a newer commit).
func (db *DB) checkEpoch(epoch uint64) error {
	if epoch <= db.committedEpoch() {
		return ErrStaleEpoch
	}
	return nil
}

// installFreeze records a FROZEN rebalance for epoch over the next (L1) layout,
// persisting the coordinator address and phase durably so recovery can resolve
// it. The current layout (L0) remains db.manifest.Shards until commit.
func (db *DB) installFreeze(epoch uint64, coordinator string, next []ShardAssignment) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.rebalance.epoch = epoch
	db.rebalance.coordinator = coordinator
	db.rebalance.pending = cloneShardAssignments(next)
	db.rebalance.phase = phaseFrozen
	db.persistRebalanceLocked()
}

// markPrepared transitions a frozen participant to PREPARED (it has pulled all
// moving data) and persists the phase.
func (db *DB) markPrepared() {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.rebalance.phase == phaseIdle {
		return
	}
	db.rebalance.phase = phasePrepared
	db.persistRebalanceLocked()
}

// commitEpoch durably records epoch as the committed floor (the linearization
// point) and moves the phase to COMMITTING. The shard layout itself is applied
// by applyShardLayout; this is the standalone decision record used by the
// coordinator's durable-decide step.
func (db *DB) commitEpoch(epoch uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if epoch <= db.manifest.RebalanceEpoch {
		return ErrStaleEpoch
	}
	db.manifest.RebalanceEpoch = epoch
	db.manifest.PendingShards = cloneShardAssignments(db.rebalance.pending)
	db.rebalance.phase = phaseCommitting
	db.rebalance.epoch = epoch
	db.persistRebalanceLocked()
	return nil
}

// clearRebalance returns to IDLE, clearing the freeze, pending layout, and
// coordinator, and persisting the cleared state. The committed RebalanceEpoch
// floor is preserved.
func (db *DB) clearRebalance() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.rebalance.phase = phaseIdle
	db.rebalance.coordinator = ""
	db.rebalance.pending = nil
	db.persistRebalanceLocked()
}

// persistRebalanceLocked mirrors db.rebalance into the manifest and persists it
// (fsync). Caller holds db.mu.
func (db *DB) persistRebalanceLocked() {
	db.manifest.RebalancePhase = db.rebalance.phase.String()
	db.manifest.RebalanceCoordinator = db.rebalance.coordinator
	if db.rebalance.phase == phaseIdle {
		db.manifest.PendingShards = nil
		db.manifest.RebalanceInFlightEpoch = 0
	} else {
		db.manifest.PendingShards = cloneShardAssignments(db.rebalance.pending)
		db.manifest.RebalanceInFlightEpoch = db.rebalance.epoch
	}
	// Best-effort persist: a save failure leaves the in-memory state intact;
	// recovery falls back to the committed floor.
	_ = db.saveManifestLocked()
}

// rebalanceSnapshot returns a copy of the current rebalance state under the lock.
func (db *DB) rebalanceSnapshot() rebalanceState {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return rebalanceState{
		epoch:       db.rebalance.epoch,
		coordinator: db.rebalance.coordinator,
		pending:     cloneShardAssignments(db.rebalance.pending),
		phase:       db.rebalance.phase,
	}
}

// isFrozenKey reports whether id is a moving key under an active freeze: the
// phase is FROZEN/PREPARED/COMMITTING AND the key's owner differs between the
// current layout (L0) and the pending layout (L1). Writes to such keys are
// rejected with ErrRebalanceInProgress until commit/abort.
func (db *DB) isFrozenKey(collection, id string) bool {
	st := db.rebalanceSnapshot()
	if st.phase == phaseIdle || len(st.pending) == 0 {
		return false
	}
	l0 := db.shardAssignments()
	members := db.legacyShardMembers()
	o0 := db.ownerForLayout(collection, id, l0, members)
	o1 := db.ownerForLayout(collection, id, st.pending, members)
	return o0 != o1
}
