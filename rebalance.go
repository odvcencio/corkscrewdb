package corkscrewdb

import (
	"errors"
	"sort"
	"strings"

	walpkg "github.com/odvcencio/corkscrewdb/wal"
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
	if err := db.applyShardLayout(normalized); err != nil {
		return err
	}
	return db.pruneUnownedData(normalized)
}

// OrchestrateRebalance coordinates a cluster-wide rebalance with a prepare,
// commit, and prune phase over the remote control plane.
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

	normalized, err := db.normalizeClusterRebalanceShards(shards)
	if err != nil {
		return err
	}
	if len(normalized) == 0 {
		return errors.New("corkscrewdb: shard assignments are required")
	}

	targets := db.rebalanceTargets(normalized)
	for _, target := range targets {
		if target == db.localMemberID() {
			if err := db.prepareRebalanceShards(normalized); err != nil {
				return err
			}
			continue
		}
		client, err := db.peerClient(target)
		if err != nil {
			return err
		}
		if err := client.PrepareRebalance(normalized); err != nil {
			return err
		}
	}
	for _, target := range targets {
		if target == db.localMemberID() {
			if err := db.applyShardLayout(normalized); err != nil {
				return err
			}
			continue
		}
		client, err := db.peerClient(target)
		if err != nil {
			return err
		}
		if err := client.CommitRebalance(normalized); err != nil {
			return err
		}
	}
	for _, target := range targets {
		if target == db.localMemberID() {
			if err := db.pruneUnownedData(normalized); err != nil {
				return err
			}
			continue
		}
		client, err := db.peerClient(target)
		if err != nil {
			return err
		}
		if err := client.PruneRebalance(normalized); err != nil {
			return err
		}
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
	coll := db.Collection(info.Name, WithBitWidth(snapshot.BitWidth))
	if coll.err != nil {
		return coll.err
	}
	for _, record := range snapshot.Records {
		if db.ownerForLayout(info.Name, record.ID, oldShards, oldMembers) != peer {
			continue
		}
		if db.ownerForLayout(info.Name, record.ID, newShards, nil) != db.localMemberID() {
			continue
		}
		for _, version := range record.Versions {
			if err := coll.loadVersion(record.ID, Version{
				Embedding:    cloneVector(version.Embedding),
				Text:         version.Text,
				Metadata:     cloneMetadata(version.Metadata),
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
	for _, entry := range entries {
		if db.ownerForLayout(info.Name, entry.VectorID, oldShards, oldMembers) != peer {
			continue
		}
		if db.ownerForLayout(info.Name, entry.VectorID, newShards, nil) != db.localMemberID() {
			continue
		}
		if err := coll.loadVersion(entry.VectorID, Version{
			Embedding:    cloneVector(entry.Embedding),
			Text:         entry.Text,
			Metadata:     cloneMetadata(entry.Metadata),
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

func (db *DB) applyShardLayout(shards []ShardAssignment) error {
	local := db.localMemberID()
	db.mu.Lock()
	defer db.mu.Unlock()

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
