package corkscrewdb

import (
	"errors"
	"hash/fnv"
	"sort"
)

func (db *DB) shouldFederate() bool {
	return db != nil && db.remote == nil && len(db.peers) > 0
}

func (db *DB) registerServeAddr(addr string) {
	db.mu.Lock()
	db.serveAddr = addr
	db.mu.Unlock()
}

func (db *DB) localMemberID() string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.serveAddr != "" {
		return db.serveAddr
	}
	if db.manifest.ActorID != "" {
		return "local://" + db.manifest.ActorID
	}
	return "local://" + db.path
}

func (db *DB) shardMembers() []string {
	members := make([]string, 0, len(db.peers)+1)
	members = append(members, db.localMemberID())
	members = append(members, db.peers...)
	sort.Strings(members)
	out := members[:0]
	var prev string
	for _, member := range members {
		if member == "" || member == prev {
			continue
		}
		out = append(out, member)
		prev = member
	}
	return out
}

func (db *DB) ownerFor(collection, id string) string {
	members := db.shardMembers()
	if len(members) == 0 {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(collection))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(id))
	return members[h.Sum64()%uint64(len(members))]
}

func (db *DB) isLocalOwner(collection, id string) bool {
	return db.ownerFor(collection, id) == db.localMemberID()
}

func (db *DB) peerClient(addr string) (*rpcClient, error) {
	if addr == "" {
		return nil, errors.New("corkscrewdb: peer address is required")
	}

	db.mu.RLock()
	if client := db.peerClients[addr]; client != nil {
		db.mu.RUnlock()
		return client, nil
	}
	token := db.token
	localEmbedding := db.manifest.Embedding
	db.mu.RUnlock()

	remote, err := Connect(addr, WithToken(token))
	if err != nil {
		return nil, err
	}
	client := remote.remote
	if client == nil {
		_ = remote.Close()
		return nil, errors.New("corkscrewdb: peer client unavailable")
	}
	if remote.manifest.Embedding != localEmbedding {
		_ = remote.Close()
		return nil, errors.New("corkscrewdb: peer embedding config mismatch")
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if existing := db.peerClients[addr]; existing != nil {
		_ = client.Close()
		return existing, nil
	}
	db.peerClients[addr] = client
	return client, nil
}

func (db *DB) closePeerClients() error {
	db.mu.Lock()
	clients := make([]*rpcClient, 0, len(db.peerClients))
	for addr, client := range db.peerClients {
		if client != nil {
			clients = append(clients, client)
		}
		delete(db.peerClients, addr)
	}
	db.mu.Unlock()

	var errs []error
	for _, client := range clients {
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func mergeSearchResultSets(k int, sets ...[]SearchResult) []SearchResult {
	if k <= 0 {
		return nil
	}
	merged := make(map[string]SearchResult)
	for _, set := range sets {
		for _, result := range set {
			if current, ok := merged[result.ID]; ok {
				switch {
				case result.Score > current.Score:
					merged[result.ID] = result
				case result.Score == current.Score && result.Version > current.Version:
					merged[result.ID] = result
				}
				continue
			}
			merged[result.ID] = result
		}
	}
	results := make([]SearchResult, 0, len(merged))
	for _, result := range merged {
		results = append(results, result)
	}
	sortSearchResults(results)
	if len(results) > k {
		results = results[:k]
	}
	return results
}
