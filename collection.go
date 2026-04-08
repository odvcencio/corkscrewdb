package corkscrewdb

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	snap "github.com/odvcencio/corkscrewdb/snapshot"
	walpkg "github.com/odvcencio/corkscrewdb/wal"
)

// Collection stores versioned vector entries within one namespace.
type Collection struct {
	db       *DB
	name     string
	bitWidth int
	seed     int64
	encoder  *encoder
	remote   *rpcClient

	mu      sync.RWMutex
	index   *index
	history map[string][]Version
	clock   *HLC
	wal     *walpkg.Manager
	dirty   bool
	err     error
	dim     int
}

// CollectionView is a point-in-time read-only collection snapshot.
type CollectionView struct {
	name    string
	encoder *encoder
	index   *index
	history map[string][]Version
	err     error
	dim     int
	remote  *rpcClient
	useAt   bool
	atClock uint64
}

func (c *Collection) Put(id string, entry Entry) error {
	return c.put(id, entry, true)
}

func (c *Collection) put(id string, entry Entry, federated bool) error {
	if err := c.usable(); err != nil {
		return err
	}
	if c.remote != nil {
		return c.remote.Put(c.name, id, entry, false)
	}
	if federated && c.db.shouldFederate() && !c.db.isLocalOwner(c.name, id) {
		client, err := c.db.peerClient(c.db.ownerFor(c.name, id))
		if err != nil {
			return err
		}
		if err := client.EnsureCollection(c.name, c.bitWidth); err != nil {
			return err
		}
		return client.Put(c.name, id, entry, true)
	}
	if strings.TrimSpace(id) == "" {
		return errors.New("corkscrewdb: id is required")
	}

	text := entry.Text
	vector := cloneVector(entry.Vector)
	metadata := cloneMetadata(entry.Metadata)
	if len(vector) == 0 {
		if text == "" {
			return errors.New("corkscrewdb: entry requires text or vector")
		}
		encoded, err := c.encoder.Encode(text)
		if err != nil {
			return err
		}
		vector = encoded
	}
	return c.putVector(id, vector, text, metadata, federated)
}

func (c *Collection) PutVector(id string, vector []float32, opts ...PutVectorOption) error {
	cfg := collectPutVectorOptions(opts)
	return c.putVectorRequest(id, vector, cfg, true)
}

func (c *Collection) putVectorRequest(id string, vector []float32, cfg putVectorConfig, federated bool) error {
	if err := c.usable(); err != nil {
		return err
	}
	if c.remote != nil {
		return c.remote.PutVector(c.name, id, vector, cfg.text, cfg.metadata, false)
	}
	if federated && c.db.shouldFederate() && !c.db.isLocalOwner(c.name, id) {
		client, err := c.db.peerClient(c.db.ownerFor(c.name, id))
		if err != nil {
			return err
		}
		if err := client.EnsureCollection(c.name, c.bitWidth); err != nil {
			return err
		}
		return client.PutVector(c.name, id, vector, cfg.text, cfg.metadata, true)
	}
	if strings.TrimSpace(id) == "" {
		return errors.New("corkscrewdb: id is required")
	}
	if len(vector) == 0 {
		return errors.New("corkscrewdb: vector is required")
	}
	return c.putVector(id, cloneVector(vector), cfg.text, cfg.metadata, federated)
}

func (c *Collection) Search(query string, k int, filters ...FilterOption) ([]SearchResult, error) {
	return c.search(query, k, filters, true)
}

func (c *Collection) search(query string, k int, filters []FilterOption, federated bool) ([]SearchResult, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	if c.remote != nil {
		return c.remote.Search(c.name, query, k, filters, false, 0, false)
	}
	if federated && c.db.shouldFederate() {
		vector, err := c.encoder.Encode(query)
		if err != nil {
			return nil, err
		}
		return c.searchVector(vector, k, filters, true)
	}
	vector, err := c.encoder.Encode(query)
	if err != nil {
		return nil, err
	}
	return c.searchVector(vector, k, filters, false)
}

func (c *Collection) SearchVector(query []float32, k int, filters ...FilterOption) ([]SearchResult, error) {
	return c.searchVector(query, k, filters, true)
}

func (c *Collection) searchVector(query []float32, k int, filters []FilterOption, federated bool) ([]SearchResult, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	if c.remote != nil {
		return c.remote.SearchVector(c.name, query, k, filters, false, 0, false)
	}
	if federated && c.db.shouldFederate() {
		local, err := c.searchVectorLocal(query, k, filters)
		if err != nil {
			return nil, err
		}
		sets := [][]SearchResult{local}
		for _, peer := range c.db.peers {
			if peer == "" || peer == c.db.localMemberID() {
				continue
			}
			client, err := c.db.peerClient(peer)
			if err != nil {
				return nil, err
			}
			results, err := client.SearchVector(c.name, query, k, filters, false, 0, true)
			if err != nil {
				return nil, err
			}
			sets = append(sets, results)
		}
		return mergeSearchResultSets(k, sets...), nil
	}
	return c.searchVectorLocal(query, k, filters)
}

func (c *Collection) searchVectorLocal(query []float32, k int, filters []FilterOption) ([]SearchResult, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	idx := c.index
	dim := c.dim
	c.mu.RUnlock()
	if idx == nil {
		return nil, nil
	}
	if len(query) != dim {
		return nil, fmt.Errorf("corkscrewdb: query dimension %d does not match collection dimension %d", len(query), dim)
	}
	return idx.Search(query, k, filters), nil
}

func (c *Collection) History(id string) ([]Version, error) {
	return c.historyFor(id, true)
}

func (c *Collection) historyFor(id string, federated bool) ([]Version, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	if c.remote != nil {
		return c.remote.History(c.name, id, false, 0, false)
	}
	if federated && c.db.shouldFederate() && !c.db.isLocalOwner(c.name, id) {
		client, err := c.db.peerClient(c.db.ownerFor(c.name, id))
		if err != nil {
			return nil, err
		}
		return client.History(c.name, id, false, 0, true)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	versions := c.history[id]
	out := make([]Version, len(versions))
	for i := range versions {
		out[i] = cloneVersion(versions[i])
	}
	return out, nil
}

func (c *Collection) Delete(id string) error {
	return c.delete(id, true)
}

func (c *Collection) delete(id string, federated bool) error {
	if err := c.usable(); err != nil {
		return err
	}
	if c.remote != nil {
		return c.remote.Delete(c.name, id, false)
	}
	if federated && c.db.shouldFederate() && !c.db.isLocalOwner(c.name, id) {
		client, err := c.db.peerClient(c.db.ownerFor(c.name, id))
		if err != nil {
			return err
		}
		return client.Delete(c.name, id, true)
	}
	if strings.TrimSpace(id) == "" {
		return errors.New("corkscrewdb: id is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var latest Version
	if versions := c.history[id]; len(versions) > 0 {
		latest = cloneVersion(versions[len(versions)-1])
	}
	version := Version{
		Text:         latest.Text,
		Metadata:     cloneMetadata(latest.Metadata),
		LamportClock: c.clock.Now(),
		ActorID:      c.clock.ActorID(),
		WallClock:    time.Now().UTC(),
		Tombstone:    true,
	}
	entry := walpkg.Entry{
		Kind:         walpkg.EntryTombstone,
		CollectionID: c.name,
		VectorID:     id,
		Text:         version.Text,
		Metadata:     cloneMetadata(version.Metadata),
		LamportClock: version.LamportClock,
		ActorID:      version.ActorID,
		WallClock:    version.WallClock,
	}
	if err := c.wal.Append(entry); err != nil {
		return err
	}
	if c.db.streamer != nil {
		c.db.streamer.Record(c.name, entry)
	}
	if _, err := c.applyVersionLocked(id, version, true); err != nil {
		return err
	}
	return nil
}

// AtTime returns a point-in-time view of the collection at the given wall-clock time.
func (c *Collection) AtTime(t time.Time) *CollectionView {
	hlc := packHLC(uint64(t.UnixMilli()), 0)
	return c.At(hlc)
}

func (c *Collection) At(maxLamport uint64) *CollectionView {
	if c == nil {
		return &CollectionView{err: errors.New("corkscrewdb: nil collection")}
	}
	if c.err != nil {
		return &CollectionView{err: c.err}
	}
	if c.remote != nil {
		return &CollectionView{
			name:    c.name,
			remote:  c.remote,
			useAt:   true,
			atClock: maxLamport,
		}
	}
	view := &CollectionView{
		name:    c.name,
		encoder: c.encoder,
		history: make(map[string][]Version),
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.err != nil {
		view.err = c.err
		return view
	}
	view.dim = c.dim
	if c.dim > 0 {
		view.index = newIndex(c.dim, c.bitWidth, c.seed)
	}
	for id, versions := range c.history {
		var visible []Version
		var latest Version
		var ok bool
		for _, version := range versions {
			if version.LamportClock > maxLamport {
				continue
			}
			visible = append(visible, cloneVersion(version))
			if !ok || latest.LamportClock < version.LamportClock || (latest.LamportClock == version.LamportClock && latest.ActorID < version.ActorID) {
				latest = version
				ok = true
			}
		}
		if len(visible) == 0 {
			continue
		}
		view.history[id] = visible
		if ok && !latest.Tombstone && view.index != nil {
			view.index.Add(id, latest.Embedding, latest.Text, latest.Metadata, latest.LamportClock)
		}
	}
	return view
}

func (v *CollectionView) Search(query string, k int, filters ...FilterOption) ([]SearchResult, error) {
	if v.err != nil {
		return nil, v.err
	}
	if v.remote != nil {
		return v.remote.Search(v.name, query, k, filters, v.useAt, v.atClock, false)
	}
	vector, err := v.encoder.Encode(query)
	if err != nil {
		return nil, err
	}
	return v.SearchVector(vector, k, filters...)
}

func (v *CollectionView) SearchVector(query []float32, k int, filters ...FilterOption) ([]SearchResult, error) {
	if v.err != nil {
		return nil, v.err
	}
	if v.remote != nil {
		return v.remote.SearchVector(v.name, query, k, filters, v.useAt, v.atClock, false)
	}
	if v.index == nil {
		return nil, nil
	}
	if len(query) != v.dim {
		return nil, fmt.Errorf("corkscrewdb: query dimension %d does not match collection dimension %d", len(query), v.dim)
	}
	return v.index.Search(query, k, filters), nil
}

func (v *CollectionView) History(id string) ([]Version, error) {
	if v.err != nil {
		return nil, v.err
	}
	if v.remote != nil {
		return v.remote.History(v.name, id, v.useAt, v.atClock, false)
	}
	versions := v.history[id]
	out := make([]Version, len(versions))
	for i := range versions {
		out[i] = cloneVersion(versions[i])
	}
	return out, nil
}

func (c *Collection) usable() error {
	if c == nil {
		return errors.New("corkscrewdb: nil collection")
	}
	if c.err != nil {
		return c.err
	}
	if c.db == nil {
		return errors.New("corkscrewdb: collection is not attached to a database")
	}
	if c.db.isClosed() {
		return errors.New("corkscrewdb: database is closed")
	}
	return nil
}

func (c *Collection) putVector(id string, vector []float32, text string, metadata map[string]string, federated bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	version := Version{
		Embedding:    cloneVector(vector),
		Text:         text,
		Metadata:     cloneMetadata(metadata),
		LamportClock: c.clock.Now(),
		ActorID:      c.clock.ActorID(),
		WallClock:    time.Now().UTC(),
	}
	if err := c.validateVersionLocked(version); err != nil {
		return err
	}
	entry := walpkg.Entry{
		Kind:         walpkg.EntryPut,
		CollectionID: c.name,
		VectorID:     id,
		Embedding:    cloneVector(version.Embedding),
		Text:         version.Text,
		Metadata:     cloneMetadata(version.Metadata),
		LamportClock: version.LamportClock,
		ActorID:      version.ActorID,
		WallClock:    version.WallClock,
	}
	if err := c.wal.Append(entry); err != nil {
		return err
	}
	if c.db.streamer != nil {
		c.db.streamer.Record(c.name, entry)
	}
	createdDim, err := c.applyVersionLocked(id, version, true)
	if err != nil {
		return err
	}
	if createdDim {
		return c.db.persistCollectionMeta(c.name, c.bitWidth, c.dim, c.seed)
	}
	return nil
}

func (c *Collection) validateVersionLocked(version Version) error {
	if version.Tombstone {
		return nil
	}
	if len(version.Embedding) == 0 {
		return errors.New("corkscrewdb: empty embedding")
	}
	if c.dim != 0 && len(version.Embedding) != c.dim {
		return fmt.Errorf("corkscrewdb: vector dimension %d does not match collection dimension %d", len(version.Embedding), c.dim)
	}
	return nil
}

func (c *Collection) applyVersionLocked(id string, version Version, markDirty bool) (bool, error) {
	if err := c.validateVersionLocked(version); err != nil {
		return false, err
	}
	// Dedup: skip if we already have this exact (actorID, lamportClock) for this ID.
	for _, existing := range c.history[id] {
		if existing.LamportClock == version.LamportClock && existing.ActorID == version.ActorID {
			return false, nil
		}
	}
	createdDim := false
	if !version.Tombstone && c.dim == 0 {
		c.dim = len(version.Embedding)
		c.index = newIndex(c.dim, c.bitWidth, c.seed)
		createdDim = true
	}

	versions := append(c.history[id], cloneVersion(version))
	sortVersions(versions)
	c.history[id] = versions
	latest := versions[len(versions)-1]
	if latest.Tombstone {
		if c.index != nil {
			c.index.Remove(id)
		}
	} else {
		if c.index == nil {
			c.index = newIndex(len(latest.Embedding), c.bitWidth, c.seed)
			c.dim = len(latest.Embedding)
			createdDim = true
		}
		c.index.Add(id, latest.Embedding, latest.Text, latest.Metadata, latest.LamportClock)
	}
	c.clock.Witness(version.LamportClock)
	if markDirty {
		c.dirty = true
	}
	return createdDim, nil
}

func (c *Collection) loadVersion(id string, version Version) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	createdDim, err := c.applyVersionLocked(id, version, false)
	if err != nil {
		return err
	}
	if createdDim {
		return c.db.persistCollectionMeta(c.name, c.bitWidth, c.dim, c.seed)
	}
	return nil
}

func (c *Collection) sync() error {
	if c.wal == nil {
		return nil
	}
	return c.wal.Sync()
}

func (c *Collection) close() error {
	var errs []error
	if c.dirty {
		if err := c.persistSnapshot(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.wal != nil {
		if err := c.wal.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *Collection) persistSnapshot() error {
	c.mu.RLock()
	if len(c.history) == 0 {
		c.mu.RUnlock()
		return nil
	}
	maxLamport := c.clock.Current()
	data := snap.Data{
		Collection: c.name,
		BitWidth:   c.bitWidth,
		Seed:       c.seed,
		Dim:        c.dim,
		MaxLamport: maxLamport,
		CreatedAt:  time.Now().UTC(),
		Records:    make([]snap.Record, 0, len(c.history)),
	}
	ids := make([]string, 0, len(c.history))
	for id := range c.history {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		record := snap.Record{ID: id, Versions: make([]snap.Version, 0, len(c.history[id]))}
		for _, version := range c.history[id] {
			record.Versions = append(record.Versions, snap.Version{
				Embedding:    cloneVector(version.Embedding),
				Text:         version.Text,
				Metadata:     cloneMetadata(version.Metadata),
				LamportClock: version.LamportClock,
				ActorID:      version.ActorID,
				WallClock:    version.WallClock,
				Tombstone:    version.Tombstone,
			})
		}
		data.Records = append(data.Records, record)
	}
	c.mu.RUnlock()

	path := filepath.Join(c.db.collectionDir(c.name), fmt.Sprintf("snapshot-%020d.csdb", maxLamport))
	if err := snap.WriteFile(path, data); err != nil {
		return err
	}
	if err := saveIndexFile(c.db.collectionIndexPath(c.name), c.index, maxLamport); err != nil {
		return err
	}
	c.mu.Lock()
	if c.clock.Current() == maxLamport {
		c.dirty = false
	}
	c.mu.Unlock()
	return c.db.pruneSnapshots(c.name, path)
}

func sortVersions(versions []Version) {
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].LamportClock != versions[j].LamportClock {
			return versions[i].LamportClock < versions[j].LamportClock
		}
		return versions[i].ActorID < versions[j].ActorID
	})
}
