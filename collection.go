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

	mu      sync.RWMutex
	index   *index
	history map[string][]Version
	clock   *lamportClock
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
}

func (c *Collection) Put(id string, entry Entry) error {
	if err := c.usable(); err != nil {
		return err
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
	return c.putVector(id, vector, text, metadata)
}

func (c *Collection) PutVector(id string, vector []float32, opts ...PutVectorOption) error {
	if err := c.usable(); err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" {
		return errors.New("corkscrewdb: id is required")
	}
	if len(vector) == 0 {
		return errors.New("corkscrewdb: vector is required")
	}
	cfg := collectPutVectorOptions(opts)
	return c.putVector(id, cloneVector(vector), cfg.text, cfg.metadata)
}

func (c *Collection) Search(query string, k int, filters ...FilterOption) ([]SearchResult, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	vector, err := c.encoder.Encode(query)
	if err != nil {
		return nil, err
	}
	return c.SearchVector(vector, k, filters...)
}

func (c *Collection) SearchVector(query []float32, k int, filters ...FilterOption) ([]SearchResult, error) {
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
	if err := c.usable(); err != nil {
		return nil, err
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
	if err := c.usable(); err != nil {
		return err
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
		LamportClock: c.clock.Tick(),
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
	if _, err := c.applyVersionLocked(id, version, true); err != nil {
		return err
	}
	return nil
}

func (c *Collection) At(maxLamport uint64) *CollectionView {
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
			if !ok || lamportBefore(latest.LamportClock, latest.ActorID, version.LamportClock, version.ActorID) {
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

func (c *Collection) putVector(id string, vector []float32, text string, metadata map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	version := Version{
		Embedding:    cloneVector(vector),
		Text:         text,
		Metadata:     cloneMetadata(metadata),
		LamportClock: c.clock.Tick(),
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
		return lamportBefore(versions[i].LamportClock, versions[i].ActorID, versions[j].LamportClock, versions[j].ActorID)
	})
}
