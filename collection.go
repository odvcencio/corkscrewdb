package corkscrewdb

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"m31labs.dev/corkscrewdb/rawstore"
	snap "m31labs.dev/corkscrewdb/snapshot"
	walpkg "m31labs.dev/corkscrewdb/wal"
	"m31labs.dev/turboquant"
)

var writeSnapshotFile = snap.WriteFile

// Collection stores versioned vector entries within one namespace.
type Collection struct {
	db       *DB
	name     string
	bitWidth int
	seed     int64
	encoder  *encoder
	remote   remoteClient

	rawStore        *rawstore.Store
	rawStoreEnabled bool
	sparseEnabled   bool
	indexType       IndexType
	hnsw            HNSWParams

	scorer Scorer // optional injected scorer (runtime-only, not persisted in meta)

	// deferHNSWBuild, when set during reopen, makes applyVersionLocked build a flat
	// index even for an HNSW collection so the costly graph build can be skipped
	// (cache adoption) or run once after the cache-freshness check (D6).
	deferHNSWBuild bool

	viewCache *viewLRU // memoizes recent At()/AtTime() view indices (D7)

	mu        sync.RWMutex
	index     indexer
	history   map[string][]Version
	sparseSet map[string]*SparseVector
	clock     *HLC
	wal       *walpkg.Manager
	dirty     bool
	err       error
	dim       int
}

// CollectionView is a point-in-time read-only collection snapshot.
type CollectionView struct {
	name          string
	encoder       *encoder
	index         indexer
	history       map[string][]Version
	sparseSet     map[string]*SparseVector
	sparseEnabled bool
	err           error
	dim           int
	remote        remoteClient
	useAt         bool
	atClock       uint64
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
		return errRemoteUnsupportedPendingDistribution
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
	return c.putVector(id, vector, text, metadata, entry.Sparse, federated)
}

func (c *Collection) PutVector(id string, vector []float32, opts ...PutVectorOption) error {
	cfg := collectPutVectorOptions(opts)
	return c.putVectorRequest(id, vector, cfg, true)
}

// PutMultiVector stores many compact child vectors under one logical parent ID.
// V1 supports only local flat quantized_only collections.
func (c *Collection) PutMultiVector(parentID string, entry MultiVectorEntry) error {
	if err := c.usable(); err != nil {
		return err
	}
	if c.remote != nil {
		return errors.New("corkscrewdb: PutMultiVector is unsupported over remote collections in v1")
	}
	if c.db.shouldFederate() {
		return errors.New("corkscrewdb: PutMultiVector is unsupported for federated collections in v1")
	}
	if strings.TrimSpace(parentID) == "" {
		return errors.New("corkscrewdb: parent id is required")
	}
	if len(entry.Children) == 0 {
		return errors.New("corkscrewdb: PutMultiVector requires at least one child")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	version := Version{
		Text:         entry.Text,
		Metadata:     cloneMetadata(entry.Metadata),
		LamportClock: c.clock.Now(),
		ActorID:      c.clock.ActorID(),
		WallClock:    time.Now().UTC(),
		Children:     make([]ChildVector, 0, len(entry.Children)),
	}
	seen := make(map[string]struct{}, len(entry.Children))
	for _, child := range entry.Children {
		childID := strings.TrimSpace(child.ID)
		if childID == "" {
			return errors.New("corkscrewdb: child id is required")
		}
		if _, ok := seen[childID]; ok {
			return fmt.Errorf("corkscrewdb: duplicate child id %q", child.ID)
		}
		seen[childID] = struct{}{}
		if err := c.validateRawVectorLocked(child.Vector); err != nil {
			return err
		}
		if version.dim == 0 {
			version.dim = len(child.Vector)
		} else if len(child.Vector) != version.dim {
			return fmt.Errorf("corkscrewdb: child vector dimension %d does not match parent dimension %d", len(child.Vector), version.dim)
		}
		qv := turboquant.NewIPWithSeed(len(child.Vector), c.bitWidth, c.seed).Quantize(child.Vector)
		version.Children = append(version.Children, ChildVector{
			ID:        childID,
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
			Quantized: &qv,
			dim:       len(child.Vector),
		})
	}
	if err := c.validateVersionLocked(version); err != nil {
		return err
	}
	walEntry := walpkg.Entry{
		Kind:         walpkg.EntryPut,
		CollectionID: c.name,
		VectorID:     parentID,
		Dim:          version.dim,
		Children:     toWALChildren(version.Children),
		Text:         version.Text,
		Metadata:     cloneMetadata(version.Metadata),
		LamportClock: version.LamportClock,
		ActorID:      version.ActorID,
		WallClock:    version.WallClock,
	}
	if err := c.wal.Append(walEntry); err != nil {
		return err
	}
	if c.db.streamer != nil {
		c.db.streamer.Record(c.name, walEntry)
	}
	createdDim, err := c.applyVersionLocked(parentID, version, true)
	if err != nil {
		return err
	}
	if createdDim {
		return c.db.persistCollectionMeta(c.name, c)
	}
	return nil
}

func (c *Collection) putVectorRequest(id string, vector []float32, cfg putVectorConfig, federated bool) error {
	if err := c.usable(); err != nil {
		return err
	}
	if c.remote != nil {
		return c.remote.PutVector(c.name, id, vector, cfg.text, cfg.metadata, false)
	}
	if federated && c.db.shouldFederate() && !c.db.isLocalOwner(c.name, id) {
		return errRemoteUnsupportedPendingDistribution
	}
	if strings.TrimSpace(id) == "" {
		return errors.New("corkscrewdb: id is required")
	}
	if len(vector) == 0 {
		return errors.New("corkscrewdb: vector is required")
	}
	return c.putVector(id, cloneVector(vector), cfg.text, cfg.metadata, nil, federated)
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

func (c *Collection) SearchParents(query string, k int, opts ...ParentSearchOption) ([]ParentSearchResult, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	if c.remote != nil {
		return nil, errors.New("corkscrewdb: SearchParents is unsupported over remote collections in v1")
	}
	if c.db.shouldFederate() {
		return nil, errors.New("corkscrewdb: SearchParents is unsupported for federated collections in v1")
	}
	vector, err := c.encoder.Encode(query)
	if err != nil {
		return nil, err
	}
	return c.SearchParentsVector(vector, k, opts...)
}

func (c *Collection) SearchParentsVector(query []float32, k int, opts ...ParentSearchOption) ([]ParentSearchResult, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	if c.remote != nil {
		return nil, errors.New("corkscrewdb: SearchParentsVector is unsupported over remote collections in v1")
	}
	if c.db.shouldFederate() {
		return nil, errors.New("corkscrewdb: SearchParentsVector is unsupported for federated collections in v1")
	}
	return c.searchParentsVectorLocal(query, k, collectParentSearchOptions(opts))
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
		for _, peer := range c.db.remoteShardTargets() {
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
	// HNSW owns its own traversal (§10); only the flat path routes through Scorer.
	flat, isFlat := idx.(*index)
	if !isFlat {
		return idx.Search(query, k, filters), nil
	}
	return c.flatSearchViaScorer(flat, query, k, filters), nil
}

// flatSearchViaScorer routes the flat search path through the active Scorer seam
// (§10). It reproduces the certified inline path: the same InnerProductPrepared
// per row, with matchesFilters pushed down (evaluated before scoring) and child/
// tombstoned rows excluded.
func (c *Collection) flatSearchViaScorer(flat *index, query []float32, k int, filters []FilterOption) []SearchResult {
	if k <= 0 {
		return nil
	}
	corpus, rowToEntry := flat.corpusSnapshot()
	if len(corpus) == 0 {
		return nil
	}
	scorer := c.scorerForSearch(flat, corpus)

	flat.mu.RLock()
	pq := flat.quantizer.PrepareQuery(query)
	// accept(row) runs the §7.4 filter pushdown on the row's entry metadata.
	accept := func(row int) bool {
		entry := flat.entries[rowToEntry[row]]
		return matchesFilters(entry.metadata, filters)
	}
	hits := scorer.ScoreTopK(pq, k, accept)
	results := make([]SearchResult, 0, len(hits))
	for _, h := range hits {
		entry := flat.entries[rowToEntry[h.Index]]
		results = append(results, SearchResult{
			ID:       entry.id,
			Score:    h.Score,
			Text:     entry.text,
			Metadata: cloneMetadata(entry.metadata),
			Version:  entry.version,
		})
	}
	flat.mu.RUnlock()
	sortSearchResults(results)
	return results
}

// corpusSetter is the optional seam by which a CPU-side injected scorer adopts the
// collection's live corpus each search. Device-resident scorers (GPU) do not
// implement it and keep their own static corpus snapshot.
type corpusSetter interface {
	SetCorpus(corpus []turboquant.IPQuantized)
}

// scorerForSearch returns the active scorer for the flat path, lazily building a
// defaultScorer over the supplied live corpus when none was injected. An injected
// scorer that implements corpusSetter is refreshed with the live corpus so its
// row indices align with corpusSnapshot.
func (c *Collection) scorerForSearch(flat *index, corpus []turboquant.IPQuantized) Scorer {
	if c.scorer != nil {
		if cs, ok := c.scorer.(corpusSetter); ok {
			cs.SetCorpus(corpus)
		}
		return c.scorer
	}
	ds := newDefaultScorer(flat.quantizer)
	ds.SetCorpus(corpus)
	return ds
}

func (c *Collection) searchParentsVectorLocal(query []float32, k int, cfg parentSearchConfig) ([]ParentSearchResult, error) {
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
	flat, ok := idx.(*index)
	if !ok {
		return nil, errors.New("corkscrewdb: SearchParentsVector requires flat quantized_only index in v1")
	}
	return flat.SearchParents(query, k, cfg)
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
		return errRemoteUnsupportedPendingDistribution
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
	hlc := packHLC(uint64(t.UnixMilli()), hlcLogicalMask) // capture all same-ms logical counters
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
		name:          c.name,
		encoder:       c.encoder,
		history:       make(map[string][]Version),
		sparseSet:     make(map[string]*SparseVector),
		sparseEnabled: c.sparseEnabled,
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.err != nil {
		view.err = c.err
		return view
	}
	view.dim = c.dim

	// Try the view-index cache. Historical view indices are immutable; the live
	// head is invalidated on write (D7). On hit we reuse the shared *index and only
	// rebuild the lightweight history/sparse scaffolding the wrapper needs.
	var flat *index
	cached := false
	if c.viewCache != nil {
		if ve, ok := c.viewCache.get(maxLamport); ok {
			flat = ve.index
			view.dim = ve.dim
			cached = true
		}
	}
	if !cached && c.dim > 0 {
		flat = newIndex(c.dim, c.bitWidth, c.seed)
	}

	for id, versions := range c.history {
		// versions is sorted ascending by (LamportClock, ActorID); binary-search the
		// last version with LamportClock <= maxLamport (the cutoff has the max logical
		// counter so same-ms later writes ARE included — preserves AtTime semantics).
		hi := sort.Search(len(versions), func(i int) bool {
			return versions[i].LamportClock > maxLamport
		})
		if hi == 0 {
			continue // no version visible at this clock
		}
		latest := versions[hi-1] // greatest (LamportClock,ActorID) <= maxLamport

		visible := make([]Version, hi)
		for i := 0; i < hi; i++ {
			visible[i] = cloneVersion(versions[i])
		}
		view.history[id] = visible

		// §3.2 (view): populate the sparse active set OUTSIDE the index guard.
		if !latest.Tombstone && latest.Sparse != nil {
			view.sparseSet[id] = latest.Sparse
		}
		// Index population only on a cache miss (a cache hit reuses the built index).
		if !cached && !latest.Tombstone && flat != nil {
			if latest.Quantized != nil {
				flat.addQuantized(id, *latest.Quantized, latest.Text, latest.Metadata, latest.LamportClock)
			} else if len(latest.Children) > 0 {
				for _, child := range latest.Children {
					flat.addPackedChild(id, latest, child)
				}
			}
		}
	}
	if flat != nil {
		view.index = flat
	}
	if !cached && flat != nil && c.viewCache != nil {
		c.viewCache.put(maxLamport, flat, view.dim)
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

func (v *CollectionView) SearchParents(query string, k int, opts ...ParentSearchOption) ([]ParentSearchResult, error) {
	if v.err != nil {
		return nil, v.err
	}
	if v.remote != nil {
		return nil, errors.New("corkscrewdb: SearchParents is unsupported over remote collections in v1")
	}
	vector, err := v.encoder.Encode(query)
	if err != nil {
		return nil, err
	}
	return v.SearchParentsVector(vector, k, opts...)
}

func (v *CollectionView) SearchParentsVector(query []float32, k int, opts ...ParentSearchOption) ([]ParentSearchResult, error) {
	if v.err != nil {
		return nil, v.err
	}
	if v.remote != nil {
		return nil, errors.New("corkscrewdb: SearchParentsVector is unsupported over remote collections in v1")
	}
	if v.index == nil {
		return nil, nil
	}
	if len(query) != v.dim {
		return nil, fmt.Errorf("corkscrewdb: query dimension %d does not match collection dimension %d", len(query), v.dim)
	}
	flat, ok := v.index.(*index)
	if !ok {
		return nil, errors.New("corkscrewdb: SearchParentsVector requires flat quantized_only index in v1")
	}
	return flat.SearchParents(query, k, collectParentSearchOptions(opts))
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

// RebuildIndex rebuilds the collection's vector index using the target algorithm.
// All existing vectors are copied into the new index structure.
func (c *Collection) RebuildIndex(target IndexType) error {
	if err := c.usable(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.index == nil {
		return nil
	}

	switch target {
	case IndexHNSW:
		params := c.hnsw
		if params.M == 0 && params.EfConstruction == 0 && params.EfSearch == 0 {
			params = defaultHNSWParams()
		}
		hw := newHNSWIndex(c.dim, c.bitWidth, c.seed, params)
		// Re-insert all live entries directly from stored codes (no raw vector).
		for id, versions := range c.history {
			if len(versions) == 0 {
				continue
			}
			latest := versions[len(versions)-1]
			if latest.Tombstone || latest.Quantized == nil {
				continue
			}
			hw.AddQuantized(id, *latest.Quantized, latest.Text, latest.Metadata, latest.LamportClock)
		}
		c.index = hw
		c.indexType = IndexHNSW
		c.hnsw = params
		if err := c.db.persistCollectionMeta(c.name, c); err != nil {
			return err
		}
	case IndexFlat:
		switch idx := c.index.(type) {
		case *index:
			return nil // already flat
		case *hnswIndex:
			c.index = idx.flat
		default:
			return errors.New("corkscrewdb: unknown index type")
		}
		c.indexType = IndexFlat
		if err := c.db.persistCollectionMeta(c.name, c); err != nil {
			return err
		}
	default:
		return fmt.Errorf("corkscrewdb: unsupported index type %d", target)
	}
	c.dirty = true
	return nil
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

func (c *Collection) putVector(id string, vector []float32, text string, metadata map[string]string, sparse *SparseVector, federated bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if sparse != nil {
		if !c.sparseEnabled {
			return errors.New("corkscrewdb: sparse vector supplied but collection was not created WithSparse")
		}
		if err := validateSparseVector(sparse); err != nil {
			return err
		}
	}

	if err := c.validateRawVectorLocked(vector); err != nil {
		return err
	}
	version := Version{
		Text:         text,
		Metadata:     cloneMetadata(metadata),
		LamportClock: c.clock.Now(),
		ActorID:      c.clock.ActorID(),
		WallClock:    time.Now().UTC(),
	}
	// Codes are always present (the hot tier).
	qv := turboquant.NewIPWithSeed(len(vector), c.bitWidth, c.seed).Quantize(vector)
	version.Quantized = &qv
	version.dim = len(vector)
	version.Sparse = cloneSparseVector(sparse)
	// Raw blob is fsynced BEFORE the WAL append below (cold tier).
	if c.rawStoreEnabled {
		hash, err := c.rawStore.Put(rawFloat32Bytes(vector))
		if err != nil {
			return err
		}
		version.RawHash = hash
	}
	if err := c.validateVersionLocked(version); err != nil {
		return err
	}
	entry := walpkg.Entry{
		Kind:         walpkg.EntryPut,
		CollectionID: c.name,
		VectorID:     id,
		Quantized:    toWALQuantized(version.Quantized),
		Dim:          version.dim,
		RawHash:      version.RawHash,
		Sparse:       toWALSparse(version.Sparse),
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
		return c.db.persistCollectionMeta(c.name, c)
	}
	return nil
}

func (c *Collection) validateVersionLocked(version Version) error {
	if version.Tombstone {
		return nil
	}
	if c.dim == 0 && version.dim == 0 {
		return errors.New("corkscrewdb: collection dimension is required for dense versions")
	}
	if c.dim != 0 && version.dim != 0 && version.dim != c.dim {
		return fmt.Errorf("corkscrewdb: vector dimension %d does not match collection dimension %d", version.dim, c.dim)
	}
	dim := c.dim
	if dim == 0 {
		dim = version.dim
	}
	if len(version.Children) > 0 {
		for _, child := range version.Children {
			if strings.TrimSpace(child.ID) == "" {
				return errors.New("corkscrewdb: child id is required")
			}
			if child.dim != 0 && child.dim != dim {
				return fmt.Errorf("corkscrewdb: child vector dimension %d does not match collection dimension %d", child.dim, dim)
			}
			if err := validateQuantizedPayload(child.Quantized, dim, c.bitWidth); err != nil {
				return err
			}
		}
		return nil
	}
	return validateQuantizedPayload(version.Quantized, dim, c.bitWidth)
}

func (c *Collection) validateRawVectorLocked(vector []float32) error {
	if len(vector) == 0 {
		return errors.New("corkscrewdb: empty embedding")
	}
	if c.dim != 0 && len(vector) != c.dim {
		return fmt.Errorf("corkscrewdb: vector dimension %d does not match collection dimension %d", len(vector), c.dim)
	}
	return nil
}

func (c *Collection) applyVersionLocked(id string, version Version, markDirty bool) (bool, error) {
	if err := c.validateVersionLocked(version); err != nil {
		return false, err
	}
	createdDim := false
	if !version.Tombstone && c.dim == 0 && version.dim > 0 {
		c.dim = version.dim
		createdDim = true
	}

	// O(log v) ordered insert by (LamportClock, ActorID) with neighborhood dedup.
	versions := c.history[id]
	pos := sort.Search(len(versions), func(i int) bool {
		if versions[i].LamportClock != version.LamportClock {
			return versions[i].LamportClock >= version.LamportClock
		}
		return versions[i].ActorID >= version.ActorID
	})
	// Neighborhood dedup: identical (LamportClock, ActorID) already present?
	if pos < len(versions) && versions[pos].LamportClock == version.LamportClock && versions[pos].ActorID == version.ActorID {
		return false, nil
	}
	cloned := cloneVersion(version)
	versions = append(versions, Version{})
	copy(versions[pos+1:], versions[pos:])
	versions[pos] = cloned
	c.history[id] = versions
	latest := versions[len(versions)-1]

	// §3.2: clear any prior sparse entry UNCONDITIONALLY, outside the
	// `c.index != nil` guard, so a tombstone or sparse-less replacement always
	// drops the stale sparse channel even when the dense index is absent.
	delete(c.sparseSet, id)

	if c.index != nil {
		c.index.Remove(id)
		if flat, ok := c.index.(*index); ok {
			flat.removePackedChildren(id)
		}
	}
	if !latest.Tombstone {
		if c.index == nil {
			// During deferred reopen, build a flat index even for HNSW collections;
			// the graph is adopted from cache or built once after the freshness check.
			if c.indexType == IndexHNSW && len(latest.Children) == 0 && !c.deferHNSWBuild {
				c.index = newHNSWIndex(c.dim, c.bitWidth, c.seed, c.hnsw)
			} else {
				c.index = newIndex(c.dim, c.bitWidth, c.seed)
			}
		}
		switch idx := c.index.(type) {
		case *index:
			if latest.Quantized != nil {
				idx.addQuantized(id, *latest.Quantized, latest.Text, latest.Metadata, latest.LamportClock)
			} else if len(latest.Children) > 0 {
				for _, child := range latest.Children {
					idx.addPackedChild(id, latest, child)
				}
			} else {
				return false, errors.New("corkscrewdb: non-tombstone version missing quantized payload")
			}
		case *hnswIndex:
			if latest.Quantized != nil {
				idx.AddQuantized(id, *latest.Quantized, latest.Text, latest.Metadata, latest.LamportClock)
			} else {
				return false, errors.New("corkscrewdb: packed parent multivectors require flat index")
			}
		default:
			return false, errors.New("corkscrewdb: unknown index type")
		}
		if c.dim == 0 {
			c.dim = c.index.Dim()
			createdDim = true
		}
	}
	// Maintain the sparse active set in lockstep with the dense index. The
	// stored version's Sparse pointer is deep-cloned at store time
	// (cloneVersion), so aliasing it here is safe.
	if !latest.Tombstone && latest.Sparse != nil {
		if c.sparseSet == nil {
			c.sparseSet = make(map[string]*SparseVector)
		}
		c.sparseSet[id] = latest.Sparse
	}
	c.clock.Witness(version.LamportClock)
	if markDirty {
		c.dirty = true
		// Live write: drop any cached view at/above this clock (the live head and
		// any future-clock view). Historical views (clock < this) stay valid (D7).
		if c.viewCache != nil {
			c.viewCache.invalidateHead(version.LamportClock)
		}
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
		return c.db.persistCollectionMeta(c.name, c)
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
		} else {
			// Caches are pure optimizations over the now-durable snapshot. Written
			// best-effort (failures swallowed, never fail close). A clean (non-dirty)
			// close writes nothing and relies on the prior close's cache (D6).
			c.writeCachesBestEffort()
		}
	}
	if c.wal != nil {
		if err := c.wal.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.rawStore != nil {
		if err := c.rawStore.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// writeCachesBestEffort persists the live search index as the .tqi/graph.hnsw
// caches, stamped with the snapshot's MaxLamport. graph.hnsw is written FIRST and
// .tqi (the lamport authority) SECOND, so a graph.hnsw without a fresh sibling
// .tqi is treated as stale on load (option (i): graph.hnsw carries no lamport).
// All failures are swallowed — the snapshot+WAL remain authoritative.
func (c *Collection) writeCachesBestEffort() {
	c.mu.RLock()
	maxLamport := c.clock.Current()
	idx := c.flatIndexForCacheLocked()
	idxType := c.indexType
	var hw *hnswIndex
	if h, ok := c.index.(*hnswIndex); ok {
		hw = h
	}
	rawStore := c.rawStoreEnabled
	sparse := c.sparseEnabled
	c.mu.RUnlock()
	if idx == nil {
		return
	}
	hnswPath := c.db.collectionHNSWPath(c.name)
	indexPath := c.db.collectionIndexPath(c.name)
	// graph.hnsw FIRST, then .tqi — option (i) ordering.
	if idxType == IndexHNSW && hw != nil {
		_ = saveHNSWFile(hnswPath, hw) // best-effort; swallowed
	}
	_ = saveIndexFile(indexPath, idx, maxLamport, rawStore, sparse) // best-effort
}

// flatIndexForCacheLocked returns the flat *index underlying the live search
// structure (the flat index itself, or the HNSW graph's wrapped flat). Caller
// holds c.mu.
func (c *Collection) flatIndexForCacheLocked() *index {
	switch idx := c.index.(type) {
	case *index:
		return idx
	case *hnswIndex:
		return idx.flat
	default:
		return nil
	}
}

func (c *Collection) persistSnapshot() error {
	c.mu.RLock()
	if len(c.history) == 0 {
		c.mu.RUnlock()
		return nil
	}
	maxLamport := c.clock.Current()
	data := snap.Data{
		Collection:    c.name,
		BitWidth:      c.bitWidth,
		Seed:          c.seed,
		Dim:           c.dim,
		RawStore:      c.rawStoreEnabled,
		SparseEnabled: c.sparseEnabled,
		MaxLamport:    maxLamport,
		CreatedAt:     time.Now().UTC(),
		Records:       make([]snap.Record, 0, len(c.history)),
	}
	ids := make([]string, 0, len(c.history))
	for id := range c.history {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		record := snap.Record{ID: id, Versions: make([]snap.Version, 0, len(c.history[id]))}
		for _, version := range c.history[id] {
			snapVersion := snap.Version{
				Quantized:    toSnapshotQuantized(version.Quantized),
				Sparse:       toSnapshotSparse(version.Sparse),
				Children:     toSnapshotChildren(version.Children),
				Text:         version.Text,
				Metadata:     cloneMetadata(version.Metadata),
				LamportClock: version.LamportClock,
				ActorID:      version.ActorID,
				WallClock:    version.WallClock,
				Tombstone:    version.Tombstone,
			}
			if c.rawStoreEnabled && len(version.RawHash) == 32 {
				snapVersion.RawHash = append([]byte(nil), version.RawHash...)
			}
			record.Versions = append(record.Versions, snapVersion)
		}
		data.Records = append(data.Records, record)
	}
	c.mu.RUnlock()

	path := filepath.Join(c.db.collectionDir(c.name), fmt.Sprintf("snapshot-%020d.csdb", maxLamport))
	if err := writeSnapshotFile(path, data); err != nil {
		return err
	}

	c.mu.Lock()
	if c.clock.Current() != maxLamport {
		// A concurrent write landed after the snapshot was taken; keep the WAL.
		c.mu.Unlock()
		return c.db.pruneSnapshots(c.name, path)
	}
	// The snapshot is the durable full-state copy after pruning. Keep writers
	// blocked between this clock check and the raw-store GC + WAL reset.
	if c.rawStoreEnabled && c.rawStore != nil {
		// reachable = union(history RawHashes, post-snapshot-WAL RawHashes).
		// Phase 1: single-writer under c.mu, MaxLamport == c.clock.Current(), so the
		// post-snapshot-WAL term is empty and reachable == the history RawHash set.
		// (Distribution phase, which adds concurrent/streamed writes, must restore the
		// second term of the union here.)
		reachable := c.reachableRawHashesLocked()
		if err := c.rawStore.GC(reachable); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	if err := c.pruneWALLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	c.dirty = false
	c.mu.Unlock()
	return c.db.pruneSnapshots(c.name, path)
}

// reachableRawHashesLocked returns the set of raw-store keys referenced by the
// current in-memory history. Caller must hold c.mu.
func (c *Collection) reachableRawHashesLocked() map[string]struct{} {
	reachable := make(map[string]struct{})
	for _, versions := range c.history {
		for _, version := range versions {
			if len(version.RawHash) == 32 {
				reachable[string(version.RawHash)] = struct{}{}
			}
		}
	}
	return reachable
}

func (c *Collection) pruneWALLocked() error {
	if c.wal == nil {
		return nil
	}
	return c.wal.PruneAndReset()
}
