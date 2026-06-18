package corkscrewdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"m31labs.dev/corkscrewdb/rawstore"
	"m31labs.dev/corkscrewdb/replica"
	snap "m31labs.dev/corkscrewdb/snapshot"
	walpkg "m31labs.dev/corkscrewdb/wal"
)

const (
	defaultBitWidth   = 2
	defaultWALMaxSize = 8 << 20
)

type dbConfig struct {
	provider       EmbeddingProvider
	providerSet    bool
	defaultBits    int
	walSegmentSize int64
	peers          []string
	shards         []ShardAssignment
	token          string
}

// Option configures Open.
type Option interface {
	applyDB(*dbConfig)
}

type dbOptionFunc func(*dbConfig)

func (f dbOptionFunc) applyDB(cfg *dbConfig) {
	f(cfg)
}

// WithProvider overrides the default embedded Eos text embedding provider.
func WithProvider(provider EmbeddingProvider) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.provider = provider
		cfg.providerSet = true
	})
}

// WithWALSegmentSize configures WAL segment rotation.
func WithWALSegmentSize(size int64) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.walSegmentSize = size
	})
}

// WithPeers records remote peer seed addresses for future federation/cluster use.
func WithPeers(peers ...string) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.peers = sanitizePeers(peers)
	})
}

// WithShards configures explicit shard ownership ranges for federation routing.
func WithShards(shards ...ShardAssignment) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.shards = cloneShardAssignments(shards)
	})
}

// WithToken configures the in-memory auth token used for future remote access.
func WithToken(token string) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.token = token
	})
}

// DB is an embedded CorkScrewDB instance.
type DB struct {
	path           string
	provider       EmbeddingProvider
	encoder        *encoder
	walSegmentSize int64
	peers          []string
	token          string
	remote         remoteClient
	serveAddr      string

	mu          sync.RWMutex
	manifest    manifest
	collections map[string]*Collection
	peerClients map[string]remoteClient
	streamer    *replica.Streamer
	closed      bool
}

type manifest struct {
	FormatVersion   int                       `json:"format_version"`
	FormatFloor     string                    `json:"format_floor"`
	ModuleVersion   string                    `json:"module_version"`
	ActorID         string                    `json:"actor_id"`
	DefaultBitWidth int                       `json:"default_bit_width"`
	Peers           []string                  `json:"peers,omitempty"`
	Shards          []ShardAssignment         `json:"shards,omitempty"`
	Embedding       embeddingConfig           `json:"embedding"`
	Collections     map[string]collectionMeta `json:"collections"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

type embeddingConfig struct {
	ID  string `json:"id"`
	Dim int    `json:"dim"`
}

type collectionMeta struct {
	BitWidth      int         `json:"bit_width"`
	Seed          int64       `json:"seed"`
	Dim           int         `json:"dim"`
	RawStore      bool        `json:"raw_store"`
	SparseEnabled bool        `json:"sparse_enabled"`
	IndexType     string      `json:"index_type"` // "flat" | "hnsw"
	HNSW          *HNSWParams `json:"hnsw,omitempty"`
}

type providerIdentifier interface {
	ProviderID() string
}

// Open creates or opens an embedded CorkScrewDB at path.
func Open(path string, opts ...Option) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("corkscrewdb: path is required")
	}
	cfg := dbConfig{
		defaultBits:    defaultBitWidth,
		walSegmentSize: defaultWALMaxSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyDB(&cfg)
		}
	}
	if !cfg.providerSet {
		cfg.provider = newDefaultProvider()
	}
	cleanPath := filepath.Clean(path)
	if err := ensureDBDir(cleanPath); err != nil {
		return nil, err
	}
	manifestData, err := loadOrCreateManifest(cleanPath, cfg.defaultBits)
	if err != nil {
		return nil, err
	}
	if err := applyRuntimeConfig(&manifestData, cfg.provider, cfg.peers, cfg.shards); err != nil {
		return nil, err
	}

	db := &DB{
		path:           cleanPath,
		provider:       cfg.provider,
		encoder:        newEncoder(cfg.provider),
		walSegmentSize: cfg.walSegmentSize,
		peers:          append([]string(nil), manifestData.Peers...),
		token:          cfg.token,
		manifest:       manifestData,
		collections:    make(map[string]*Collection),
		peerClients:    make(map[string]remoteClient),
		streamer:       replica.NewStreamer(),
	}
	if err := db.saveManifest(); err != nil {
		return nil, err
	}

	for name, meta := range manifestData.Collections {
		coll, err := db.loadCollection(name, meta)
		if err != nil {
			return nil, err
		}
		db.collections[name] = coll
	}
	return db, nil
}

// Collection opens or creates a named collection.
func (db *DB) Collection(name string, opts ...CollectionOption) *Collection {
	if db == nil {
		return &Collection{err: errors.New("corkscrewdb: nil database")}
	}
	if db.isClosed() {
		return &Collection{err: errors.New("corkscrewdb: database is closed"), db: db, encoder: db.encoder}
	}
	if !validCollectionName(name) {
		return &Collection{err: fmt.Errorf("corkscrewdb: invalid collection name %q", name), db: db, encoder: db.encoder}
	}
	if db.remote != nil {
		return db.remoteCollection(name, opts...)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if existing, ok := db.collections[name]; ok {
		var cfg collectionConfig
		for _, opt := range opts {
			if opt != nil {
				opt.applyCollection(&cfg)
			}
		}
		if cfg.seed < 0 {
			return &Collection{db: db, name: name, bitWidth: existing.bitWidth, seed: existing.seed, encoder: db.encoder, err: errors.New("corkscrewdb: quantizer seed must be >= 0")}
		}
		if cfg.bitWidth != 0 && cfg.bitWidth != existing.bitWidth {
			return &Collection{
				db:       db,
				name:     name,
				bitWidth: existing.bitWidth,
				seed:     existing.seed,
				encoder:  db.encoder,
				err:      fmt.Errorf("corkscrewdb: collection %q already exists with bit width %d", name, existing.bitWidth),
			}
		}
		if cfg.seed != 0 && cfg.seed != existing.seed {
			return &Collection{
				db:       db,
				name:     name,
				bitWidth: existing.bitWidth,
				seed:     existing.seed,
				encoder:  db.encoder,
				err:      fmt.Errorf("corkscrewdb: collection %q already exists with quantizer seed %d", name, existing.seed),
			}
		}
		if cfg.rawStoreSet && cfg.rawStore != existing.rawStoreEnabled {
			return &Collection{
				db:       db,
				name:     name,
				bitWidth: existing.bitWidth,
				seed:     existing.seed,
				encoder:  db.encoder,
				err:      fmt.Errorf("corkscrewdb: collection %q already exists with raw store %v", name, existing.rawStoreEnabled),
			}
		}
		return existing
	}

	cfg := defaultCollectionConfig(db.manifest.DefaultBitWidth)
	for _, opt := range opts {
		if opt != nil {
			opt.applyCollection(&cfg)
		}
	}
	if cfg.bitWidth < 2 {
		return &Collection{db: db, name: name, encoder: db.encoder, err: errors.New("corkscrewdb: bit width must be >= 2")}
	}
	if cfg.seed < 0 {
		return &Collection{db: db, name: name, encoder: db.encoder, err: errors.New("corkscrewdb: quantizer seed must be >= 0")}
	}

	meta := collectionMeta{
		BitWidth:      cfg.bitWidth,
		Seed:          cfg.seed,
		RawStore:      cfg.rawStore,
		SparseEnabled: cfg.sparseEnabled,
		IndexType:     indexTypeString(cfg.indexType),
	}
	if cfg.indexType == IndexHNSW {
		hnsw := cfg.hnsw
		meta.HNSW = &hnsw
	}
	coll, err := db.newCollection(name, meta)
	if err != nil {
		return &Collection{db: db, name: name, encoder: db.encoder, err: err}
	}
	coll.scorer = cfg.scorer // runtime-only injection (§10); not persisted in meta
	meta.Seed = coll.seed
	db.collections[name] = coll
	db.manifest.Collections[name] = meta
	if err := db.saveManifestLocked(); err != nil {
		coll.err = err
	}
	return coll
}

// DropCollection removes a named collection and all its data from disk.
func (db *DB) DropCollection(name string) error {
	if db == nil {
		return errors.New("corkscrewdb: nil database")
	}
	if db.isClosed() {
		return errors.New("corkscrewdb: database is closed")
	}
	if db.remote != nil {
		return db.remote.DropCollection(name)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	coll, ok := db.collections[name]
	if !ok {
		return nil
	}
	if err := coll.close(); err != nil {
		return err
	}
	delete(db.collections, name)
	delete(db.manifest.Collections, name)
	if err := db.saveManifestLocked(); err != nil {
		return err
	}
	return os.RemoveAll(db.collectionDir(name))
}

// Close flushes snapshots and closes all active WAL writers.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil
	}
	collections := make([]*Collection, 0, len(db.collections))
	for _, coll := range db.collections {
		collections = append(collections, coll)
	}
	remote := db.remote
	db.mu.RUnlock()

	var errs []error
	if remote != nil {
		if err := remote.Close(); err != nil {
			errs = append(errs, err)
		}
		db.mu.Lock()
		db.closed = true
		db.mu.Unlock()
		return errors.Join(errs...)
	}
	for _, coll := range collections {
		if err := coll.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := db.closePeerClients(); err != nil {
		errs = append(errs, err)
	}
	if err := db.encoder.Close(); err != nil {
		errs = append(errs, err)
	}
	db.mu.Lock()
	db.closed = true
	db.mu.Unlock()
	return errors.Join(errs...)
}

func (db *DB) saveManifest() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.saveManifestLocked()
}

// RemoteInfo fetches RPCInfoResponse over the wire when db is a remote
// client (returned by Connect). On an embedded DB it returns the
// manifest-level Embedding and peer metadata synthesized from local
// state. Callers use this to verify the server reports the expected
// embedding provider ID at startup so a misconfigured DB fails fast.
func (db *DB) RemoteInfo() (RPCInfoResponse, error) {
	if db == nil {
		return RPCInfoResponse{}, errors.New("corkscrewdb: nil database")
	}
	if db.remote != nil {
		return db.remote.Info()
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return RPCInfoResponse{
		PackageVersion: db.manifest.ModuleVersion,
		Embedding:      db.manifest.Embedding,
		Peers:          append([]string(nil), db.peers...),
		Collections:    db.rpcCollectionInfoLocked(),
		Shards:         cloneShardAssignments(db.manifest.Shards),
	}, nil
}

func (db *DB) rpcCollectionInfo() []RPCCollectionInfo {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.rpcCollectionInfoLocked()
}

// rpcCollectionInfoLocked returns the collection info slice without acquiring
// the db lock; callers must hold db.mu.RLock or db.mu.Lock.
func (db *DB) rpcCollectionInfoLocked() []RPCCollectionInfo {
	if len(db.manifest.Collections) == 0 {
		return nil
	}
	names := make([]string, 0, len(db.manifest.Collections))
	for name := range db.manifest.Collections {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]RPCCollectionInfo, len(names))
	for i, name := range names {
		out[i] = RPCCollectionInfo{
			Name:     name,
			BitWidth: db.manifest.Collections[name].BitWidth,
		}
	}
	return out
}

func (db *DB) isClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}

func ensureDBDir(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("corkscrewdb: path %q exists and is not a directory", path)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "collections"), 0o755); err != nil {
		return err
	}
	return nil
}

func loadOrCreateManifest(path string, defaultBits int) (manifest, error) {
	manifestPath := filepath.Join(path, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return manifest{}, err
		}
		now := time.Now().UTC()
		m := manifest{
			FormatVersion:   2,
			FormatFloor:     "v0.3.0",
			ModuleVersion:   PackageVersion,
			ActorID:         generateActorID(),
			DefaultBitWidth: defaultBits,
			Collections:     make(map[string]collectionMeta),
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		if err := writeManifest(manifestPath, m); err != nil {
			return manifest{}, err
		}
		return m, nil
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest{}, err
	}
	if m.FormatVersion < 2 {
		return manifest{}, fmt.Errorf("%w: manifest format version %d", ErrFormatTooOld, m.FormatVersion)
	}
	if m.Collections == nil {
		m.Collections = make(map[string]collectionMeta)
	}
	if m.DefaultBitWidth == 0 {
		m.DefaultBitWidth = defaultBits
	}
	return m, nil
}

func writeManifest(path string, data manifest) error {
	data.ModuleVersion = PackageVersion
	data.UpdatedAt = time.Now().UTC()
	buf, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(buf, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (db *DB) saveManifestLocked() error {
	return writeManifest(filepath.Join(db.path, "manifest.json"), db.manifest)
}

func (db *DB) persistCollectionMeta(name string, c *Collection) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	meta := db.manifest.Collections[name]
	meta.BitWidth = c.bitWidth
	meta.Dim = c.dim
	meta.Seed = c.seed
	meta.RawStore = c.rawStoreEnabled
	meta.SparseEnabled = c.sparseEnabled
	meta.IndexType = indexTypeString(c.indexType)
	if c.indexType == IndexHNSW {
		hnsw := c.hnsw
		meta.HNSW = &hnsw
	} else {
		meta.HNSW = nil
	}
	db.manifest.Collections[name] = meta
	return db.saveManifestLocked()
}

// cacheAdoptHook / cacheRebuildHook are test-only instrumentation: exactly one
// fires per HNSW reopen — adopt when a fresh cache is used, rebuild when the graph
// is reconstructed from history codes.
var (
	cacheAdoptHook   func()
	cacheRebuildHook func()
)

func (db *DB) loadCollection(name string, meta collectionMeta) (*Collection, error) {
	coll, err := db.newCollection(name, meta)
	if err != nil {
		return nil, err
	}
	// Defer the HNSW graph build so a fresh cache can be adopted without paying the
	// per-node insert cost (D6). Flat collections are unaffected.
	if coll.indexType == IndexHNSW {
		coll.deferHNSWBuild = true
	}

	snapshotPath, err := snap.FindLatestFile(db.collectionDir(name))
	if err != nil {
		return nil, err
	}
	var snapshotMax uint64
	if snapshotPath != "" {
		data, err := snap.LoadFile(snapshotPath)
		if err != nil {
			// Snapshot is corrupt — fall back to full WAL replay.
			snapshotPath = ""
		}
		if snapshotPath != "" {
			if data.BitWidth != 0 {
				coll.bitWidth = data.BitWidth
			}
			if data.Seed != 0 {
				coll.seed = data.Seed
			}
			if data.Dim != 0 {
				coll.dim = data.Dim
			}
			for _, record := range data.Records {
				for _, version := range record.Versions {
					if err := coll.loadVersion(record.ID, Version{
						Quantized:    fromSnapshotQuantized(version.Quantized),
						RawHash:      append([]byte(nil), version.RawHash...),
						Sparse:       fromSnapshotSparse(version.Sparse),
						Children:     fromSnapshotChildren(version.Children),
						dim:          data.Dim,
						Text:         version.Text,
						Metadata:     cloneMetadata(version.Metadata),
						LamportClock: version.LamportClock,
						ActorID:      version.ActorID,
						WallClock:    version.WallClock,
						Tombstone:    version.Tombstone,
					}); err != nil {
						return nil, err
					}
				}
			}
			snapshotMax = data.MaxLamport
		}
	}

	walBeyondSnapshot := false
	segments, err := walpkg.ListSegments(db.collectionWALDir(name))
	if err != nil {
		return nil, err
	}
	for _, segment := range segments {
		reader, err := walpkg.NewReader(segment)
		if err != nil {
			return nil, err
		}
		for reader.Next() {
			entry := reader.Entry()
			if entry.LamportClock <= snapshotMax {
				continue
			}
			walBeyondSnapshot = true
			if err := coll.loadVersion(entry.VectorID, Version{
				Quantized:    fromWALQuantized(entry.Quantized),
				RawHash:      append([]byte(nil), entry.RawHash...),
				Sparse:       fromWALSparse(entry.Sparse),
				Children:     fromWALChildren(entry.Children),
				dim:          entry.Dim,
				Text:         entry.Text,
				Metadata:     cloneMetadata(entry.Metadata),
				LamportClock: entry.LamportClock,
				ActorID:      entry.ActorID,
				WallClock:    entry.WallClock,
				Tombstone:    entry.Kind == walpkg.EntryTombstone,
			}); err != nil {
				_ = reader.Close()
				return nil, err
			}
		}
		if err := reader.Err(); err != nil {
			_ = reader.Close()
			return nil, err
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}
	// HNSW cache adoption / build (D6). The graph build was deferred; the live
	// search structure is currently a flat *index over the live codes. Adopt a fresh
	// cached graph when possible, else build the graph from the flat codes.
	if coll.indexType == IndexHNSW {
		coll.deferHNSWBuild = false
		if err := db.finishHNSWLoad(coll, snapshotMax, walBeyondSnapshot); err != nil {
			return nil, err
		}
	}

	coll.mu.Lock()
	coll.dirty = false
	coll.mu.Unlock()
	if coll.dim != meta.Dim || coll.bitWidth != meta.BitWidth || coll.seed != meta.Seed {
		if err := db.persistCollectionMeta(name, coll); err != nil {
			return nil, err
		}
	}
	return coll, nil
}

// finishHNSWLoad adopts a fresh graph.hnsw cache for an HNSW collection, or builds
// the graph from the flat live codes. Adoption is gated on: WAL empty beyond the
// snapshot, a sibling .tqi cache fresh at the snapshot lamport, a present
// graph.hnsw, and matching live node counts. Any miss falls back to a clean
// rebuild — the snapshot+WAL stay authoritative, so a stale/corrupt cache only
// costs a rebuild, never a wrong result.
func (db *DB) finishHNSWLoad(coll *Collection, snapshotMax uint64, walBeyondSnapshot bool) error {
	coll.mu.Lock()
	defer coll.mu.Unlock()

	flat, ok := coll.index.(*index)
	if !ok {
		// Nothing live (empty collection) or already a graph: build only if flat.
		if coll.index == nil {
			return nil
		}
		if _, isHW := coll.index.(*hnswIndex); isHW {
			return nil
		}
	}

	if flat != nil && !walBeyondSnapshot {
		if hw := db.tryAdoptFreshHNSW(coll, flat, snapshotMax); hw != nil {
			coll.index = hw
			if cacheAdoptHook != nil {
				cacheAdoptHook()
			}
			return nil
		}
	}

	// Rebuild the graph from the flat live codes (cache stale/absent/corrupt).
	if cacheRebuildHook != nil {
		cacheRebuildHook()
	}
	coll.rebuildHNSWFromFlatLocked(flat)
	return nil
}

// tryAdoptFreshHNSW returns the adopted cached graph when the .tqi sibling is fresh
// (lamport == snapshotMax) and node counts match; otherwise nil. The flat passed in
// (built during deferred load) provides the live codes the loaded graph wraps.
func (db *DB) tryAdoptFreshHNSW(coll *Collection, flat *index, snapshotMax uint64) *hnswIndex {
	hnswPath := db.collectionHNSWPath(coll.name)
	if _, err := os.Stat(hnswPath); err != nil {
		return nil // no graph cache
	}
	cachedFlat, idxLamport, err := db.tryLoadCollectionIndex(coll.name)
	if err != nil || cachedFlat == nil {
		return nil // no/corrupt .tqi -> graph cache is not fresh (option (i))
	}
	if idxLamport != snapshotMax {
		return nil // .tqi stale relative to the snapshot
	}
	if cachedFlat.Len() != flat.Len() {
		return nil // node-count mismatch
	}
	// Load the graph over the cached flat (which carries the persisted live codes,
	// ordered to match the saved layers). A CRC/version/count failure -> rebuild.
	hw, err := loadHNSWFile(hnswPath, cachedFlat, coll.hnswParamsOrDefault())
	if err != nil {
		return nil
	}
	return hw
}

// rebuildHNSWFromFlatLocked builds the HNSW graph from the flat index's live codes
// and installs it as the collection's index. Caller holds coll.mu.
func (c *Collection) rebuildHNSWFromFlatLocked(flat *index) {
	params := c.hnswParamsOrDefault()
	hw := newHNSWIndex(c.dim, c.bitWidth, c.seed, params)
	if flat != nil {
		for _, e := range flat.snapshotEntries() {
			if e.child {
				continue
			}
			hw.AddQuantized(e.id, e.qv, e.text, e.metadata, e.version)
		}
	}
	c.index = hw
}

func (c *Collection) hnswParamsOrDefault() HNSWParams {
	params := c.hnsw
	if params.M == 0 && params.EfConstruction == 0 && params.EfSearch == 0 {
		return defaultHNSWParams()
	}
	return params
}

func (db *DB) newCollection(name string, meta collectionMeta) (*Collection, error) {
	if meta.BitWidth == 0 {
		meta.BitWidth = db.manifest.DefaultBitWidth
	}
	if meta.Seed == 0 {
		meta.Seed = generateSeed()
	}
	if err := os.MkdirAll(db.collectionDir(name), 0o755); err != nil {
		return nil, err
	}
	manager, err := walpkg.NewManager(db.collectionWALDir(name), db.walSegmentSize)
	if err != nil {
		return nil, err
	}
	indexType := IndexFlat
	if meta.IndexType == "hnsw" {
		indexType = IndexHNSW
	}
	hnsw := defaultHNSWParams()
	if meta.HNSW != nil {
		hnsw = *meta.HNSW
	}
	coll := &Collection{
		db:              db,
		name:            name,
		bitWidth:        meta.BitWidth,
		seed:            meta.Seed,
		encoder:         db.encoder,
		rawStoreEnabled: meta.RawStore,
		sparseEnabled:   meta.SparseEnabled,
		indexType:       indexType,
		hnsw:            hnsw,
		history:         make(map[string][]Version),
		sparseSet:       make(map[string]*SparseVector),
		clock:           newHLC(db.manifest.ActorID),
		wal:             manager,
		dim:             meta.Dim,
		viewCache:       newViewLRU(viewCacheSize),
	}
	if meta.RawStore {
		store, err := rawstore.Open(db.rawStoreDir(name))
		if err != nil {
			_ = manager.Close()
			return nil, err
		}
		coll.rawStore = store
	}
	return coll, nil
}

func (db *DB) pruneWAL(name string) error {
	db.mu.RLock()
	coll := db.collections[name]
	db.mu.RUnlock()
	if coll == nil || coll.wal == nil {
		return nil
	}
	return coll.wal.PruneAndReset()
}

func (db *DB) pruneSnapshots(name, keep string) error {
	matches, err := filepath.Glob(filepath.Join(db.collectionDir(name), "snapshot-*.csdb"))
	if err != nil {
		return err
	}
	for _, match := range matches {
		if match == keep {
			continue
		}
		if err := os.Remove(match); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (db *DB) collectionDir(name string) string {
	return filepath.Join(db.path, "collections", name)
}

func (db *DB) collectionIndexDir(name string) string {
	return filepath.Join(db.collectionDir(name), "index")
}

func (db *DB) collectionIndexPath(name string) string {
	return filepath.Join(db.collectionIndexDir(name), "quantized.tqi")
}

func (db *DB) collectionHNSWPath(name string) string {
	return filepath.Join(db.collectionIndexDir(name), "graph.hnsw")
}

func (db *DB) collectionWALDir(name string) string {
	return filepath.Join(db.collectionDir(name), "wal")
}

func (db *DB) rawStoreDir(name string) string {
	return filepath.Join(db.collectionDir(name), "raw")
}

func validCollectionName(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.Contains(name, string(os.PathSeparator))
}

func applyRuntimeConfig(m *manifest, provider EmbeddingProvider, peers []string, shards []ShardAssignment) error {
	if m.Collections == nil {
		m.Collections = make(map[string]collectionMeta)
	}
	desired := describeEmbeddingProvider(provider)
	switch {
	case m.Embedding.ID == "":
		m.Embedding = desired
	case m.Embedding != desired:
		return fmt.Errorf("corkscrewdb: embedding config mismatch: manifest=%s/%d runtime=%s/%d", m.Embedding.ID, m.Embedding.Dim, desired.ID, desired.Dim)
	}
	if len(m.Shards) > 0 || len(shards) > 0 {
		activeShards := m.Shards
		if len(shards) > 0 {
			activeShards = shards
		}
		normalized, err := normalizeShardAssignments(activeShards)
		if err != nil {
			return err
		}
		m.Shards = normalized
	}
	activePeers := m.Peers
	if len(peers) > 0 {
		activePeers = peers
	}
	activePeers = sanitizePeers(append(append([]string(nil), activePeers...), peersFromShardAssignments(m.Shards)...))
	if len(activePeers) > 0 {
		m.Peers = activePeers
	}
	return nil
}

func describeEmbeddingProvider(provider EmbeddingProvider) embeddingConfig {
	if provider == nil {
		return embeddingConfig{ID: "none", Dim: 0}
	}
	if named, ok := provider.(providerIdentifier); ok {
		return embeddingConfig{ID: named.ProviderID(), Dim: provider.Dim()}
	}
	return embeddingConfig{ID: reflect.TypeOf(provider).String(), Dim: provider.Dim()}
}

func sanitizePeers(peers []string) []string {
	if len(peers) == 0 {
		return nil
	}
	out := make([]string, 0, len(peers))
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		out = append(out, peer)
	}
	return out
}

// reserve-only: wired in the Performance phase
func (db *DB) tryLoadCollectionIndex(name string) (*index, uint64, error) {
	path := db.collectionIndexPath(name)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	return loadIndexFile(path)
}
