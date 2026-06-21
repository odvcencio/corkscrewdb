package corkscrewdb

import (
	"sort"
	"time"

	"m31labs.dev/turboquant"
)

// Entry is the input payload for Put operations.
type Entry struct {
	Text     string
	Vector   []float32
	Metadata map[string]string
	Sparse   *SparseVector
}

// Version is one immutable version in an entry's history.
type Version struct {
	Quantized    *turboquant.IPQuantized // ALWAYS present for non-tombstone dense versions
	RawHash      []byte                  // blake3-256 ref into the raw store; nil iff WithoutRawStore or tombstone
	Sparse       *SparseVector           // optional lexical/learned-sparse channel
	Children     []ChildVector           // multivector children (quantized; ordinal-compactible)
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
	Tombstone    bool
	dim          int
}

// MultiVectorEntry is the input payload for a packed parent multivector write.
type MultiVectorEntry struct {
	Text     string
	Metadata map[string]string
	Children []MultiVectorChild
}

// MultiVectorChild is one compact child vector stored under a logical parent.
type MultiVectorChild struct {
	ID       string
	Vector   []float32
	Text     string
	Metadata map[string]string
}

// ChildVector is one immutable packed child in a parent version (quantized-only).
type ChildVector struct {
	ID        string
	Quantized *turboquant.IPQuantized
	Text      string
	Metadata  map[string]string
	dim       int
}

// SearchResult is one ranked similarity-search hit.
type SearchResult struct {
	ID       string
	Score    float32
	Text     string
	Metadata map[string]string
	Version  uint64
}

// ParentSearchResult is one parent result rolled up from the highest-scoring child.
type ParentSearchResult struct {
	ID            string
	Score         float32
	Text          string
	Metadata      map[string]string
	Version       uint64
	ChildID       string
	ChildScore    float32
	ChildText     string
	ChildMetadata map[string]string
}

func sortSearchResults(results []SearchResult) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
	})
}

// FilterOption restricts search results by exact metadata match.
type FilterOption struct {
	key   string
	value string
}

// Filter creates a metadata filter for search operations.
func Filter(key, value string) FilterOption {
	return FilterOption{key: key, value: value}
}

func (f FilterOption) matches(meta map[string]string) bool {
	if meta == nil {
		return false
	}
	return meta[f.key] == f.value
}

type putVectorConfig struct {
	text     string
	metadata map[string]string
}

// PutVectorOption configures PutVector calls.
type PutVectorOption interface {
	applyPutVector(*putVectorConfig)
}

type putVectorOptionFunc func(*putVectorConfig)

func (f putVectorOptionFunc) applyPutVector(cfg *putVectorConfig) {
	f(cfg)
}

// WithMetadata attaches metadata to PutVector.
func WithMetadata(meta map[string]string) PutVectorOption {
	return putVectorOptionFunc(func(cfg *putVectorConfig) {
		cfg.metadata = cloneMetadata(meta)
	})
}

// WithText stores source text alongside a PutVector write.
func WithText(text string) PutVectorOption {
	return putVectorOptionFunc(func(cfg *putVectorConfig) {
		cfg.text = text
	})
}

type parentSearchConfig struct {
	parentFilters  []FilterOption
	childFilters   []FilterOption
	childOverfetch int
}

// ParentSearchOption configures SearchParents and SearchParentsVector calls.
type ParentSearchOption interface {
	applyParentSearch(*parentSearchConfig)
}

type parentSearchOptionFunc func(*parentSearchConfig)

func (f parentSearchOptionFunc) applyParentSearch(cfg *parentSearchConfig) {
	f(cfg)
}

// WithParentFilters restricts parent results by exact parent metadata matches.
func WithParentFilters(filters ...FilterOption) ParentSearchOption {
	return parentSearchOptionFunc(func(cfg *parentSearchConfig) {
		cfg.parentFilters = append([]FilterOption(nil), filters...)
	})
}

// WithChildFilters restricts parent results by exact winning-child metadata matches.
func WithChildFilters(filters ...FilterOption) ParentSearchOption {
	return parentSearchOptionFunc(func(cfg *parentSearchConfig) {
		cfg.childFilters = append([]FilterOption(nil), filters...)
	})
}

// WithChildOverfetch reserves future approximate-index child overfetch control.
// Local flat v1 parent search is exact and rejects positive overfetch values.
func WithChildOverfetch(n int) ParentSearchOption {
	return parentSearchOptionFunc(func(cfg *parentSearchConfig) {
		cfg.childOverfetch = n
	})
}

// IndexType selects the vector index algorithm.
type IndexType int

const (
	// IndexFlat is a brute-force flat scan (default).
	IndexFlat IndexType = iota
	// IndexHNSW is a Hierarchical Navigable Small World graph index.
	IndexHNSW
)

type collectionConfig struct {
	bitWidth      int
	seed          int64
	indexType     IndexType
	indexTypeSet  bool // true iff WithIndexType was supplied (IndexFlat is the zero value)
	hnsw          HNSWParams
	hnswSet       bool // true iff WithHNSWParams was supplied
	rawStore      bool // default true; cleared by WithoutRawStore
	rawStoreSet   bool
	sparseEnabled bool
	scorer        Scorer // optional injected scorer; flat path uses defaultScorer when nil
}

// HNSWParams is the persisted HNSW configuration.
type HNSWParams struct {
	M              int // max connections per layer (default 16)
	EfConstruction int // build-time beam width (default 200)
	EfSearch       int // query-time beam width (default 50)
}

func defaultCollectionConfig(bitWidth int) collectionConfig {
	return collectionConfig{bitWidth: bitWidth, rawStore: true}
}

// CollectionOption configures Collection creation.
type CollectionOption interface {
	applyCollection(*collectionConfig)
}

type collectionOptionFunc func(*collectionConfig)

func (f collectionOptionFunc) applyCollection(cfg *collectionConfig) {
	f(cfg)
}

// WithBitWidth sets the collection quantization bit width.
func WithBitWidth(bits int) CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) {
		cfg.bitWidth = bits
	})
}

// WithQuantizerSeed sets the collection quantizer seed for reproducible indexes.
func WithQuantizerSeed(seed int64) CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) {
		cfg.seed = seed
	})
}

// WithIndexType selects the vector index algorithm.
func WithIndexType(t IndexType) CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) {
		cfg.indexType = t
		cfg.indexTypeSet = true
	})
}

// WithHNSWParams configures and persists HNSW-specific parameters.
func WithHNSWParams(p HNSWParams) CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) {
		cfg.hnsw = p
		cfg.hnswSet = true
	})
}

// WithoutRawStore opts out of the default content-addressed raw vector store.
func WithoutRawStore() CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) {
		cfg.rawStore = false
		cfg.rawStoreSet = true
	})
}

// WithSparse enables the sparse channel for this collection.
func WithSparse() CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) { cfg.sparseEnabled = true })
}

// WithScorer injects a custom Scorer into the collection (frozen API seam).
// The injected scorer is active on the flat (non-HNSW) search path: it is
// called for unfiltered queries and bypassed in favour of the default scorer
// when filters are present (the CUDA kernel scores the whole corpus; per-row
// accept cannot be pushed down). The scorer is runtime-only and not persisted.
func WithScorer(s Scorer) CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) { cfg.scorer = s })
}

func cloneMetadata(meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return nil
	}
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out
}

func cloneVector(vec []float32) []float32 {
	if len(vec) == 0 {
		return nil
	}
	out := make([]float32, len(vec))
	copy(out, vec)
	return out
}

func cloneVersion(v Version) Version {
	var qv *turboquant.IPQuantized
	if v.Quantized != nil {
		cloned := cloneQuantized(*v.Quantized)
		qv = &cloned
	}
	return Version{
		Quantized:    qv,
		RawHash:      append([]byte(nil), v.RawHash...),
		Sparse:       cloneSparseVector(v.Sparse),
		Text:         v.Text,
		Metadata:     cloneMetadata(v.Metadata),
		Children:     cloneChildVectors(v.Children),
		LamportClock: v.LamportClock,
		ActorID:      v.ActorID,
		WallClock:    v.WallClock,
		Tombstone:    v.Tombstone,
		dim:          v.dim,
	}
}

func cloneChildVectors(children []ChildVector) []ChildVector {
	if len(children) == 0 {
		return nil
	}
	out := make([]ChildVector, len(children))
	for i, child := range children {
		var qv *turboquant.IPQuantized
		if child.Quantized != nil {
			cloned := cloneQuantized(*child.Quantized)
			qv = &cloned
		}
		out[i] = ChildVector{
			ID:        child.ID,
			Quantized: qv,
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
			dim:       child.dim,
		}
	}
	return out
}

func matchesFilters(meta map[string]string, filters []FilterOption) bool {
	for _, f := range filters {
		if !f.matches(meta) {
			return false
		}
	}
	return true
}

func collectPutVectorOptions(opts []PutVectorOption) putVectorConfig {
	var cfg putVectorConfig
	for _, opt := range opts {
		if opt != nil {
			opt.applyPutVector(&cfg)
		}
	}
	return cfg
}

func collectParentSearchOptions(opts []ParentSearchOption) parentSearchConfig {
	var cfg parentSearchConfig
	for _, opt := range opts {
		if opt != nil {
			opt.applyParentSearch(&cfg)
		}
	}
	return cfg
}
