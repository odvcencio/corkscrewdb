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
}

// Version is one immutable version in an entry's history.
type Version struct {
	Embedding    []float32
	Text         string
	Metadata     map[string]string
	Children     []MultiVectorChildVersion
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
	Tombstone    bool
	quantized    *turboquant.IPQuantized
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

// MultiVectorChildVersion is one immutable packed child in a parent version.
type MultiVectorChildVersion struct {
	ID        string
	Embedding []float32
	Text      string
	Metadata  map[string]string
	quantized *turboquant.IPQuantized
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

// VectorStorageMode controls how vector payloads are stored durably.
type VectorStorageMode string

const (
	// VectorStorageRaw stores raw float embeddings in WAL and snapshots.
	VectorStorageRaw VectorStorageMode = "raw"
	// VectorStorageQuantizedOnly stores only TurboQuant payloads in WAL and snapshots.
	VectorStorageQuantizedOnly VectorStorageMode = "quantized_only"
)

type collectionConfig struct {
	bitWidth        int
	seed            int64
	indexType       IndexType
	hnswM           int
	hnswEfConstruct int
	hnswEfSearch    int
	vectorStorage   VectorStorageMode
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
	})
}

// WithHNSWParams configures HNSW-specific parameters.
func WithHNSWParams(m, efConstruction, efSearch int) CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) {
		cfg.hnswM = m
		cfg.hnswEfConstruct = efConstruction
		cfg.hnswEfSearch = efSearch
	})
}

// WithVectorStorage selects how vectors are persisted for a collection.
func WithVectorStorage(mode VectorStorageMode) CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) {
		cfg.vectorStorage = mode
	})
}

// WithQuantizedOnlyPersistence stores only quantized vector payloads durably.
func WithQuantizedOnlyPersistence() CollectionOption {
	return WithVectorStorage(VectorStorageQuantizedOnly)
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
	if v.quantized != nil {
		cloned := cloneQuantized(*v.quantized)
		qv = &cloned
	}
	return Version{
		Embedding:    cloneVector(v.Embedding),
		Text:         v.Text,
		Metadata:     cloneMetadata(v.Metadata),
		Children:     cloneMultiVectorChildVersions(v.Children),
		LamportClock: v.LamportClock,
		ActorID:      v.ActorID,
		WallClock:    v.WallClock,
		Tombstone:    v.Tombstone,
		quantized:    qv,
		dim:          v.dim,
	}
}

func cloneMultiVectorChildVersions(children []MultiVectorChildVersion) []MultiVectorChildVersion {
	if len(children) == 0 {
		return nil
	}
	out := make([]MultiVectorChildVersion, len(children))
	for i, child := range children {
		var qv *turboquant.IPQuantized
		if child.quantized != nil {
			cloned := cloneQuantized(*child.quantized)
			qv = &cloned
		}
		out[i] = MultiVectorChildVersion{
			ID:        child.ID,
			Embedding: cloneVector(child.Embedding),
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
			quantized: qv,
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
