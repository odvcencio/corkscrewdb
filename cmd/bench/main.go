// Command bench is a reproducible ANN benchmark for CorkScrewDB over a standard
// ann-benchmarks dataset. It reports recall@10 vs the dataset's exact
// ground-truth neighbors, query throughput (serial + parallel), index build
// time, and resident bytes/vector — for both the flat quantized index and HNSW.
//
// It is a standalone main (NOT part of `go test ./...`) so it never slows CI.
//
// Dataset prep (one-time, offline; the Go harness has no python/HDF5 dep):
//
//	cmd/bench/fetch_dataset.sh glove-25-angular
//
// Run (full base — the dataset's provided ground-truth applies verbatim):
//
//	go run ./cmd/bench -data testdata/ann/glove-25-angular.csb
//
// Flags let you cap the base/query count for a quick smoke run, choose bit
// widths, and tune HNSW.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corkscrew "m31labs.dev/corkscrewdb"
)

const csbMagic = "CSBENCH1"

// dataset is a loaded .csb ann-benchmarks dataset.
type dataset struct {
	distance int // 0 = inner-product / angular-normalized, 1 = euclidean
	dim      int
	base     [][]float32 // nbase x dim
	query    [][]float32 // nquery x dim
	gt       [][]int32   // nquery x gtk (indices into base)
}

func main() {
	var (
		dataPath   = flag.String("data", "testdata/ann/glove-25-angular.csb", "path to .csb dataset")
		bitsCSV    = flag.String("bits", "2", "comma-separated quantizer bit widths to bench")
		seed       = flag.Int64("seed", 12345, "quantizer seed (reproducibility)")
		maxBase    = flag.Int("maxbase", 0, "cap base vectors (0 = all; recomputes ground-truth if < full)")
		maxQuery   = flag.Int("maxquery", 1000, "cap query vectors (0 = all)")
		k          = flag.Int("k", 10, "recall@k")
		parWorker  = flag.Int("parallel", runtime.NumCPU(), "parallel query workers for the parallel QPS measure")
		efSearch   = flag.Int("efsearch", 100, "HNSW efSearch at query time")
		efConst    = flag.Int("efconstruction", 200, "HNSW efConstruction at build time")
		hnswM      = flag.Int("m", 16, "HNSW M (max neighbors per layer)")
		skipHNSW   = flag.Bool("skiphnsw", false, "skip the HNSW pass (flat only)")
		useRebuild = flag.Bool("rebuild", false, "build HNSW via RebuildIndex (default params M=16/efC=200/efS=50) instead of an HNSW-typed collection")
	)
	flag.Parse()

	bits, err := parseBits(*bitsCSV)
	if err != nil {
		fatal(err)
	}

	ds, err := loadCSB(*dataPath)
	if err != nil {
		fatal(fmt.Errorf("load dataset: %w (run cmd/bench/fetch_dataset.sh first)", err))
	}

	if *maxQuery > 0 && *maxQuery < len(ds.query) {
		ds.query = ds.query[:*maxQuery]
		ds.gt = ds.gt[:*maxQuery]
	}

	subset := false
	if *maxBase > 0 && *maxBase < len(ds.base) {
		// Subset of the base: the provided ground-truth was computed over the FULL
		// base and is no longer valid, so recompute exact neighbors over the subset.
		// Copy into a fresh slice so the full 1.18M backing array is freed and does
		// not pollute query-time GC pressure / the resident measure.
		sub := make([][]float32, *maxBase)
		copy(sub, ds.base[:*maxBase])
		ds.base = sub
		fmt.Fprintf(os.Stderr, "subset base=%d: recomputing exact ground-truth (brute force)...\n", len(ds.base))
		ds.gt = recomputeGroundTruth(ds, *k)
		runtime.GC()
		subset = true
	}

	printHeader(ds, *dataPath, subset, *seed, *k)

	for _, b := range bits {
		fmt.Printf("\n=== bit width %d ===\n", b)
		// Flat pass.
		runPass(ds, b, *seed, *k, *parWorker, corkscrew.IndexFlat, corkscrew.HNSWParams{}, false)
		if *skipHNSW {
			continue
		}
		// HNSW pass.
		runPass(ds, b, *seed, *k, *parWorker, corkscrew.IndexHNSW, corkscrew.HNSWParams{
			M: *hnswM, EfConstruction: *efConst, EfSearch: *efSearch,
		}, *useRebuild)
	}
}

// runPass builds one index (flat or HNSW) at a bit width and prints its row.
//
// Flat: insert all base vectors into a default (flat) collection; build time is
// the insert loop. HNSW: create an HNSW-typed collection carrying the requested
// params and insert all base vectors — the graph is built incrementally during
// insertion, so build time is the honest end-to-end graph construction cost.
//
// NOTE on RebuildIndex: the task mentions building HNSW "via RebuildIndex".
// RebuildIndex(IndexHNSW) works, but on a collection created flat it always uses
// the DEFAULT HNSW params (M=16/efC=200/efS=50) — WithHNSWParams is only honored
// when the collection is created with IndexHNSW. Since efSearch materially drives
// recall, we create the HNSW collection up front so the -efsearch flag is real.
// (-rebuild forces the RebuildIndex path at default params for comparison.)
func runPass(ds *dataset, bits int, seed int64, k, parWorkers int, idxType corkscrew.IndexType, hp corkscrew.HNSWParams, useRebuild bool) {
	tmp, err := os.MkdirTemp("", "csbench-*")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(tmp)

	db, err := corkscrew.Open(tmp, corkscrew.WithProvider(&fixedDimProvider{dim: ds.dim}))
	if err != nil {
		fatal(err)
	}

	opts := []corkscrew.CollectionOption{
		corkscrew.WithBitWidth(bits),
		corkscrew.WithQuantizerSeed(seed),
		corkscrew.WithoutRawStore(), // quantized-only: resident/disk reflect the headline form
	}
	if idxType == corkscrew.IndexHNSW && !useRebuild {
		opts = append(opts, corkscrew.WithIndexType(corkscrew.IndexHNSW), corkscrew.WithHNSWParams(hp))
	}
	coll := db.Collection("ann", opts...)

	// ---- Build: insert all base vectors ----
	runtime.GC()
	heapBefore := heapAlloc()
	buildStart := time.Now()
	for i, v := range ds.base {
		if err := coll.PutVector(strconv.Itoa(i), v); err != nil {
			fatal(fmt.Errorf("put %d: %w", i, err))
		}
	}
	buildDur := time.Since(buildStart)

	label := "flat"
	if idxType == corkscrew.IndexHNSW {
		if useRebuild {
			// Convert flat -> HNSW via RebuildIndex (default params).
			rebuildStart := time.Now()
			if err := coll.RebuildIndex(corkscrew.IndexHNSW); err != nil {
				fatal(fmt.Errorf("RebuildIndex(HNSW): %w", err))
			}
			buildDur += time.Since(rebuildStart)
			label = "hnsw(rebuild,defaults M=16,efC=200,efS=50)"
		} else {
			label = fmt.Sprintf("hnsw(M=%d,efC=%d,efS=%d)", hp.M, hp.EfConstruction, hp.EfSearch)
		}
	}

	runtime.GC()
	heapAfter := heapAlloc()

	recall := measureRecall(coll, ds, k)
	qps := measureSerialQPS(coll, ds, k)
	par := measureParallelQPS(coll, ds, k, parWorkers)

	n := len(ds.base)
	// Resident bytes/vector: live Go heap attributable to the built index
	// (heap-after-build minus the pre-build baseline), divided by vector count.
	// Measured via runtime.MemStats.HeapAlloc after a forced GC so freed query
	// scratch and the previous pass do not leak into the number. Clamp to >=0.
	var residentBV float64
	if heapAfter > heapBefore {
		residentBV = float64(heapAfter-heapBefore) / float64(n)
	}
	codeBV := packedCodeBytes(ds.dim, bits)

	// Disk bytes/vector: closing the DB flushes a snapshot; size it then.
	if err := db.Close(); err != nil {
		fatal(fmt.Errorf("close: %w", err))
	}
	diskBV := snapshotBytesPerVector(filepath.Join(tmp, "collections", "ann"), n)

	printRow(label, recall, qps, par, buildDur, residentBV, codeBV, diskBV, k)
}

// measureRecall returns mean recall@k over all queries vs ground-truth.
func measureRecall(coll *corkscrew.Collection, ds *dataset, k int) float64 {
	var total float64
	for qi, q := range ds.query {
		res, err := coll.SearchVector(q, k)
		if err != nil {
			fatal(err)
		}
		got := make(map[int]struct{}, len(res))
		for _, r := range res {
			if id, err := strconv.Atoi(r.ID); err == nil {
				got[id] = struct{}{}
			}
		}
		truth := ds.gt[qi]
		limit := k
		if limit > len(truth) {
			limit = len(truth)
		}
		hit := 0
		for i := 0; i < limit; i++ {
			if _, ok := got[int(truth[i])]; ok {
				hit++
			}
		}
		total += float64(hit) / float64(limit)
	}
	return total / float64(len(ds.query))
}

func measureSerialQPS(coll *corkscrew.Collection, ds *dataset, k int) float64 {
	// Warm up on a few queries so the first measured query does not eat
	// one-time lazy init / page-in cost.
	warm := 16
	if warm > len(ds.query) {
		warm = len(ds.query)
	}
	for i := 0; i < warm; i++ {
		if _, err := coll.SearchVector(ds.query[i], k); err != nil {
			fatal(err)
		}
	}
	start := time.Now()
	for _, q := range ds.query {
		if _, err := coll.SearchVector(q, k); err != nil {
			fatal(err)
		}
	}
	return float64(len(ds.query)) / time.Since(start).Seconds()
}

func measureParallelQPS(coll *corkscrew.Collection, ds *dataset, k, workers int) float64 {
	if workers < 1 {
		workers = 1
	}
	var idx int64 = -1
	var done int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&idx, 1)
				if int(i) >= len(ds.query) {
					return
				}
				if _, err := coll.SearchVector(ds.query[i], k); err != nil {
					fatal(err)
				}
				atomic.AddInt64(&done, 1)
			}
		}()
	}
	wg.Wait()
	return float64(done) / time.Since(start).Seconds()
}

// recomputeGroundTruth computes exact top-k neighbors over a (subset) base using
// the dataset metric. Used only when -maxbase subsets the base.
func recomputeGroundTruth(ds *dataset, k int) [][]int32 {
	out := make([][]int32, len(ds.query))
	type scored struct {
		idx   int
		score float64
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.NumCPU())
	for qi := range ds.query {
		wg.Add(1)
		sem <- struct{}{}
		go func(qi int) {
			defer wg.Done()
			defer func() { <-sem }()
			q := ds.query[qi]
			hits := make([]scored, len(ds.base))
			for bi, b := range ds.base {
				hits[bi] = scored{idx: bi, score: similarity(ds.distance, q, b)}
			}
			sort.Slice(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
			n := k
			if n > len(hits) {
				n = len(hits)
			}
			row := make([]int32, n)
			for i := 0; i < n; i++ {
				row[i] = int32(hits[i].idx)
			}
			out[qi] = row
		}(qi)
	}
	wg.Wait()
	return out
}

// similarity returns a higher-is-closer score for the dataset metric.
func similarity(distance int, a, b []float32) float64 {
	if distance == 1 { // euclidean: negate squared L2 so larger = closer
		var s float64
		for i := range a {
			d := float64(a[i] - b[i])
			s += d * d
		}
		return -s
	}
	var s float64 // inner product (== cosine on normalized angular vectors)
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

// ---- storage measurement ----

func packedCodeBytes(dim, bits int) float64 {
	mse := (dim*(bits-1) + 7) / 8 // (b-1) bits/coord
	signs := (dim + 7) / 8        // 1 bit/coord
	resNorm := 4                  // float32
	return float64(mse + signs + resNorm)
}

func heapAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// snapshotBytesPerVector sizes the largest snapshot file in collDir / vector count.
func snapshotBytesPerVector(collDir string, n int) float64 {
	if n == 0 {
		return 0
	}
	var best int64
	entries, err := os.ReadDir(collDir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".csdb") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.Size() > best {
			best = info.Size()
		}
	}
	return float64(best) / float64(n)
}

// ---- dataset loading ----

func loadCSB(path string) (*dataset, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	magic := make([]byte, 8)
	if _, err := io.ReadFull(f, magic); err != nil {
		return nil, err
	}
	if string(magic) != csbMagic {
		return nil, fmt.Errorf("bad magic %q", magic)
	}
	hdr := make([]byte, 5*4)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return nil, err
	}
	distance := int(int32(binary.LittleEndian.Uint32(hdr[0:])))
	dim := int(int32(binary.LittleEndian.Uint32(hdr[4:])))
	nbase := int(int32(binary.LittleEndian.Uint32(hdr[8:])))
	nquery := int(int32(binary.LittleEndian.Uint32(hdr[12:])))
	gtk := int(int32(binary.LittleEndian.Uint32(hdr[16:])))

	ds := &dataset{distance: distance, dim: dim}
	ds.base, err = readFloatMatrix(f, nbase, dim)
	if err != nil {
		return nil, fmt.Errorf("base: %w", err)
	}
	ds.query, err = readFloatMatrix(f, nquery, dim)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	ds.gt, err = readInt32Matrix(f, nquery, gtk)
	if err != nil {
		return nil, fmt.Errorf("gt: %w", err)
	}
	return ds, nil
}

func readFloatMatrix(r io.Reader, rows, cols int) ([][]float32, error) {
	buf := make([]byte, rows*cols*4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	out := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		row := make([]float32, cols)
		base := i * cols * 4
		for j := 0; j < cols; j++ {
			row[j] = math.Float32frombits(binary.LittleEndian.Uint32(buf[base+j*4:]))
		}
		out[i] = row
	}
	return out, nil
}

func readInt32Matrix(r io.Reader, rows, cols int) ([][]int32, error) {
	buf := make([]byte, rows*cols*4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	out := make([][]int32, rows)
	for i := 0; i < rows; i++ {
		row := make([]int32, cols)
		base := i * cols * 4
		for j := 0; j < cols; j++ {
			row[j] = int32(binary.LittleEndian.Uint32(buf[base+j*4:]))
		}
		out[i] = row
	}
	return out, nil
}

// ---- output ----

func printHeader(ds *dataset, path string, subset bool, seed int64, k int) {
	metric := "angular/cosine (inner product on L2-normalized vectors)"
	if ds.distance == 1 {
		metric = "euclidean (-||a-b||^2)"
	}
	gtStr := "dataset-provided exact neighbors (full base)"
	if subset {
		gtStr = "recomputed exact (base subset, brute force)"
	}
	fmt.Printf("CorkScrewDB ANN benchmark\n")
	fmt.Printf("  dataset:   %s\n", path)
	fmt.Printf("  vectors:   base=%d query=%d dim=%d gtk=%d\n", len(ds.base), len(ds.query), ds.dim, len(ds.gt[0]))
	fmt.Printf("  metric:    %s\n", metric)
	fmt.Printf("  groundtr.: %s\n", gtStr)
	fmt.Printf("  seed:      %d   recall@%d\n", seed, k)
	fmt.Printf("  cpu:       %s (%d threads)\n", cpuModel(), runtime.NumCPU())
	fmt.Printf("  go:        %s\n", runtime.Version())
}

func printRow(name string, recall, qps, par float64, build time.Duration, residentBV, codeBV, diskBV float64, k int) {
	fmt.Printf("%-26s recall@%d=%.4f  qps_serial=%.0f  qps_parallel=%.0f  build=%s  resident=%.1f B/vec  code=%.1f B/vec  disk=%.1f B/vec\n",
		name, k, recall, qps, par, build.Round(time.Millisecond), residentBV, codeBV, diskBV)
}

func parseBits(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		b, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("bad bit width %q", p)
		}
		out = append(out, b)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no bit widths")
	}
	return out, nil
}

func cpuModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return runtime.GOARCH
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "model name") {
			if i := strings.Index(line, ":"); i >= 0 {
				return strings.TrimSpace(line[i+1:])
			}
		}
	}
	return runtime.GOARCH
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "bench:", err)
	os.Exit(1)
}

// fixedDimProvider is a stub EmbeddingProvider whose only role is to satisfy the
// DB provider-dim contract. The bench never calls text Search, only SearchVector
// with raw float vectors, so Encode is unused for queries.
type fixedDimProvider struct{ dim int }

func (p *fixedDimProvider) Encode(string) ([]float32, error) { return make([]float32, p.dim), nil }
func (p *fixedDimProvider) EncodeBatch(texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = make([]float32, p.dim)
	}
	return out, nil
}
func (p *fixedDimProvider) Dim() int           { return p.dim }
func (p *fixedDimProvider) Close() error       { return nil }
func (p *fixedDimProvider) ProviderID() string { return "bench-fixed-dim" }
