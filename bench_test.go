package corkscrewdb

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

const (
	benchDim  = 384
	benchBits = 2
	benchSeed = int64(12345)
)

func benchRandVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

func benchOpenDB(b *testing.B) *DB {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bench.csdb")
	db, err := Open(path, WithProvider(&mockProvider{dim: benchDim}))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func benchPopulate(b *testing.B, db *DB, n int) *Collection {
	b.Helper()
	rng := rand.New(rand.NewSource(benchSeed))
	coll := db.Collection("bench", WithBitWidth(benchBits))
	for i := 0; i < n; i++ {
		vec := benchRandVec(rng, benchDim)
		if err := coll.PutVector(fmt.Sprintf("v%d", i), vec); err != nil {
			b.Fatal(err)
		}
	}
	return coll
}

func BenchmarkPut(b *testing.B) {
	db := benchOpenDB(b)
	coll := db.Collection("bench", WithBitWidth(benchBits))
	rng := rand.New(rand.NewSource(benchSeed))
	vecs := make([][]float32, b.N)
	for i := range vecs {
		vecs[i] = benchRandVec(rng, benchDim)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := coll.PutVector(fmt.Sprintf("v%d", i), vecs[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutText(b *testing.B) {
	db := benchOpenDB(b)
	coll := db.Collection("bench", WithBitWidth(benchBits))
	texts := []string{
		"the auth module uses webauthn passkeys",
		"database migrations are append only",
		"vector quantization reduces memory usage",
		"distributed systems need consensus",
		"the corkscrew rotation decorrelates coordinates",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		text := texts[i%len(texts)]
		if err := coll.Put(fmt.Sprintf("doc-%d", i), Entry{Text: text}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchSearch(b *testing.B, n int) {
	db := benchOpenDB(b)
	coll := benchPopulate(b, db, n)
	rng := rand.New(rand.NewSource(benchSeed + 1))
	query := benchRandVec(rng, benchDim)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := coll.SearchVector(query, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSearch1K(b *testing.B)   { benchSearch(b, 1_000) }
func BenchmarkSearch10K(b *testing.B)  { benchSearch(b, 10_000) }
func BenchmarkSearch100K(b *testing.B) { benchSearch(b, 100_000) }

func BenchmarkSearchParallel10K(b *testing.B) {
	db := benchOpenDB(b)
	coll := benchPopulate(b, db, 10_000)
	rng := rand.New(rand.NewSource(benchSeed + 1))
	query := benchRandVec(rng, benchDim)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := coll.SearchVector(query, 10); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSearchWithFilter10K(b *testing.B) {
	db := benchOpenDB(b)
	rng := rand.New(rand.NewSource(benchSeed))
	coll := db.Collection("bench", WithBitWidth(benchBits))
	sources := []string{"review", "design", "code", "test"}
	for i := 0; i < 10_000; i++ {
		vec := benchRandVec(rng, benchDim)
		meta := map[string]string{"source": sources[i%len(sources)]}
		if err := coll.PutVector(fmt.Sprintf("v%d", i), vec, WithMetadata(meta)); err != nil {
			b.Fatal(err)
		}
	}
	query := benchRandVec(rand.New(rand.NewSource(benchSeed+1)), benchDim)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := coll.SearchVector(query, 10, Filter("source", "review")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHistory(b *testing.B) {
	db := benchOpenDB(b)
	coll := db.Collection("bench", WithBitWidth(benchBits))
	rng := rand.New(rand.NewSource(benchSeed))
	for i := 0; i < 100; i++ {
		if err := coll.PutVector("target", benchRandVec(rng, benchDim)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := coll.History("target"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenClose(b *testing.B) {
	// Populate a database, then benchmark Open + Close (snapshot + WAL recovery).
	path := filepath.Join(b.TempDir(), "lifecycle.csdb")
	provider := &mockProvider{dim: benchDim}
	db, err := Open(path, WithProvider(provider))
	if err != nil {
		b.Fatal(err)
	}
	rng := rand.New(rand.NewSource(benchSeed))
	coll := db.Collection("bench", WithBitWidth(benchBits))
	for i := 0; i < 1000; i++ {
		if err := coll.PutVector(fmt.Sprintf("v%d", i), benchRandVec(rng, benchDim)); err != nil {
			b.Fatal(err)
		}
	}
	db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := Open(path, WithProvider(provider))
		if err != nil {
			b.Fatal(err)
		}
		db.Close()
	}
}
