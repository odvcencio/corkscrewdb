package corkscrewdb

import (
	"path/filepath"
	"testing"

	"github.com/odvcencio/barracuda/artifact/barr"
	"github.com/odvcencio/barracuda/compiler"
	mantaruntime "github.com/odvcencio/barracuda/runtime"
	"github.com/odvcencio/barracuda/runtime/backend"
)

func TestLoadMantaProviderEncodes(t *testing.T) {
	path := writeTinyMantaProviderPackage(t)
	provider, err := LoadMantaProvider(path)
	if err != nil {
		t.Fatalf("load Manta provider: %v", err)
	}
	defer provider.Close()

	named, ok := provider.(interface{ ProviderID() string })
	if !ok {
		t.Fatal("expected Manta provider to expose ProviderID")
	}
	if got := named.ProviderID(); got != "tiny-manta-provider" {
		t.Fatalf("provider id = %q, want %q", got, "tiny-manta-provider")
	}
	if got := provider.Dim(); got != 3 {
		t.Fatalf("provider dim = %d, want 3", got)
	}

	vec, err := provider.Encode("hello world")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(vec) != 3 {
		t.Fatalf("embedding len = %d, want 3", len(vec))
	}
	var nonZero bool
	for _, value := range vec {
		if value != 0 {
			nonZero = true
			break
		}
	}
	if !nonZero {
		t.Fatal("expected non-zero Manta embedding")
	}

	batch, err := provider.EncodeBatch([]string{"hello world", "", "hello friend"})
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	for i, row := range batch {
		if len(row) != 3 {
			t.Fatalf("batch row %d len = %d, want 3", i, len(row))
		}
	}
	for _, value := range batch[1] {
		if value != 0 {
			t.Fatalf("expected blank-text embedding to stay zero, got %v", batch[1])
		}
	}
}

func TestOpenDefaultsToMantaProvider(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if db.manifest.Embedding.ID != "manta-embed-v0" {
		t.Fatalf("default embedding provider id = %q, want %q", db.manifest.Embedding.ID, "manta-embed-v0")
	}
	if db.manifest.Embedding.Dim <= 0 {
		t.Fatalf("default embedding dim = %d, want > 0", db.manifest.Embedding.Dim)
	}
}

func writeTinyMantaProviderPackage(t *testing.T) string {
	t.Helper()
	source := []byte(`
param token_embedding: q8[V, D] @weight("weights/token_embedding")
param projection: q8[D, E] @weight("weights/projection")

pipeline embed_pooled(tokens: i32[T]) -> f16[E] {
    let hidden_q = gather(token_embedding, tokens)
    let hidden = dequant(hidden_q)
    let projection_f = dequant(projection)
    let projected = @matmul(hidden, projection_f)
    return mean_pool(projected)
}

pipeline embed_pooled_batch(tokens: i32[B, T]) -> f16[B, E] {
    let hidden_q = gather(token_embedding, tokens)
    let hidden = dequant(hidden_q)
    let projection_f = dequant(projection)
    let projected = @matmul(hidden, projection_f)
    return mean_pool(projected)
}
`)
	bundle, err := compiler.Build(source, compiler.Options{ModuleName: "tiny_manta_provider"})
	if err != nil {
		t.Fatalf("build provider module: %v", err)
	}
	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "tiny_manta_provider.mll")
	if err := barr.WriteFile(artifactPath, bundle.Artifact); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	manifest := mantaruntime.EmbeddingManifest{
		Name:                "tiny-manta-provider",
		PooledEntry:         "embed_pooled",
		BatchEntry:          "embed_pooled_batch",
		TokenInput:          "tokens",
		OutputName:          "result",
		OutputDType:         "f16",
		TokenEmbeddingParam: "token_embedding",
		ProjectionParam:     "projection",
		Tokenizer: mantaruntime.TokenizerManifest{
			VocabSize:   8,
			MaxSequence: 8,
			PadID:       0,
			BOSID:       1,
			EOSID:       2,
			UnknownID:   3,
		},
	}
	if err := manifest.WriteFile(mantaruntime.DefaultEmbeddingManifestPath(artifactPath)); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	tokenizer := mantaruntime.TokenizerFile{
		Version:      mantaruntime.TokenizerFileVersion,
		Tokens:       []string{"[PAD]", "[CLS]", "[SEP]", "[UNK]", "hello", "world", "there", "friend"},
		PadToken:     "[PAD]",
		BOSToken:     "[CLS]",
		EOSToken:     "[SEP]",
		UnknownToken: "[UNK]",
	}
	if err := tokenizer.WriteFile(mantaruntime.DefaultTokenizerPath(artifactPath)); err != nil {
		t.Fatalf("write tokenizer: %v", err)
	}
	weights := mantaruntime.NewWeightFile(map[string]*backend.Tensor{
		"token_embedding": backend.NewTensorQ8([]int{8, 4}, []float32{
			0, 0, 0, 0,
			0.1, 0.1, 0.1, 0.1,
			0.2, 0.2, 0.2, 0.2,
			0.05, 0.05, 0.05, 0.05,
			1.0, 0.0, 0.2, 0.1,
			0.0, 1.0, 0.2, 0.1,
			0.5, 0.5, 0.1, 0.1,
			0.7, 0.2, 0.3, 0.1,
		}),
		"projection": backend.NewTensorQ8([]int{4, 3}, []float32{
			1.0, 0.1, 0.0,
			0.0, 1.0, 0.1,
			0.2, 0.2, 1.0,
			0.1, 0.0, 0.4,
		}),
	})
	if err := weights.WriteFile(mantaruntime.DefaultWeightFilePath(artifactPath)); err != nil {
		t.Fatalf("write weights: %v", err)
	}
	plan := mantaruntime.NewMemoryPlan(bundle.Artifact, weights.Weights, mantaruntime.MemoryPlanOptions{})
	if err := plan.WriteFile(mantaruntime.DefaultMemoryPlanPath(artifactPath)); err != nil {
		t.Fatalf("write memory plan: %v", err)
	}
	return artifactPath
}
