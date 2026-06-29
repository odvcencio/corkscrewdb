package corkscrewdb

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	eosartifact "m31labs.dev/eos/artifact/eos"
	"m31labs.dev/eos/compiler"
	eosruntime "m31labs.dev/eos/runtime"
	"m31labs.dev/eos/runtime/backend"
)

func TestLoadImportedBGECandidateProviderRequiresPackagePath(t *testing.T) {
	provider, err := LoadImportedBGECandidateProvider(" \t ")
	if err == nil {
		t.Fatal("expected empty package path error, got nil")
	}
	if provider != nil {
		t.Fatalf("provider = %#v, want nil", provider)
	}
	if !strings.Contains(err.Error(), "imported BGE candidate package path is required") {
		t.Fatalf("error = %q, want package path requirement", err.Error())
	}
}

func TestImportedBGECandidateProviderMetadataConstants(t *testing.T) {
	if ImportedBGECandidateProviderID != "corkscrewdb-imported-bge-eos-embed-v1-candidate" {
		t.Fatalf("provider id = %q", ImportedBGECandidateProviderID)
	}
	wantFingerprint := "eos-imported-bge:" +
		eosruntime.ImportedBERTEmbedderCandidatePackageSHA256 + ":" +
		eosruntime.ImportedBERTEmbedderCandidatePackageIdentitySHA256
	if got := importedBGECandidateBackendFingerprint(); got != wantFingerprint {
		t.Fatalf("backend fingerprint = %q, want %q", got, wantFingerprint)
	}
}

func TestLoadImportedBGECandidateProviderIntegration(t *testing.T) {
	path := strings.TrimSpace(os.Getenv("CORKSCREWDB_IMPORTED_BGE_CANDIDATE_PACKAGE"))
	if path == "" {
		t.Skip("set CORKSCREWDB_IMPORTED_BGE_CANDIDATE_PACKAGE to run imported-BGE candidate integration")
	}
	provider, err := LoadImportedBGECandidateProvider(path)
	if err != nil {
		t.Fatalf("load imported BGE candidate provider: %v", err)
	}
	defer provider.Close()

	if got := provider.Dim(); got != 384 {
		t.Fatalf("dim = %d, want 384", got)
	}
	named, ok := provider.(interface{ ProviderID() string })
	if !ok {
		t.Fatal("provider does not expose ProviderID")
	}
	if got := named.ProviderID(); got != ImportedBGECandidateProviderID {
		t.Fatalf("provider id = %q, want %q", got, ImportedBGECandidateProviderID)
	}
	deterministic, ok := provider.(interface{ Deterministic() bool })
	if !ok || !deterministic.Deterministic() {
		t.Fatalf("deterministic capability = %v/%v, want true/true", ok, ok && deterministic.Deterministic())
	}
	fingerprinted, ok := provider.(interface{ BackendFingerprint() string })
	if !ok {
		t.Fatal("provider does not expose BackendFingerprint")
	}
	if got, want := fingerprinted.BackendFingerprint(), importedBGECandidateBackendFingerprint(); got != want {
		t.Fatalf("backend fingerprint = %q, want %q", got, want)
	}

	blank, err := provider.Encode(" \n ")
	if err != nil {
		t.Fatalf("encode blank: %v", err)
	}
	assertZeroVector(t, blank, 384)

	query, err := provider.Encode("Which document discusses LDL cholesterol treatment with statins?")
	if err != nil {
		t.Fatalf("encode query: %v", err)
	}
	assertNonZeroVector(t, query, 384)

	batch, err := provider.EncodeBatch([]string{
		"Statins reduce LDL cholesterol and cardiovascular risk.",
		"",
		"Kafka consumers track offsets and retry failed messages.",
	})
	if err != nil {
		t.Fatalf("encode batch documents: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("batch rows = %d, want 3", len(batch))
	}
	assertNonZeroVector(t, batch[0], 384)
	assertZeroVector(t, batch[1], 384)
	assertNonZeroVector(t, batch[2], 384)
}

func TestEmbeddedDefaultEosProviderAssetHashes(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "artifact",
			path: "assets/corkscrewdb-default-embedder/corkscrewdb-default-embedder.mll",
			want: defaultEosProviderArtifactSHA256,
		},
		{
			name: "tokenizer",
			path: "assets/corkscrewdb-default-embedder/corkscrewdb-default-embedder.tokenizer.mll",
			want: defaultEosProviderTokenizerSHA256,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := defaultEosProviderAssets.ReadFile(tc.path)
			if err != nil {
				t.Fatalf("read embedded %s: %v", tc.path, err)
			}
			sum := sha256.Sum256(data)
			if got := hex.EncodeToString(sum[:]); got != tc.want {
				t.Fatalf("%s sha256 = %s, want %s", tc.name, got, tc.want)
			}
		})
	}
}

func TestLoadEosProviderEncodes(t *testing.T) {
	path := writeTinyEosProviderPackage(t)
	provider, err := LoadEosProvider(path)
	if err != nil {
		t.Fatalf("load Eos provider: %v", err)
	}
	defer provider.Close()

	named, ok := provider.(interface{ ProviderID() string })
	if !ok {
		t.Fatal("expected Eos provider to expose ProviderID")
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
		t.Fatal("expected non-zero Eos embedding")
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

func TestLoadEosProviderWithIDOverridesManifestName(t *testing.T) {
	path := writeTinyEosProviderPackage(t)
	provider, err := LoadEosProviderWithID("corkscrewdb-default-embedder", path)
	if err != nil {
		t.Fatalf("load Eos provider with ID: %v", err)
	}
	defer provider.Close()

	named, ok := provider.(interface{ ProviderID() string })
	if !ok {
		t.Fatal("expected Eos provider to expose ProviderID")
	}
	if got := named.ProviderID(); got != "corkscrewdb-default-embedder" {
		t.Fatalf("provider id = %q, want %q", got, "corkscrewdb-default-embedder")
	}
	if got := provider.Dim(); got != 3 {
		t.Fatalf("provider dim = %d, want 3", got)
	}
}

func TestOpenDefaultsToEosProvider(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if db.manifest.Embedding.ID != defaultEosProviderID {
		t.Fatalf("default embedding provider id = %q, want %q", db.manifest.Embedding.ID, defaultEosProviderID)
	}
	if db.manifest.Embedding.Dim != 256 {
		t.Fatalf("default embedding dim = %d, want 256", db.manifest.Embedding.Dim)
	}
	vec, err := db.provider.Encode("hello world")
	if err != nil {
		t.Fatalf("encode with default provider: %v", err)
	}
	if len(vec) != 256 {
		t.Fatalf("default embedding len = %d, want 256", len(vec))
	}
	var nonZero bool
	for _, value := range vec {
		if value != 0 {
			nonZero = true
			break
		}
	}
	if !nonZero {
		t.Fatal("expected non-zero default Eos embedding")
	}
}

func writeTinyEosProviderPackage(t *testing.T) string {
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
	if err := eosartifact.WriteFile(artifactPath, bundle.Artifact); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	manifest := eosruntime.EmbeddingManifest{
		Name:                "tiny-manta-provider",
		PooledEntry:         "embed_pooled",
		BatchEntry:          "embed_pooled_batch",
		TokenInput:          "tokens",
		OutputName:          "result",
		OutputDType:         "f16",
		TokenEmbeddingParam: "token_embedding",
		ProjectionParam:     "projection",
		Tokenizer: eosruntime.TokenizerManifest{
			VocabSize:   8,
			MaxSequence: 8,
			PadID:       0,
			BOSID:       1,
			EOSID:       2,
			UnknownID:   3,
		},
	}
	if err := manifest.WriteFile(eosruntime.DefaultEmbeddingManifestPath(artifactPath)); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	tokenizer := eosruntime.TokenizerFile{
		Version:      eosruntime.TokenizerFileVersion,
		Tokens:       []string{"[PAD]", "[CLS]", "[SEP]", "[UNK]", "hello", "world", "there", "friend"},
		PadToken:     "[PAD]",
		BOSToken:     "[CLS]",
		EOSToken:     "[SEP]",
		UnknownToken: "[UNK]",
	}
	if err := tokenizer.WriteFile(eosruntime.DefaultTokenizerPath(artifactPath)); err != nil {
		t.Fatalf("write tokenizer: %v", err)
	}
	weights := eosruntime.NewWeightFile(map[string]*backend.Tensor{
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
	if err := weights.WriteFile(eosruntime.DefaultWeightFilePath(artifactPath)); err != nil {
		t.Fatalf("write weights: %v", err)
	}
	plan := eosruntime.NewMemoryPlan(bundle.Artifact, weights.Weights, eosruntime.MemoryPlanOptions{})
	if err := plan.WriteFile(eosruntime.DefaultMemoryPlanPath(artifactPath)); err != nil {
		t.Fatalf("write memory plan: %v", err)
	}
	return artifactPath
}

func assertZeroVector(t *testing.T, vec []float32, wantDim int) {
	t.Helper()
	if len(vec) != wantDim {
		t.Fatalf("vector len = %d, want %d", len(vec), wantDim)
	}
	for _, value := range vec {
		if value != 0 {
			t.Fatalf("expected zero vector, got %v", vec)
		}
	}
}

func assertNonZeroVector(t *testing.T, vec []float32, wantDim int) {
	t.Helper()
	if len(vec) != wantDim {
		t.Fatalf("vector len = %d, want %d", len(vec), wantDim)
	}
	for _, value := range vec {
		if value != 0 {
			return
		}
	}
	t.Fatalf("expected non-zero vector, got %v", vec)
}
