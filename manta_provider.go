package corkscrewdb

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	mantaruntime "github.com/odvcencio/barracuda/runtime"
	"github.com/odvcencio/barracuda/runtime/backends/cuda"
	"github.com/odvcencio/barracuda/runtime/backends/metal"
)

// LoadMantaProvider loads a Manta embedding package from disk.
func LoadMantaProvider(artifactPath string) (EmbeddingProvider, error) {
	return loadMantaProvider(loadMantaConfig{
		ArtifactPath: artifactPath,
	})
}

// LoadBarracudaProvider loads a Manta embedding package from disk.
//
// Deprecated: use LoadMantaProvider.
func LoadBarracudaProvider(artifactPath string) (EmbeddingProvider, error) {
	return LoadMantaProvider(artifactPath)
}

type mantaProvider struct {
	id        string
	dim       int
	runtime   *mantaruntime.Runtime
	model     *mantaruntime.EmbeddingModel
	tokenizer *mantaruntime.BPETokenizer

	mu      sync.Mutex
	closeMu sync.Mutex
	tempDir string
	closed  bool
}

type loadMantaConfig struct {
	ID            string
	ArtifactPath  string
	TokenizerPath string
	TempDir       string
}

func loadMantaProvider(cfg loadMantaConfig) (*mantaProvider, error) {
	if strings.TrimSpace(cfg.ArtifactPath) == "" {
		return nil, fmt.Errorf("corkscrewdb: Manta artifact path is required")
	}
	rt := mantaruntime.New(cuda.New(), metal.New())
	model, err := rt.LoadEmbeddingPackage(context.Background(), cfg.ArtifactPath)
	if err != nil {
		if cfg.TempDir != "" {
			_ = os.RemoveAll(cfg.TempDir)
		}
		return nil, err
	}
	manifest := model.Manifest()
	tokenizerPath := cfg.TokenizerPath
	if tokenizerPath == "" {
		tokenizerPath = mantaruntime.DefaultTokenizerPath(cfg.ArtifactPath)
	}
	tokenizerFile, err := mantaruntime.ReadTokenizerFile(tokenizerPath)
	if err != nil {
		if cfg.TempDir != "" {
			_ = os.RemoveAll(cfg.TempDir)
		}
		return nil, err
	}
	tokenizer, err := mantaruntime.NewBPETokenizer(tokenizerFile, manifest.Tokenizer)
	if err != nil {
		if cfg.TempDir != "" {
			_ = os.RemoveAll(cfg.TempDir)
		}
		return nil, err
	}
	dim, err := detectMantaEmbeddingDim(model)
	if err != nil {
		if cfg.TempDir != "" {
			_ = os.RemoveAll(cfg.TempDir)
		}
		return nil, err
	}
	id := cfg.ID
	if id == "" {
		if manifest.Name != "" {
			id = manifest.Name
		} else {
			base := filepath.Base(cfg.ArtifactPath)
			id = strings.TrimSuffix(base, filepath.Ext(base))
		}
	}
	return &mantaProvider{
		id:        id,
		dim:       dim,
		runtime:   rt,
		model:     model,
		tokenizer: tokenizer,
		tempDir:   cfg.TempDir,
	}, nil
}

func (p *mantaProvider) Encode(text string) ([]float32, error) {
	if p == nil {
		return nil, fmt.Errorf("corkscrewdb: nil Manta provider")
	}
	if strings.TrimSpace(text) == "" {
		return make([]float32, p.dim), nil
	}
	tokens, _, err := p.tokenizer.Encode(text)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	result, err := p.model.Embed(context.Background(), tokens)
	if err != nil {
		return nil, err
	}
	return append([]float32(nil), result.Embeddings.F32...), nil
}

func (p *mantaProvider) EncodeBatch(texts []string) ([][]float32, error) {
	if p == nil {
		return nil, fmt.Errorf("corkscrewdb: nil Manta provider")
	}
	out := make([][]float32, len(texts))
	batches := make([][]int32, 0, len(texts))
	positions := make([]int, 0, len(texts))
	for i, text := range texts {
		if strings.TrimSpace(text) == "" {
			out[i] = make([]float32, p.dim)
			continue
		}
		tokens, _, err := p.tokenizer.Encode(text)
		if err != nil {
			return nil, err
		}
		batches = append(batches, tokens)
		positions = append(positions, i)
	}
	if len(batches) == 0 {
		return out, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	result, err := p.model.EmbedBatch(context.Background(), batches)
	if err != nil {
		return nil, err
	}
	if len(result.Embeddings.Shape) != 2 || result.Embeddings.Shape[1] != p.dim {
		return nil, fmt.Errorf("corkscrewdb: Manta batch embedding shape %v does not match dim %d", result.Embeddings.Shape, p.dim)
	}
	for row, pos := range positions {
		start := row * p.dim
		out[pos] = append([]float32(nil), result.Embeddings.F32[start:start+p.dim]...)
	}
	return out, nil
}

func (p *mantaProvider) Dim() int {
	if p == nil {
		return 0
	}
	return p.dim
}

func (p *mantaProvider) Close() error {
	if p == nil {
		return nil
	}
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.tempDir != "" {
		return os.RemoveAll(p.tempDir)
	}
	return nil
}

func (p *mantaProvider) ProviderID() string {
	if p == nil {
		return ""
	}
	return p.id
}

func newEmbeddedMantaProvider(id string, assets fs.FS, dir string) (*mantaProvider, error) {
	if assets == nil {
		return nil, fmt.Errorf("corkscrewdb: embedded Manta assets are required")
	}
	tempDir, err := os.MkdirTemp("", "corkscrewdb-"+id+"-")
	if err != nil {
		return nil, err
	}
	if err := copyEmbeddedDir(tempDir, assets, dir); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	artifactPath := filepath.Join(tempDir, id+".mll")
	return loadMantaProvider(loadMantaConfig{
		ID:            id,
		ArtifactPath:  artifactPath,
		TokenizerPath: filepath.Join(tempDir, id+".tokenizer.mll"),
		TempDir:       tempDir,
	})
}

func copyEmbeddedDir(dstRoot string, assets fs.FS, dir string) error {
	return fs.WalkDir(assets, dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		data, err := fs.ReadFile(assets, path)
		if err != nil {
			return err
		}
		outPath := filepath.Join(dstRoot, rel)
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return err
		}
		return os.WriteFile(outPath, data, 0o644)
	})
}

func detectMantaEmbeddingDim(model *mantaruntime.EmbeddingModel) (int, error) {
	if model == nil {
		return 0, fmt.Errorf("corkscrewdb: Manta embedding model is not loaded")
	}
	manifest := model.Manifest()
	token := manifest.Tokenizer.BOSID
	switch {
	case token > 0:
	case manifest.Tokenizer.UnknownID >= 0:
		token = manifest.Tokenizer.UnknownID
	case manifest.Tokenizer.PadID >= 0:
		token = manifest.Tokenizer.PadID
	default:
		token = 0
	}
	result, err := model.Embed(context.Background(), []int32{token})
	if err != nil {
		return 0, err
	}
	if result.Embeddings == nil || len(result.Embeddings.Shape) != 1 {
		return 0, fmt.Errorf("corkscrewdb: Manta embedding bootstrap returned invalid shape %v", result.Embeddings.Shape)
	}
	return result.Embeddings.Shape[0], nil
}
