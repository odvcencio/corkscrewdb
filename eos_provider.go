package corkscrewdb

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	eosruntime "m31labs.dev/eos/runtime"
	"m31labs.dev/eos/runtime/backends/cuda"
	"m31labs.dev/eos/runtime/backends/metal"
)

// LoadEosProvider loads a Eos embedding package from disk.
func LoadEosProvider(artifactPath string) (EmbeddingProvider, error) {
	return loadEosProvider(loadEosConfig{
		ArtifactPath: artifactPath,
	})
}

// LoadEosProviderWithID loads a Eos embedding package from disk and exposes it
// under providerID in CorkScrewDB manifests and RemoteInfo responses.
func LoadEosProviderWithID(providerID, artifactPath string) (EmbeddingProvider, error) {
	return loadEosProvider(loadEosConfig{
		ID:           strings.TrimSpace(providerID),
		ArtifactPath: artifactPath,
	})
}

type eosProvider struct {
	id        string
	dim       int
	runtime   *eosruntime.Runtime
	model     *eosruntime.EmbeddingModel
	tokenizer *eosruntime.BPETokenizer

	mu      sync.Mutex
	closeMu sync.Mutex
	tempDir string
	closed  bool
}

type loadEosConfig struct {
	ID            string
	ArtifactPath  string
	TokenizerPath string
	TempDir       string
}

func loadEosProvider(cfg loadEosConfig) (*eosProvider, error) {
	if strings.TrimSpace(cfg.ArtifactPath) == "" {
		return nil, fmt.Errorf("corkscrewdb: Eos artifact path is required")
	}
	rt := eosruntime.New(cuda.New(), metal.New())
	model, err := rt.LoadEmbeddingPackage(context.Background(), cfg.ArtifactPath)
	if err != nil {
		if cfg.TempDir != "" {
			_ = os.RemoveAll(cfg.TempDir)
		}
		return nil, err
	}
	manifest := model.Manifest()
	var tokenizerFile eosruntime.TokenizerFile
	if cfg.TokenizerPath == "" {
		var ok bool
		if tokenizerFile, ok = model.TokenizerFile(); !ok {
			tokenizerPath := eosruntime.DefaultTokenizerPath(cfg.ArtifactPath)
			tokenizerFile, err = eosruntime.ReadTokenizerFile(tokenizerPath)
			if err != nil {
				if cfg.TempDir != "" {
					_ = os.RemoveAll(cfg.TempDir)
				}
				return nil, err
			}
		}
	} else {
		tokenizerFile, err = eosruntime.ReadTokenizerFile(cfg.TokenizerPath)
		if err != nil {
			if cfg.TempDir != "" {
				_ = os.RemoveAll(cfg.TempDir)
			}
			return nil, err
		}
	}
	tokenizer, err := eosruntime.NewBPETokenizer(tokenizerFile, manifest.Tokenizer)
	if err != nil {
		if cfg.TempDir != "" {
			_ = os.RemoveAll(cfg.TempDir)
		}
		return nil, err
	}
	dim, err := detectEosEmbeddingDim(model)
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
	return &eosProvider{
		id:        id,
		dim:       dim,
		runtime:   rt,
		model:     model,
		tokenizer: tokenizer,
		tempDir:   cfg.TempDir,
	}, nil
}

func (p *eosProvider) Encode(text string) ([]float32, error) {
	if p == nil {
		return nil, fmt.Errorf("corkscrewdb: nil Eos provider")
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

func (p *eosProvider) EncodeBatch(texts []string) ([][]float32, error) {
	if p == nil {
		return nil, fmt.Errorf("corkscrewdb: nil Eos provider")
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
		return nil, fmt.Errorf("corkscrewdb: Eos batch embedding shape %v does not match dim %d", result.Embeddings.Shape, p.dim)
	}
	for row, pos := range positions {
		start := row * p.dim
		out[pos] = append([]float32(nil), result.Embeddings.F32[start:start+p.dim]...)
	}
	return out, nil
}

func (p *eosProvider) Dim() int {
	if p == nil {
		return 0
	}
	return p.dim
}

func (p *eosProvider) Close() error {
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

func (p *eosProvider) ProviderID() string {
	if p == nil {
		return ""
	}
	return p.id
}

func newEmbeddedEosProvider(id string, assets fs.FS, dir string) (*eosProvider, error) {
	if assets == nil {
		return nil, fmt.Errorf("corkscrewdb: embedded Eos assets are required")
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
	return loadEosProvider(loadEosConfig{
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

func detectEosEmbeddingDim(model *eosruntime.EmbeddingModel) (int, error) {
	if model == nil {
		return 0, fmt.Errorf("corkscrewdb: Eos embedding model is not loaded")
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
		return 0, fmt.Errorf("corkscrewdb: Eos embedding bootstrap returned invalid shape %v", result.Embeddings.Shape)
	}
	return result.Embeddings.Shape[0], nil
}
