package corkscrewdb

import "errors"

// EmbeddingProvider wraps a source of text embeddings.
// Implementations must be safe for concurrent use.
type EmbeddingProvider interface {
	Encode(text string) ([]float32, error)
	EncodeBatch(texts []string) ([][]float32, error)
	Dim() int
	Close() error
}

type encoder struct {
	provider EmbeddingProvider
}

func newEncoder(provider EmbeddingProvider) *encoder {
	return &encoder{provider: provider}
}

func (e *encoder) Encode(text string) ([]float32, error) {
	if e.provider == nil {
		return nil, errors.New("corkscrewdb: no embedding provider configured")
	}
	return e.provider.Encode(text)
}

func (e *encoder) EncodeBatch(texts []string) ([][]float32, error) {
	if e.provider == nil {
		return nil, errors.New("corkscrewdb: no embedding provider configured")
	}
	return e.provider.EncodeBatch(texts)
}

func (e *encoder) Dim() int {
	if e.provider == nil {
		return 0
	}
	return e.provider.Dim()
}

func (e *encoder) Close() error {
	if e.provider == nil {
		return nil
	}
	return e.provider.Close()
}
