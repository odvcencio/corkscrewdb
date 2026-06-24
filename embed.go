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

// deterministicProvider is an optional capability interface that an
// EmbeddingProvider may implement to declare that its EncodeBatch output is
// deterministic across calls and across restarts on the same host, given the
// same backend binary (see §5.6 and §9.4 of the v0.4.0 spec).
type deterministicProvider interface {
	Deterministic() bool
}

// backendFingerprintProvider is an optional capability interface that an
// EmbeddingProvider may implement to expose a stable opaque fingerprint
// identifying the exact backend+artifact combination in use. The fingerprint
// is stored in the collection manifest at create time and re-verified on open
// to fast-fail divergent hosts before any query (§9.4 backend-fingerprint pin).
type backendFingerprintProvider interface {
	BackendFingerprint() string
}

// providerDeterministic probes enc.provider for the deterministicProvider
// capability and returns its answer. If the provider does not implement the
// interface, it returns false. The probe is on enc.provider, NOT on enc
// itself — *encoder intentionally does not forward capability methods.
func providerDeterministic(enc *encoder) bool {
	if enc == nil || enc.provider == nil {
		return false
	}
	dp, ok := enc.provider.(deterministicProvider)
	if !ok {
		return false
	}
	return dp.Deterministic()
}

// providerBackendFingerprint probes enc.provider for the
// backendFingerprintProvider capability and returns its fingerprint. Returns
// the empty string if the provider does not implement the interface.
func providerBackendFingerprint(enc *encoder) string {
	if enc == nil || enc.provider == nil {
		return ""
	}
	bp, ok := enc.provider.(backendFingerprintProvider)
	if !ok {
		return ""
	}
	return bp.BackendFingerprint()
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
