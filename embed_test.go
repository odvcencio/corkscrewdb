package corkscrewdb

import "testing"

type mockProvider struct {
	dim int
}

func (m *mockProvider) Encode(text string) ([]float32, error) {
	v := make([]float32, m.dim)
	for i, r := range text {
		if i >= m.dim {
			break
		}
		v[i] = float32(r) / 1000.0
	}
	return v, nil
}

func (m *mockProvider) EncodeBatch(texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for i, text := range texts {
		vec, err := m.Encode(text)
		if err != nil {
			return nil, err
		}
		results[i] = vec
	}
	return results, nil
}

func (m *mockProvider) Dim() int     { return m.dim }
func (m *mockProvider) Close() error { return nil }
func (m *mockProvider) ProviderID() string {
	return "mock-provider"
}

func TestEncoderWithProvider(t *testing.T) {
	enc := newEncoder(&mockProvider{dim: 32})
	vec, err := enc.Encode("hello")
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) != 32 {
		t.Fatalf("dim = %d, want 32", len(vec))
	}
	if vec[0] == 0 {
		t.Fatal("expected non-zero embedding")
	}
}

func TestEncoderDim(t *testing.T) {
	enc := newEncoder(&mockProvider{dim: 384})
	if enc.Dim() != 384 {
		t.Fatalf("Dim = %d, want 384", enc.Dim())
	}
}

func TestEncoderNilProviderErrors(t *testing.T) {
	enc := newEncoder(nil)
	_, err := enc.Encode("hello")
	if err == nil {
		t.Fatal("expected error with nil provider")
	}
}

func TestBuiltinProviderDeterministic(t *testing.T) {
	p := newBuiltinProvider()
	a, err := p.Encode("hello world")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Encode("hello world")
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != defaultEmbeddingDim {
		t.Fatalf("dim = %d, want %d", len(a), defaultEmbeddingDim)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("embedding mismatch at %d", i)
		}
	}
}
