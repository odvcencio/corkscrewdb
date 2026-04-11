package corkscrewdb

import (
	"math"
	"strings"
	"unicode"
)

const defaultEmbeddingDim = 384

// builtinProvider produces deterministic keyword-based embeddings using FNV
// hashing. Word unigrams and bigrams are hashed into sparse feature vectors.
// This is the zero-dependency fallback when the embedded Manta model is
// unavailable. It provides keyword matching, not semantic similarity. "dog"
// and "canine" produce unrelated vectors.
type builtinProvider struct {
	dim int
}

func newBuiltinProvider() EmbeddingProvider {
	return &builtinProvider{dim: defaultEmbeddingDim}
}

func (p *builtinProvider) Encode(text string) ([]float32, error) {
	vec := make([]float32, p.dim)
	if strings.TrimSpace(text) == "" {
		return vec, nil
	}

	normalized := normalizeText(text)
	if normalized == "" {
		return vec, nil
	}

	features := hashedFeatures(normalized)
	for _, feature := range features {
		accumulateFeature(vec, feature)
	}
	normalizeVector(vec)
	return vec, nil
}

func (p *builtinProvider) EncodeBatch(texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		vec, err := p.Encode(text)
		if err != nil {
			return nil, err
		}
		out[i] = vec
	}
	return out, nil
}

func (p *builtinProvider) Dim() int {
	return p.dim
}

func (p *builtinProvider) Close() error {
	return nil
}

func (p *builtinProvider) ProviderID() string {
	return "builtin-deterministic-v1"
}

func normalizeText(text string) string {
	text = strings.ToLower(text)
	text = strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), unicode.IsSpace(r):
			return r
		default:
			return ' '
		}
	}, text)
	return strings.Join(strings.Fields(text), " ")
}

func hashedFeatures(text string) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}
	features := make([]string, 0, len(words)*2)
	for i, word := range words {
		features = append(features, "w:"+word)
		if i+1 < len(words) {
			features = append(features, "p:"+word+"_"+words[i+1])
		}
	}
	return features
}

func accumulateFeature(vec []float32, feature string) {
	var seed uint64 = 1469598103934665603
	for _, r := range feature {
		seed ^= uint64(r)
		seed *= 1099511628211
	}
	for i := 0; i < 4; i++ {
		seed = splitmix64(seed + uint64(i)*0x9e3779b97f4a7c15)
		idx := int(seed % uint64(len(vec)))
		weight := float32(1.0 / float64(i+1))
		vec[idx] += weight
	}
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func normalizeVector(vec []float32) {
	var sum float64
	for _, v := range vec {
		sum += float64(v * v)
	}
	if sum == 0 {
		return
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range vec {
		vec[i] *= inv
	}
}
