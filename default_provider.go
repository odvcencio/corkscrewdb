package corkscrewdb

import "embed"

//go:embed assets/manta-embed-v0/*.mll
var mantaEmbedV0Assets embed.FS

func newDefaultProvider() EmbeddingProvider {
	provider, err := newEmbeddedMantaProvider("manta-embed-v0", mantaEmbedV0Assets, "assets/manta-embed-v0")
	if err == nil {
		return provider
	}
	return newBuiltinProvider()
}
