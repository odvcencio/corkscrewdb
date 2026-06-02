package corkscrewdb

import "embed"

//go:embed assets/manta-embed-v0/*.mll
var eosEmbedV0Assets embed.FS

func newDefaultProvider() EmbeddingProvider {
	provider, err := newEmbeddedEosProvider("manta-embed-v0", eosEmbedV0Assets, "assets/manta-embed-v0")
	if err == nil {
		return provider
	}
	return newBuiltinProvider()
}
