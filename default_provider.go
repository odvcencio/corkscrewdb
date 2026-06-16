package corkscrewdb

import "embed"

const defaultEosProviderID = "corkscrewdb-default-embedder"

//go:embed assets/corkscrewdb-default-embedder/*.mll
var defaultEosProviderAssets embed.FS

func newDefaultProvider() EmbeddingProvider {
	provider, err := newEmbeddedEosProvider(defaultEosProviderID, defaultEosProviderAssets, "assets/corkscrewdb-default-embedder")
	if err == nil {
		return provider
	}
	return newBuiltinProvider()
}
