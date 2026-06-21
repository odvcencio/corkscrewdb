package corkscrewdb

import "embed"

const defaultEosProviderID = "corkscrewdb-default-embedder"
const defaultEosProviderArtifactSHA256 = "f494915a0d78b24205d5018bb701bf40cabbedee4bc8b96b6a1920b19131da5a"
const defaultEosProviderTokenizerSHA256 = "64cf63223cb3f97125040677a573e6ab6c625cff1f6f338f4e680a4c9f7a42f5"

//go:embed assets/corkscrewdb-default-embedder/*.mll
var defaultEosProviderAssets embed.FS

func newDefaultProvider() EmbeddingProvider {
	provider, err := newEmbeddedEosProvider(defaultEosProviderID, defaultEosProviderAssets, "assets/corkscrewdb-default-embedder")
	if err == nil {
		return provider
	}
	return newBuiltinProvider()
}
