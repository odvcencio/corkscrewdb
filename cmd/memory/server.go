package main

import (
	"log"
	"net/http"
)

// NewHandler composes all routes. Agent paths wrap the agent auth
// middleware; admin paths wrap the admin auth middleware; healthz/readyz
// are unauthenticated.
//
// Each handler group registers on its own sub-mux, and the sub-muxes are
// mounted on the root with middleware applied at the mount point. This
// keeps auth wrapping at the route-group level (one wrap per subtree)
// rather than per-route, and cooperates with Go 1.22's method+path
// patterns because the sub-mux still owns method dispatch internally.
//
// A nil logger is accepted and propagated to handlers, which fall back
// to log.Default in their own constructors.
func NewHandler(cfg Config, clients *DBClients, logger *log.Logger) http.Handler {
	memoriesMux := http.NewServeMux()
	NewMemoriesHandler(clients, logger).Register(memoriesMux)
	NewSearchHandler(clients, logger).Register(memoriesMux)

	adminMux := http.NewServeMux()
	NewAdminHandler(clients, logger).Register(adminMux)

	healthMux := http.NewServeMux()
	NewHealthHandler(clients, logger).Register(healthMux)

	root := http.NewServeMux()

	// Agent subtree: both the /memories subtree (GET /memories/{collection}/{id},
	// /versions, DELETE, search) and the exact POST /memories route. The
	// exact-match registration is required because Go's ServeMux treats
	// "/memories/" as a subtree pattern that does not match a request to
	// "/memories" without a trailing slash.
	agentMemories := AgentAuth(memoriesMux, cfg.AgentTokens)
	root.Handle("/memories", agentMemories)
	root.Handle("/memories/", agentMemories)

	// Admin subtree.
	root.Handle("/admin/", AdminAuth(adminMux, cfg.AdminToken))

	// Health: unauthenticated. Only exact matches so an accidental
	// /healthz/evil path does not slip through.
	root.Handle("/healthz", healthMux)
	root.Handle("/readyz", healthMux)

	return root
}
