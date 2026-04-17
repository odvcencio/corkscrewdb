package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/odvcencio/corkscrewdb"
)

// HealthHandler serves the liveness (/healthz) and readiness (/readyz)
// probes. Liveness is deliberately dependency-free so a crash-looping
// DB connection cannot mark the pod dead; readiness actively verifies
// the primary can answer and that the agent-memory bootstrap has run.
type HealthHandler struct {
	clients *DBClients
	timeout time.Duration
	logger  *log.Logger
}

// NewHealthHandler returns a handler with the default 1s readiness
// probe budget. A nil logger falls back to log.Default.
func NewHealthHandler(clients *DBClients, logger *log.Logger) *HealthHandler {
	if logger == nil {
		logger = log.Default()
	}
	return &HealthHandler{clients: clients, timeout: 1 * time.Second, logger: logger}
}

// Register installs the /healthz and /readyz routes. Neither requires
// authentication — probes come from the kubelet and from the memory
// service's own sidecar scripts, not from human callers.
func (h *HealthHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.handleHealth)
	mux.HandleFunc("GET /readyz", h.handleReady)
}

// writeJSON encodes v with the agreed content-type and status.
func (h *HealthHandler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Printf("health: encode response: %v", err)
	}
}

// handleHealth implements GET /healthz — it must never fail because of
// external state. A pod that is serving HTTP is alive; readiness is a
// separate concern.
func (h *HealthHandler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleReady implements GET /readyz. It returns 200 only when (a) the
// rw CorkScrewDB client can answer RemoteInfo() within the probe
// budget, and (b) the agent-memory system collection has been
// materialized (i.e. the bootstrap job has run and wrote a record).
//
// On any failure it returns 503 with {"ok":false,"reason":"..."}
// naming the failing step so an operator can tell "rw info" from
// "agent-memory probe".
func (h *HealthHandler) handleReady(w http.ResponseWriter, r *http.Request) {
	if h.clients == nil {
		h.writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":     false,
			"reason": "clients not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// Step A: rw info. The RemoteInfo response also carries the list
	// of collections the primary knows about, so we reuse it for Step
	// B without a second round trip.
	info, err := raceCtx(ctx, func() (corkscrewdb.RPCInfoResponse, error) {
		return h.clients.WriteClient().RemoteInfo()
	})
	if err != nil {
		h.writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":     false,
			"reason": fmt.Sprintf("rw info: %v", err),
		})
		return
	}

	// Step B: agent-memory probe. We can't use History against the
	// server — the server auto-creates the collection on any access,
	// so a History("__readyz_probe__") against a missing agent-memory
	// returns (nil, nil) and silently brings the collection into
	// existence. That's exactly what we don't want pre-bootstrap.
	//
	// Instead, require that agent-memory is already present in the
	// RemoteInfo().Collections list — the bootstrap Job puts a real
	// record into it, which is what advances a fresh cluster from
	// "listening" to "ready".
	if !containsCollection(info.Collections, "agent-memory") {
		h.writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":     false,
			"reason": "agent-memory probe: collection not materialized",
		})
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// containsCollection returns true when list names collection name.
func containsCollection(list []corkscrewdb.RPCCollectionInfo, name string) bool {
	for _, c := range list {
		if c.Name == name {
			return true
		}
	}
	return false
}

// raceCtx runs fn in a goroutine and races its completion against
// ctx.Done(). If ctx fires first, the late-arriving result is drained
// in a detached goroutine so the caller does not block and the
// result-channel send does not panic. The channel is buffered(1) so
// the producer goroutine can always write and exit cleanly.
func raceCtx[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	type result struct {
		val T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn()
		ch <- result{v, err}
	}()
	select {
	case r := <-ch:
		return r.val, r.err
	case <-ctx.Done():
		// Drain the late result so the producer goroutine can exit.
		go func() { <-ch }()
		var zero T
		return zero, ctx.Err()
	}
}
