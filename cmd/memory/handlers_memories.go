package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/odvcencio/corkscrewdb"
)

// MemoriesHandler serves the /memories/* CRUD endpoints backed by
// CorkScrewDB. Writes go to the primary; reads honor the
// X-Read-Staleness header to route through the read replica.
type MemoriesHandler struct {
	clients *DBClients
	logger  *log.Logger
}

// NewMemoriesHandler returns a handler wired to clients. A nil logger
// falls back to log.Default so callers don't have to supply one.
func NewMemoriesHandler(clients *DBClients, logger *log.Logger) *MemoriesHandler {
	if logger == nil {
		logger = log.Default()
	}
	return &MemoriesHandler{clients: clients, logger: logger}
}

// Register installs the handler's routes on mux. The caller is expected
// to wrap the whole mux in agent auth so AgentFromContext works below.
func (h *MemoriesHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /memories", h.handlePost)
	mux.HandleFunc("GET /memories/{collection}/{id}", h.handleGet)
	mux.HandleFunc("GET /memories/{collection}/{id}/versions", h.handleVersions)
	mux.HandleFunc("DELETE /memories/{collection}/{id}", h.handleDelete)
}

// postRequest is the JSON body accepted by POST /memories.
type postRequest struct {
	Collection string            `json:"collection"`
	ID         string            `json:"id,omitempty"`
	Text       string            `json:"text"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// postResponse is returned by a successful POST.
type postResponse struct {
	ID  string `json:"id"`
	HLC uint64 `json:"hlc"`
}

// versionPayload is the shape used by GET /{id} and GET /{id}/versions.
type versionPayload struct {
	HLC      uint64            `json:"hlc"`
	ActorID  string            `json:"actor_id,omitempty"`
	Wall     time.Time         `json:"wall,omitempty"`
	Text     string            `json:"text"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Deleted  bool              `json:"deleted,omitempty"`
}

// staleReadOK returns true when the caller opted into replica reads via
// the X-Read-Staleness header. Only consulted on GETs.
func staleReadOK(r *http.Request) bool {
	return r.Header.Get("X-Read-Staleness") == "replica-ok"
}

// writeJSON encodes v as JSON with the agreed content-type and status.
// Errors during encoding are logged but not surfaced to the client —
// the response has already started by then.
func (h *MemoriesHandler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Printf("memories: encode response: %v", err)
	}
}

// writeError sends a JSON error body with the given status. msg is used
// verbatim — callers pass safe values like "not found" or "internal".
func (h *MemoriesHandler) writeError(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, map[string]string{"error": msg})
}

// mapWriteError turns a CorkScrewDB error from a write path into an
// HTTP status + public message. Unknown errors become 500/"internal".
func (h *MemoriesHandler) mapWriteError(w http.ResponseWriter, err error, where string) {
	if err == nil {
		return
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "dimension") && strings.Contains(msg, "does not match"):
		h.writeError(w, http.StatusConflict, "embedding-space mismatch")
	case strings.Contains(msg, "embedding config mismatch"):
		h.writeError(w, http.StatusConflict, "embedding-space mismatch")
	default:
		h.logger.Printf("memories: %s: %v", where, err)
		h.writeError(w, http.StatusInternalServerError, "internal")
	}
}

// newEntryID generates a 16-byte hex ID for POST requests that do not
// specify one. crypto/rand should not fail in practice; if it does we
// bubble the error up so the handler responds 500.
func newEntryID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

// handlePost implements POST /memories.
func (h *MemoriesHandler) handlePost(w http.ResponseWriter, r *http.Request) {
	agent, ok := AgentFromContext(r.Context())
	if !ok {
		h.logger.Printf("memories: POST missing agent in context; middleware misconfigured")
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}

	var req postRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}
	if strings.TrimSpace(req.Collection) == "" {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}
	if strings.TrimSpace(req.Text) == "" {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	id := strings.TrimSpace(req.ID)
	if id == "" {
		generated, err := newEntryID()
		if err != nil {
			h.logger.Printf("memories: generate id: %v", err)
			h.writeError(w, http.StatusInternalServerError, "internal")
			return
		}
		id = generated
	}

	// Agent injection: we allocate a fresh map so the caller's body
	// cannot alias any server-side state, then force agent=server.
	meta := make(map[string]string, len(req.Metadata)+1)
	for k, v := range req.Metadata {
		meta[k] = v
	}
	meta["agent"] = agent

	db := h.clients.WriteClient()
	coll := db.Collection(req.Collection)
	if err := coll.Put(id, corkscrewdb.Entry{Text: req.Text, Metadata: meta}); err != nil {
		h.mapWriteError(w, err, "put")
		return
	}

	// Pull back the latest HLC for this id so the response matches what
	// we actually wrote. Using the rw client guarantees read-your-writes.
	hlc, err := latestHLC(coll, id)
	if err != nil {
		h.logger.Printf("memories: post history lookup: %v", err)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	h.writeJSON(w, http.StatusOK, postResponse{ID: id, HLC: hlc})
}

// handleGet implements GET /memories/{collection}/{id}.
func (h *MemoriesHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	if _, ok := AgentFromContext(r.Context()); !ok {
		h.logger.Printf("memories: GET missing agent in context; middleware misconfigured")
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	collection := r.PathValue("collection")
	id := r.PathValue("id")
	if collection == "" || id == "" {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	db := h.clients.ReadClient(staleReadOK(r))
	coll := db.Collection(collection)

	versions, err := resolveVersions(coll, id, r.URL.Query().Get("at"))
	if err != nil {
		if errors.Is(err, errInvalidAt) {
			h.writeError(w, http.StatusBadRequest, "invalid request")
			return
		}
		h.logger.Printf("memories: get history: %v", err)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	latest, ok := pickLatestVisible(versions)
	if !ok {
		h.writeError(w, http.StatusNotFound, "not found")
		return
	}
	h.writeJSON(w, http.StatusOK, toPayload(latest))
}

// handleVersions implements GET /memories/{collection}/{id}/versions.
func (h *MemoriesHandler) handleVersions(w http.ResponseWriter, r *http.Request) {
	if _, ok := AgentFromContext(r.Context()); !ok {
		h.logger.Printf("memories: versions missing agent in context; middleware misconfigured")
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	collection := r.PathValue("collection")
	id := r.PathValue("id")
	if collection == "" || id == "" {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	db := h.clients.ReadClient(staleReadOK(r))
	coll := db.Collection(collection)
	history, err := coll.History(id)
	if err != nil {
		h.logger.Printf("memories: versions history: %v", err)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	if len(history) == 0 {
		h.writeError(w, http.StatusNotFound, "not found")
		return
	}
	out := make([]versionPayload, 0, len(history))
	for _, v := range history {
		out = append(out, toPayload(v))
	}
	h.writeJSON(w, http.StatusOK, map[string]any{"versions": out})
}

// handleDelete implements DELETE /memories/{collection}/{id}.
func (h *MemoriesHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if _, ok := AgentFromContext(r.Context()); !ok {
		h.logger.Printf("memories: DELETE missing agent in context; middleware misconfigured")
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	collection := r.PathValue("collection")
	id := r.PathValue("id")
	if collection == "" || id == "" {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	db := h.clients.WriteClient()
	coll := db.Collection(collection)
	if err := coll.Delete(id); err != nil {
		h.mapWriteError(w, err, "delete")
		return
	}
	// 204 No Content — no JSON body. We still set Content-Type for
	// consistency with the rest of the surface; the body stays empty.
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusNoContent)
}

// errInvalidAt is returned by resolveVersions when the ?at= query
// parameter fails to parse as a uint64.
var errInvalidAt = errors.New("memories: invalid at query")

// resolveVersions returns the history slice for id, optionally filtered
// through a point-in-time view if atRaw is non-empty.
func resolveVersions(coll *corkscrewdb.Collection, id, atRaw string) ([]corkscrewdb.Version, error) {
	if atRaw == "" {
		return coll.History(id)
	}
	at, err := strconv.ParseUint(atRaw, 10, 64)
	if err != nil {
		return nil, errInvalidAt
	}
	return coll.At(at).History(id)
}

// pickLatestVisible returns the newest non-tombstone version in history.
// History is ordered ascending by HLC so we iterate from the tail. If
// the tail is a tombstone the entry is treated as deleted — 404.
func pickLatestVisible(history []corkscrewdb.Version) (corkscrewdb.Version, bool) {
	if len(history) == 0 {
		return corkscrewdb.Version{}, false
	}
	latest := history[len(history)-1]
	if latest.Tombstone {
		return corkscrewdb.Version{}, false
	}
	return latest, true
}

// latestHLC returns the HLC of the tail of id's history. Used after a
// Put to echo the caller's freshly-written version.
func latestHLC(coll *corkscrewdb.Collection, id string) (uint64, error) {
	history, err := coll.History(id)
	if err != nil {
		return 0, err
	}
	if len(history) == 0 {
		return 0, errors.New("memories: empty history after put")
	}
	return history[len(history)-1].LamportClock, nil
}

// toPayload converts a CorkScrewDB Version into its wire-format JSON
// payload. The raw embedding is intentionally dropped.
func toPayload(v corkscrewdb.Version) versionPayload {
	return versionPayload{
		HLC:      v.LamportClock,
		ActorID:  v.ActorID,
		Wall:     v.WallClock,
		Text:     v.Text,
		Metadata: v.Metadata,
		Deleted:  v.Tombstone,
	}
}
