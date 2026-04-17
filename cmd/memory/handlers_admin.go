package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/odvcencio/corkscrewdb"
)

// AdminHandler serves the /admin/* endpoints. Routes are registered
// without auth on this struct — the wiring layer wraps the admin mux in
// AdminAuth. Each handler defensively checks for the admin context
// marker so a mis-wired mux fails closed with 500 rather than serving
// sensitive surface unauthenticated.
type AdminHandler struct {
	clients *DBClients
	logger  *log.Logger
}

// systemCollectionName is the well-known collection that carries admin
// metadata records. It is special-cased during self-describing
// bootstrap: when the caller creates this collection the admin record
// lives inside it rather than in a separate system collection.
const systemCollectionName = "agent-memory"

// registryID is the id within systemCollectionName that stores the list
// of admin-registered collection names.
const registryID = "__admin/registry"

// adminCollectionIDPrefix is prepended to a collection name to form the
// id of its admin record. The agent-memory collection stores its own
// record under the reserved id "__admin/collections/agent-memory".
const adminCollectionIDPrefix = "__admin/collections/"

// NewAdminHandler returns a handler wired to clients. A nil logger
// falls back to log.Default so callers don't have to supply one.
func NewAdminHandler(clients *DBClients, logger *log.Logger) *AdminHandler {
	if logger == nil {
		logger = log.Default()
	}
	return &AdminHandler{clients: clients, logger: logger}
}

// Register installs the admin routes on mux. Caller must wrap the mux
// in AdminAuth so IsAdminContext is set — every handler here will 500
// if that marker is absent.
func (h *AdminHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/collections", h.handleCreateCollection)
	mux.HandleFunc("GET /admin/collections", h.handleListCollections)
	mux.HandleFunc("GET /admin/shards", h.handleShards)
}

// createCollectionRequest is the body accepted by POST /admin/collections.
type createCollectionRequest struct {
	Name      string `json:"name"`
	BitWidth  int    `json:"bit_width"`
	Retention string `json:"retention"`
}

// collectionRecord is the JSON shape stored in the admin record entry
// and echoed back on POST / GET.
type collectionRecord struct {
	Name       string `json:"name"`
	BitWidth   int    `json:"bit_width"`
	Retention  string `json:"retention"`
	CreatedHLC uint64 `json:"created_hlc"`
}

// writeJSON encodes v as JSON with the agreed content-type and status.
func (h *AdminHandler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Printf("admin: encode response: %v", err)
	}
}

// writeError sends a JSON error body with the given status. msg is used
// verbatim — callers pass safe, operator-chosen strings only.
func (h *AdminHandler) writeError(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, map[string]string{"error": msg})
}

// requireAdminContext is the defensive check shared by every handler.
// It returns true when the request is authorized to proceed. On a
// failed check it writes a 500 with a generic body and logs the path.
func (h *AdminHandler) requireAdminContext(w http.ResponseWriter, r *http.Request) bool {
	if !IsAdminContext(r.Context()) {
		h.logger.Printf("admin handler reached without admin context: path=%s", r.URL.Path)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return false
	}
	return true
}

// systemCollection returns the CorkScrewDB collection that holds admin
// metadata. Writes go through the rw client so admin operations always
// see read-your-writes.
func (h *AdminHandler) systemCollection() *corkscrewdb.Collection {
	return h.clients.WriteClient().Collection(systemCollectionName)
}

// adminRecordID returns the entry id under which the admin record for
// collection name is stored. Every collection's record lives in
// agent-memory except agent-memory itself, which is self-describing.
func adminRecordID(name string) string {
	return adminCollectionIDPrefix + name
}

// recordCollectionFor returns the collection where an admin record for
// name lives. agent-memory is self-describing; every other collection's
// record lives in the agent-memory system collection.
func (h *AdminHandler) recordCollectionFor(name string) *corkscrewdb.Collection {
	if name == systemCollectionName {
		return h.clients.WriteClient().Collection(systemCollectionName)
	}
	return h.systemCollection()
}

// readCollectionRecord returns the admin record for name built from the
// earliest surviving version's HLC plus the metadata embedded in the
// latest version's JSON text. The second return value is false if no
// record has ever been written, or if the record is currently
// tombstoned.
//
// We carry created_hlc out-of-band (via history[0].LamportClock) rather
// than embedding it in the JSON text so idempotent re-POSTs of the same
// record report the original create-time HLC even if the JSON body has
// been rewritten since.
func (h *AdminHandler) readCollectionRecord(name string) (collectionRecord, bool, error) {
	coll := h.recordCollectionFor(name)
	history, err := coll.History(adminRecordID(name))
	if err != nil {
		return collectionRecord{}, false, err
	}
	if len(history) == 0 {
		return collectionRecord{}, false, nil
	}
	latest := history[len(history)-1]
	if latest.Tombstone {
		return collectionRecord{}, false, nil
	}
	var rec collectionRecord
	if err := json.Unmarshal([]byte(latest.Text), &rec); err != nil {
		return collectionRecord{}, false, err
	}
	// Overwrite whatever HLC the blob carried with the authoritative one
	// from the first version's Lamport clock.
	rec.CreatedHLC = history[0].LamportClock
	return rec, true, nil
}

// writeCollectionRecord materializes the target collection with the
// declared bit width and writes the admin record JSON under the
// reserved id. Returns the HLC assigned to the freshly-written record.
func (h *AdminHandler) writeCollectionRecord(req createCollectionRequest) (collectionRecord, error) {
	// Materialize the target collection so callers who only read it back
	// via coll.Put / coll.Search will observe the declared bit width.
	_ = h.clients.WriteClient().Collection(req.Name, corkscrewdb.WithBitWidth(req.BitWidth))

	// Assemble the record JSON. CreatedHLC gets filled in after the Put
	// from the tail of history, so we serialize with a zero value here.
	rec := collectionRecord{
		Name:      req.Name,
		BitWidth:  req.BitWidth,
		Retention: req.Retention,
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return collectionRecord{}, err
	}

	coll := h.recordCollectionFor(req.Name)
	if err := coll.Put(adminRecordID(req.Name), corkscrewdb.Entry{Text: string(body)}); err != nil {
		return collectionRecord{}, err
	}
	history, err := coll.History(adminRecordID(req.Name))
	if err != nil {
		return collectionRecord{}, err
	}
	if len(history) == 0 {
		return collectionRecord{}, errEmptyHistory
	}
	// Create-time HLC is the first version's Lamport clock; under
	// writeCollectionRecord this is the version we just wrote, but we
	// read it from history[0] so future rewrites don't perturb it.
	rec.CreatedHLC = history[0].LamportClock

	if err := h.appendRegistry(req.Name); err != nil {
		return collectionRecord{}, err
	}
	return rec, nil
}

// errEmptyHistory signals a post-Put history read that returned no
// versions. Not expected in practice; kept as a sentinel for tests.
var errEmptyHistory = &adminInternalError{"empty history after put"}

// adminInternalError is a local error type so handler code can log the
// underlying detail without leaking it to clients.
type adminInternalError struct{ msg string }

func (e *adminInternalError) Error() string { return e.msg }

// appendRegistry adds name to the registry entry (idempotent). The
// registry is stored as a JSON array of strings in the agent-memory
// collection. A missing registry is treated as empty.
func (h *AdminHandler) appendRegistry(name string) error {
	coll := h.clients.WriteClient().Collection(systemCollectionName)
	names, err := h.readRegistry()
	if err != nil {
		return err
	}
	for _, n := range names {
		if n == name {
			return nil
		}
	}
	names = append(names, name)
	body, err := json.Marshal(names)
	if err != nil {
		return err
	}
	return coll.Put(registryID, corkscrewdb.Entry{Text: string(body)})
}

// readRegistry returns the names of admin-registered collections. An
// absent or tombstoned registry is treated as an empty list.
func (h *AdminHandler) readRegistry() ([]string, error) {
	coll := h.clients.WriteClient().Collection(systemCollectionName)
	history, err := coll.History(registryID)
	if err != nil {
		return nil, err
	}
	if len(history) == 0 {
		return nil, nil
	}
	latest := history[len(history)-1]
	if latest.Tombstone {
		return nil, nil
	}
	text := strings.TrimSpace(latest.Text)
	if text == "" {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal([]byte(text), &names); err != nil {
		return nil, err
	}
	return names, nil
}

// handleCreateCollection implements POST /admin/collections. It is
// idempotent on an exact {name, bit_width, retention} match, 409 on a
// mismatch, and 200 on a first-time create.
func (h *AdminHandler) handleCreateCollection(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdminContext(w, r) {
		return
	}
	var req createCollectionRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}
	if req.BitWidth <= 0 {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}
	if strings.TrimSpace(req.Retention) == "" {
		h.writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	existing, found, err := h.readCollectionRecord(req.Name)
	if err != nil {
		h.logger.Printf("admin: read record: %v", err)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	if found {
		if existing.BitWidth != req.BitWidth || existing.Retention != req.Retention {
			h.writeJSON(w, http.StatusConflict, map[string]any{
				"error": "collection config mismatch",
				"existing": map[string]any{
					"name":        existing.Name,
					"bit_width":   existing.BitWidth,
					"retention":   existing.Retention,
					"created_hlc": existing.CreatedHLC,
				},
				"requested": map[string]any{
					"name":      req.Name,
					"bit_width": req.BitWidth,
					"retention": req.Retention,
				},
			})
			return
		}
		// Idempotent echo — same HLC as the original create.
		h.writeJSON(w, http.StatusOK, existing)
		return
	}

	created, err := h.writeCollectionRecord(req)
	if err != nil {
		h.logger.Printf("admin: write record: %v", err)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	h.writeJSON(w, http.StatusOK, created)
}

// handleListCollections implements GET /admin/collections. It walks the
// admin registry and returns the current record for each listed name.
// If a registry entry points at a name whose record has been deleted
// (an edge case that shouldn't happen in practice) we silently skip it
// — the registry is a best-effort index, not a source of truth.
func (h *AdminHandler) handleListCollections(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdminContext(w, r) {
		return
	}
	names, err := h.readRegistry()
	if err != nil {
		h.logger.Printf("admin: read registry: %v", err)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	out := make([]collectionRecord, 0, len(names))
	for _, name := range names {
		rec, found, err := h.readCollectionRecord(name)
		if err != nil {
			h.logger.Printf("admin: read record %q: %v", name, err)
			h.writeError(w, http.StatusInternalServerError, "internal")
			return
		}
		if !found {
			// Registry mentions a name whose record is gone. Simplification:
			// skip it. A future cleanup pass could rewrite the registry.
			continue
		}
		out = append(out, rec)
	}
	h.writeJSON(w, http.StatusOK, map[string]any{"collections": out})
}

// handleShards implements GET /admin/shards. It proxies the primary's
// RemoteInfo().Shards as JSON. A nil slice is rendered as an empty
// array so the wire shape is stable.
func (h *AdminHandler) handleShards(w http.ResponseWriter, r *http.Request) {
	if !h.requireAdminContext(w, r) {
		return
	}
	info, err := h.clients.WriteClient().RemoteInfo()
	if err != nil {
		h.logger.Printf("admin: remote info: %v", err)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}
	shards := info.Shards
	if shards == nil {
		shards = []corkscrewdb.ShardAssignment{}
	}
	h.writeJSON(w, http.StatusOK, map[string]any{"shards": shards})
}
