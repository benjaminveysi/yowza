package framework

import (
	"encoding/json"
	"net/http"

	"github.com/yowza/yowza"
	"github.com/yowza/yowza/cloud"
)

// GenericUpdate represents a normalized incoming message from any messenger.
// GenericUpdate represents a normalized incoming message from any messenger or service.
type GenericUpdate struct {
	UpdateID string `json:"update_id"`
	UserID   string `json:"user_id"`
	Text     string `json:"text"`
	// Additional fields for dynamic payloads
	RawPayload []byte
	Provider   string
}

// Context is passed to developers to handle the current update and manage state.
type Context struct {
	DB     *yowza.YowzaDB
	Update GenericUpdate
}

// Engine represents the core framework server
type Engine struct {
	db      *yowza.YowzaDB
	manager *cloud.Manager
	handler func(ctx *Context)
}

// NewEngine creates a new framework engine. It takes an optional cloud manager
// to automatically flush WALs after processing requests.
func NewEngine(db *yowza.YowzaDB, manager *cloud.Manager) *Engine {
	return &Engine{
		db:      db,
		manager: manager,
	}
}

// DB returns the associated database instance.
func (e *Engine) DB() *yowza.YowzaDB {
	return e.db
}

// Manager returns the associated cloud manager.
func (e *Engine) Manager() *cloud.Manager {
	return e.manager
}

// Handler returns the main handler function.
func (e *Engine) Handler() func(ctx *Context) {
	return e.handler
}

// OnMessage registers the main handler function for incoming updates.
func (e *Engine) OnMessage(handler func(ctx *Context)) {
	e.handler = handler
}

// ServeHTTP implements the http.Handler interface.
// It acts as the Idempotency Engine, checking for duplicate updates.
func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 10 MB body cap: an unbounded webhook body is a trivial memory
	// exhaustion vector against a handler that decodes it in full.
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var update GenericUpdate
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	if update.UpdateID == "" {
		http.Error(w, "Missing update_id", http.StatusBadRequest)
		return
	}

	// Idempotency check + claim, atomically: PutIfAbsent checks and
	// inserts under a single db.mu.Lock(). The old Get-then-Put used two
	// separate locks, so two concurrent webhooks could both observe
	// "missing" and both run the handler.
	// Key format: idemp:update_id
	// Note: the handler below returns no error, so there is no
	// "release the key on handler error" step to add here: the current
	// behaviour keeps the key once claimed (a retry is dropped), and
	// changing that would turn a duplicate-drop into a re-run. Kept.
	idempKey := "idemp:" + update.UpdateID
	inserted, err := e.db.PutIfAbsent(idempKey, []byte("1"))
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if !inserted {
		// Duplicate update detected. Drop it and return 200 OK.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK - Duplicate Dropped"))
		return
	}

	ctx := &Context{
		DB:     e.db,
		Update: update,
	}

	if e.handler != nil {
		e.handler(ctx)
	}

	// Trigger WAL sync if cloud manager is connected. This is
	// synchronous on purpose: an async (go) call would let concurrent
	// requests interleave their WAL batches and scramble WAL order.
	// Manager.SyncWAL itself is additionally serialized by a mutex.
	if e.manager != nil {
		e.manager.SyncWAL()
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
