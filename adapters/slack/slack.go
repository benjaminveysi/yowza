package slack

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/yowza/yowza/framework"
)

// SlackEvent represents the inner event structure of a Slack webhook
type SlackEvent struct {
	Type    string `json:"type"`
	User    string `json:"user"`
	Text    string `json:"text"`
	Channel string `json:"channel"`
	Ts      string `json:"ts"` // Timestamp is often used as a unique identifier for messages
}

// SlackUpdate represents the root update object sent by Slack Events API.
type SlackUpdate struct {
	Token     string      `json:"token"`
	TeamID    string      `json:"team_id"`
	APIAppID  string      `json:"api_app_id"`
	Event     *SlackEvent `json:"event"`
	Type      string      `json:"type"`
	EventID   string      `json:"event_id"` // Unique ID for idempotency
	EventTime int64       `json:"event_time"`
	Challenge string      `json:"challenge"` // For URL verification
}

// SlackAdapter wraps the generic Yowza Engine to handle native Slack Webhooks.
type SlackAdapter struct {
	engine *framework.Engine
}

func NewSlackAdapter(engine *framework.Engine) *SlackAdapter {
	return &SlackAdapter{
		engine: engine,
	}
}

// ServeHTTP implements the http.Handler interface for Slack specific payloads.
func (sa *SlackAdapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 10 MB body cap (see framework.go ServeHTTP).
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Cannot read body", http.StatusBadRequest)
		return
	}

	var sUpdate SlackUpdate
	if err := json.Unmarshal(bodyBytes, &sUpdate); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// Slack URL Verification Handshake
	if sUpdate.Type == "url_verification" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(sUpdate.Challenge))
		return
	}

	// We only process message events
	if sUpdate.Event == nil || sUpdate.Event.Type != "message" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Ignore bot messages to prevent loops
	if sUpdate.Event.User == "" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Convert to GenericUpdate
	genUpdate := framework.GenericUpdate{
		UpdateID:   sUpdate.EventID, // Slack guarantees EventID uniqueness
		UserID:     sUpdate.Event.User,
		Text:       sUpdate.Event.Text,
		Provider:   "slack",
		RawPayload: bodyBytes,
	}

	// Idempotency Check: L1 Cache check via Yowza!
	// Atomic idempotency claim (see YowzaDB.PutIfAbsent).
	idempKey := "idemp:slack:" + genUpdate.UpdateID
	inserted, err := sa.engine.DB().PutIfAbsent(idempKey, []byte("1"))
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if !inserted {
		// Duplicate update detected. Drop it and return 200 OK.
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := &framework.Context{
		DB:     sa.engine.DB(),
		Update: genUpdate,
	}

	if sa.engine.Handler() != nil {
		sa.engine.Handler()(ctx)
	}

	// Synchronous WAL sync: see adapters/telegram for why the async
	// (go) form was removed.
	if sa.engine.Manager() != nil {
		sa.engine.Manager().SyncWAL()
	}

	w.WriteHeader(http.StatusOK)
}
