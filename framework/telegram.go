package framework

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// TelegramChat represents a chat in the Telegram API.
type TelegramChat struct {
	ID int64 `json:"id"`
}

// TelegramMessage represents a message in the Telegram API.
type TelegramMessage struct {
	MessageID int64        `json:"message_id"`
	Chat      TelegramChat `json:"chat"`
	Text      string       `json:"text"`
}

// TelegramUpdate represents the root update object sent by Telegram Webhooks.
type TelegramUpdate struct {
	UpdateID int64            `json:"update_id"`
	Message  *TelegramMessage `json:"message"`
}

// TelegramAdapter wraps the generic Yowza Engine to handle native Telegram Webhooks.
type TelegramAdapter struct {
	engine *Engine
}

func NewTelegramAdapter(engine *Engine) *TelegramAdapter {
	return &TelegramAdapter{
		engine: engine,
	}
}

// ServeHTTP implements the http.Handler interface for Telegram specific payloads.
func (ta *TelegramAdapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 10 MB body cap (see framework.go ServeHTTP).
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var tUpdate TelegramUpdate
	if err := json.NewDecoder(r.Body).Decode(&tUpdate); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// We only care about messages for this simple adapter
	if tUpdate.Message == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Convert to GenericUpdate
	genUpdate := GenericUpdate{
		UpdateID: fmt.Sprintf("%d", tUpdate.UpdateID),
		UserID:   fmt.Sprintf("%d", tUpdate.Message.Chat.ID),
		Text:     tUpdate.Message.Text,
	}

	// Idempotency: atomic claim via PutIfAbsent, not Get+Put. The old
	// two-call form had a TOCTOU race: two concurrent webhooks for the
	// same update could both read "not present" and both run the handler.
	idempKey := "idemp:" + genUpdate.UpdateID
	inserted, err := ta.engine.db.PutIfAbsent(idempKey, []byte("1"))
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
		DB:     ta.engine.db,
		Update: genUpdate,
	}

	if ta.engine.handler != nil {
		ta.engine.handler(ctx)
	}

	// Synchronous WAL sync: see framework.go ServeHTTP for why the
	// async (go) form was removed.
	if ta.engine.manager != nil {
		ta.engine.manager.SyncWAL()
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
