package telegram

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/yowza/yowza/framework"
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
	engine *framework.Engine
}

func NewTelegramAdapter(engine *framework.Engine) *TelegramAdapter {
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
	genUpdate := framework.GenericUpdate{
		UpdateID: fmt.Sprintf("%d", tUpdate.UpdateID),
		UserID:   fmt.Sprintf("%d", tUpdate.Message.Chat.ID),
		Text:     tUpdate.Message.Text,
	}

	// Idempotency Check: L1 Cache check via Yowza!
	// Atomic idempotency claim (see YowzaDB.PutIfAbsent).
	idempKey := "idemp:" + genUpdate.UpdateID
	inserted, err := ta.engine.DB().PutIfAbsent(idempKey, []byte("1"))
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

	ctx := &framework.Context{
		DB:     ta.engine.DB(),
		Update: genUpdate,
	}

	if ta.engine.Handler() != nil {
		ta.engine.Handler()(ctx)
	}

	// Synchronous WAL sync: an async (go) call would scramble WAL order
	// across concurrent requests. Manager.SyncWAL is also mutex-serialized.
	if ta.engine.Manager() != nil {
		ta.engine.Manager().SyncWAL()
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
