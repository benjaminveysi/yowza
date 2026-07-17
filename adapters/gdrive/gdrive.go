package gdrive

import (
	"io"
	"net/http"

	"github.com/yowza/yowza/framework"
)

// GDriveWebhook represents a generic push notification from Google Drive.
// Since GDrive sends headers rather than a structured JSON body for resource state changes,
// we parse the headers to build the payload.
type GDriveWebhook struct {
	ChannelID     string `json:"channel_id"`
	ResourceID    string `json:"resource_id"`
	ResourceState string `json:"resource_state"`
	MessageNum    string `json:"message_num"`
}

// GDriveAdapter wraps the generic Yowza Engine to handle native Google Drive Webhooks.
type GDriveAdapter struct {
	engine *framework.Engine
}

func NewGDriveAdapter(engine *framework.Engine) *GDriveAdapter {
	return &GDriveAdapter{
		engine: engine,
	}
}

// ServeHTTP implements the http.Handler interface for Google Drive push notifications.
func (gda *GDriveAdapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	channelID := r.Header.Get("X-Goog-Channel-ID")
	resourceID := r.Header.Get("X-Goog-Resource-ID")
	resourceState := r.Header.Get("X-Goog-Resource-State")
	messageNum := r.Header.Get("X-Goog-Message-Number")

	if channelID == "" || resourceID == "" {
		http.Error(w, "Missing Google Drive Headers", http.StatusBadRequest)
		return
	}

	// Read optional JSON body if present, capped at 10 MB
	// (see framework.go ServeHTTP). A body that exceeds the cap is a
	// bad request, not something to silently truncate and process.
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Cannot read body", http.StatusBadRequest)
		return
	}

	// Build the generic update
	genUpdate := framework.GenericUpdate{
		UpdateID:   channelID + ":" + messageNum, // Unique ID per message per channel
		UserID:     "gdrive_system",
		Text:       resourceState, // e.g. "sync", "add", "update", "trash"
		Provider:   "gdrive",
		RawPayload: bodyBytes,
	}

	// Idempotency Check
	// Atomic idempotency claim (see YowzaDB.PutIfAbsent).
	idempKey := "idemp:gdrive:" + genUpdate.UpdateID
	inserted, err := gda.engine.DB().PutIfAbsent(idempKey, []byte("1"))
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if !inserted {
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := &framework.Context{
		DB:     gda.engine.DB(),
		Update: genUpdate,
	}

	if gda.engine.Handler() != nil {
		gda.engine.Handler()(ctx)
	}

	if gda.engine.Manager() != nil {
		gda.engine.Manager().SyncWAL()
	}

	w.WriteHeader(http.StatusOK)
}
