package framework

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestTelegramAdapter(t *testing.T) {
	// 1. Mock engine
	adapter := NewTelegramAdapter(&Engine{
		db: nil, // We'd mock DB if we needed full integration, but we can test decoding logic purely
	})

	// To test just decoding, we mock ServeHTTP with a fake DB.
	// Since we need DB for Idempotency, let's just write a direct parser test.
	
	payload := `{
		"update_id": 987654321,
		"message": {
			"message_id": 111,
			"chat": {
				"id": 123456789
			},
			"text": "/start"
		}
	}`

	req := httptest.NewRequest("POST", "/", bytes.NewBuffer([]byte(payload)))
	req.Header.Set("Content-Type", "application/json")

	var tUpdate TelegramUpdate
	err := json.NewDecoder(req.Body).Decode(&tUpdate)
	if err != nil {
		t.Fatalf("Failed to decode standard telegram update: %v", err)
	}

	if tUpdate.UpdateID != 987654321 {
		t.Errorf("Expected 987654321, got %d", tUpdate.UpdateID)
	}
	if tUpdate.Message.Chat.ID != 123456789 {
		t.Errorf("Expected chat ID 123456789, got %d", tUpdate.Message.Chat.ID)
	}
	if tUpdate.Message.Text != "/start" {
		t.Errorf("Expected text /start, got %s", tUpdate.Message.Text)
	}
	
	_ = adapter // avoid unused
}
