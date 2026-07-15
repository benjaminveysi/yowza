package slack

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/yowza/yowza"
	"github.com/yowza/yowza/framework"
)

func TestSlackAdapter(t *testing.T) {
	wasmBytes, err := os.ReadFile("../../build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer db.Close()

	engine := framework.NewEngine(db, nil)

	handled := false
	engine.OnMessage(func(ctx *framework.Context) {
		if ctx.Update.Text == "hello slack" {
			handled = true
		}
	})

	adapter := NewSlackAdapter(engine)

	// Test URL Verification
	challengePayload := []byte(`{"type": "url_verification", "challenge": "ch12345"}`)
	req1 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(challengePayload))
	req1.Header.Set("Content-Type", "application/json")
	rr1 := httptest.NewRecorder()

	adapter.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusOK {
		t.Errorf("Expected status 200 for URL verification, got %v", rr1.Code)
	}
	if rr1.Body.String() != "ch12345" {
		t.Errorf("Expected challenge 'ch12345', got '%v'", rr1.Body.String())
	}

	// Test Event handling
	eventPayload := []byte(`{
		"type": "event_callback",
		"event_id": "ev_123",
		"event": {
			"type": "message",
			"user": "U123",
			"text": "hello slack"
		}
	}`)
	req2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(eventPayload))
	req2.Header.Set("Content-Type", "application/json")
	rr2 := httptest.NewRecorder()

	adapter.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Errorf("Expected status 200 for event, got %v", rr2.Code)
	}
	if !handled {
		t.Errorf("Engine handler was not called for valid Slack event")
	}

	// Test Idempotency (Sending exact same event again)
	handled = false
	req3 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(eventPayload))
	req3.Header.Set("Content-Type", "application/json")
	rr3 := httptest.NewRecorder()

	adapter.ServeHTTP(rr3, req3)

	if rr3.Code != http.StatusOK {
		t.Errorf("Expected status 200 for duplicate event, got %v", rr3.Code)
	}
	if handled {
		t.Errorf("Engine handler was incorrectly called for a duplicate event")
	}
}
