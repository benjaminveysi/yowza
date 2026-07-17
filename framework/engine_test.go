package framework

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/yowza/yowza"
)

func TestFrameworkEngine(t *testing.T) {
	// Read the compiled Wasm binary
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to initialize YowzaDB: %v", err)
	}
	defer db.Close()

	engine := NewEngine(db, nil) // no cloud manager for these tests

	callCount := 0
	engine.OnMessage(func(ctx *Context) {
		callCount++
		
		if ctx.Update.Text == "set" {
			ctx.SetState("my_state")
		} else if ctx.Update.Text == "get" {
			state, _ := ctx.GetState()
			if state != "my_state" {
				t.Errorf("Expected state 'my_state', got '%s'", state)
			}
		}
	})

	// 1. Test First Update
	update1 := GenericUpdate{
		UpdateID: "req-1",
		UserID:   "user_123",
		Text:     "set",
	}
	body, _ := json.Marshal(update1)
	req := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
	w := httptest.NewRecorder()
	
	engine.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", w.Code)
	}
	if callCount != 1 {
		t.Errorf("Expected 1 call, got %d", callCount)
	}

	// 2. Test Duplicate Update (Idempotency Engine)
	reqDup := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
	wDup := httptest.NewRecorder()
	
	engine.ServeHTTP(wDup, reqDup)
	
	if wDup.Code != http.StatusOK {
		t.Errorf("Expected 200 OK for duplicate, got %d", wDup.Code)
	}
	if callCount != 1 {
		t.Errorf("Idempotency failed: handler was called %d times instead of 1", callCount)
	}

	// 3. Test Second Update (Get State)
	update2 := GenericUpdate{
		UpdateID: "req-2",
		UserID:   "user_123",
		Text:     "get",
	}
	body2, _ := json.Marshal(update2)
	req2 := httptest.NewRequest("POST", "/", bytes.NewBuffer(body2))
	w2 := httptest.NewRecorder()
	
	engine.ServeHTTP(w2, req2)
	
	if w2.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", w2.Code)
	}
	if callCount != 2 {
		t.Errorf("Expected 2 calls, got %d", callCount)
	}
}
