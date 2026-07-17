package gdrive

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/yowza/yowza"
	"github.com/yowza/yowza/framework"
)

func TestGDriveAdapter(t *testing.T) {
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
		if ctx.Update.Text == "update" && ctx.Update.Provider == "gdrive" {
			handled = true
		}
	})

	adapter := NewGDriveAdapter(engine)

	// Test Event handling
	req1 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer([]byte{}))
	req1.Header.Set("X-Goog-Channel-ID", "ch123")
	req1.Header.Set("X-Goog-Resource-ID", "res456")
	req1.Header.Set("X-Goog-Resource-State", "update")
	req1.Header.Set("X-Goog-Message-Number", "1")

	rr1 := httptest.NewRecorder()
	adapter.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %v", rr1.Code)
	}
	if !handled {
		t.Errorf("Engine handler was not called for valid GDrive event")
	}

	// Test Idempotency
	handled = false
	req2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer([]byte{}))
	req2.Header.Set("X-Goog-Channel-ID", "ch123")
	req2.Header.Set("X-Goog-Resource-ID", "res456")
	req2.Header.Set("X-Goog-Resource-State", "update")
	req2.Header.Set("X-Goog-Message-Number", "1") // Same message number = duplicate

	rr2 := httptest.NewRecorder()
	adapter.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Errorf("Expected status 200 for duplicate event, got %v", rr2.Code)
	}
	if handled {
		t.Errorf("Engine handler was incorrectly called for a duplicate event")
	}
}
