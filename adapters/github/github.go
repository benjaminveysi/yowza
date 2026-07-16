package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"

	"github.com/yowza/yowza/framework"
)

// GitHubPayload represents a generic GitHub Webhook payload
type GitHubPayload struct {
	Action     string `json:"action"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
}

type GitHubAdapter struct {
	engine *framework.Engine
	secret string
}

func NewGitHubAdapter(engine *framework.Engine, secret string) *GitHubAdapter {
	return &GitHubAdapter{
		engine: engine,
		secret: secret,
	}
}

// verifySignature validates the HMAC payload signature.
// Fail closed: if no secret is configured, no signature can verify, so
// every webhook is rejected. The old code returned true here, which
// meant "no secret configured" silently meant "accept any webhook".
func (ga *GitHubAdapter) verifySignature(payload []byte, signatureHeader string) bool {
	if ga.secret == "" {
		return false
	}

	if len(signatureHeader) < 8 || signatureHeader[:7] != "sha256=" {
		return false
	}

	signatureHex := signatureHeader[7:]
	mac := hmac.New(sha256.New, []byte(ga.secret))
	mac.Write(payload)
	expectedMAC := hex.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(signatureHex), []byte(expectedMAC))
}

func (ga *GitHubAdapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	signatureHeader := r.Header.Get("X-Hub-Signature-256")
	if !ga.verifySignature(bodyBytes, signatureHeader) {
		http.Error(w, "invalid request signature", http.StatusUnauthorized)
		return
	}

	eventID := r.Header.Get("X-GitHub-Delivery")
	eventType := r.Header.Get("X-GitHub-Event")

	if eventID == "" {
		http.Error(w, "Missing GitHub Event ID", http.StatusBadRequest)
		return
	}

	var payload GitHubPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	genUpdate := framework.GenericUpdate{
		UpdateID:   eventID,
		UserID:     payload.Sender.Login,
		Text:       eventType + ":" + payload.Action, // e.g. "issues:opened"
		Provider:   "github",
		RawPayload: bodyBytes,
	}

	// Atomic idempotency claim (see YowzaDB.PutIfAbsent).
	idempKey := "idemp:github:" + genUpdate.UpdateID
	inserted, err := ga.engine.DB().PutIfAbsent(idempKey, []byte("1"))
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if !inserted {
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := &framework.Context{
		DB:     ga.engine.DB(),
		Update: genUpdate,
	}

	if ga.engine.Handler() != nil {
		ga.engine.Handler()(ctx)
	}

	if ga.engine.Manager() != nil {
		ga.engine.Manager().SyncWAL()
	}

	w.WriteHeader(http.StatusOK)
}
