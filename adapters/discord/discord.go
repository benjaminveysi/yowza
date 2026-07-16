package discord

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/yowza/yowza/framework"
)

// DiscordInteraction represents a Discord Webhook Interaction
type DiscordInteraction struct {
	Type  int    `json:"type"`
	ID    string `json:"id"`
	Token string `json:"token"`
	Data  struct {
		Name string `json:"name"`
	} `json:"data"`
	User struct {
		ID string `json:"id"`
	} `json:"user"`
	Member struct {
		User struct {
			ID string `json:"id"`
		} `json:"user"`
	} `json:"member"`
}

type DiscordAdapter struct {
	engine    *framework.Engine
	publicKey ed25519.PublicKey // Required by Discord to verify requests
}

func NewDiscordAdapter(engine *framework.Engine, pubKeyHex string) (*DiscordAdapter, error) {
	pk, err := hex.DecodeString(pubKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid discord public key: %v", err)
	}

	return &DiscordAdapter{
		engine:    engine,
		publicKey: pk,
	}, nil
}

// verifyRequest validates the Ed25519 signature sent by Discord and the
// freshness of X-Signature-Timestamp. Discord signs timestamp+body, but
// without a freshness check a captured signed request replays forever:
// a timestamp more than 5 minutes from now (allowing for clock skew) or
// more than 5 minutes in the past is rejected before the signature is
// even checked.
func (da *DiscordAdapter) verifyRequest(r *http.Request, body []byte) bool {
	signatureHex := r.Header.Get("X-Signature-Ed25519")
	timestamp := r.Header.Get("X-Signature-Timestamp")

	if signatureHex == "" || timestamp == "" {
		return false
	}

	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return false
	}
	const freshnessWindow = 5 * 60 // seconds
	now := time.Now().Unix()
	if ts < now-freshnessWindow || ts > now+freshnessWindow {
		return false
	}

	signature, err := hex.DecodeString(signatureHex)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return false
	}

	msg := append([]byte(timestamp), body...)
	return ed25519.Verify(da.publicKey, msg, signature)
}

func (da *DiscordAdapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	// Discord mandates Ed25519 signature validation
	if !da.verifyRequest(r, bodyBytes) {
		http.Error(w, "invalid request signature", http.StatusUnauthorized)
		return
	}

	var dInteraction DiscordInteraction
	if err := json.Unmarshal(bodyBytes, &dInteraction); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// Ping interaction
	if dInteraction.Type == 1 {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"type": 1}`)) // PONG
		return
	}

	userID := dInteraction.User.ID
	if userID == "" {
		userID = dInteraction.Member.User.ID
	}

	genUpdate := framework.GenericUpdate{
		UpdateID:   dInteraction.ID,
		UserID:     userID,
		Text:       dInteraction.Data.Name, // Treat slash command name as text
		Provider:   "discord",
		RawPayload: bodyBytes,
	}

	// Atomic idempotency claim (see YowzaDB.PutIfAbsent): the old
	// Get-then-Put used two locks and two concurrent webhooks could
	// both run the handler.
	idempKey := "idemp:discord:" + genUpdate.UpdateID
	inserted, err := da.engine.DB().PutIfAbsent(idempKey, []byte("1"))
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if !inserted {
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := &framework.Context{
		DB:     da.engine.DB(),
		Update: genUpdate,
	}

	if da.engine.Handler() != nil {
		da.engine.Handler()(ctx)
	}

	if da.engine.Manager() != nil {
		da.engine.Manager().SyncWAL()
	}

	// Acknowledge interaction to Discord
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"type": 4, "data": {"content": "Command received"}}`))
}
