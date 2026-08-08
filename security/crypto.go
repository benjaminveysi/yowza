package security

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/chacha20poly1305"
)

// GenerateEd25519KeyPair generates a new public/private key pair for API authentication.
func GenerateEd25519KeyPair() (string, string, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", err
	}
	return hex.EncodeToString(pub), hex.EncodeToString(priv), nil
}

// SignPayload creates an Ed25519 signature for a given payload (e.g., HTTP request body).
func SignPayload(privateKeyHex string, payload []byte) (string, error) {
	privBytes, err := hex.DecodeString(privateKeyHex)
	if err != nil {
		return "", fmt.Errorf("invalid private key: %v", err)
	}

	if len(privBytes) != ed25519.PrivateKeySize {
		return "", errors.New("invalid private key size")
	}

	priv := ed25519.PrivateKey(privBytes)
	sig := ed25519.Sign(priv, payload)

	return hex.EncodeToString(sig), nil
}

// VerifySignature validates that a payload was signed by the owner of the public key.
func VerifySignature(publicKeyHex string, payload []byte, signatureHex string) bool {
	pubBytes, err := hex.DecodeString(publicKeyHex)
	if err != nil || len(pubBytes) != ed25519.PublicKeySize {
		return false
	}

	sigBytes, err := hex.DecodeString(signatureHex)
	if err != nil || len(sigBytes) != ed25519.SignatureSize {
		return false
	}

	pub := ed25519.PublicKey(pubBytes)
	return ed25519.Verify(pub, payload, sigBytes)
}

// GenerateEncryptionKey generates a secure 32-byte key for ChaCha20-Poly1305.
func GenerateEncryptionKey() (string, error) {
	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := rand.Read(key); err != nil {
		return "", err
	}
	return hex.EncodeToString(key), nil
}

// EncryptData encrypts WALs or Snapshots for Data-at-Rest security using ChaCha20-Poly1305.
func EncryptData(keyHex string, plaintext []byte) ([]byte, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != chacha20poly1305.KeySize {
		return nil, errors.New("invalid encryption key")
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	// We use XChaCha20-Poly1305 which allows 24-byte random nonces safely.
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	ciphertext := aead.Seal(nil, nonce, plaintext, nil)

	// Prepend nonce to ciphertext for decryption
	return append(nonce, ciphertext...), nil
}

// DecryptData decrypts Data-at-Rest.
func DecryptData(keyHex string, encryptedData []byte) ([]byte, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != chacha20poly1305.KeySize {
		return nil, errors.New("invalid encryption key")
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	nonceSize := aead.NonceSize()
	if len(encryptedData) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce := encryptedData[:nonceSize]
	ciphertext := encryptedData[nonceSize:]

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed (wrong key or corrupted data): %v", err)
	}

	return plaintext, nil
}

// Canonical messages for yowzad authentication (step 3).
//
// The daemon's /put endpoint signs the request body, which is correct
// because the body carries the key and value. But /get and /del take the
// key from the query string and /join takes id+addr from the query string,
// while their bodies are empty -- so signing only the body signed nothing
// at all, and any self-generated key could replay a request for any key.
// These canonical strings bind the signature to the actual parameters:
//
//	Get:  "yowzad:get\n" + key
//	Delete: "yowzad:del\n" + key
//	Join: "yowzad:join\n" + id + "\n" + addr
//
// The "\n" separator (not ":") is used because addr itself contains ":"
// (host:port). The server (cmd/yowzad) and the client (cmd/yowzactl) must
// sign exactly these bytes; they are defined here, in the shared security
// package, so the two sides cannot drift apart. Do not change these
// strings without updating both sides at once: old signatures would
// stop verifying.
func CanonicalGetMessage(key string) []byte {
	return []byte("yowzad:get\n" + key)
}

func CanonicalDeleteMessage(key string) []byte {
	return []byte("yowzad:del\n" + key)
}

func CanonicalJoinMessage(id, addr string) []byte {
	return []byte("yowzad:join\n" + id + "\n" + addr)
}

// BuildAuthHeader construct the authorization header string
func BuildAuthHeader(pubKey, signature string) string {
	return fmt.Sprintf("Ed25519 pub=%s,sig=%s", pubKey, signature)
}

// ParseAuthHeader extracts the public key and signature from the authorization header
func ParseAuthHeader(header string) (pubKey, signature string, err error) {
	if !strings.HasPrefix(header, "Ed25519 ") {
		return "", "", errors.New("invalid auth scheme")
	}

	parts := strings.Split(strings.TrimPrefix(header, "Ed25519 "), ",")
	if len(parts) != 2 {
		return "", "", errors.New("invalid auth format")
	}

	for _, p := range parts {
		kv := strings.Split(p, "=")
		if len(kv) != 2 {
			continue
		}
		if kv[0] == "pub" {
			pubKey = kv[1]
		} else if kv[0] == "sig" {
			signature = kv[1]
		}
	}

	if pubKey == "" || signature == "" {
		return "", "", errors.New("missing pub or sig in auth header")
	}

	return pubKey, signature, nil
}
