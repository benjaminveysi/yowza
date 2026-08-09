package security

import (
	"bytes"
	"testing"
)

func TestEd25519Auth(t *testing.T) {
	pub, priv, err := GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("Key generation failed: %v", err)
	}

	payload := []byte(`{"key":"user:1", "value":"Alice"}`)

	sig, err := SignPayload(priv, payload)
	if err != nil {
		t.Fatalf("Signing failed: %v", err)
	}

	// Verify valid signature
	if !VerifySignature(pub, payload, sig) {
		t.Errorf("Valid signature was rejected")
	}

	// Verify invalid payload
	badPayload := []byte(`{"key":"user:1", "value":"Bob"}`)
	if VerifySignature(pub, badPayload, sig) {
		t.Errorf("Invalid payload was accepted")
	}

	// Verify Auth Header Parsing
	header := BuildAuthHeader(pub, sig)
	pPub, pSig, err := ParseAuthHeader(header)
	if err != nil {
		t.Fatalf("ParseAuthHeader failed: %v", err)
	}
	if pPub != pub || pSig != sig {
		t.Errorf("Auth header parsing mismatch")
	}
}

func TestChaCha20Encryption(t *testing.T) {
	key, err := GenerateEncryptionKey()
	if err != nil {
		t.Fatalf("Key generation failed: %v", err)
	}

	plaintext := []byte("secret database snapshot data")

	ciphertext, err := EncryptData(key, plaintext)
	if err != nil {
		t.Fatalf("Encryption failed: %v", err)
	}

	if bytes.Equal(plaintext, ciphertext) {
		t.Errorf("Ciphertext is identical to plaintext")
	}

	decrypted, err := DecryptData(key, ciphertext)
	if err != nil {
		t.Fatalf("Decryption failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("Decrypted text does not match original plaintext")
	}

	// Test bad key
	badKey, _ := GenerateEncryptionKey()
	_, err = DecryptData(badKey, ciphertext)
	if err == nil {
		t.Errorf("Decryption succeeded with incorrect key")
	}
}
