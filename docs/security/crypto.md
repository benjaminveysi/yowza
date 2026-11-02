# ChaCha20-Poly1305 Encryption

## Data at Rest
When pushing Snapshots or WAL segments to public cloud storage (like S3) or untrusted intermediary nodes, data must be encrypted at rest.

## AEAD Cipher
Yowza! employs `ChaCha20-Poly1305` as its primary encryption algorithm (`security/crypto.go`). It is an Authenticated Encryption with Associated Data (AEAD) cipher.

- **Speed:** ChaCha20 is exceptionally fast, especially on edge hardware lacking hardware-accelerated AES instructions (like many ARM-based serverless nodes).
- **Authentication:** Poly1305 ensures that the ciphertext has not been tampered with. If a single bit is flipped in the S3 bucket, decryption will fail, preventing the database from ingesting corrupted state.

Before any byte leaves the edge node, it is encrypted. When a node bootstraps, it downloads the ciphertext and decrypts it locally in memory before mapping it to the Wasm Engine.
