# Ed25519 Authentication

## Zero-Trust Architecture
Yowza! is designed for the edge, where networks are fundamentally untrusted. Whether you are running the REST Daemon (`yowzad`) or syncing via Redis, every request must be cryptographically verified.

## Ed25519 Signatures
We utilize the `crypto/ed25519` package for ultra-fast public-key cryptography.
- **Keys:** Every node and client is configured with a Private/Public key pair.
- **Signing:** When a client issues a request (e.g., pushing a WAL sync), it generates an Ed25519 signature over the payload using its Private Key.
- **Verification:** The receiving node validates the signature against the known Public Key of the cluster.

If the signature is invalid or missing, the request is immediately dropped with a `401 Unauthorized`. This prevents malicious actors from injecting false state into the database.
