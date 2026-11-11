package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/yowza/yowza"
	"github.com/yowza/yowza/cluster"
	"github.com/yowza/yowza/security"
)

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// checkAuth verifies the Ed25519 signature over signedMessage against the
// Authorization header, and enforces the allowlist.
//   - If authKeysMap is empty, everyone is denied by default (401) unless
//     allowUnauthenticated is explicitly set, in which case any validly
//     signed request is accepted (the old behaviour for an empty map,
//     now opt-in only).
//   - If authKeysMap is non-empty, the public key must be in it AND the
//     signature must verify.
//
// signedMessage is the canonical bytes for the endpoint: the body for
// /put, security.CanonicalGetMessage(key) for /get, and so on. Signing
// the (empty) body for /get, /del or /join would sign nothing, so those
// endpoints must pass their canonical message here, never the body.
func checkAuth(r *http.Request, signedMessage []byte, authKeysMap map[string]bool, allowUnauthenticated bool) bool {
	pubKey, sig, err := security.ParseAuthHeader(r.Header.Get("Authorization"))
	if err != nil {
		return false
	}
	if len(authKeysMap) == 0 && !allowUnauthenticated {
		return false
	}
	if !security.VerifySignature(pubKey, signedMessage, sig) {
		return false
	}
	if len(authKeysMap) > 0 && !authKeysMap[pubKey] {
		return false
	}
	return true
}

func main() {
	port := flag.Int("port", 6379, "Port to listen on for the Yowza Daemon")
	raftID := flag.String("raft-id", "node0", "Raft Node ID")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7000", "Raft Bind Address")
	raftDir := flag.String("raft-dir", "./raft-data", "Raft Data Directory")
	raftBootstrap := flag.Bool("raft-bootstrap", false, "Bootstrap the Raft cluster")

	// Security config
	authorizedKeys := flag.String("auth-keys", "", "Comma-separated list of authorized Ed25519 public keys (hex)")
	allowUnauthenticated := flag.Bool("allow-unauthenticated", false, "Allow requests from any validly signed key when --auth-keys is empty (default: deny everyone when no authorized keys are configured)")
	flag.Parse()

	authKeysMap := make(map[string]bool)
	for _, k := range strings.Split(*authorizedKeys, ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			authKeysMap[k] = true
		}
	}

	// 1. Initialize Wasm Database
	// Attempt to load 64-bit Wasm first, fallback to 32-bit Wasm
	wasmBytes, err := os.ReadFile("yowza64.wasm")
	if err != nil {
		wasmBytes, err = os.ReadFile("build/yowza64.wasm")
	}

	if err != nil {
		// Fallback to 32-bit Wasm
		log.Println("yowza64.wasm not found. Falling back to 32-bit execution...")
		wasmBytes, err = os.ReadFile("yowza.wasm")
		if err != nil {
			wasmBytes, err = os.ReadFile("build/yowza.wasm")
			if err != nil {
				log.Fatalf("Could not read Wasm engine binaries. Please compile with 'make all'. Error: %v", err)
			}
		}
	} else {
		log.Println("Loaded 64-bit WebAssembly Engine successfully.")
	}

	db, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		log.Fatalf("Failed to initialize Yowza DB: %v", err)
	}
	defer db.Close()

	// 2. Setup Raft Node
	raftNode, err := cluster.SetupRaftNode(*raftID, *raftAddr, *raftDir, *raftBootstrap, db)
	if err != nil {
		log.Fatalf("Failed to setup Raft node: %v", err)
	}

	// 3. Setup REST API over TCP
	mux := http.NewServeMux()

	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// 1MB request body limit
		r.Body = http.MaxBytesReader(w, r.Body, 1048576)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body (max 1MB)", http.StatusInternalServerError)
			return
		}
		r.Body.Close()

		// Auth check: signature over the body (which carries key+value)
		// must be valid AND the public key must be authorized.
		if !checkAuth(r, body, authKeysMap, *allowUnauthenticated) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var req PutRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		// Apply via Raft instead of local DB directly
		walEntry := yowza.WalEntry{
			Opcode: yowza.OpcodePut,
			Key:    []byte(req.Key),
			Value:  []byte(req.Value),
		}

		logData, _ := json.Marshal(walEntry)
		f := raftNode.Apply(logData, 500*1000*1000) // 500ms timeout

		if err := f.Error(); err != nil {
			http.Error(w, fmt.Sprintf("Raft apply failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1048576)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusInternalServerError)
			return
		}
		r.Body.Close()

		// The key comes from the query string, so the signature must
		// cover it (security.CanonicalGetMessage), not the empty body.
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing key", http.StatusBadRequest)
			return
		}
		_ = body // Body is consumed but not signed for /get.

		if !checkAuth(r, security.CanonicalGetMessage(key), authKeysMap, *allowUnauthenticated) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		val, err := db.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if val == nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(val)
	})

	mux.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1048576)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusInternalServerError)
			return
		}
		r.Body.Close()

		// As for /get, the signature must cover the query-string key.
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing key", http.StatusBadRequest)
			return
		}
		_ = body // Body is consumed but not signed for /del.

		if !checkAuth(r, security.CanonicalDeleteMessage(key), authKeysMap, *allowUnauthenticated) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Apply via Raft
		walEntry := yowza.WalEntry{
			Opcode: yowza.OpcodeDel,
			Key:    []byte(key),
		}

		logData, _ := json.Marshal(walEntry)
		f := raftNode.Apply(logData, 500*1000*1000)

		if err := f.Error(); err != nil {
			http.Error(w, fmt.Sprintf("Raft apply failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1048576)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusInternalServerError)
			return
		}
		r.Body.Close()

		// id+addr come from the query string, so the signature must
		// cover them (security.CanonicalJoinMessage), not the empty body.
		nodeID := r.URL.Query().Get("id")
		nodeAddr := r.URL.Query().Get("addr")

		if nodeID == "" || nodeAddr == "" {
			http.Error(w, "Missing id or addr", http.StatusBadRequest)
			return
		}
		_ = body // Body is consumed but not signed for /join.

		if !checkAuth(r, security.CanonicalJoinMessage(nodeID, nodeAddr), authKeysMap, *allowUnauthenticated) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		f := raftNode.AddVoter(cluster.ServerID(nodeID), cluster.ServerAddress(nodeAddr), 0, 0)
		if err := f.Error(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Joined"))
	})

	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("[Yowzad] Daemon listening on %s (PID: %d)\n", addr, os.Getpid())

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
