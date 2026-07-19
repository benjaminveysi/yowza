package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"

	"github.com/yowza/yowza/container"
	"github.com/yowza/yowza/security"
)

func printUsage() {
	fmt.Println("Usage: yowzactl <command> [arguments]")
	fmt.Println("Commands:")
	fmt.Println("  start     Launch the Yowza Daemon inside an isolated container sandbox")
	fmt.Println("  keygen    Generate a new Ed25519 Keypair for secure API requests")
	fmt.Println("  put       Put a key-value pair securely via the Daemon API")
	fmt.Println("  get       Get a value by key securely via the Daemon API")
	fmt.Println("  del       Delete a key securely via the Daemon API")
	fmt.Println("  join      Ask a Daemon to add a Raft voter securely via the Daemon API")
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "start":
		startCmd := flag.NewFlagSet("start", flag.ExitOnError)
		port := startCmd.String("port", "6379", "Port for yowzad to listen on")
		startCmd.Parse(os.Args[2:])

		// Run a PREBUILT daemon binary inside the sandbox, never
		// `go run`: go run needs the Go toolchain and the host source
		// tree inside the sandbox, which defeats the sandbox.
		//
		// Deliberate deviation from the audit's "./yowzad" suggestion:
		// in this repo ./yowzad is the C TCP daemon (built by make from
		// src/yowzad.c, positional port argument, default 6380), NOT
		// the Go HTTP daemon that yowzactl's put/get/del/join commands
		// talk to. Running ./yowzad with "-port 6379" would make the C
		// daemon atoi("-port") == 0 and silently listen on a random
		// port, while the HTTP clients would still target 6379. The
		// Go daemon binary is therefore named yowzad-go to avoid
		// colliding with the C binary in the same directory.
		daemonBin := "./yowzad-go"
		if _, err := os.Stat(daemonBin); err != nil {
			// Last resort: yowzad-go in PATH.
			if p, lerr := exec.LookPath("yowzad-go"); lerr == nil {
				daemonBin = p
			} else {
				log.Fatalf("[Yowzactl] Prebuilt Go daemon binary not found: build it first with `go build -o yowzad-go ./cmd/yowzad`, then run `yowzactl start` again.")
			}
		}
		// Forward the user's extra arguments after the flags, instead
		// of dropping them: startCmd.Args() is what remains of os.Args
		// after flag parsing.
		cmd := append([]string{daemonBin, "-port", *port}, startCmd.Args()...)

		fmt.Println("[Yowzactl] Initializing Yowza-box Sandbox...")
		sandbox := container.NewSandbox(cmd...)
		// The daemon must listen on a host port, so it must NOT be
		// put in a fresh network namespace (whose loopback starts DOWN).
		sandbox.IsolateNetwork = false
		sandbox.MemoryLimitMB = 128

		if err := sandbox.Run(); err != nil {
			log.Fatalf("[Yowzactl] Container Error: %v\nMake sure to run with sudo for namespace isolation.", err)
		}

	case "keygen":
		pub, priv, err := security.GenerateEd25519KeyPair()
		if err != nil {
			log.Fatalf("Failed to generate keys: %v", err)
		}
		fmt.Printf("Public Key:  %s\n", pub)
		fmt.Printf("Private Key: %s\n", priv)
		fmt.Println("\nKeep your Private Key secret! Provide your Public Key to the yowzad server.")

	case "put":
		putCmd := flag.NewFlagSet("put", flag.ExitOnError)
		host := putCmd.String("host", "http://localhost:6379", "Daemon host")
		privKey := putCmd.String("privkey", "", "Your Ed25519 Private Key hex")
		pubKey := putCmd.String("pubkey", "", "Your Ed25519 Public Key hex")
		key := putCmd.String("k", "", "Key")
		val := putCmd.String("v", "", "Value")
		putCmd.Parse(os.Args[2:])

		if *privKey == "" || *pubKey == "" || *key == "" {
			fmt.Println("Missing required arguments. Use -privkey, -pubkey, -k, and -v.")
			os.Exit(1)
		}

		payload := map[string]string{"key": *key, "value": *val}
		bodyBytes, _ := json.Marshal(payload)

		sig, err := security.SignPayload(*privKey, bodyBytes)
		if err != nil {
			log.Fatalf("Signature failed: %v", err)
		}

		req, err := http.NewRequest("POST", *host+"/put", bytes.NewBuffer(bodyBytes))
		if err != nil {
			log.Fatalf("Request error: %v", err)
		}

		req.Header.Set("Authorization", security.BuildAuthHeader(*pubKey, sig))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("Failed to execute request: %v", err)
		}
		defer resp.Body.Close()

		resBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("Status: %s\nResponse: %s\n", resp.Status, string(resBody))

	case "get":
		getCmd := flag.NewFlagSet("get", flag.ExitOnError)
		host := getCmd.String("host", "http://localhost:6379", "Daemon host")
		privKey := getCmd.String("privkey", "", "Your Ed25519 Private Key hex")
		pubKey := getCmd.String("pubkey", "", "Your Ed25519 Public Key hex")
		getCmd.Parse(os.Args[2:])

		args := getCmd.Args()
		if len(args) == 0 {
			fmt.Println("Please provide a key to get.")
			os.Exit(1)
		}
		key := args[0]

		if *privKey == "" || *pubKey == "" {
			fmt.Println("Missing required arguments. Use -privkey and -pubkey.")
			os.Exit(1)
		}

		// The server verifies the signature over the canonical message
		// for this key (security.CanonicalGetMessage), not over an empty
		// body: signing the body would sign nothing and the server
		// would reject the request.
		sig, err := security.SignPayload(*privKey, security.CanonicalGetMessage(key))
		if err != nil {
			log.Fatalf("Signature failed: %v", err)
		}

		req, err := http.NewRequest("GET", *host+"/get?key="+url.QueryEscape(key), nil)
		if err != nil {
			log.Fatalf("Request error: %v", err)
		}
		req.Header.Set("Authorization", security.BuildAuthHeader(*pubKey, sig))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("Failed to execute request: %v", err)
		}
		defer resp.Body.Close()

		resBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("Status: %s\nResponse: %s\n", resp.Status, string(resBody))

	case "del":
		delCmd := flag.NewFlagSet("del", flag.ExitOnError)
		host := delCmd.String("host", "http://localhost:6379", "Daemon host")
		privKey := delCmd.String("privkey", "", "Your Ed25519 Private Key hex")
		pubKey := delCmd.String("pubkey", "", "Your Ed25519 Public Key hex")
		delCmd.Parse(os.Args[2:])

		args := delCmd.Args()
		if len(args) == 0 {
			fmt.Println("Please provide a key to delete.")
			os.Exit(1)
		}
		key := args[0]

		if *privKey == "" || *pubKey == "" {
			fmt.Println("Missing required arguments. Use -privkey and -pubkey.")
			os.Exit(1)
		}

		sig, err := security.SignPayload(*privKey, security.CanonicalDeleteMessage(key))
		if err != nil {
			log.Fatalf("Signature failed: %v", err)
		}

		req, err := http.NewRequest("DELETE", *host+"/del?key="+url.QueryEscape(key), nil)
		if err != nil {
			log.Fatalf("Request error: %v", err)
		}
		req.Header.Set("Authorization", security.BuildAuthHeader(*pubKey, sig))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("Failed to execute request: %v", err)
		}
		defer resp.Body.Close()

		resBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("Status: %s\nResponse: %s\n", resp.Status, string(resBody))

	case "join":
		joinCmd := flag.NewFlagSet("join", flag.ExitOnError)
		host := joinCmd.String("host", "http://localhost:6379", "Daemon host")
		privKey := joinCmd.String("privkey", "", "Your Ed25519 Private Key hex")
		pubKey := joinCmd.String("pubkey", "", "Your Ed25519 Public Key hex")
		nodeID := joinCmd.String("id", "", "Raft Node ID to add")
		nodeAddr := joinCmd.String("addr", "", "Raft Node Address to add")
		joinCmd.Parse(os.Args[2:])

		if *privKey == "" || *pubKey == "" || *nodeID == "" || *nodeAddr == "" {
			fmt.Println("Missing required arguments. Use -privkey, -pubkey, -id, and -addr.")
			os.Exit(1)
		}

		sig, err := security.SignPayload(*privKey, security.CanonicalJoinMessage(*nodeID, *nodeAddr))
		if err != nil {
			log.Fatalf("Signature failed: %v", err)
		}

		req, err := http.NewRequest("POST", *host+"/join?id="+url.QueryEscape(*nodeID)+"&addr="+url.QueryEscape(*nodeAddr), nil)
		if err != nil {
			log.Fatalf("Request error: %v", err)
		}
		req.Header.Set("Authorization", security.BuildAuthHeader(*pubKey, sig))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("Failed to execute request: %v", err)
		}
		defer resp.Body.Close()

		resBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("Status: %s\nResponse: %s\n", resp.Status, string(resBody))

	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}
