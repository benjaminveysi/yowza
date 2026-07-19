package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/yowza/yowza"
	"github.com/yowza/yowza/cloud"
	"github.com/yowza/yowza/framework"
)

func main() {
	devMode := flag.Bool("dev", false, "Start local dev tunnel and HTTP server")
	port := flag.Int("port", 8080, "Port to listen on")
	flag.Parse()

	if !*devMode {
		fmt.Println("Usage: yowza run --dev")
		os.Exit(1)
	}

	fmt.Println("Starting Yowza! in Developer Mode...")

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

	// 2. Initialize Cloud Sync (Local File Backend for dev tunnel)
	// In production, this could be Redis or S3 backend
	backend := cloud.NewLocalFileBackend("dev_cloud.wal", "dev_cloud.snap")
	manager := cloud.NewManager(db, backend)

	// Bootstrap (Cold Start Recovery)
	if err := manager.Bootstrap(); err != nil {
		log.Printf("Bootstrap warning: %v", err)
	}

	// 3. Initialize Framework
	engine := framework.NewEngine(db, manager)

	// Sample application logic
	engine.OnMessage(func(ctx *framework.Context) {
		fmt.Printf("[Update %s] Received from %s: %s\n", ctx.Update.UpdateID, ctx.Update.UserID, ctx.Update.Text)

		state, _ := ctx.GetState()

		if ctx.Update.Text == "/start" {
			fmt.Println(" -> Sending greeting and setting state to 'step_1'")
			ctx.SetState("step_1")
			return
		}

		if state == "step_1" {
			fmt.Println(" -> User is in step_1. Moving to step_2")
			ctx.SetState("step_2")
			return
		}

		if state == "step_2" {
			fmt.Println(" -> User is in step_2. Clearing state.")
			ctx.ClearState()
			return
		}

		fmt.Println(" -> Unknown state or command.")
	})

	// 3. Start Dev Tunnel / Server
	addr := fmt.Sprintf(":%d", *port)
	fmt.Printf("Yowza! HTTP Server listening on http://localhost%s\n", addr)
	fmt.Printf("Dev Tunnel Placeholder: Run 'ngrok http %d' to expose this port to webhooks.\n", *port)

	if err := http.ListenAndServe(addr, engine); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
