// Command raftcoord runs one Raft coordinator node as a
// real OS process: peer RPC on --peer-addr, client/admin
// HTTP on --api-addr, persisted state in --dir.
//
// Flags:
//
//	--id        node ID (e.g. n1)
//	--peer-addr peer RPC listen address (e.g. 127.0.0.1:18101)
//	--api-addr  client/admin listen address (e.g. 127.0.0.1:18201)
//	--dir       node state directory
//	--peers     comma-separated id=addr pairs for the static peer set
//	            (must include this node)
//
// The process prints "raftcoord ready api=<addr> peer=<addr>"
// on stdout once both listeners are up, then serves until
// killed (SIGTERM/SIGINT) or killed outright (SIGKILL,
// which is how tests simulate a crash: persisted Raft
// state survives, in-memory leadership does not).
//
// This binary exists so Track B acceptance tests run
// against REAL separate processes (distinct dirs and
// ports), not in-process simulations: kill the leader
// process, partition by blocking peers via the admin
// API, and restart processes from their persisted
// dirs to prove committed entries survive.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/yowza/yowza"
)

func main() {
	id := flag.String("id", "", "node ID")
	peerAddr := flag.String("peer-addr", "", "peer RPC listen address")
	apiAddr := flag.String("api-addr", "", "client/admin listen address")
	dir := flag.String("dir", "", "node state directory")
	peersFlag := flag.String("peers", "", "comma-separated id=addr pairs for the static peer set")
	flag.Parse()
	if *id == "" || *peerAddr == "" || *apiAddr == "" || *dir == "" || *peersFlag == "" {
		fmt.Fprintln(os.Stderr, "usage: raftcoord --id ID --peer-addr ADDR --api-addr ADDR --dir DIR --peers id=addr,id=addr,...")
		os.Exit(2)
	}
	var peers []yowza.RaftPeer
	for _, pair := range strings.Split(*peersFlag, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			log.Fatalf("bad peer pair %q", pair)
		}
		peers = append(peers, yowza.RaftPeer{ID: kv[0], Addr: kv[1]})
	}
	sess, err := yowza.NewRaftCoordinator(yowza.RaftConfig{
		ID: *id, Peers: peers, Dir: *dir, ListenAddr: *peerAddr,
	})
	if err != nil {
		log.Fatalf("new raft coordinator: %v", err)
	}
	if err := sess.Start(); err != nil {
		log.Fatalf("start raft coordinator: %v", err)
	}
	api, err := yowza.StartRaftCoordServer(sess, *apiAddr)
	if err != nil {
		log.Fatalf("start api server: %v", err)
	}
	fmt.Printf("raftcoord ready api=%s peer=%s\n", api.Addr(), sess.Node().Addr())
	// Block until a termination signal. SIGKILL (the
	// crash simulation) cannot be caught and needs no
	// handling: persisted state is already on disk.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	_ = api.Close()
	sess.Stop()
}
