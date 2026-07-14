package cloud

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/yowza/yowza"
	"github.com/yowza/yowza/security"
)

// miniRESPServer implements just enough of the Redis RESP2 protocol
// (PING, HELLO, RPUSH, LRANGE, SET, GET, DEL) over a localhost TCP socket
// to test RedisBackend end-to-end (including XChaCha20-Poly1305 encryption)
// with zero external dependencies.
type miniRESPServer struct {
	mu    sync.Mutex
	kv    map[string]string
	lists map[string][]string
}

func startMiniRESP(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	s := &miniRESPServer{
		kv:    make(map[string]string),
		lists: make(map[string][]string),
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serveConn(conn)
		}
	}()
	return ln.Addr().String()
}

func (s *miniRESPServer) serveConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		args, err := readRESPArray(r)
		if err != nil || len(args) == 0 {
			return
		}
		cmd := strings.ToUpper(args[0])
		s.mu.Lock()
		switch cmd {
		case "PING":
			_, _ = conn.Write([]byte("+PONG\r\n"))
		case "HELLO":
			_, _ = conn.Write([]byte("-ERR unknown command\r\n"))
		case "CLIENT":
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "RPUSH":
			key := args[1]
			s.lists[key] = append(s.lists[key], args[2:]...)
			_, _ = fmt.Fprintf(conn, ":%d\r\n", len(s.lists[key]))
		case "LRANGE":
			key := args[1]
			items := s.lists[key]
			_, _ = fmt.Fprintf(conn, "*%d\r\n", len(items))
			for _, it := range items {
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(it), it)
			}
		case "SET":
			s.kv[args[1]] = args[2]
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "GET":
			val, ok := s.kv[args[1]]
			if !ok {
				_, _ = conn.Write([]byte("$-1\r\n"))
			} else {
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
			}
		case "DEL":
			for _, k := range args[1:] {
				delete(s.kv, k)
				delete(s.lists, k)
			}
			_, _ = fmt.Fprintf(conn, ":1\r\n")
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
		s.mu.Unlock()
	}
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if !strings.HasPrefix(line, "*") {
		return strings.Fields(line), nil
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimSuffix(strings.TrimSuffix(hdr, "\n"), "\r")
		length, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf[:length]))
	}
	return out, nil
}

func TestRedisBackendEncryptedEndToEnd(t *testing.T) {
	addr := startMiniRESP(t)
	encKey, err := security.GenerateEncryptionKey()
	if err != nil {
		t.Fatalf("GenerateEncryptionKey failed: %v", err)
	}

	rb, err := NewRedisBackend("redis://"+addr+"/0", "yowzatest", encKey)
	if err != nil {
		t.Fatalf("NewRedisBackend failed: %v", err)
	}

	// 1. Empty snapshot and WAL
	snap, err := rb.LoadSnapshot()
	if err != nil || snap != nil {
		t.Fatalf("expected nil snapshot, got %v err=%v", snap, err)
	}
	wal, err := rb.LoadWAL()
	if err != nil || len(wal) != 0 {
		t.Fatalf("expected empty WAL, got %v err=%v", wal, err)
	}

	// 2. SaveWAL + LoadWAL (encrypted)
	if err := rb.SaveWAL([]yowza.WalEntry{
		{Opcode: yowza.OpcodePut, Key: []byte("rk1"), Value: []byte("rv1")},
		{Opcode: yowza.OpcodePutCRDT, Key: []byte("rk2"), Value: []byte("rv2"), Timestamp: 777},
	}); err != nil {
		t.Fatalf("SaveWAL failed: %v", err)
	}
	loadedWAL, err := rb.LoadWAL()
	if err != nil || len(loadedWAL) != 2 {
		t.Fatalf("LoadWAL failed: len=%d err=%v", len(loadedWAL), err)
	}
	if string(loadedWAL[0].Key) != "rk1" || loadedWAL[1].Timestamp != 777 {
		t.Fatalf("unexpected WAL contents: %+v", loadedWAL)
	}

	// 3. SaveSnapshot clears WAL and encrypts snapshot
	if err := rb.SaveSnapshot([]byte("REDIS_SNAP_PAYLOAD")); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	loadedSnap, err := rb.LoadSnapshot()
	if err != nil || string(loadedSnap) != "REDIS_SNAP_PAYLOAD" {
		t.Fatalf("LoadSnapshot mismatch: %q err=%v", loadedSnap, err)
	}
	walAfter, err := rb.LoadWAL()
	if err != nil || len(walAfter) != 0 {
		t.Fatalf("expected WAL cleared after SaveSnapshot, got %d entries", len(walAfter))
	}
}
