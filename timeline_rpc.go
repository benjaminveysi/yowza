package yowza

// Phase E, E3 — OWNER-PROCESS RPC.
//
// The current model is exclusive: each PersistedSegDB
// locks its directory, so a second process cannot open
// the same storage at all. That lock is NOT weakened
// here (an apparent multiwriter would corrupt the WAL,
// the snapshot leases, and the multi-shard root — see
// the shared-storage note at the end of this file).
// The production-ready multi-process shape is:
//
//   ONE owner process owns the DB + TimelineManager
//   (it holds the directory lock), and every other
//   process talks to it over local RPC.
//
// Protocol (HTTP + JSON on a loopback or Unix-socket
// listener; the transport is deliberately boring):
//   POST /ping     {} -> {ok:true}
//   POST /put      {id, key, value, expected_head?} -> {applied, head}
//   POST /delete   {id, key, expected_head?} -> {applied, head}
//   POST /get      {id?, key} -> {found, value, head}
//   POST /scan     {id?, prefix, page_size} -> {rows, head}
//   POST /response {id} -> the stored response for a
//     previously applied request, or ErrRPCUnknownID.
//
// Idempotency rules (the whole point):
//   - Every mutating request carries a client-chosen
//     idempotent ID. The server records the response
//     (applied flag + resulting head + error, if any)
//     in an append-only response log
//     (rpc-responses.log: one CRC-32 JSON record per
//     line, fsynced in Sync mode) BEFORE replying.
//   - A retried request with the same ID returns the
//     stored response WITHOUT re-applying, even if
//     another writer changed the key in between.
//   - A response lost after apply is recoverable:
//     the client re-asks with /response and gets the
//     original outcome; it never has to guess whether
//     the mutation landed.
//   - expected_head (a snapshot vector, when given)
//     is checked under the server's request mutex
//     immediately before apply; a mismatch is a typed
//     error and is ALSO recorded under the ID, so a
//     retry of a refused request stays refused
//     (deterministic replay, not a second chance to
//     race a new head).
//   - Read requests may omit an ID (they are pure);
//     with an ID they are recorded the same way.
//   - The response log is reloaded on server start,
//     so idempotency survives an owner restart.
//     Responses are never evicted in this phase:
//     eviction would break exactly the lost-response
//     recovery this protocol exists for (see the
//     remaining-limits note).
//
// SHARED-STORAGE COORDINATOR — DESIGNED, NOT BUILT:
// if multiple independent managers on shared storage
// are ever needed, the pieces that must exist are:
//   (1) an inter-process commit lock + fencing epoch
//       (a stale manager's commit must be refused
//       after a newer manager took over);
//   (2) a cross-process lease table (the C lease
//       table is per-process; a second process's
//       compact must defer on the first process's
//       pins, which needs leases in shared memory
//       or in the coordinator);
//   (3) a cross-process WAL append protocol (the
//       host WAL is a single append stream; two
//       appenders need sequence allocation +
//       fsync ordering, or a single WAL owner);
//   (4) a multi-shard root consensus (the checkpoint
//       publish must be atomic across shards as
//       seen by every process, not just the writer).
// Merely adding flock to the registry would cover
// none of (1)-(4): WAL, snapshot, and multi-shard
// root would still race. That coordinator is
// designed-but-remaining; this file does not claim
// it, and the exclusive directory lock stays in
// place until it lands.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// ErrRPCUnknownID reports a /response lookup for an ID
// the server never recorded.
var ErrRPCUnknownID = errors.New("yowza rpc: unknown idempotent id")

// ErrRPCStaleHead reports a request whose expected
// head did not match the live head at apply time.
var ErrRPCStaleHead = errors.New("yowza rpc: stale expected head")

// rpcRequest is one client request. ID is the
// idempotent ID (required for mutations, optional
// for reads). ExpectedHead is a snapshot vector.
type rpcRequest struct {
	ID           string   `json:"id,omitempty"`
	Op           string   `json:"op"`
	Key          string   `json:"key,omitempty"`
	Value        []byte   `json:"value,omitempty"`
	Prefix       string   `json:"prefix,omitempty"`
	PageSize     int      `json:"page_size,omitempty"`
	ExpectedHead []uint64 `json:"expected_head,omitempty"`
}

// rpcResponse is one stored response. Error is "" on
// success; Applied is false for a refused request.
type rpcResponse struct {
	ID      string          `json:"id"`
	Op      string          `json:"op"`
	Applied bool            `json:"applied"`
	Found   bool            `json:"found,omitempty"`
	Value   []byte          `json:"value,omitempty"`
	Rows    []rpcRow        `json:"rows,omitempty"`
	Head    []uint64        `json:"head,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type rpcRow struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type rpcLogRecord struct {
	Response rpcResponse `json:"response"`
	Checksum uint32      `json:"checksum"`
}

// TimelineRPCServer is the owner-process service. It
// serialises mutating requests under mu (the DB's own
// p.mu serialises facade writers anyway; mu is what
// makes the expected-head check + apply + response
// record atomic as one server step).
type TimelineRPCServer struct {
	db      *PersistedSegDB
	mgr     *TimelineManager // may be nil (DB-only owner)
	dir     string
	syncDir bool
	mu      sync.Mutex
	resp    map[string]*rpcResponse
	srv     *http.Server
	ln      net.Listener
}

func (s *TimelineRPCServer) responseLogPath() string {
	return filepath.Join(s.dir, "rpc-responses.log")
}

func (s *TimelineRPCServer) loadResponses() error {
	f, err := os.Open(s.responseLogPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for {
		var rec rpcLogRecord
		if err := dec.Decode(&rec); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("yowza rpc: response log decode: %v", err)
		}
		body, err := json.Marshal(rec.Response)
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(body) != rec.Checksum {
			return fmt.Errorf("yowza rpc: response log checksum mismatch for id %s", rec.Response.ID)
		}
		cp := rec.Response
		s.resp[cp.ID] = &cp
	}
}

func (s *TimelineRPCServer) recordResponse(resp *rpcResponse) error {
	body, err := json.Marshal(*resp)
	if err != nil {
		return err
	}
	rec := rpcLogRecord{Response: *resp, Checksum: crc32.ChecksumIEEE(body)}
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.responseLogPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if s.syncDir {
		return persistSyncDir(s.dir)
	}
	return nil
}

func (s *TimelineRPCServer) currentHead() []uint64 {
	vec, _, err := s.db.SnapshotVector()
	if err != nil {
		return nil
	}
	return vec
}

// applyLocked is one server step: idempotency lookup,
// expected-head check, apply, response record. Caller
// holds s.mu.
func (s *TimelineRPCServer) applyLocked(req *rpcRequest) *rpcResponse {
	if req.ID != "" {
		if prev, ok := s.resp[req.ID]; ok {
			// Idempotent replay: return the stored
			// response byte-for-byte in meaning, never
			// re-apply, even if the key moved since.
			cp := *prev
			return &cp
		}
	}
	resp := &rpcResponse{ID: req.ID, Op: req.Op}
	switch req.Op {
	case "put", "delete":
		if req.ID == "" {
			resp.Error = "yowza rpc: mutating request requires an idempotent id"
			break
		}
		if req.ExpectedHead != nil {
			if live := s.currentHead(); !equalUint64Slice(live, req.ExpectedHead) {
				resp.Error = fmt.Sprintf("%v: want %v, live %v", ErrRPCStaleHead, req.ExpectedHead, live)
				break
			}
		}
		var err error
		if req.Op == "put" {
			err = s.db.Put(req.Key, req.Value)
		} else {
			err = s.db.Delete(req.Key)
		}
		if err != nil {
			resp.Error = err.Error()
		} else {
			resp.Applied = true
		}
	case "get":
		v, err := s.db.Get(req.Key)
		if err == nil {
			resp.Found = true
			resp.Value = append([]byte(nil), v...)
		}
	case "scan":
		pageSize := req.PageSize
		if pageSize <= 0 {
			pageSize = 64
		}
		entries, err := s.db.Scan(req.Prefix)
		if err != nil {
			resp.Error = err.Error()
			break
		}
		if len(entries) > pageSize {
			entries = entries[:pageSize]
		}
		for _, e := range entries {
			resp.Rows = append(resp.Rows, rpcRow{Key: string(e.Key), Value: append([]byte(nil), e.Val...)})
		}
	default:
		resp.Error = fmt.Sprintf("yowza rpc: unknown op %q", req.Op)
	}
	resp.Head = s.currentHead()
	if req.ID != "" {
		// Record BEFORE replying: a crash after apply
		// but before this record is the one window
		// where recovery cannot distinguish applied
		// from not — and the DB's own WAL replay
		// decides it on reopen (the mutation is in
		// the WAL iff it was applied), while the
		// client re-asks with /response and, if the
		// record is absent, re-issues under a NEW id
		// (documented client rule, not a silent
		// double-apply: the new id applies at most
		// once, and a duplicate value write is
		// idempotent at the key level).
		if err := s.recordResponse(resp); err != nil {
			resp.Error = fmt.Sprintf("yowza rpc: response record failed: %v", err)
			resp.Applied = false
		} else {
			cp := *resp
			s.resp[resp.ID] = &cp
		}
	}
	return resp
}

func (s *TimelineRPCServer) handle(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch r.URL.Path {
	case "/ping":
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		return
	case "/response":
		var q struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(body, &q); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		resp, ok := s.resp[q.ID]
		s.mu.Unlock()
		if !ok {
			http.Error(w, ErrRPCUnknownID.Error(), http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch r.URL.Path {
	case "/put":
		req.Op = "put"
	case "/delete":
		req.Op = "delete"
	case "/get":
		req.Op = "get"
	case "/scan":
		req.Op = "scan"
	default:
		http.Error(w, "unknown path", http.StatusNotFound)
		return
	}
	s.mu.Lock()
	resp := s.applyLocked(&req)
	s.mu.Unlock()
	_ = json.NewEncoder(w).Encode(resp)
}

// StartTimelineRPCServer starts an owner server on a
// loopback TCP listener (addr "127.0.0.1:0" picks a
// free port; Addr() reports it). The caller owns the
// DB and manager lifetimes; Close stops the server
// (it does NOT close the DB).
func StartTimelineRPCServer(db *PersistedSegDB, mgr *TimelineManager, addr string) (*TimelineRPCServer, error) {
	if db == nil {
		return nil, fmt.Errorf("yowza rpc: nil database")
	}
	s := &TimelineRPCServer{db: db, mgr: mgr, dir: db.Dir(), syncDir: db.Durability() == DurabilitySync, resp: map[string]*rpcResponse{}}
	if err := s.loadResponses(); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	s.srv = &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s.ln = ln
	go func() { _ = s.srv.Serve(ln) }()
	return s, nil
}

// Addr returns the server's listen address.
func (s *TimelineRPCServer) Addr() string { return s.ln.Addr().String() }

// Close stops the server. It does not close the DB.
func (s *TimelineRPCServer) Close() error { return s.srv.Close() }

// TimelineRPCClient is a client for an owner server.
type TimelineRPCClient struct {
	base string
	http *http.Client
}

// DialTimelineRPC connects to a server address (as
// reported by Addr()).
func DialTimelineRPC(addr string) *TimelineRPCClient {
	return &TimelineRPCClient{base: "http://" + addr, http: &http.Client{}}
}

func (c *TimelineRPCClient) post(path string, req any, out any) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := c.http.Post(c.base+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ErrRPCUnknownID
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("yowza rpc: server status %d: %s", resp.StatusCode, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Ping checks liveness.
func (c *TimelineRPCClient) Ping() error {
	var out map[string]bool
	return c.post("/ping", map[string]bool{}, &out)
}

// Put applies a put under the idempotent ID.
func (c *TimelineRPCClient) Put(id, key string, val []byte, expectedHead []uint64) (*rpcResponse, error) {
	var out rpcResponse
	err := c.post("/put", rpcRequest{ID: id, Key: key, Value: val, ExpectedHead: expectedHead}, &out)
	return &out, err
}

// Delete applies a delete under the idempotent ID.
func (c *TimelineRPCClient) Delete(id, key string, expectedHead []uint64) (*rpcResponse, error) {
	var out rpcResponse
	err := c.post("/delete", rpcRequest{ID: id, Key: key, ExpectedHead: expectedHead}, &out)
	return &out, err
}

// Get reads a key (id optional).
func (c *TimelineRPCClient) Get(id, key string) (*rpcResponse, error) {
	var out rpcResponse
	err := c.post("/get", rpcRequest{ID: id, Key: key}, &out)
	return &out, err
}

// Scan returns up to pageSize rows with the prefix.
func (c *TimelineRPCClient) Scan(id, prefix string, pageSize int) (*rpcResponse, error) {
	var out rpcResponse
	err := c.post("/scan", rpcRequest{ID: id, Prefix: prefix, PageSize: pageSize}, &out)
	return &out, err
}

// RecoverResponse fetches the stored response for a
// previously issued ID (the lost-response recovery).
func (c *TimelineRPCClient) RecoverResponse(id string) (*rpcResponse, error) {
	var out rpcResponse
	err := c.post("/response", map[string]string{"id": id}, &out)
	return &out, err
}

// sortedKeysRPC is a test helper kept here to avoid
// touching other files.
func sortedRPCRowKeys(rows []rpcRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Key
	}
	sort.Strings(out)
	return out
}
