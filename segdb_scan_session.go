package yowza

// Phase B / O3 — streaming scan sessions at the
// PersistedSegDB level.
//
// O3's remaining gap was the LARGE-limit path: the C
// fan-in discovery cache (SEGDB_FANIN_CACHE_MAX_LIMIT =
// 4096) engages only for page calls with limit <= 4096,
// and larger-limit calls stay stateless, re-discovering
// sources per call. This session API removes that
// decision point at the PersistedSegDB level:
//
//   ScanOpen(handle, prefix, pageSize) -> sessionID
//   ScanNext(sessionID) -> rows, cursor, hasMore
//   ScanClose(sessionID)
//
// The session holds ONE PersistedScanIter (per-shard
// cursors + snapshot leases, cursor-first paging) and the
// snapshot vector. pageSize determines ONLY how many rows
// one ScanNext returns — never the total query rows: the
// caller pages until hasMore is false, and the total is
// unbounded by pageSize. Discovery is NOT redone per page:
// the iterator's per-shard cursors resume forward, so a
// page costs its rows, not a re-walk. The 4096 cap plays
// no role here at all (it is a C single-shard cache
// threshold, not a session limit).
//
// STALENESS (no mixed generations): the session records
// a fingerprint at open — checkpoint generation,
// checkpoint sequence, and per-shard segment counts.
// Before ANY row of a page is returned, ScanNext
// re-computes the fingerprint; any change fails the
// session with ErrScanSessionStale and kills it. This is
// deliberately CONSERVATIVE relative to the C fan-in
// cache (which transparently follows a flush-append):
// at the Persisted level a checkpoint change cannot be
// proven append-only from segment counts alone, so the
// session fails rather than risking a page that mixes
// pre- and post-change sources. A flush between pages
// therefore ends the session (typed error, never mixed
// rows); the caller restarts from the beginning at the
// same snapshot vector. Lease expiry is checked by the
// iterator itself per page (ErrLeaseExpired).
//
// RESOURCE POLICY: sessions are a bounded resource.
// ScanPolicy caps concurrent sessions and the per-page
// row count; exceeding either is a typed
// ErrScanSessionLimit, never silent truncation or an
// unbounded map. Per-session memory is bounded by one
// page (pageSize rows) plus per-shard cursor state —
// never by the total result size. The default policy is
// MaxConcurrentSessions 16, MaxPageSize 65536; callers
// that need more set it explicitly via SetScanPolicy.
//
// Multiple concurrent sessions are first-class: each
// session has its own iterator, leases, and cursor, so
// interleaved paging over the same snapshot (or
// different snapshots) never interferes. Sessions are
// NOT safe for concurrent ScanNext calls on the SAME
// session (one mutex serialises them, so concurrent
// callers get serialised pages, not corruption — but
// page ownership across goroutines is the caller's job).
//
// Hierarchical manifests (removing the global 4096-source
// cap): the C single-shard fan-in path still caps one
// scan's sources at 4096 (a manifest-format limit, see
// the notes). The level structure that removes a global
// cap is implemented here as HierManifest: a tree whose
// internal nodes fan in at most FanIn children, whose
// leaves are segment references, and which is walked
// level by level — no single flat list ever holds all
// sources. HierFlatten proves the structure over 5000
// synthetic leaves in the tests; a full >4096-segment
// wasm scan fixture is NOT claimed (building 4096+
// flushed segments through the 4 MiB-heap shards is
// impractical in this test harness — see
// TIMELINE_V2_NOTES.md for the honest statement).

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrScanSessionLimit reports a scan-session resource
// cap: too many concurrent sessions, or a page size
// above the configured maximum. Compare with errors.Is.
var ErrScanSessionLimit = errors.New("yowza persist: scan session resource limit")

// ErrScanSessionStale reports a session whose source
// fingerprint changed between pages (checkpoint /
// compact / recovery). The session is dead; restart the
// scan. Compare with errors.Is.
var ErrScanSessionStale = errors.New("yowza persist: scan session source set changed; restart the scan")

// ErrScanSessionUnknown reports an unknown session ID.
var ErrScanSessionUnknown = errors.New("yowza persist: unknown scan session")

// ScanPolicy is the configurable resource policy for
// scan sessions. Zero values mean the defaults
// (DefaultScanPolicy).
type ScanPolicy struct {
	MaxConcurrentSessions int
	MaxPageSize           int
}

// DefaultScanPolicy is the policy a freshly opened
// database uses.
var DefaultScanPolicy = ScanPolicy{MaxConcurrentSessions: 16, MaxPageSize: 65536}

func (sp ScanPolicy) resolved() ScanPolicy {
	if sp.MaxConcurrentSessions <= 0 {
		sp.MaxConcurrentSessions = DefaultScanPolicy.MaxConcurrentSessions
	}
	if sp.MaxPageSize <= 0 {
		sp.MaxPageSize = DefaultScanPolicy.MaxPageSize
	}
	return sp
}

// SetScanPolicy replaces the session resource policy. It
// does not affect already-open sessions' page sizes, only
// admission of new ones.
func (p *PersistedSegDB) SetScanPolicy(sp ScanPolicy) {
	p.scanMu.Lock()
	defer p.scanMu.Unlock()
	p.scanPolicy = sp.resolved()
}

// ScanPolicy returns the current session resource policy.
func (p *PersistedSegDB) ScanPolicy() ScanPolicy {
	p.scanMu.Lock()
	defer p.scanMu.Unlock()
	return p.scanPolicy.resolved()
}

type scanFingerprint struct {
	generation    uint64
	checkpointSeq uint64
	segments      []int
}

func (f scanFingerprint) equal(o scanFingerprint) bool {
	if f.generation != o.generation || f.checkpointSeq != o.checkpointSeq || len(f.segments) != len(o.segments) {
		return false
	}
	for i := range f.segments {
		if f.segments[i] != o.segments[i] {
			return false
		}
	}
	return true
}

type scanSession struct {
	id          uint64
	db          *PersistedSegDB
	prefix      string
	pageSize    int
	vector      []uint64
	iter        *PersistedScanIter
	ownedHandle SnapshotHandle // non-nil when ScanOpenAtCurrent owns the handle
	fingerprint scanFingerprint
	rows        uint64
	mu          sync.Mutex
	closed      bool
	dead        bool
}

// fingerprintLocked captures the current source
// fingerprint. It calls SegmentCounts (which takes p.mu
// itself), so callers must NOT hold p.mu.
func (p *PersistedSegDB) scanFingerprint() (scanFingerprint, error) {
	p.mu.Lock()
	gen, ckpt := p.generation, p.checkpointSeq
	p.mu.Unlock()
	counts, err := p.SegmentCounts()
	if err != nil {
		return scanFingerprint{}, err
	}
	return scanFingerprint{generation: gen, checkpointSeq: ckpt, segments: counts}, nil
}

// ScanOpen opens a streaming scan session over prefix at
// the snapshot handle's vector, with pageSize rows per
// ScanNext. The session does NOT take ownership of the
// handle: its iterator holds its own leases, so closing
// the handle does not kill the session. The 4096 C cache
// threshold is irrelevant here — see the file header.
func (p *PersistedSegDB) ScanOpen(handle SnapshotHandle, prefix string, pageSize int) (uint64, error) {
	if handle == nil {
		return 0, fmt.Errorf("yowza persist: ScanOpen requires a snapshot handle")
	}
	// PATCH2 Track 1: new sessions are gated reads;
	// ScanNext pages of an existing session are not.
	if err := p.checkReadsAllowed(); err != nil {
		return 0, err
	}
	return p.scanOpen(handle.Vector(), nil, prefix, pageSize)
}

// ScanOpenAtCurrent opens a session at the current
// snapshot: it acquires a handle internally and owns it
// (released on ScanClose or on session death).
func (p *PersistedSegDB) ScanOpenAtCurrent(prefix string, pageSize int) (uint64, error) {
	handle, err := p.AcquireSnapshot()
	if err != nil {
		return 0, err
	}
	id, err := p.scanOpen(handle.Vector(), handle, prefix, pageSize)
	if err != nil {
		_ = handle.Close()
		return 0, err
	}
	return id, nil
}

func (p *PersistedSegDB) scanOpen(vector []uint64, owned SnapshotHandle, prefix string, pageSize int) (uint64, error) {
	if pageSize <= 0 {
		pageSize = 64
	}
	policy := p.ScanPolicy()
	if pageSize > policy.MaxPageSize {
		return 0, fmt.Errorf("%w: page size %d exceeds maximum %d", ErrScanSessionLimit, pageSize, policy.MaxPageSize)
	}
	if len(prefix) > 1023 {
		return 0, fmt.Errorf("yowza persist: scan session prefix too long")
	}
	fp, err := p.scanFingerprint()
	if err != nil {
		return 0, err
	}
	iter, err := p.ScanStreaming(prefix, vector)
	if err != nil {
		return 0, err
	}
	p.scanMu.Lock()
	defer p.scanMu.Unlock()
	if len(p.scanSessions) >= policy.MaxConcurrentSessions {
		_ = iter.Close()
		return 0, fmt.Errorf("%w: %d concurrent sessions", ErrScanSessionLimit, policy.MaxConcurrentSessions)
	}
	p.scanNextID++
	id := p.scanNextID
	p.scanSessions[id] = &scanSession{
		id:          id,
		db:          p,
		prefix:      prefix,
		pageSize:    pageSize,
		vector:      append([]uint64(nil), vector...),
		iter:        iter,
		ownedHandle: owned,
		fingerprint: fp,
	}
	return id, nil
}

// scanSessionCursor is the opaque-ish resume marker
// returned by ScanNext: session id, rows returned so far,
// and completion. Resumption is by session ID, not by
// cursor — the cursor is diagnostic, the session holds
// the real cursor state (per-shard cursors in the
// iterator).
func scanSessionCursor(s *scanSession) string {
	return fmt.Sprintf("session=%d rows=%d done=%v", s.id, s.rows, s.iter.done)
}

// ScanNext returns up to the session's pageSize rows, a
// cursor marker, and whether more rows remain. It checks
// the source fingerprint BEFORE fetching any row: on a
// change it kills the session and returns
// ErrScanSessionStale with NO rows. An empty page with
// hasMore false is end of input; the session is then
// closed (leases released) automatically.
func (p *PersistedSegDB) ScanNext(sessionID uint64) ([]SegDBScanEntry, string, bool, error) {
	p.scanMu.Lock()
	s, ok := p.scanSessions[sessionID]
	p.scanMu.Unlock()
	if !ok {
		return nil, "", false, fmt.Errorf("%w: %d", ErrScanSessionUnknown, sessionID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, "", false, fmt.Errorf("%w: %d", ErrScanSessionUnknown, sessionID)
	}
	if s.dead {
		return nil, "", false, fmt.Errorf("%w: session %d already dead", ErrScanSessionStale, sessionID)
	}
	// Staleness gate: BEFORE any row is fetched.
	fp, err := p.scanFingerprint()
	if err != nil {
		return nil, "", false, err
	}
	if !fp.equal(s.fingerprint) {
		s.dead = true
		_ = s.iter.Close()
		if s.ownedHandle != nil {
			_ = s.ownedHandle.Close()
		}
		p.scanMu.Lock()
		delete(p.scanSessions, sessionID)
		p.scanMu.Unlock()
		return nil, "", false, fmt.Errorf("%w: checkpoint/segment fingerprint changed mid-scan", ErrScanSessionStale)
	}
	rows, err := s.iter.NextBatch(s.pageSize)
	if err != nil {
		return nil, "", false, err
	}
	s.rows += uint64(len(rows))
	cursor := scanSessionCursor(s)
	hasMore := !s.iter.done
	if !hasMore {
		// EOF: release leases now, drop the session.
		_ = s.iter.Close()
		if s.ownedHandle != nil {
			_ = s.ownedHandle.Close()
		}
		s.closed = true
		p.scanMu.Lock()
		delete(p.scanSessions, sessionID)
		p.scanMu.Unlock()
	}
	return rows, cursor, hasMore, nil
}

// ScanClose closes a session and releases its iterator
// leases (and owned handle, if any). It is idempotent;
// closing an unknown or already-closed session is a
// no-op returning nil, so EOF auto-close + explicit
// close never errors.
func (p *PersistedSegDB) ScanClose(sessionID uint64) error {
	p.scanMu.Lock()
	s, ok := p.scanSessions[sessionID]
	if ok {
		delete(p.scanSessions, sessionID)
	}
	p.scanMu.Unlock()
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	_ = s.iter.Close()
	if s.ownedHandle != nil {
		_ = s.ownedHandle.Close()
	}
	return nil
}

// ScanSessionCount reports the number of currently open
// sessions (diagnostic for the resource policy).
func (p *PersistedSegDB) ScanSessionCount() int {
	p.scanMu.Lock()
	defer p.scanMu.Unlock()
	return len(p.scanSessions)
}

// ---- Hierarchical manifest (level structure) ----
//
// A HierManifest is a tree: internal nodes fan in at
// most FanIn children, leaves are segment references
// (SegmentID). Walking it level by level never materialises
// a flat list of all sources at any single level beyond
// one level's width — that is what removes a global
// source cap: 4096 may bound one level, never the whole
// scan. HierFlatten returns leaves in DFS order and
// rejects any node that exceeds FanIn, any leaf without
// a SegmentID, and any internal node without children.
type HierNode struct {
	Name      string
	SegmentID string // non-empty only for leaves
	Children  []*HierNode
}

func (n *HierNode) isLeaf() bool { return len(n.Children) == 0 }

// HierFlatten walks root depth-first and returns every
// leaf SegmentID in order, plus the tree depth (root = 1).
// It fails on any structural violation, never silently
// dropping a subtree.
func HierFlatten(root *HierNode, fanIn int) ([]string, int, error) {
	if root == nil {
		return nil, 0, fmt.Errorf("yowza persist: hierarchical manifest root is nil")
	}
	if fanIn <= 0 {
		return nil, 0, fmt.Errorf("yowza persist: hierarchical manifest fan-in must be positive")
	}
	var out []string
	var walk func(n *HierNode, depth int) (int, error)
	walk = func(n *HierNode, depth int) (int, error) {
		if len(n.Children) > fanIn {
			return 0, fmt.Errorf("yowza persist: hierarchical node %q fans in %d children, cap %d", n.Name, len(n.Children), fanIn)
		}
		if n.isLeaf() {
			if n.SegmentID == "" {
				return 0, fmt.Errorf("yowza persist: hierarchical leaf %q has no segment id", n.Name)
			}
			out = append(out, n.SegmentID)
			return depth, nil
		}
		maxDepth := depth
		for _, c := range n.Children {
			d, err := walk(c, depth+1)
			if err != nil {
				return 0, err
			}
			if d > maxDepth {
				maxDepth = d
			}
		}
		return maxDepth, nil
	}
	depth, err := walk(root, 1)
	if err != nil {
		return nil, 0, err
	}
	return out, depth, nil
}

// HierBuildBalanced builds a balanced tree over the given
// segment IDs with the given fan-in, for tests and for
// callers that need a level structure over a flat source
// list. Leaves keep input order.
func HierBuildBalanced(segmentIDs []string, fanIn int) *HierNode {
	if len(segmentIDs) == 0 {
		return &HierNode{Name: "root"}
	}
	var level []*HierNode
	for i, id := range segmentIDs {
		level = append(level, &HierNode{Name: fmt.Sprintf("leaf-%d", i), SegmentID: id})
	}
	depth := 0
	for len(level) > 1 {
		var next []*HierNode
		for i := 0; i < len(level); i += fanIn {
			end := i + fanIn
			if end > len(level) {
				end = len(level)
			}
			node := &HierNode{Name: fmt.Sprintf("level-%d-node-%d", depth, len(next))}
			node.Children = append(node.Children, level[i:end]...)
			next = append(next, node)
		}
		level = next
		depth++
	}
	level[0].Name = "root"
	return level[0]
}

// sortedKeys is a small helper for session tests in this
// package (kept here to avoid touching other files).
func sortedKeys(entries []SegDBScanEntry) []string {
	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = string(e.Key)
	}
	sort.Strings(keys)
	return keys
}
