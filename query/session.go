package query

import (
	"context"
	"errors"
	"fmt"
	"sync"

	yowza "github.com/yowza/yowza"
)

// GATE 1 — per-query snapshot sessions (Gate 1.3).
//
// The old coordinator in segdb_source.go managed one epoch per
// database from the first Open, keyed by open-iterator count:
// two concurrent queries over one SegDB shared the earliest
// active epoch's sequence, so a query that started after a new
// commit could still read the older sequence. A Session fixes
// this by giving ONE query execution its own snapshot, held to
// the end of that execution:
//
//   - The first Open for a SegDB inside a session acquires a
//     lease for that database (SegDB.AcquireLease, which
//     captures the current sequence atomically with the
//     registration) and every later Open for the same database
//     in the same session reuses that sequence and lease.
//   - The first Open for a PersistedSegDB inside a session
//     captures one snapshot vector (PersistedSegDB.
//     SnapshotVector, atomic under the database lock) and every
//     later Open reuses that vector. There is no atomic
//     snapshot across DIFFERENT databases: each database gets
//     its own session entry, captured at its first Open in the
//     session. Cross-database queries therefore still have
//     per-database snapshot isolation, exactly as before — the
//     session does not pretend otherwise.
//   - The session, not the iterator, owns the leases: closing
//     an iterator does NOT release its session's lease, so a
//     query's snapshot survives across all its tables' Opens
//     and closes. Session.Close releases every lease; after
//     that, any iterator still paging under the session fails
//     with ErrSnapshotExpired (typed, via errors.Is), never
//     with partial apparently-successful rows.
//   - The engine (engine.go) creates one fresh session per
//     Execute call when the caller did not supply one, and
//     closes it when Execute returns. A caller-supplied session
//     (WithSession) is NEVER closed by the engine: the caller
//     owns its lifetime, and may reuse it for a second Execute
//     — which deliberately re-reads the SAME snapshot (a
//     session is one snapshot; a fresh snapshot needs a fresh
//     session).
//
// Leases for persisted shards are the one gap in this file:
// PersistedSegDB's shards are private to the root package, so a
// session cannot acquire per-shard SegDB leases for them from
// here. A persisted session therefore gives snapshot
// consistency (one vector) but NOT compact-protection: a
// PersistedSegDB.Compact during a persisted session query can
// still merge away history the session's vector needs. The
// exact root-package addition that closes this gap
// (AcquireShardLeases / ReleaseShardLeases on PersistedSegDB,
// plus lease registration inside PersistedScanIter) is
// specified in GATE1_NOTES.md for the parent to apply in
// segdb_persist.go. Until then, persisted compact-protection
// is a documented open item, not a claimed property.
//
// Sources that do not understand sessions (MemorySource,
// YowzaDBSource) ignore the session in the context entirely;
// their own snapshot semantics are unchanged.

// ErrSnapshotExpired reports a read attempted under a session
// or lease that is already closed or released. Compare with
// errors.Is. Iterators surface it from NextBatch; they never
// return partial rows alongside it.
var ErrSnapshotExpired = errors.New("query: snapshot session or lease expired")

// ErrSessionClosed reports use of a Session after Close.
var ErrSessionClosed = errors.New("query: session already closed")

type sessionCtxKey struct{}

// Session is one query execution's snapshot: one sequence (and
// lease) per SegDB, one vector per PersistedSegDB.
type Session struct {
	mu        sync.Mutex
	closed    bool
	seg       map[*yowza.SegDB]*sessionSegEntry
	persisted map[*yowza.PersistedSegDB]*sessionPersistedEntry
}

type sessionSegEntry struct {
	leaseID uint64
	seq     uint64
}

type sessionPersistedEntry struct {
	vector []uint64
}

// NewSession creates an empty session. It holds no snapshot
// until a source first Opens inside it (lazy capture keeps a
// session that never reads from ever touching storage).
func NewSession() *Session {
	return &Session{
		seg:       map[*yowza.SegDB]*sessionSegEntry{},
		persisted: map[*yowza.PersistedSegDB]*sessionPersistedEntry{},
	}
}

// WithSession returns a context carrying sess for source Opens.
// A nil session is a no-op (the context is returned unchanged).
func WithSession(ctx context.Context, sess *Session) context.Context {
	if sess == nil {
		return ctx
	}
	return context.WithValue(ctx, sessionCtxKey{}, sess)
}

func sessionFromCtx(ctx context.Context) *Session {
	if ctx == nil {
		return nil
	}
	sess, _ := ctx.Value(sessionCtxKey{}).(*Session)
	return sess
}

// segLease returns the session's lease (id and sequence) for
// db, acquiring one on first use. The lease is owned by the
// session and released by Close, never by an iterator Close.
func (s *Session) segLease(db *yowza.SegDB) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, 0, fmt.Errorf("%w", ErrSessionClosed)
	}
	if e, ok := s.seg[db]; ok {
		return e.leaseID, e.seq, nil
	}
	id, seq, err := db.AcquireLease()
	if err != nil {
		return 0, 0, err
	}
	s.seg[db] = &sessionSegEntry{leaseID: id, seq: seq}
	return id, seq, nil
}

// persistedVector returns the session's snapshot vector for
// db, capturing one on first use. The returned slice is a copy;
// callers may not mutate the session's stored vector.
func (s *Session) persistedVector(db *yowza.PersistedSegDB) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("%w", ErrSessionClosed)
	}
	if e, ok := s.persisted[db]; ok {
		return append([]uint64(nil), e.vector...), nil
	}
	vec, _, err := db.SnapshotVector()
	if err != nil {
		return nil, err
	}
	s.persisted[db] = &sessionPersistedEntry{vector: append([]uint64(nil), vec...)}
	return append([]uint64(nil), vec...), nil
}

// Close releases every SegDB lease the session holds. It is
// idempotent. Iterators opened under the session are NOT
// closed here (the engine closes its own iterators); a later
// NextBatch on such an iterator fails with
// ErrSnapshotExpired, because its lease is gone.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	for db, e := range s.seg {
		if err := db.ReleaseLease(e.leaseID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.seg = map[*yowza.SegDB]*sessionSegEntry{}
	return firstErr
}
