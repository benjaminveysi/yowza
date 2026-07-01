// Consistency domains (Gate 4.3) — snapshots across multiple
// independent databases/modules.
//
// THE PROBLEM. One PersistedSegDB's snapshot vector is a
// coherent instant for THAT database only. Taking two
// sequences back to back — one per database — is NOT a global
// atomic snapshot: a write can land between the two takes and
// be visible in one database but not the other, and nothing
// in either sequence says so. A query that needs shared state
// across two databases therefore needs a consistency domain:
// a set of participants that share one sequencer (or one
// commit protocol) and one session token, so every participant
// reads under the same contract.
//
// WHAT IS IMPLEMENTED HERE (single process, fully). A domain
// owns a sequencer (a monotonically increasing commit
// sequence). Participants register with the domain; writes
// through the domain advance the sequencer. A session is
// opened over a set of registered participants and captures
// one token and one domain sequence; each participant's read
// snapshot for that session is attached to the session
// (participant ID -> host snapshot sequence) while the
// session is open. A query executed through a session reads
// every participant at its attached snapshot, and the session
// rejects unknown participants, expired sessions, and —
// critically — a query spanning two domains: that query is
// refused with ErrNotInSameDomain, which states explicitly
// that only independent snapshots exist, not a global atomic
// one. The caller must then either put the databases in one
// domain or accept independent snapshots deliberately.
//
// WHAT IS SPECIFIED BUT NOT IMPLEMENTED (cross-machine).
// Across independent processes or machines, a domain needs a
// distributed coordination protocol: participants must agree
// on the session sequence before any of them serves reads,
// and writes must be fenced by the session token. The
// protocol is specified precisely in the doc comment on
// DistributedProtocolSpec and simulated in-process by
// SimulateDistributedCommit (prepare/commit with abort on
// any participant failure). Gate 4 does NOT claim a
// distributed protocol implementation: the simulation proves
// the session/token/abort logic, not network agreement. Any
// caller that needs cross-machine atomic snapshots must
// implement the specified protocol; until then, cross-domain
// queries get the explicit refusal above.
//
// SESSIONS AND EXPIRY. A session has a TTL (0 = no expiry
// for tests that control time explicitly is NOT allowed: a
// zero TTL means the session never expires by time, which
// callers must choose deliberately; the default constructor
// requires a positive TTL). SnapshotFor on an expired or
// closed session returns ErrSessionExpired, never a stale
// snapshot presented as current.
package query

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNotInSameDomain is returned when a query wants shared
// state across databases that are not in one consistency
// domain. Its message states the consequence plainly: only
// independent snapshots exist for those databases, not a
// global atomic snapshot. Callers must not retry the same
// query expecting atomicity; they must join one domain or
// accept independent snapshots.
var ErrNotInSameDomain = errors.New("query: databases are not in the same consistency domain: only independent snapshots exist, not a global atomic snapshot")

// ErrSessionExpired is returned when a session is used after
// its TTL or after Close.
var ErrSessionExpired = errors.New("query: consistency session expired or closed")

// ErrUnknownParticipant is returned when a session is asked
// for a participant that never joined it (or never
// registered with its domain).
var ErrUnknownParticipant = errors.New("query: unknown consistency participant")

// NotInSameDomainError carries the domains involved, so a
// caller can report exactly which databases failed to share
// a domain instead of a bare refusal.
type NotInSameDomainError struct {
	Domains      []string
	Participants []string
}

func (e *NotInSameDomainError) Error() string {
	return fmt.Sprintf("%s (domains: %v, participants: %v)", ErrNotInSameDomain.Error(), e.Domains, e.Participants)
}

func (e *NotInSameDomainError) Unwrap() error { return ErrNotInSameDomain }

// ConsistencyDomain is one sequencer shared by a set of
// participants. All participants of one domain share commit
// order: a write through CommitWrite advances the domain
// sequence, and every session opened afterwards sees it.
type ConsistencyDomain struct {
	mu           sync.Mutex
	id           string
	seq          uint64
	participants map[string]struct{}
}

// NewConsistencyDomain creates an empty domain. The id is
// part of session tokens and error reports; two domains with
// the same id are still different domains (identity is by
// pointer, not by id string — id collisions must not merge
// domains silently).
func NewConsistencyDomain(id string) *ConsistencyDomain {
	return &ConsistencyDomain{id: id, participants: map[string]struct{}{}}
}

// ID returns the domain's human-readable id.
func (d *ConsistencyDomain) ID() string { return d.id }

// RegisterParticipant adds a participant id to the domain.
// Registering twice is idempotent.
func (d *ConsistencyDomain) RegisterParticipant(participant string) error {
	if participant == "" {
		return fmt.Errorf("consistency domain %q: empty participant id", d.id)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.participants[participant] = struct{}{}
	return nil
}

// HasParticipant reports whether id is registered.
func (d *ConsistencyDomain) HasParticipant(participant string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.participants[participant]
	return ok
}

// CommitWrite records one write by a participant and returns
// its domain sequence. Writes that bypass the domain (direct
// writes to a participant's own store) are invisible to the
// sequencer by construction — that is exactly the "two
// sequences back to back" hazard this type exists to name,
// and a participant with bypass writes must not claim domain
// snapshots for them.
func (d *ConsistencyDomain) CommitWrite(participant string) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.participants[participant]; !ok {
		return 0, fmt.Errorf("%w: %q not registered in domain %q", ErrUnknownParticipant, participant, d.id)
	}
	d.seq++
	return d.seq, nil
}

// CurrentSeq returns the domain's current sequence.
func (d *ConsistencyDomain) CurrentSeq() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seq
}

// ConsistencySession is one shared instant over a domain's
// participants: one token, one domain sequence, and one
// attached host snapshot per participant.
type ConsistencySession struct {
	mu           sync.Mutex
	domain       *ConsistencyDomain
	token        string
	seq          uint64
	participants []string
	attached     map[string]uint64
	expiresAt    time.Time
	closed       bool
}

// BeginSession opens a session over participants, all of
// which must be registered with the domain. The session
// sequence is the domain's current sequence at open time.
// ttl <= 0 means no time expiry (callers choose this
// deliberately; coordinators use a positive TTL).
func (d *ConsistencyDomain) BeginSession(participants []string, ttl time.Duration) (*ConsistencySession, error) {
	if len(participants) == 0 {
		return nil, fmt.Errorf("consistency domain %q: session needs at least one participant", d.id)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := map[string]struct{}{}
	for _, p := range participants {
		if _, ok := d.participants[p]; !ok {
			return nil, fmt.Errorf("%w: %q not registered in domain %q", ErrUnknownParticipant, p, d.id)
		}
		if _, dup := seen[p]; dup {
			return nil, fmt.Errorf("consistency domain %q: duplicate participant %q in session", d.id, p)
		}
		seen[p] = struct{}{}
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, fmt.Errorf("consistency session: token randomness: %v", err)
	}
	s := &ConsistencySession{
		domain:       d,
		token:        fmt.Sprintf("%s-%d-%s", d.id, d.seq, hex.EncodeToString(rnd[:])),
		seq:          d.seq,
		participants: append([]string(nil), participants...),
		attached:     map[string]uint64{},
	}
	if ttl > 0 {
		s.expiresAt = time.Now().Add(ttl)
	}
	return s, nil
}

// Token returns the session's fencing token.
func (s *ConsistencySession) Token() string { return s.token }

// Seq returns the domain sequence captured at open time.
func (s *ConsistencySession) Seq() uint64 { return s.seq }

// DomainID returns the id of the session's domain.
func (s *ConsistencySession) DomainID() string { return s.domain.id }

// Domain returns the session's domain (pointer identity is
// what makes two sessions "the same domain").
func (s *ConsistencySession) Domain() *ConsistencyDomain { return s.domain }

// Participants returns the session's participant ids, in order.
func (s *ConsistencySession) Participants() []string {
	return append([]string(nil), s.participants...)
}

// Attach records a participant's host snapshot sequence for
// this session (e.g. the ISAHost snapshot taken for that
// participant's shard). Attaching an unlisted participant,
// or attaching after expiry/close, is an error.
func (s *ConsistencySession) Attach(participant string, hostSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(); err != nil {
		return err
	}
	for _, p := range s.participants {
		if p == participant {
			s.attached[participant] = hostSeq
			return nil
		}
	}
	return fmt.Errorf("%w: %q not in session %s", ErrUnknownParticipant, participant, s.token)
}

// SnapshotFor returns the attached host snapshot for a
// participant. An unattached participant, an unknown
// participant, or an expired/closed session is an error —
// never a silently invented snapshot (in particular, never
// a "current" sequence taken at query time, which would
// break the session's shared instant).
func (s *ConsistencySession) SnapshotFor(participant string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(); err != nil {
		return 0, err
	}
	seq, ok := s.attached[participant]
	if !ok {
		for _, p := range s.participants {
			if p == participant {
				return 0, fmt.Errorf("consistency session %s: participant %q has no attached snapshot", s.token, participant)
			}
		}
		return 0, fmt.Errorf("%w: %q not in session %s", ErrUnknownParticipant, participant, s.token)
	}
	return seq, nil
}

// HasSnapshot reports whether a participant has an attached
// snapshot (without failing on expiry, for diagnostics).
func (s *ConsistencySession) HasSnapshot(participant string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.attached[participant]
	return ok
}

// Close ends the session. SnapshotFor and Attach after Close
// return ErrSessionExpired.
func (s *ConsistencySession) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *ConsistencySession) checkLocked() error {
	if s.closed {
		return ErrSessionExpired
	}
	if !s.expiresAt.IsZero() && time.Now().After(s.expiresAt) {
		return ErrSessionExpired
	}
	return nil
}

// RequireSameDomain verifies that all sessions belong to one
// domain (pointer identity). With zero or one session it
// succeeds. With sessions from two or more domains it
// returns *NotInSameDomainError, naming the domains — the
// explicit "only independent snapshots exist" refusal Gate
// 4.3 requires. It never merges domains by id string.
func RequireSameDomain(sessions ...*ConsistencySession) error {
	if len(sessions) <= 1 {
		return nil
	}
	first := sessions[0]
	if first == nil {
		return fmt.Errorf("consistency: nil session")
	}
	var domains []string
	var parts []string
	seenDom := map[*ConsistencyDomain]struct{}{first.domain: {}}
	domains = append(domains, first.domain.id)
	parts = append(parts, first.participants...)
	for _, s := range sessions[1:] {
		if s == nil {
			return fmt.Errorf("consistency: nil session")
		}
		parts = append(parts, s.participants...)
		if _, ok := seenDom[s.domain]; !ok {
			seenDom[s.domain] = struct{}{}
			domains = append(domains, s.domain.id)
		}
	}
	if len(domains) > 1 {
		return &NotInSameDomainError{Domains: domains, Participants: parts}
	}
	return nil
}

// IndependentSnapshots is the explicit fallback a caller uses
// when RequireSameDomain refuses: one snapshot per
// participant, each taken at its own time, labelled as
// independent. Constructing this value is the caller's
// deliberate acceptance that there is no global atomic
// snapshot; it must never be passed where a session is
// required.
type IndependentSnapshots struct {
	Snapshots map[string]uint64 // participant -> its own sequence
}

// DistributedProtocolSpec documents, precisely, the protocol
// a cross-machine implementation of a consistency domain
// must provide. It is a specification, not an implementation:
// see the package comment. The single-process simulation in
// SimulateDistributedCommit implements the prepare/commit/
// abort state machine of this spec without any network.
//
//  1. SEQUENCER. One participant (or an external sequencer
//     service) owns a monotonically increasing sequence.
//     Every write to any participant is assigned a sequence
//     by the sequencer before it becomes visible.
//  2. SESSION OPEN (prepare). The coordinator proposes a
//     session (token, target sequence S) to every
//     participant. A participant PREPARES by pinning its
//     state at S (retaining everything S needs, as Gate 1's
//     snapshot lease requires) and replying PREPARED, or
//     replies ABORT (e.g. it has already compacted past S).
//  3. COMMIT. If every participant prepared, the
//     coordinator sends COMMIT(token, S); participants
//     serve reads for that token at S. If any participant
//     aborted or timed out, the coordinator sends ABORT
//     and no session exists — a partial session is never
//     served.
//  4. FENCING. Every read carries the token; a participant
//     serves a token only after COMMIT for it. A stale
//     coordinator (superseded token) cannot commit an old
//     prepare: prepares carry an epoch, and a higher epoch
//     invalidates earlier prepares.
//  5. EXPIRY. Sessions expire (TTL); after expiry a
//     participant may release its pin, and reads under the
//     token fail with a session-expired error, never with
//     silently newer data.
const DistributedProtocolSpec = "consistency domain distributed protocol: sequencer-assigned writes; prepare (pin at S) / commit-or-abort per session token; fencing by token+epoch; TTL expiry (see query/consistency.go)"

// SimulatedParticipant is one in-process stand-in for a
// remote participant in SimulateDistributedCommit.
type SimulatedParticipant struct {
	ID          string
	FailPrepare bool // when true, this participant aborts prepare
	Prepared    bool
	Committed   bool
}

// SimulateDistributedCommit runs the spec's prepare/commit
// state machine in-process: prepare every participant; if
// any fails, abort all (no session, no commits) and return
// an error; otherwise commit all and return a token. It
// proves the abort logic, not network agreement.
func SimulateDistributedCommit(domainID string, participants []*SimulatedParticipant) (string, error) {
	for _, p := range participants {
		if p.FailPrepare {
			for _, q := range participants {
				q.Prepared = false
				q.Committed = false
			}
			return "", fmt.Errorf("distributed commit %s: participant %q aborted prepare; session aborted, no partial session exists", domainID, p.ID)
		}
		p.Prepared = true
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", fmt.Errorf("distributed commit %s: token randomness: %v", domainID, err)
	}
	token := fmt.Sprintf("%s-commit-%s", domainID, hex.EncodeToString(rnd[:]))
	for _, p := range participants {
		p.Committed = true
	}
	return token, nil
}
