package query

import (
	"errors"
	"testing"
	"time"
)

func TestGate4DomainSessionLifecycle(t *testing.T) {
	dom := NewConsistencyDomain("dom-a")
	if err := dom.RegisterParticipant("db1"); err != nil {
		t.Fatalf("register db1: %v", err)
	}
	if err := dom.RegisterParticipant("db2"); err != nil {
		t.Fatalf("register db2: %v", err)
	}
	if _, err := dom.CommitWrite("db1"); err != nil {
		t.Fatalf("commit db1: %v", err)
	}
	if _, err := dom.CommitWrite("db2"); err != nil {
		t.Fatalf("commit db2: %v", err)
	}
	if got := dom.CurrentSeq(); got != 2 {
		t.Fatalf("domain seq = %d, want 2", got)
	}
	if _, err := dom.CommitWrite("ghost"); !errors.Is(err, ErrUnknownParticipant) {
		t.Fatalf("commit by unregistered participant = %v, want ErrUnknownParticipant", err)
	}
	sess, err := dom.BeginSession([]string{"db1", "db2"}, time.Minute)
	if err != nil {
		t.Fatalf("BeginSession: %v", err)
	}
	if sess.Seq() != 2 {
		t.Fatalf("session seq = %d, want 2 (domain seq at open)", sess.Seq())
	}
	if err := sess.Attach("db1", 41); err != nil {
		t.Fatalf("Attach db1: %v", err)
	}
	if err := sess.Attach("db2", 42); err != nil {
		t.Fatalf("Attach db2: %v", err)
	}
	if v, err := sess.SnapshotFor("db1"); err != nil || v != 41 {
		t.Fatalf("SnapshotFor db1 = %d, %v", v, err)
	}
	if _, err := sess.SnapshotFor("ghost"); !errors.Is(err, ErrUnknownParticipant) {
		t.Fatalf("SnapshotFor ghost = %v, want ErrUnknownParticipant", err)
	}
	if err := sess.Attach("ghost", 1); !errors.Is(err, ErrUnknownParticipant) {
		t.Fatalf("Attach ghost = %v, want ErrUnknownParticipant", err)
	}
	// A write after the session opened must not move the
	// session's captured sequence.
	if _, err := dom.CommitWrite("db1"); err != nil {
		t.Fatalf("commit after open: %v", err)
	}
	if sess.Seq() != 2 {
		t.Fatalf("session seq moved to %d after a later write, want 2", sess.Seq())
	}
	sess.Close()
	if _, err := sess.SnapshotFor("db1"); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("SnapshotFor after Close = %v, want ErrSessionExpired", err)
	}
}

func TestGate4SessionExpiry(t *testing.T) {
	dom := NewConsistencyDomain("dom-exp")
	_ = dom.RegisterParticipant("db1")
	sess, err := dom.BeginSession([]string{"db1"}, time.Millisecond)
	if err != nil {
		t.Fatalf("BeginSession: %v", err)
	}
	_ = sess.Attach("db1", 7)
	time.Sleep(5 * time.Millisecond)
	if _, err := sess.SnapshotFor("db1"); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("SnapshotFor after TTL = %v, want ErrSessionExpired", err)
	}
}

func TestGate4RequireSameDomainRefuses(t *testing.T) {
	d1 := NewConsistencyDomain("dom-1")
	d2 := NewConsistencyDomain("dom-2")
	_ = d1.RegisterParticipant("a")
	_ = d2.RegisterParticipant("b")
	s1, err := d1.BeginSession([]string{"a"}, time.Minute)
	if err != nil {
		t.Fatalf("session 1: %v", err)
	}
	s2, err := d2.BeginSession([]string{"b"}, time.Minute)
	if err != nil {
		t.Fatalf("session 2: %v", err)
	}
	err = RequireSameDomain(s1, s2)
	if err == nil {
		t.Fatalf("cross-domain sessions must be refused")
	}
	if !errors.Is(err, ErrNotInSameDomain) {
		t.Fatalf("error = %v, want ErrNotInSameDomain", err)
	}
	var nd *NotInSameDomainError
	if !errors.As(err, &nd) {
		t.Fatalf("error %T is not *NotInSameDomainError", err)
	}
	if len(nd.Domains) != 2 {
		t.Fatalf("domains = %v, want 2", nd.Domains)
	}
	// Same domain passes, even with two sessions.
	s1b, err := d1.BeginSession([]string{"a"}, time.Minute)
	if err != nil {
		t.Fatalf("session 1b: %v", err)
	}
	if err := RequireSameDomain(s1, s1b); err != nil {
		t.Fatalf("same-domain sessions must pass: %v", err)
	}
	// Two domains with the SAME id string are still
	// different domains (identity is by pointer).
	d1clone := NewConsistencyDomain("dom-1")
	_ = d1clone.RegisterParticipant("a")
	sClone, err := d1clone.BeginSession([]string{"a"}, time.Minute)
	if err != nil {
		t.Fatalf("clone session: %v", err)
	}
	if err := RequireSameDomain(s1, sClone); err == nil {
		t.Fatalf("same-id different-domain sessions must be refused")
	}
}

func TestGate4SimulatedDistributedCommit(t *testing.T) {
	parts := []*SimulatedParticipant{{ID: "p1"}, {ID: "p2"}, {ID: "p3"}}
	token, err := SimulateDistributedCommit("dom-sim", parts)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if token == "" {
		t.Fatalf("empty token on success")
	}
	for _, p := range parts {
		if !p.Prepared || !p.Committed {
			t.Fatalf("participant %s prepared=%v committed=%v, want both true", p.ID, p.Prepared, p.Committed)
		}
	}
	// One aborting participant aborts the whole session:
	// no partial session, no commits.
	parts2 := []*SimulatedParticipant{{ID: "p1"}, {ID: "bad", FailPrepare: true}, {ID: "p3"}}
	if _, err := SimulateDistributedCommit("dom-sim", parts2); err == nil {
		t.Fatalf("aborting participant must fail the commit")
	}
	for _, p := range parts2 {
		if p.Committed {
			t.Fatalf("participant %s committed despite abort", p.ID)
		}
	}
	if DistributedProtocolSpec == "" {
		t.Fatalf("distributed protocol spec must be documented")
	}
}
