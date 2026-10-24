package yowza

// Phase A (reliability patch) tests for timelines. These are
// white-box tests (same package): fault injection replaces a
// timeline's WAL writer, and the publish directory-sync step
// is indirected, so sync/dir-sync failures can be proven
// deterministically without a real filesystem fault.

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// timelineFaultWAL wraps a real WAL file and injects
// failures on demand. Each failure kind is counted so a
// test can fail exactly one call (e.g. the append's sync
// but not the rollback's sync) or fail it persistently.
type timelineFaultWAL struct {
	f                 *os.File
	failWriteOnce     bool
	failWriteBytes    int
	writeErr          error
	failSyncTimes     int
	syncErr           error
	failTruncateTimes int
	truncateErr       error
}

func (w *timelineFaultWAL) Write(p []byte) (int, error) {
	if w.failWriteOnce {
		w.failWriteOnce = false
		n := w.failWriteBytes
		if n > len(p) {
			n = len(p)
		}
		if n > 0 {
			if _, err := w.f.Write(p[:n]); err != nil {
				return 0, err
			}
		}
		return n, w.writeErr
	}
	return w.f.Write(p)
}

func (w *timelineFaultWAL) Sync() error {
	if w.failSyncTimes > 0 {
		w.failSyncTimes--
		return w.syncErr
	}
	return w.f.Sync()
}

func (w *timelineFaultWAL) Truncate(n int64) error {
	if w.failTruncateTimes > 0 {
		w.failTruncateTimes--
		return w.truncateErr
	}
	return w.f.Truncate(n)
}

// --- A2: WAL replay distinguishes corruption from tail ---

func timelinePhaseAWriteFile(t *testing.T, dir, name string, recs ...[]byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	var all []byte
	for _, r := range recs {
		all = append(all, r...)
	}
	if err := os.WriteFile(p, all, 0o644); err != nil {
		t.Fatalf("write wal fixture: %v", err)
	}
	return p
}

func TestTimelineWALReplayCorruptionVsTail(t *testing.T) {
	dir := t.TempDir()
	put1, err := timelineEncodeRecord(1, timelineWALPut, "a", []byte("1"), false, nil)
	if err != nil {
		t.Fatalf("encode put1: %v", err)
	}
	put2, err := timelineEncodeRecord(2, timelineWALPut, "b", []byte("2"), false, nil)
	if err != nil {
		t.Fatalf("encode put2: %v", err)
	}
	put3, err := timelineEncodeRecord(3, timelineWALPut, "c", []byte("3"), false, nil)
	if err != nil {
		t.Fatalf("encode put3: %v", err)
	}
	healthy := append(append([]byte{}, put1...), put2...)

	// Healthy file: full replay, no tail, next seq = 3.
	p := timelinePhaseAWriteFile(t, dir, "healthy.wal", put1, put2)
	ents, validLen, last, tail, err := timelineReplayWAL(p)
	if err != nil || tail || len(ents) != 2 || last != 2 || validLen != int64(len(healthy)) {
		t.Fatalf("healthy replay = %d entries, validLen %d, last %d, tail %v, err %v", len(ents), validLen, last, tail, err)
	}

	// Header-incomplete tail (3 stray bytes): truncatable.
	p = timelinePhaseAWriteFile(t, dir, "headtail.wal", put1, put2, []byte{0x00, 0x01, 0x02})
	ents, validLen, last, tail, err = timelineReplayWAL(p)
	if err != nil || !tail || len(ents) != 2 || last != 2 || validLen != int64(len(healthy)) {
		t.Fatalf("header tail replay = %d entries, validLen %d, last %d, tail %v, err %v", len(ents), validLen, last, tail, err)
	}

	// Body-incomplete tail (header + half the body):
	// truncatable, and only this tail may be truncated.
	p = timelinePhaseAWriteFile(t, dir, "bodytail.wal", put1, put2, put3[:8+len(put3)/2])
	ents, validLen, last, tail, err = timelineReplayWAL(p)
	if err != nil || !tail || len(ents) != 2 || last != 2 || validLen != int64(len(healthy)) {
		t.Fatalf("body tail replay = %d entries, validLen %d, last %d, tail %v, err %v", len(ents), validLen, last, tail, err)
	}

	// Complete body, failed CRC: corruption, NOT a tail.
	// Flipping a value byte keeps framing intact.
	badCRC := append([]byte{}, put3...)
	badCRC[len(badCRC)-1] ^= 0xFF
	p = timelinePhaseAWriteFile(t, dir, "badcrc.wal", put1, put2, badCRC)
	before, _ := os.ReadFile(p)
	_, _, _, tail, err = timelineReplayWAL(p)
	if !errors.Is(err, ErrTimelineCorrupt) || tail {
		t.Fatalf("bad CRC replay err = %v, tail = %v, want ErrTimelineCorrupt + tail=false", err, tail)
	}
	// A valid record AFTER the corrupt one must not be
	// accepted either: the suffix is never a valid WAL.
	p = timelinePhaseAWriteFile(t, dir, "badcrcsuffix.wal", put1, badCRC, put2)
	_, _, _, tail, err = timelineReplayWAL(p)
	if !errors.Is(err, ErrTimelineCorrupt) || tail {
		t.Fatalf("bad CRC + suffix replay err = %v, tail = %v, want Corrupt + tail=false", err, tail)
	}
	// Replay is read-only: the corrupt file is left exactly
	// as found, for inspection.
	after, _ := os.ReadFile(filepath.Join(dir, "badcrc.wal"))
	if string(before) != string(after) {
		t.Fatalf("replay modified the corrupt file")
	}

	// Sequence gap (1, 3): typed sequence error.
	put3skip, err := timelineEncodeRecord(3, timelineWALPut, "c", []byte("3"), false, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	p = timelinePhaseAWriteFile(t, dir, "gap.wal", put1, put3skip)
	_, _, _, tail, err = timelineReplayWAL(p)
	if !errors.Is(err, ErrTimelineWALSequence) || tail {
		t.Fatalf("gap replay err = %v, tail = %v, want ErrTimelineWALSequence + tail=false", err, tail)
	}
	// Duplicate sequence (1, 1): same typed error.
	p = timelinePhaseAWriteFile(t, dir, "dup.wal", put1, put1)
	_, _, _, tail, err = timelineReplayWAL(p)
	if !errors.Is(err, ErrTimelineWALSequence) || tail {
		t.Fatalf("dup replay err = %v, tail = %v, want ErrTimelineWALSequence + tail=false", err, tail)
	}

	// Zero body length: illegal length -> corruption.
	p = timelinePhaseAWriteFile(t, dir, "zerolen.wal", []byte{0, 0, 0, 0, 0, 0, 0, 0})
	_, _, _, tail, err = timelineReplayWAL(p)
	if !errors.Is(err, ErrTimelineCorrupt) || tail {
		t.Fatalf("zero-len replay err = %v, tail = %v, want Corrupt + tail=false", err, tail)
	}

	// Unknown record type with a VALID CRC: corruption.
	{
		body := []byte{1, 0, 0, 0, 0, 0, 0, 0, 9}
		framed := make([]byte, 8+len(body))
		framed[0] = byte(len(body))
		putCRC := crc32.ChecksumIEEE(body)
		framed[4] = byte(putCRC)
		framed[5] = byte(putCRC >> 8)
		framed[6] = byte(putCRC >> 16)
		framed[7] = byte(putCRC >> 24)
		copy(framed[8:], body)
		p = timelinePhaseAWriteFile(t, dir, "badtype.wal", framed)
		_, _, _, tail, err = timelineReplayWAL(p)
		if !errors.Is(err, ErrTimelineCorrupt) || tail {
			t.Fatalf("bad type replay err = %v, tail = %v, want Corrupt + tail=false", err, tail)
		}
	}

	// Merge payload inconsistencies -> corruption each.
	fp := timelineFingerprint(map[string][]byte{})
	validMerge := func(mut func(*timelineMergePayload)) []byte {
		mp := &timelineMergePayload{
			SourceID: "child", TargetID: "parent",
			SourceHead: 1, TargetBefore: 0, NewHead: 1,
			Policy:  "fail",
			Applied: []timelineMergeAppliedKey{{Key: "k", Value: []byte("v")}},
			BaseFP:  fp,
		}
		mut(mp)
		rec, err := timelineEncodeRecord(1, timelineWALMerge, "", nil, false, mp)
		if err != nil {
			t.Fatalf("encode merge: %v", err)
		}
		return rec
	}
	for name, rec := range map[string][]byte{
		"fail-with-conflicts": validMerge(func(mp *timelineMergePayload) { mp.Conflicted = []string{"k"} }),
		"newhead-mismatch":    validMerge(func(mp *timelineMergePayload) { mp.NewHead = 2 }),
		"unsorted-applied": validMerge(func(mp *timelineMergePayload) {
			mp.Applied = []timelineMergeAppliedKey{{Key: "b", Value: []byte("2")}, {Key: "a", Value: []byte("1")}}
		}),
		"empty-applied":     validMerge(func(mp *timelineMergePayload) { mp.Applied = nil }),
		"bad-policy":        validMerge(func(mp *timelineMergePayload) { mp.Policy = "last-writer-wins" }),
		"self-merge":        validMerge(func(mp *timelineMergePayload) { mp.TargetID = "child" }),
		"short-fingerprint": validMerge(func(mp *timelineMergePayload) { mp.BaseFP = "abc" }),
	} {
		p = timelinePhaseAWriteFile(t, dir, name+".wal", rec)
		_, _, _, tail, err = timelineReplayWAL(p)
		if !errors.Is(err, ErrTimelineCorrupt) || tail {
			t.Fatalf("merge %s replay err = %v, tail = %v, want Corrupt + tail=false", name, err, tail)
		}
	}
	// A valid merge record replays cleanly.
	p = timelinePhaseAWriteFile(t, dir, "goodmerge.wal", validMerge(func(mp *timelineMergePayload) {}))
	ents, _, last, tail, err = timelineReplayWAL(p)
	if err != nil || tail || len(ents) != 1 || last != 1 || ents[0].Merge == nil {
		t.Fatalf("good merge replay = %+v, tail %v, err %v", ents, tail, err)
	}
}

// --- A3: poison, both sync-error outcomes ---

func timelinePhaseAInstallFault(t *testing.T, mgr *TimelineManager, id string, fw *timelineFaultWAL) {
	t.Helper()
	ts, ok := mgr.timelines[id]
	if !ok {
		t.Fatalf("no timeline %s", id)
	}
	fw.f = ts.wal
	ts.walIO = fw
}

// Sync fails, truncate also fails -> indeterminate poison;
// recovery (reopen) finds the record PRESENT.
func TestTimelineWALPoisonSyncRecordPresent(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if _, err := mgr.ForkFromDB("tl"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			if _, err := mgr.Fork("tl", "child"); err != nil {
				t.Fatalf("fork child: %v", err)
			}
			// "solo" has no children, so its delete path
			// reaches the poison check (a delete of "tl"
			// would be refused for children first).
			if _, err := mgr.ForkFromDB("solo"); err != nil {
				t.Fatalf("fork solo: %v", err)
			}
			timelineMustPut(t, mgr, "tl", "k1", "v1")
			timelineMustPut(t, mgr, "solo", "s1", "sv")
			fw := &timelineFaultWAL{
				failSyncTimes:     1,
				syncErr:           errors.New("injected sync failure"),
				failTruncateTimes: 99,
				truncateErr:       errors.New("injected truncate failure"),
			}
			timelinePhaseAInstallFault(t, mgr, "tl", fw)
			err := mgr.Put("tl", "k2", []byte("v2"))
			var ierr *IndeterminateTimelineWALError
			if !errors.As(err, &ierr) || !errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("poison put err = %v, want *IndeterminateTimelineWALError", err)
			}
			if ierr.TimelineID != "tl" || ierr.Seq != 2 || ierr.Stage != "truncate-after-sync" {
				t.Fatalf("indeterminate error = %+v, want timeline tl seq 2 stage truncate-after-sync", ierr)
			}
			// In-memory state unchanged by the failed append.
			if h, _ := mgr.HeadSeq("tl"); h != 1 {
				t.Fatalf("head after poison = %d, want 1", h)
			}
			timelineMustMissing(t, mgr, "tl", "k2")
			// No blind retry: the next write returns the
			// same poison, not a duplicate sequence.
			if err := mgr.Put("tl", "k3", []byte("v3")); !errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("retry put err = %v, want indeterminate", err)
			}
			// Merges and structural changes on the poisoned
			// timeline are forbidden; other timelines still work.
			head, _ := mgr.HeadSeq("tl")
			if _, err := mgr.Merge("child", "tl", TimelineConflictFail, head); !errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("merge into poisoned err = %v, want indeterminate", err)
			}
			timelinePhaseAInstallFault(t, mgr, "solo", &timelineFaultWAL{
				failSyncTimes:     1,
				syncErr:           errors.New("injected sync failure"),
				failTruncateTimes: 99,
				truncateErr:       errors.New("injected truncate failure"),
			})
			if err := mgr.Put("solo", "s2", []byte("sv2")); !errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("solo poison put err = %v, want indeterminate", err)
			}
			if err := mgr.DeleteTimeline("solo"); !errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("delete poisoned err = %v, want indeterminate", err)
			}
			if err := mgr.FlushTimeline("tl"); !errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("flush poisoned err = %v, want indeterminate", err)
			}
			if _, err := mgr.Fork("tl", "grand"); !errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("fork from poisoned err = %v, want indeterminate", err)
			}
			timelineMustPut(t, mgr, "child", "ck", "cv")
			// Reads of the unchanged view still work.
			timelineMustGet(t, mgr, "tl", "k1", "v1")
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			// Recovery: the record IS present (truncate
			// failed, full valid record stayed in the WAL).
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer p2.Close()
			defer mgr2.Close()
			timelineMustGet(t, mgr2, "tl", "k2", "v2")
			if h, _ := mgr2.HeadSeq("tl"); h != 2 {
				t.Fatalf("recovered head = %d, want 2 (record present)", h)
			}
			// Next write uses seq 3: no duplicate sequence
			// is ever accepted.
			timelineMustPut(t, mgr2, "tl", "k3", "v3")
			if h, _ := mgr2.HeadSeq("tl"); h != 3 {
				t.Fatalf("head after recovery put = %d, want 3", h)
			}
			timelineMustGet(t, mgr2, "tl", "k3", "v3")
		})
	}
}

// Sync fails, truncate + its sync succeed -> definite
// failure; recovery finds the record ABSENT.
func TestTimelineWALPoisonSyncRecordAbsent(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if _, err := mgr.ForkFromDB("tla"); err != nil {
				t.Fatalf("fork tla: %v", err)
			}
			if _, err := mgr.ForkFromDB("tlb"); err != nil {
				t.Fatalf("fork tlb: %v", err)
			}
			timelineMustPut(t, mgr, "tla", "base", "b")
			timelineMustPut(t, mgr, "tlb", "base", "b")
			// tla: definite failure, then a retry with the
			// same sequence succeeds (sequence reusable).
			timelinePhaseAInstallFault(t, mgr, "tla", &timelineFaultWAL{
				failSyncTimes: 1, syncErr: errors.New("injected sync failure"),
			})
			err := mgr.Put("tla", "k", []byte("bad"))
			if err == nil || errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("definite put err = %v, want non-nil non-indeterminate", err)
			}
			if !strings.Contains(err.Error(), "definite") {
				t.Fatalf("definite put err = %v, want it labelled definite", err)
			}
			if h, _ := mgr.HeadSeq("tla"); h != 1 {
				t.Fatalf("tla head after definite failure = %d, want 1", h)
			}
			timelineMustPut(t, mgr, "tla", "k", "good")
			// tlb: definite failure, NO retry before recovery.
			timelinePhaseAInstallFault(t, mgr, "tlb", &timelineFaultWAL{
				failSyncTimes: 1, syncErr: errors.New("injected sync failure"),
			})
			if err := mgr.Put("tlb", "k", []byte("bad")); err == nil || errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("tlb definite put err = %v, want non-nil non-indeterminate", err)
			}
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer p2.Close()
			defer mgr2.Close()
			// tla: retry value present, failed value gone.
			timelineMustGet(t, mgr2, "tla", "k", "good")
			// tlb: record absent, head unchanged, sequence
			// reusable after recovery.
			timelineMustMissing(t, mgr2, "tlb", "k")
			if h, _ := mgr2.HeadSeq("tlb"); h != 1 {
				t.Fatalf("tlb recovered head = %d, want 1 (record absent)", h)
			}
			timelineMustPut(t, mgr2, "tlb", "k", "after")
			timelineMustGet(t, mgr2, "tlb", "k", "after")
		})
	}
}

// Partial write + successful truncate -> definite failure.
func TestTimelineWALPoisonWritePartialDefinite(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			_, mgr := timelineOpenForTest(t, bin, dir, 1)
			if _, err := mgr.ForkFromDB("tl"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "tl", "k1", "v1")
			timelinePhaseAInstallFault(t, mgr, "tl", &timelineFaultWAL{
				failWriteOnce:  true,
				failWriteBytes: 4,
				writeErr:       errors.New("injected partial write failure"),
			})
			err := mgr.Put("tl", "k2", []byte("v2"))
			if err == nil || errors.Is(err, ErrTimelineWALIndeterminate) {
				t.Fatalf("partial-write put err = %v, want non-nil non-indeterminate", err)
			}
			if h, _ := mgr.HeadSeq("tl"); h != 1 {
				t.Fatalf("head after partial write = %d, want 1", h)
			}
			timelineMustPut(t, mgr, "tl", "k2", "v2")
			timelineMustGet(t, mgr, "tl", "k2", "v2")
		})
	}
}

// --- A4: three-state publish ---

func TestTimelinePublishThreeStates(t *testing.T) {
	doc := &timelineRegistryDoc{Version: timelineRegistryVersion, NextNumber: 2}
	// Pre-rename failure: directory does not exist.
	if status, err := timelinePublishRegistryWithStatus(filepath.Join(t.TempDir(), "nope"), doc); status != TimelinePublishNotPublished || err == nil {
		t.Fatalf("missing dir publish = %s, err %v, want not-published + error", status, err)
	}
	// Durable success.
	dir := t.TempDir()
	if status, err := timelinePublishRegistryWithStatus(dir, doc); status != TimelinePublishDurable || err != nil {
		t.Fatalf("good publish = %s, err %v, want durable + nil", status, err)
	}
	// Post-rename failure: rename happened, dir sync failed.
	old := timelineSyncDirFunc
	t.Cleanup(func() { timelineSyncDirFunc = old })
	timelineSyncDirFunc = func(string) error { return errors.New("injected dir sync failure") }
	doc2 := &timelineRegistryDoc{Version: timelineRegistryVersion, NextNumber: 3}
	status, err := timelinePublishRegistryWithStatus(dir, doc2)
	if status != TimelinePublishIndeterminate || !errors.Is(err, ErrTimelinePublishIndeterminate) {
		t.Fatalf("dir-sync-fail publish = %s, err %v, want indeterminate", status, err)
	}
	// The new registry IS the file on disk (rename done);
	// only its durability is unknown.
	loaded, exists, err := timelineLoadRegistry(dir)
	if err != nil || !exists || loaded.NextNumber != 3 {
		t.Fatalf("registry after indeterminate publish = %+v, exists %v, err %v, want next_number 3", loaded, exists, err)
	}
	if _, err := os.Stat(filepath.Join(dir, timelineRegistryTmpFile)); !os.IsNotExist(err) {
		t.Fatalf("tmp file still present after rename: %v", err)
	}
}

// Fork publish indeterminate: timeline + WAL survive, the
// manager freezes mutations, recovery sees the fork.
func TestTimelinePublishIndeterminateFreezesManager(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("base", []byte("b")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("tl"); err != nil {
				t.Fatalf("fork tl: %v", err)
			}
			timelineMustPut(t, mgr, "tl", "k", "v")
			old := timelineSyncDirFunc
			t.Cleanup(func() { timelineSyncDirFunc = old })
			failed := false
			timelineSyncDirFunc = func(d string) error {
				if !failed {
					failed = true
					return errors.New("injected dir sync failure")
				}
				return timelineSyncDir(d)
			}
			_, err := mgr.Fork("tl", "newtl")
			if !errors.Is(err, ErrTimelinePublishIndeterminate) {
				t.Fatalf("indeterminate fork err = %v, want ErrTimelinePublishIndeterminate", err)
			}
			// No blind rollback: the new timeline's WAL
			// file still exists and it is still registered.
			if _, err := os.Stat(filepath.Join(dir, "timeline-newtl.wal")); err != nil {
				t.Fatalf("newtl WAL missing after indeterminate fork: %v", err)
			}
			if _, ok := mgr.timelines["newtl"]; !ok {
				t.Fatalf("newtl unregistered after indeterminate fork")
			}
			// Mutations frozen; reads still work.
			if err := mgr.Put("tl", "x", []byte("1")); !errors.Is(err, ErrTimelinePublishIndeterminate) {
				t.Fatalf("frozen put err = %v, want indeterminate", err)
			}
			if err := mgr.DeleteTimeline("tl"); !errors.Is(err, ErrTimelinePublishIndeterminate) {
				t.Fatalf("frozen delete err = %v, want indeterminate", err)
			}
			if _, err := mgr.ForkFromDB("other"); !errors.Is(err, ErrTimelinePublishIndeterminate) {
				t.Fatalf("frozen fork err = %v, want indeterminate", err)
			}
			timelineMustGet(t, mgr, "tl", "k", "v")
			timelineSyncDirFunc = old
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close (recovery path): %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			// Recovery determines the outcome: rename had
			// happened, so the fork stands.
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer p2.Close()
			defer mgr2.Close()
			if _, err := mgr2.Info("newtl"); err != nil {
				t.Fatalf("newtl info after recovery: %v", err)
			}
			timelineMustPut(t, mgr2, "newtl", "nk", "nv")
		})
	}
}

// Delete publish indeterminate: WAL + leases kept, the
// in-memory delete is reverted, recovery sees the tombstone.
func TestTimelineDeletePublishIndeterminateKeepsWAL(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if _, err := mgr.ForkFromDB("tl"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "tl", "k", "v")
			old := timelineSyncDirFunc
			t.Cleanup(func() { timelineSyncDirFunc = old })
			failed := false
			timelineSyncDirFunc = func(d string) error {
				if !failed {
					failed = true
					return errors.New("injected dir sync failure")
				}
				return timelineSyncDir(d)
			}
			err := mgr.DeleteTimeline("tl")
			if !errors.Is(err, ErrTimelinePublishIndeterminate) {
				t.Fatalf("indeterminate delete err = %v, want indeterminate", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "timeline-tl.wal")); err != nil {
				t.Fatalf("tl WAL deleted on indeterminate delete: %v", err)
			}
			// Delete not acknowledged: the timeline still
			// reads in the live (frozen) manager.
			timelineMustGet(t, mgr, "tl", "k", "v")
			timelineSyncDirFunc = old
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer p2.Close()
			defer mgr2.Close()
			if _, err := mgr2.Get("tl", "k"); !errors.Is(err, ErrTimelineDeleted) {
				t.Fatalf("tl after recovery err = %v, want ErrTimelineDeleted (tombstone durable)", err)
			}
			if _, err := mgr2.ForkFromDB("tl"); !errors.Is(err, ErrTimelineIDReuse) {
				t.Fatalf("reused id after recovery err = %v, want ErrTimelineIDReuse", err)
			}
		})
	}
}

// Merge publish indeterminate: the merge stands in memory
// and on reopen, while the manager freezes.
func TestTimelineMergePublishIndeterminateStands(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("base", []byte("b")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork parent: %v", err)
			}
			if _, err := mgr.Fork("parent", "child"); err != nil {
				t.Fatalf("fork child: %v", err)
			}
			timelineMustPut(t, mgr, "child", "merged", "yes")
			old := timelineSyncDirFunc
			t.Cleanup(func() { timelineSyncDirFunc = old })
			failed := false
			timelineSyncDirFunc = func(d string) error {
				if !failed {
					failed = true
					return errors.New("injected dir sync failure")
				}
				return timelineSyncDir(d)
			}
			head, _ := mgr.HeadSeq("parent")
			_, err := mgr.Merge("child", "parent", TimelineConflictFail, head)
			if !errors.Is(err, ErrTimelinePublishIndeterminate) {
				t.Fatalf("indeterminate merge err = %v, want indeterminate", err)
			}
			// Merge result stands in memory (WAL durable).
			timelineMustGet(t, mgr, "parent", "merged", "yes")
			if err := mgr.Put("parent", "x", []byte("1")); !errors.Is(err, ErrTimelinePublishIndeterminate) {
				t.Fatalf("frozen put after merge err = %v, want indeterminate", err)
			}
			timelineSyncDirFunc = old
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer p2.Close()
			defer mgr2.Close()
			timelineMustGet(t, mgr2, "parent", "merged", "yes")
		})
	}
}

// --- A5: concurrent opening ---

func TestTimelineManagerConcurrentOpen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			// Sequential second open while the first is live.
			mgr, err := NewTimelineManager(p)
			if err != nil {
				t.Fatalf("first manager: %v", err)
			}
			if _, err := NewTimelineManager(p); !errors.Is(err, ErrTimelineManagerExists) {
				t.Fatalf("second open err = %v, want ErrTimelineManagerExists", err)
			}
			if err := mgr.Close(); err != nil {
				t.Fatalf("close first: %v", err)
			}
			// Concurrent opens: exactly one wins.
			const n = 8
			start := make(chan struct{})
			results := make([]error, n)
			managers := make([]*TimelineManager, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					managers[i], results[i] = NewTimelineManager(p)
				}(i)
			}
			close(start)
			wg.Wait()
			wins := 0
			for i, err := range results {
				if err == nil {
					wins++
				} else if !errors.Is(err, ErrTimelineManagerExists) {
					t.Fatalf("opener %d err = %v, want nil or Exists", i, err)
				}
			}
			if wins != 1 {
				t.Fatalf("concurrent open wins = %d, want exactly 1 (errs %v)", wins, results)
			}
			for _, m := range managers {
				if m != nil {
					if err := m.Close(); err != nil {
						t.Fatalf("winner close: %v", err)
					}
				}
			}
			// Reservation freed: a fresh open succeeds.
			mgr2, err := NewTimelineManager(p)
			if err != nil {
				t.Fatalf("open after concurrent race: %v", err)
			}
			mgr2.Close()
		})
	}
}

// A failed open (corrupt WAL) frees the reservation and
// cleans up, so a later open after repair succeeds.
func TestTimelineManagerOpenFailureCleansUp(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if _, err := mgr.ForkFromDB("tl"); err != nil {
				t.Fatalf("fork tl: %v", err)
			}
			if _, err := mgr.ForkFromDB("tl2"); err != nil {
				t.Fatalf("fork tl2: %v", err)
			}
			timelineMustPut(t, mgr, "tl", "k", "v")
			timelineMustPut(t, mgr, "tl2", "k", "v")
			walPath := filepath.Join(dir, "timeline-tl.wal")
			orig, err := os.ReadFile(walPath)
			if err != nil {
				t.Fatalf("read wal: %v", err)
			}
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			// Corrupt a value byte of the first record:
			// framing intact, CRC fails -> open must fail
			// with Corrupt, not silently truncate.
			bad := append([]byte{}, orig...)
			// Record layout: [len][crc][seq 8][type 1]
			// [klen 4]["k"][vlen 4]["v"] -> value byte
			// at offset 8+8+1+4+1+4 = 26.
			if len(bad) <= 26 {
				t.Fatalf("wal too short to corrupt: %d bytes", len(bad))
			}
			bad[26] ^= 0xFF
			if err := os.WriteFile(walPath, bad, 0o644); err != nil {
				t.Fatalf("write corrupt wal: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			_, err = NewTimelineManager(p2)
			if !errors.Is(err, ErrTimelineCorrupt) {
				t.Fatalf("corrupt open err = %v, want ErrTimelineCorrupt", err)
			}
			// Reservation freed and no manager left behind.
			timelineManagersMu.Lock()
			_, inManagers := timelineManagers[dir]
			_, inOpening := timelineManagersOpening[dir]
			timelineManagersMu.Unlock()
			if inManagers || inOpening {
				t.Fatalf("failed open left dir in managers=%v opening=%v", inManagers, inOpening)
			}
			// Repair and reopen: works, both timelines intact.
			if err := os.WriteFile(walPath, orig, 0o644); err != nil {
				t.Fatalf("restore wal: %v", err)
			}
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen after repair: %v", err)
			}
			defer p2.Close()
			defer mgr2.Close()
			timelineMustGet(t, mgr2, "tl", "k", "v")
			timelineMustGet(t, mgr2, "tl2", "k", "v")
			timelineMustPut(t, mgr2, "tl", "k2", "v2")
		})
	}
}

// --- A1: lock races, close interference, lock order ---

// Put/Delete/Get/Scan concurrent with Close: no panic, no
// race, and the reopened WAL holds exactly the acknowledged
// puts (a single sequential writer's acknowledged set is a
// prefix, and Close cannot interrupt a Put mid-append
// because Put holds the read lock through the append).
func TestTimelineOpsConcurrentWithClose(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if _, err := mgr.ForkFromDB("tl"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "tl", "seed", "s")
			var acked int64
			var closedFlag int64
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					k := fmt.Sprintf("k%03d", i)
					err := mgr.Put("tl", k, []byte(fmt.Sprintf("v%03d", i)))
					if err != nil {
						if errors.Is(err, ErrTimelineManagerClosed) {
							return
						}
						t.Errorf("put %s: %v", k, err)
						return
					}
					atomic.StoreInt64(&acked, int64(i+1))
				}
			}()
			// Readers: errors must be nil or Closed only.
			for r := 0; r < 2; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 200; i++ {
						if _, err := mgr.Get("tl", "seed"); err != nil && !errors.Is(err, ErrTimelineManagerClosed) {
							t.Errorf("get seed: %v", err)
							return
						}
						if _, err := mgr.Scan("tl", ""); err != nil && !errors.Is(err, ErrTimelineManagerClosed) {
							t.Errorf("scan: %v", err)
							return
						}
					}
				}()
			}
			// Closer: close mid-stream. Each Put fsyncs,
			// so the writer is slow enough that the close
			// usually lands mid-stream; the assertion
			// (reopened == acknowledged) holds for any
			// acked count, including 0 and 100.
			time.Sleep(5 * time.Millisecond)
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close: %v", err)
			}
			atomic.StoreInt64(&closedFlag, 1)
			_ = closedFlag
			wg.Wait()
			n := atomic.LoadInt64(&acked)
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			// Reopen: exactly the acknowledged keys present.
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer p2.Close()
			defer mgr2.Close()
			rows, err := mgr2.Scan("tl", "k")
			if err != nil {
				t.Fatalf("reopen scan: %v", err)
			}
			if int64(len(rows)) != n {
				t.Fatalf("reopened keys = %d, acknowledged = %d (want exact match)", len(rows), n)
			}
			for i := int64(0); i < n; i++ {
				want := fmt.Sprintf("k%03d", i)
				if string(rows[i].Key) != want {
					t.Fatalf("reopened row %d = %q, want %q (prefix property)", i, rows[i].Key, want)
				}
			}
		})
	}
}

// Parallel changes on parent and child, with reads
// interleaved: the child never sees the parent's
// post-fork writes, the parent never sees the child's.
func TestTimelineParentChildParallelOps(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			_ = p
			if err := mgr.db.Put("shared", []byte("base")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork parent: %v", err)
			}
			if _, err := mgr.Fork("parent", "child"); err != nil {
				t.Fatalf("fork child: %v", err)
			}
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					if err := mgr.Put("parent", fmt.Sprintf("p%02d", i), []byte("pv")); err != nil {
						t.Errorf("parent put: %v", err)
					}
					if err := mgr.Put("child", fmt.Sprintf("c%02d", i), []byte("cv")); err != nil {
						t.Errorf("child put: %v", err)
					}
				}(i)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := mgr.Put("parent", "shared", []byte("pval")); err != nil {
					t.Errorf("parent shared put: %v", err)
				}
				if err := mgr.Put("parent", "ponly", []byte("p")); err != nil {
					t.Errorf("parent ponly put: %v", err)
				}
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := mgr.Put("child", "shared", []byte("cval")); err != nil {
					t.Errorf("child shared put: %v", err)
				}
				if err := mgr.Put("child", "conly", []byte("c")); err != nil {
					t.Errorf("child conly put: %v", err)
				}
			}()
			// Interleaved readers (must never error here:
			// the manager is open throughout).
			for r := 0; r < 2; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 50; i++ {
						if _, err := mgr.Get("child", "shared"); err != nil && !errors.Is(err, ErrTimelineKeyNotFound) {
							t.Errorf("child get shared: %v", err)
							return
						}
						if _, err := mgr.Scan("parent", ""); err != nil {
							t.Errorf("parent scan: %v", err)
							return
						}
					}
				}()
			}
			wg.Wait()
			timelineMustGet(t, mgr, "parent", "shared", "pval")
			timelineMustGet(t, mgr, "child", "shared", "cval")
			timelineMustMissing(t, mgr, "child", "ponly")
			timelineMustMissing(t, mgr, "parent", "conly")
			for i := 0; i < 10; i++ {
				timelineMustGet(t, mgr, "parent", fmt.Sprintf("p%02d", i), "pv")
				timelineMustGet(t, mgr, "child", fmt.Sprintf("c%02d", i), "cv")
				timelineMustMissing(t, mgr, "child", fmt.Sprintf("p%02d", i))
				timelineMustMissing(t, mgr, "parent", fmt.Sprintf("c%02d", i))
			}
		})
	}
}

// Merge on related timelines (different IDs) raced against
// Gets, Scans, and Compares: must complete without
// deadlock, with exactly one correct-head merge winning
// and no partial child key set in the parent.
func TestTimelineRelatedMergeLockOrder(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			_, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := mgr.db.Put("base", []byte("b")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork parent: %v", err)
			}
			if _, err := mgr.Fork("parent", "childA"); err != nil {
				t.Fatalf("fork childA: %v", err)
			}
			if _, err := mgr.Fork("parent", "childB"); err != nil {
				t.Fatalf("fork childB: %v", err)
			}
			for i := 0; i < 5; i++ {
				timelineMustPut(t, mgr, "childA", fmt.Sprintf("a%02d", i), "av")
				timelineMustPut(t, mgr, "childB", fmt.Sprintf("b%02d", i), "bv")
			}
			head, _ := mgr.HeadSeq("parent")
			results := make([]error, 2)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, results[0] = mgr.Merge("childA", "parent", TimelineConflictFail, head)
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, results[1] = mgr.Merge("childB", "parent", TimelineConflictFail, head)
			}()
			// Lock-order stress: Gets/Scans/Compares on
			// overlapping chains while merges run.
			for r := 0; r < 4; r++ {
				wg.Add(1)
				go func(r int) {
					defer wg.Done()
					for i := 0; i < 50; i++ {
						switch r % 4 {
						case 0:
							if _, err := mgr.Get("childA", "base"); err != nil {
								t.Errorf("get childA: %v", err)
								return
							}
						case 1:
							if _, err := mgr.Scan("childB", ""); err != nil {
								t.Errorf("scan childB: %v", err)
								return
							}
						case 2:
							if _, err := mgr.Compare("childA", "childB"); err != nil {
								t.Errorf("compare siblings: %v", err)
								return
							}
						case 3:
							if _, err := mgr.Compare("parent", "childA"); err != nil {
								t.Errorf("compare parent/child: %v", err)
								return
							}
						}
					}
				}(r)
			}
			wg.Wait()
			wins := 0
			for _, err := range results {
				if err == nil {
					wins++
				} else if !errors.Is(err, ErrTimelineStaleHead) {
					t.Fatalf("merge race err = %v, want nil or StaleHead", err)
				}
			}
			if wins != 1 {
				t.Fatalf("merge race wins = %d, want exactly 1", wins)
			}
			// No partial child key set in the parent.
			// Prefixes are "a0"/"b0" (not "a"/"b") so
			// the branch key "base" never matches.
			for _, tc := range []struct{ prefix string }{{"a0"}, {"b0"}} {
				rows, err := mgr.Scan("parent", tc.prefix)
				if err != nil {
					t.Fatalf("scan parent: %v", err)
				}
				if len(rows) != 0 && len(rows) != 5 {
					t.Fatalf("parent has %d keys with prefix %q: partial merge", len(rows), tc.prefix)
				}
			}
		})
	}
}
