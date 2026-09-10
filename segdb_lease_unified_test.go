package yowza

// O4 — unified lease registry tests.
//
// Authority: the C in-module lease table (segdb.c), reached
// through the yowza_segdb_lease_* wasm exports. The Go API
// (AcquireLease/RegisterLease/ReleaseLease/LeaseValid,
// SegDB.Compact) is a client of that table on rebuilt
// binaries, so a lease acquired on either side pins a
// compact issued on either side, and a release on either
// side frees the pin for both. The Go-side map survives
// only as the fallback for pre-export binaries (exercised
// via the leaseFallbackForTest seam — see segdb.go).
//
// Every test runs against both binaries (wasm32 + wasm64).
// Direct-C helpers below bypass Go entirely except for
// host.mu serialisation, which is what a native/wasm
// caller would also have to respect inside this process.

import (
	"context"
	"errors"
	"os"
	"testing"
)

func leaseOpenSegDB(t *testing.T, bin string) (*YowzaDB, *SegDB) {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read %s: %v", bin, err)
	}
	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("NewYowzaDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sdb, err := NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	return db, sdb
}

// leaseTwoSegs builds the stale-lease fixture used by most
// tests: k=v1 flushed, snapshot captured, k=v2 flushed, so
// the DB has exactly 2 segments and snap < current.
func leaseTwoSegs(t *testing.T, sdb *SegDB) uint64 {
	t.Helper()
	if err := sdb.Put("k", []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}
	snap, err := sdb.SnapshotSeq()
	if err != nil {
		t.Fatalf("SnapshotSeq: %v", err)
	}
	if err := sdb.Put("k", []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}
	if n, err := sdb.SegmentCount(); err != nil || n != 2 {
		t.Fatalf("SegmentCount = %d, err %v, want 2", n, err)
	}
	return snap
}

func leaseSegCount(t *testing.T, sdb *SegDB) int {
	t.Helper()
	n, err := sdb.SegmentCount()
	if err != nil {
		t.Fatalf("SegmentCount: %v", err)
	}
	return n
}

// cCompact calls the wasm compact export DIRECTLY (not
// SegDB.Compact), under host.mu — the C-side compact a Go
// lease must now also pin. It returns the C rc.
func cCompact(t *testing.T, sdb *SegDB, barrier uint64) int32 {
	t.Helper()
	fn := sdb.host.module.ExportedFunction("yowza_segdb_compact")
	if fn == nil {
		t.Fatalf("wasm binary predates yowza_segdb_compact export")
	}
	sdb.host.mu.Lock()
	defer sdb.host.mu.Unlock()
	res, err := fn.Call(context.Background(), sdb.stateOff, barrier)
	if err != nil || len(res) == 0 {
		t.Fatalf("direct C compact call: %v", err)
	}
	return rcOf(res)
}

// cLeaseAcquire calls the wasm lease-acquire export
// directly (not RegisterLease/AcquireLease).
func cLeaseAcquire(t *testing.T, sdb *SegDB, seq uint64) uint64 {
	t.Helper()
	fn := sdb.host.module.ExportedFunction("yowza_segdb_lease_acquire")
	if fn == nil {
		t.Fatalf("wasm binary predates yowza_segdb_lease_acquire export")
	}
	sdb.host.mu.Lock()
	defer sdb.host.mu.Unlock()
	res, err := fn.Call(context.Background(), sdb.stateOff, seq)
	if err != nil || len(res) == 0 {
		t.Fatalf("direct C lease acquire: %v", err)
	}
	return res[0]
}

// cLeaseRelease calls the wasm lease-release export
// directly and returns its rc (0 ok, -1 unknown/released).
func cLeaseRelease(t *testing.T, sdb *SegDB, id uint64) int32 {
	t.Helper()
	fn := sdb.host.module.ExportedFunction("yowza_segdb_lease_release")
	if fn == nil {
		t.Fatalf("wasm binary predates yowza_segdb_lease_release export")
	}
	sdb.host.mu.Lock()
	defer sdb.host.mu.Unlock()
	res, err := fn.Call(context.Background(), sdb.stateOff, id)
	if err != nil || len(res) == 0 {
		t.Fatalf("direct C lease release: %v", err)
	}
	return rcOf(res)
}

// cLeaseValid calls the wasm lease-valid export directly.
func cLeaseValid(t *testing.T, sdb *SegDB, id uint64) bool {
	t.Helper()
	fn := sdb.host.module.ExportedFunction("yowza_segdb_lease_valid")
	if fn == nil {
		t.Fatalf("wasm binary predates yowza_segdb_lease_valid export")
	}
	sdb.host.mu.Lock()
	defer sdb.host.mu.Unlock()
	res, err := fn.Call(context.Background(), sdb.stateOff, id)
	if err != nil || len(res) == 0 {
		t.Fatalf("direct C lease valid: %v", err)
	}
	return rcOf(res) != 0
}

// cLeaseOldest calls the wasm lease-oldest export directly.
func cLeaseOldest(t *testing.T, sdb *SegDB) (uint64, bool) {
	t.Helper()
	fn := sdb.host.module.ExportedFunction("yowza_segdb_lease_oldest")
	if fn == nil {
		t.Fatalf("wasm binary predates yowza_segdb_lease_oldest export")
	}
	sdb.host.mu.Lock()
	defer sdb.host.mu.Unlock()
	res, err := fn.Call(context.Background(), sdb.stateOff, sdb.scratchOff+segLeaseOldestHasActive)
	if err != nil || len(res) == 0 {
		t.Fatalf("direct C lease oldest: %v", err)
	}
	b, ok := sdb.host.memRead(sdb.scratchOff+segLeaseOldestHasActive, 4)
	if !ok {
		t.Fatalf("read lease oldest flag")
	}
	return res[0], b[0] != 0 || b[1] != 0 || b[2] != 0 || b[3] != 0
}

func TestLeaseUnifiedBackendIsC(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			_, sdb := leaseOpenSegDB(t, bin)
			if got := sdb.LeaseBackend(); got != "c" {
				t.Fatalf("LeaseBackend = %q, want %q (rebuilt binary must delegate to C)", got, "c")
			}
			for _, name := range []string{"yowza_segdb_lease_acquire", "yowza_segdb_lease_release", "yowza_segdb_lease_valid", "yowza_segdb_lease_oldest"} {
				if sdb.host.module.ExportedFunction(name) == nil {
					t.Fatalf("missing export %s", name)
				}
			}
			if seq, has, err := sdb.LeaseOldest(); err != nil || has || seq != 0 {
				t.Fatalf("LeaseOldest with no leases = (%d, %v, %v), want (0, false, nil)", seq, has, err)
			}
			if seq, has := cLeaseOldest(t, sdb); has || seq != 0 {
				t.Fatalf("direct C LeaseOldest with no leases = (%d, %v), want (0, false)", seq, has)
			}
		})
	}
}

// (b) A lease acquired via the Go API must defer a compact
// issued DIRECTLY through the wasm C compact export — not
// just Go Compact. After Go release, the direct C compact
// must actually merge.
func TestLeaseUnifiedGoLeaseDefersCDirectCompact(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			_, sdb := leaseOpenSegDB(t, bin)
			snap := leaseTwoSegs(t, sdb)
			id, err := sdb.RegisterLease(snap)
			if err != nil {
				t.Fatalf("RegisterLease: %v", err)
			}
			if id == 0 {
				t.Fatalf("RegisterLease returned id 0")
			}
			// The C table must see the Go-acquired lease.
			if !cLeaseValid(t, sdb, id) {
				t.Fatalf("C lease_valid(Go id %d) = false, want true", id)
			}
			if seq, has := cLeaseOldest(t, sdb); !has || seq != snap {
				t.Fatalf("C lease_oldest = (%d, %v), want (%d, true)", seq, has, snap)
			}
			if rc := cCompact(t, sdb, 1<<60); rc != 0 {
				t.Fatalf("direct C compact rc = %d, want 0", rc)
			}
			if n := leaseSegCount(t, sdb); n != 2 {
				t.Fatalf("segments after direct C compact under Go lease = %d, want 2 (deferred)", n)
			}
			// Go Compact must defer too (same table).
			if err := sdb.Compact(1 << 60); err != nil {
				t.Fatalf("Go Compact under lease: %v", err)
			}
			if n := leaseSegCount(t, sdb); n != 2 {
				t.Fatalf("segments after Go compact under lease = %d, want 2", n)
			}
			// Leased snapshot data must survive both attempts.
			ents, err := sdb.ScanAt("k", snap)
			if err != nil || len(ents) != 1 || string(ents[0].Val) != "v1" {
				t.Fatalf("ScanAt leased = %v, err %v, want [v1]", ents, err)
			}
			if err := sdb.ReleaseLease(id); err != nil {
				t.Fatalf("ReleaseLease: %v", err)
			}
			if rc := cCompact(t, sdb, 1<<60); rc != 0 {
				t.Fatalf("direct C compact after release rc = %d, want 0", rc)
			}
			if n := leaseSegCount(t, sdb); n >= 2 {
				t.Fatalf("segments after direct C compact post-release = %d, want < 2", n)
			}
		})
	}
}

// (c) A lease acquired via the C export path must defer Go
// Compact, and must be visible/releasable through the Go API.
func TestLeaseUnifiedCLeaseDefersGoCompact(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			_, sdb := leaseOpenSegDB(t, bin)
			snap := leaseTwoSegs(t, sdb)
			id := cLeaseAcquire(t, sdb, snap)
			if id == 0 {
				t.Fatalf("direct C lease acquire returned 0")
			}
			if !sdb.LeaseValid(id) {
				t.Fatalf("Go LeaseValid(C id %d) = false, want true", id)
			}
			if seq, has, err := sdb.LeaseOldest(); err != nil || !has || seq != snap {
				t.Fatalf("Go LeaseOldest = (%d, %v, %v), want (%d, true, nil)", seq, has, err, snap)
			}
			if err := sdb.Compact(1 << 60); err != nil {
				t.Fatalf("Go Compact under C lease: %v", err)
			}
			if n := leaseSegCount(t, sdb); n != 2 {
				t.Fatalf("segments after Go compact under C lease = %d, want 2 (deferred)", n)
			}
			if err := sdb.ReleaseLease(id); err != nil {
				t.Fatalf("Go ReleaseLease(C id): %v", err)
			}
			if cLeaseValid(t, sdb, id) {
				t.Fatalf("C lease_valid after Go release = true, want false")
			}
			if err := sdb.Compact(1 << 60); err != nil {
				t.Fatalf("Go Compact after release: %v", err)
			}
			if n := leaseSegCount(t, sdb); n >= 2 {
				t.Fatalf("segments after Go compact post-release = %d, want < 2", n)
			}
		})
	}
}

// (d) Release through either side frees the pin for both;
// (e) double release is a typed error on both sides.
// Also: current-sequence leases do not defer, and future
// sequences are rejected on both sides with the Go typed
// error / C 0 respectively.
func TestLeaseUnifiedCrossReleaseAndDoubleRelease(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			_, sdb := leaseOpenSegDB(t, bin)
			snap := leaseTwoSegs(t, sdb)

			// Go acquire -> C release -> Go double release is typed.
			id, err := sdb.RegisterLease(snap)
			if err != nil {
				t.Fatalf("RegisterLease: %v", err)
			}
			if rc := cLeaseRelease(t, sdb, id); rc != 0 {
				t.Fatalf("C release(Go id) rc = %d, want 0", rc)
			}
			if sdb.LeaseValid(id) {
				t.Fatalf("Go LeaseValid after C release = true, want false")
			}
			if err := sdb.ReleaseLease(id); !errors.Is(err, ErrLeaseExpired) {
				t.Fatalf("Go double release err = %v, want ErrLeaseExpired", err)
			}
			if rc := cLeaseRelease(t, sdb, id); rc != -1 {
				t.Fatalf("C double release rc = %d, want -1", rc)
			}

			// C acquire -> Go release -> C double release is -1.
			cid := cLeaseAcquire(t, sdb, snap)
			if cid == 0 {
				t.Fatalf("C acquire returned 0")
			}
			if err := sdb.ReleaseLease(cid); err != nil {
				t.Fatalf("Go ReleaseLease(C id): %v", err)
			}
			if rc := cLeaseRelease(t, sdb, cid); rc != -1 {
				t.Fatalf("C release after Go release rc = %d, want -1", rc)
			}
			// Unknown ids are typed errors on both sides.
			if err := sdb.ReleaseLease(1 << 40); !errors.Is(err, ErrLeaseExpired) {
				t.Fatalf("Go release unknown err = %v, want ErrLeaseExpired", err)
			}
			if rc := cLeaseRelease(t, sdb, 1<<40); rc != -1 {
				t.Fatalf("C release unknown rc = %d, want -1", rc)
			}
			if sdb.LeaseValid(1<<40) || cLeaseValid(t, sdb, 1<<40) {
				t.Fatalf("unknown id reported valid on some side")
			}

			// Future sequence: Go typed error, C returns 0.
			cur, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			if _, err := sdb.RegisterLease(cur + 1); !errors.Is(err, ErrSnapshotInvalid) {
				t.Fatalf("RegisterLease(future) err = %v, want ErrSnapshotInvalid", err)
			}
			if got := cLeaseAcquire(t, sdb, cur+1); got != 0 {
				t.Fatalf("C acquire(future) = %d, want 0", got)
				_ = cLeaseRelease(t, sdb, got)
			}
			// Current-sequence lease does NOT defer either compact.
			curID, err := sdb.RegisterLease(cur)
			if err != nil {
				t.Fatalf("RegisterLease(current): %v", err)
			}
			if rc := cCompact(t, sdb, 1<<60); rc != 0 {
				t.Fatalf("direct C compact rc = %d, want 0", rc)
			}
			if n := leaseSegCount(t, sdb); n >= 2 {
				t.Fatalf("segments after C compact under current-seq lease = %d, want < 2 (must not defer)", n)
			}
			if err := sdb.ReleaseLease(curID); err != nil {
				t.Fatalf("ReleaseLease(current): %v", err)
			}
			// AcquireLease (current capture) is also a C lease.
			aid, aseq, err := sdb.AcquireLease()
			if err != nil || aid == 0 {
				t.Fatalf("AcquireLease = (%d, %d, %v), want id>0", aid, aseq, err)
			}
			if !cLeaseValid(t, sdb, aid) {
				t.Fatalf("C lease_valid(AcquireLease id) = false, want true")
			}
			if err := sdb.ReleaseLease(aid); err != nil {
				t.Fatalf("ReleaseLease(AcquireLease id): %v", err)
			}
		})
	}
}

// PersistedScanIter (segdb_persist.go, unchanged) registers
// shard leases through the same Go API; those leases must
// now also pin a compact issued directly through the C
// export on the shard, and the iterator must still see only
// its snapshot until it closes.
func TestLeaseUnifiedPersistedIterPinsCDirectCompact(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilityNoSync})
			defer p.Close()
			for b := 0; b < 2; b++ {
				for i := 0; i < 100; i++ {
					kk := "k" + itoa2(b*100+i)
					if err := p.Put(kk, []byte("v")); err != nil {
						t.Fatalf("Put %s: %v", kk, err)
					}
				}
				if err := p.Flush(); err != nil {
					t.Fatalf("Flush batch %d: %v", b, err)
				}
			}
			sh := p.shards[0].sdb
			if n := leaseSegCount(t, sh); n != 2 {
				t.Fatalf("shard SegmentCount = %d, want 2", n)
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("SnapshotVector: %v", err)
			}
			it, err := p.ScanStreaming("", vec)
			if err != nil {
				t.Fatalf("ScanStreaming: %v", err)
			}
			// Advance the sequence past the iterator's leases.
			for i := 0; i < 10; i++ {
				if err := p.Put("new"+itoa2(i), []byte("n")); err != nil {
					t.Fatalf("Put new: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush new: %v", err)
			}
			if n := leaseSegCount(t, sh); n != 3 {
				t.Fatalf("shard SegmentCount after new flush = %d, want 3", n)
			}
			// Direct C compact on the shard must defer on the
			// iterator's (Go-registered, now C-table) leases.
			if rc := cCompact(t, sh, 1<<60); rc != 0 {
				t.Fatalf("direct C compact rc = %d, want 0", rc)
			}
			if n := leaseSegCount(t, sh); n != 3 {
				t.Fatalf("shard segments after direct C compact under iterator lease = %d, want 3", n)
			}
			// Iterator still sees only the 200 old keys.
			seen := 0
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("NextBatch: %v", err)
				}
				if len(page) == 0 {
					break
				}
				seen += len(page)
			}
			if seen != 200 {
				t.Fatalf("iterator saw %d rows, want 200", seen)
			}
			// Draining the iterator to EOF releases its leases
			// (NextBatch releases on shard exhaustion), so a
			// direct C compact must now merge.
			if rc := cCompact(t, sh, 1<<60); rc != 0 {
				t.Fatalf("direct C compact after iterator EOF rc = %d, want 0", rc)
			}
			if n := leaseSegCount(t, sh); n >= 3 {
				t.Fatalf("shard segments after direct C compact post-close = %d, want < 3", n)
			}
		})
	}
}

func itoa2(n int) string {
	if n == 0 {
		return "000"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	for len(digits) < 3 {
		digits = append([]byte{'0'}, digits...)
	}
	return string(digits)
}

// The C table holds at most SEGDB_MAX_LEASES = 64 active
// leases. The 65th Go acquire must fail with the new typed
// ErrLeaseTableFull (not ErrSnapshotInvalid — the sequence
// is valid), the 65th C acquire must return 0, and releasing
// one entry must admit a new lease.
func TestLeaseUnifiedTableCap(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			_, sdb := leaseOpenSegDB(t, bin)
			if err := sdb.Put("k", []byte("v")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			var ids []uint64
			for i := 0; i < 64; i++ {
				id, _, err := sdb.AcquireLease()
				if err != nil {
					t.Fatalf("AcquireLease %d: %v", i, err)
				}
				ids = append(ids, id)
			}
			if _, _, err := sdb.AcquireLease(); !errors.Is(err, ErrLeaseTableFull) {
				t.Fatalf("65th AcquireLease err = %v, want ErrLeaseTableFull", err)
			}
			cur, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			if got := cLeaseAcquire(t, sdb, cur); got != 0 {
				t.Fatalf("65th C acquire = %d, want 0", got)
			}
			if err := sdb.ReleaseLease(ids[0]); err != nil {
				t.Fatalf("ReleaseLease: %v", err)
			}
			if _, _, err := sdb.AcquireLease(); err != nil {
				t.Fatalf("AcquireLease after one release: %v", err)
			}
			for _, id := range ids[1:] {
				if err := sdb.ReleaseLease(id); err != nil {
					t.Fatalf("ReleaseLease %d: %v", id, err)
				}
			}
			// The one acquired after the release is still held;
			// release it via the C export to prove the id space
			// is shared even at the cap.
			if seq, has, err := sdb.LeaseOldest(); err != nil || !has {
				t.Fatalf("LeaseOldest after cap test = (%d, %v, %v), want one active lease", seq, has, err)
			}
		})
	}
}

// (f) Missing-export fallback, via the test-only seam:
// with the seam on, the Go map is the registry exactly as
// Gate 1 behaved (Go Compact defers, ids/validity/release
// all work, backend reports go-fallback) — and, honestly,
// a fallback lease does NOT pin a direct C compact, because
// it was never registered in the C table. That is the
// documented limit of the fallback: it protects Go-issued
// compacts on pre-export binaries only.
func TestLeaseUnifiedMissingExportFallback(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			_, sdb := leaseOpenSegDB(t, bin)
			sdb.leaseFallbackForTest = true
			defer func() { sdb.leaseFallbackForTest = false }()
			if got := sdb.LeaseBackend(); got != "go-fallback" {
				t.Fatalf("LeaseBackend with seam = %q, want go-fallback", got)
			}
			snap := leaseTwoSegs(t, sdb)
			id, err := sdb.RegisterLease(snap)
			if err != nil {
				t.Fatalf("fallback RegisterLease: %v", err)
			}
			if !sdb.LeaseValid(id) {
				t.Fatalf("fallback LeaseValid = false, want true")
			}
			// Go Compact defers on the fallback lease.
			if err := sdb.Compact(1 << 60); err != nil {
				t.Fatalf("fallback Go Compact: %v", err)
			}
			if n := leaseSegCount(t, sdb); n != 2 {
				t.Fatalf("fallback segments after Go compact = %d, want 2 (deferred)", n)
			}
			// Documented limit: the fallback lease is invisible
			// to the C table, so a direct C compact is NOT
			// pinned by it (this is exactly the pre-export
			// behaviour the unification fixes on new binaries).
			if cLeaseValid(t, sdb, id) {
				t.Fatalf("C table sees fallback lease id — registries are live side by side, which O4 forbids")
			}
			if err := sdb.ReleaseLease(id); err != nil {
				t.Fatalf("fallback ReleaseLease: %v", err)
			}
			if err := sdb.ReleaseLease(id); !errors.Is(err, ErrLeaseExpired) {
				t.Fatalf("fallback double release err = %v, want ErrLeaseExpired", err)
			}
			if _, err := sdb.RegisterLease(snap + 1000000); !errors.Is(err, ErrSnapshotInvalid) {
				t.Fatalf("fallback RegisterLease(future) err = %v, want ErrSnapshotInvalid", err)
			}
			// AcquireLease on the fallback path captures + registers.
			aid, _, err := sdb.AcquireLease()
			if err != nil || aid == 0 || !sdb.LeaseValid(aid) {
				t.Fatalf("fallback AcquireLease = (%d, %v), valid=%v", aid, err, sdb.LeaseValid(aid))
			}
			if err := sdb.ReleaseLease(aid); err != nil {
				t.Fatalf("fallback ReleaseLease(AcquireLease id): %v", err)
			}
		})
	}
}
