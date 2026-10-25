package yowza

// Track 3 tests: parity between the Go TimelineVM path
// and the C/WASM host path (TimelineWASMHost), plus the
// host's error cases. Every test loops over both wasm
// binaries, like the Phase D tests.
//
// Parity is asserted for: GET/PUT/DELETE (legacy,
// remote, CRDT), staged writes, TX commit staging
// (put/get/del/assert/unique/abort), and the empty-base
// and older-vector pins. Scan is the documented split:
// the Go path scans, the C host rejects OP_SCAN upfront
// with ErrTimelineWASMScanUnsupported and stages
// nothing. Failed C programs also stage nothing (the
// one-shot export returns an error pointer, so there
// is nothing to drain), while the Go VM keeps legacy
// puts staged before the failure — that divergence is
// asserted as host behaviour, not claimed as parity.

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func wasmHostOpen(t *testing.T, bin string, shards int) *PersistedSegDB {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read %s: %v", bin, err)
	}
	dir := t.TempDir()
	p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: shards, Durability: DurabilityNoSync})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func wasmHostBase(t *testing.T, p *PersistedSegDB, pairs map[string]string) {
	t.Helper()
	for k, v := range pairs {
		if err := p.Put(k, []byte(v)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func wasmHostHandle(t *testing.T, p *PersistedSegDB) *TimelineHandle {
	t.Helper()
	h, err := NewTimelineHandle(p)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func wasmHostNew(t *testing.T, bin string, dst *TimelineHandle) *TimelineWASMHost {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read %s: %v", bin, err)
	}
	host, err := NewTimelineWASMHost(wasmBytes, dst)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	return host
}

// wasmHostTxAssert builds [kind u8][klen][key][vlen][val].
func wasmHostTxAssert(b []byte, kind byte, k, v string) []byte {
	b = append(b, OpcodeTxAssert, kind)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	b = phaseDVMU32(b, uint32(len(v)))
	b = append(b, v...)
	return b
}

func wasmHostPutRemote(b []byte, k, v string) []byte {
	b = append(b, OpcodePutRemote)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	b = phaseDVMU32(b, uint32(len(v)))
	b = append(b, v...)
	return b
}

func wasmHostDelRemote(b []byte, k string) []byte {
	b = append(b, OpcodeDelRemote)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	return b
}

func wasmHostPutCRDT(b []byte, k, v string, ts uint64) []byte {
	b = append(b, OpcodePutCRDT)
	b = phaseDVMU64(b, ts)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	b = phaseDVMU32(b, uint32(len(v)))
	b = append(b, v...)
	return b
}

func wasmHostTxPutCRDT(b []byte, k, v string, ts uint64, flags byte) []byte {
	b = append(b, OpcodeTxPutCRDT)
	b = phaseDVMU64(b, ts)
	b = append(b, flags)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	b = phaseDVMU32(b, uint32(len(v)))
	b = append(b, v...)
	return b
}

func wasmHostCompareResults(t *testing.T, got, want []TimelineVMResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("results %d, want %d (got %+v, want %+v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Op != want[i].Op || got[i].Key != want[i].Key || got[i].Found != want[i].Found || string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("result %d = op %02x key %q found %v val %q, want op %02x key %q found %v val %q",
				i, got[i].Op, got[i].Key, got[i].Found, got[i].Value,
				want[i].Op, want[i].Key, want[i].Found, want[i].Value)
		}
	}
}

func wasmHostExpectHandle(t *testing.T, h *TimelineHandle, want map[string]string) {
	t.Helper()
	got := phaseDHandleScanAll(t, h, "")
	phaseDExpectMap(t, got, want)
}

// The shared parity program: legacy GET/PUT/DEL plus a
// TX commit (put/get/del) and a TX abort program.
// Expected results and final handle view are the same
// on both paths.
func TestTimelineWASMHostParityBasic(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"base:0": "b0", "base:1": "b1", "base:2": "b2"})
			hGo := wasmHostHandle(t, p)
			hC := wasmHostHandle(t, p)
			// Pre-staged writes on both handles, identical.
			for _, h := range []*TimelineHandle{hGo, hC} {
				if err := h.Put("pre:staged", []byte("ps")); err != nil {
					t.Fatalf("pre-stage: %v", err)
				}
			}
			prog := []byte{}
			prog = phaseDVMGet(prog, "base:0")
			prog = phaseDVMGet(prog, "pre:staged")
			prog = phaseDVMPut(prog, "vm:k", "vmv")
			prog = phaseDVMGet(prog, "vm:k")
			prog = phaseDVMDel(prog, "base:1")
			prog = phaseDVMGet(prog, "base:1")
			prog = phaseDVMTxBegin(prog, 1)
			prog = phaseDVMTxPut(prog, "tx:k", "txv", 0)
			prog = phaseDVMTxGet(prog, "tx:k")
			prog = phaseDVMTxDel(prog, "base:2")
			prog = phaseDVMTxGet(prog, "base:2")
			prog = append(prog, OpcodeTxCommit)
			prog = phaseDVMGet(prog, "tx:k")
			prog = phaseDVMGet(prog, "base:2")
			prog = append(prog, OpcodeExit)
			want := []TimelineVMResult{
				{Op: OpcodeGet, Key: "base:0", Found: true, Value: []byte("b0")},
				{Op: OpcodeGet, Key: "pre:staged", Found: true, Value: []byte("ps")},
				{Op: OpcodeGet, Key: "vm:k", Found: true, Value: []byte("vmv")},
				{Op: OpcodeGet, Key: "base:1", Found: false},
				{Op: OpcodeTxGet, Key: "tx:k", Found: true, Value: []byte("txv")},
				{Op: OpcodeTxGet, Key: "base:2", Found: false},
				{Op: OpcodeGet, Key: "tx:k", Found: true, Value: []byte("txv")},
				{Op: OpcodeGet, Key: "base:2", Found: false},
			}
			goRes, err := NewTimelineVM(hGo).Execute(prog)
			if err != nil {
				t.Fatalf("go vm: %v", err)
			}
			wasmHostCompareResults(t, goRes, want)
			snap, err := TimelineWASMSnapshotFromHandle(hC)
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			cRes, err := wasmHostNew(t, bin, hC).Execute(prog, snap)
			if err != nil {
				t.Fatalf("c host: %v", err)
			}
			wasmHostCompareResults(t, cRes, want)
			wasmHostCompareResults(t, cRes, goRes)
			wantView := map[string]string{"base:0": "b0", "pre:staged": "ps", "vm:k": "vmv", "tx:k": "txv"}
			wasmHostExpectHandle(t, hGo, wantView)
			wasmHostExpectHandle(t, hC, wantView)
			// Abort program: same on both paths, nothing staged.
			abort := []byte{}
			abort = phaseDVMTxBegin(abort, 2)
			abort = phaseDVMTxPut(abort, "abort:k", "x", 0)
			abort = append(abort, OpcodeTxAbort)
			abort = phaseDVMGet(abort, "abort:k")
			abort = append(abort, OpcodeExit)
			goRes, err = NewTimelineVM(hGo).Execute(abort)
			if err != nil {
				t.Fatalf("go abort: %v", err)
			}
			if len(goRes) != 1 || goRes[0].Found {
				t.Fatalf("go abort visible: %+v", goRes)
			}
			snap, err = TimelineWASMSnapshotFromHandle(hC)
			if err != nil {
				t.Fatalf("snapshot2: %v", err)
			}
			cRes, err = wasmHostNew(t, bin, hC).Execute(abort, snap)
			if err != nil {
				t.Fatalf("c abort: %v", err)
			}
			if len(cRes) != 1 || cRes[0].Found {
				t.Fatalf("c abort visible: %+v", cRes)
			}
			wasmHostExpectHandle(t, hC, wantView)
			// Drained writes are real handle writes: merging
			// the C handle lands them on the main head.
			if err := hC.MergeToMain(); err != nil {
				t.Fatalf("merge c handle: %v", err)
			}
			for k, v := range wantView {
				got, err := p.Get(k)
				if err != nil || string(got) != v {
					t.Fatalf("main %s = %q err %v, want %q", k, got, err, v)
				}
			}
			if _, err := p.Get("base:1"); err == nil {
				t.Fatalf("main still sees deleted base:1")
			}
		})
	}
}

func TestTimelineWASMHostParityTxAssertUnique(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"a:k": "av", "u:k": "uv"})
			hGo := wasmHostHandle(t, p)
			hC := wasmHostHandle(t, p)
			prog := []byte{}
			prog = phaseDVMTxBegin(prog, 1)
			prog = wasmHostTxAssert(prog, 1, "a:k", "")
			prog = wasmHostTxAssert(prog, 0, "missing:k", "")
			prog = wasmHostTxAssert(prog, 2, "a:k", "av")
			prog = phaseDVMTxPut(prog, "new:k", "nv", 0)
			prog = phaseDVMTxPut(prog, "uniq:k", "u1", 0x01)
			prog = append(prog, OpcodeTxCommit)
			prog = phaseDVMGet(prog, "new:k")
			prog = phaseDVMGet(prog, "uniq:k")
			prog = append(prog, OpcodeExit)
			want := []TimelineVMResult{
				{Op: OpcodeGet, Key: "new:k", Found: true, Value: []byte("nv")},
				{Op: OpcodeGet, Key: "uniq:k", Found: true, Value: []byte("u1")},
			}
			goRes, err := NewTimelineVM(hGo).Execute(prog)
			if err != nil {
				t.Fatalf("go vm: %v", err)
			}
			wasmHostCompareResults(t, goRes, want)
			snap, _ := TimelineWASMSnapshotFromHandle(hC)
			cRes, err := wasmHostNew(t, bin, hC).Execute(prog, snap)
			if err != nil {
				t.Fatalf("c host: %v", err)
			}
			wasmHostCompareResults(t, cRes, want)
			// Assert failure: both error.
			bad := []byte{}
			bad = phaseDVMTxBegin(bad, 2)
			bad = wasmHostTxAssert(bad, 1, "missing:k", "")
			bad = append(bad, OpcodeExit)
			if _, err := NewTimelineVM(hGo).Execute(bad); err == nil {
				t.Fatalf("go assert failure must error")
			}
			snap, _ = TimelineWASMSnapshotFromHandle(hC)
			if _, err := wasmHostNew(t, bin, hC).Execute(bad, snap); err == nil {
				t.Fatalf("c assert failure must error")
			}
			// UNIQUE failure on an existing base key: both
			// error at commit, nothing staged either way.
			dup := []byte{}
			dup = phaseDVMTxBegin(dup, 3)
			dup = phaseDVMTxPut(dup, "u:k", "new", 0x01)
			dup = append(dup, OpcodeTxCommit)
			dup = append(dup, OpcodeExit)
			if _, err := NewTimelineVM(hGo).Execute(dup); err == nil {
				t.Fatalf("go unique failure must error")
			}
			snap, _ = TimelineWASMSnapshotFromHandle(hC)
			if _, err := wasmHostNew(t, bin, hC).Execute(dup, snap); err == nil {
				t.Fatalf("c unique failure must error")
			}
			wasmHostExpectHandle(t, hC, map[string]string{"a:k": "av", "u:k": "uv", "new:k": "nv", "uniq:k": "u1"})
		})
	}
}

func TestTimelineWASMHostParityRemoteCRDT(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"r:old": "rv", "r:del": "dv"})
			hGo := wasmHostHandle(t, p)
			hC := wasmHostHandle(t, p)
			prog := []byte{}
			prog = wasmHostPutRemote(prog, "r:new", "rn")
			prog = phaseDVMGet(prog, "r:new")
			prog = wasmHostPutCRDT(prog, "r:crdt", "cv", 42)
			prog = phaseDVMGet(prog, "r:crdt")
			prog = wasmHostDelRemote(prog, "r:del")
			prog = phaseDVMGet(prog, "r:del")
			prog = phaseDVMTxBegin(prog, 1)
			prog = wasmHostTxPutCRDT(prog, "r:txcrdt", "tc", 7, 0)
			prog = phaseDVMTxGet(prog, "r:txcrdt")
			prog = append(prog, OpcodeTxCommit)
			prog = phaseDVMGet(prog, "r:txcrdt")
			prog = append(prog, OpcodeExit)
			want := []TimelineVMResult{
				{Op: OpcodeGet, Key: "r:new", Found: true, Value: []byte("rn")},
				{Op: OpcodeGet, Key: "r:crdt", Found: true, Value: []byte("cv")},
				{Op: OpcodeGet, Key: "r:del", Found: false},
				{Op: OpcodeTxGet, Key: "r:txcrdt", Found: true, Value: []byte("tc")},
				{Op: OpcodeGet, Key: "r:txcrdt", Found: true, Value: []byte("tc")},
			}
			goRes, err := NewTimelineVM(hGo).Execute(prog)
			if err != nil {
				t.Fatalf("go vm: %v", err)
			}
			wasmHostCompareResults(t, goRes, want)
			snap, _ := TimelineWASMSnapshotFromHandle(hC)
			cRes, err := wasmHostNew(t, bin, hC).Execute(prog, snap)
			if err != nil {
				t.Fatalf("c host: %v", err)
			}
			wasmHostCompareResults(t, cRes, want)
			wasmHostExpectHandle(t, hC, map[string]string{"r:old": "rv", "r:new": "rn", "r:crdt": "cv", "r:txcrdt": "tc"})
		})
	}
}

// Pinned older vector: main advances after the handle
// is taken; both paths still read the pinned values,
// never the advanced head.
func TestTimelineWASMHostOlderVector(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"k:1": "v1", "k:2": "v1"})
			hGo := wasmHostHandle(t, p)
			hC := wasmHostHandle(t, p)
			// Advance main past the handle's base.
			if err := p.Put("k:1", []byte("v2")); err != nil {
				t.Fatalf("advance put: %v", err)
			}
			if err := p.Put("k:new", []byte("v2-new")); err != nil {
				t.Fatalf("advance put new: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			prog := []byte{}
			prog = phaseDVMGet(prog, "k:1")
			prog = phaseDVMGet(prog, "k:new")
			prog = phaseDVMPut(prog, "k:staged", "sv")
			prog = phaseDVMGet(prog, "k:staged")
			prog = append(prog, OpcodeExit)
			want := []TimelineVMResult{
				{Op: OpcodeGet, Key: "k:1", Found: true, Value: []byte("v1")},
				{Op: OpcodeGet, Key: "k:new", Found: false},
				{Op: OpcodeGet, Key: "k:staged", Found: true, Value: []byte("sv")},
			}
			goRes, err := NewTimelineVM(hGo).Execute(prog)
			if err != nil {
				t.Fatalf("go vm: %v", err)
			}
			wasmHostCompareResults(t, goRes, want)
			snap, _ := TimelineWASMSnapshotFromHandle(hC)
			cRes, err := wasmHostNew(t, bin, hC).Execute(prog, snap)
			if err != nil {
				t.Fatalf("c host: %v", err)
			}
			wasmHostCompareResults(t, cRes, want)
			wasmHostExpectHandle(t, hC, map[string]string{"k:1": "v1", "k:2": "v1", "k:staged": "sv"})
		})
	}
}

// Snapshot 0 is the empty base, not "current": a C
// program under it sees neither base keys nor
// main-head-only keys, only its own staged writes.
func TestTimelineWASMHostEmptyBaseSnapshotZero(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"base:k": "bv"})
			h := wasmHostHandle(t, p)
			if err := p.Put("main:k", []byte("mv")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			prog := []byte{}
			prog = phaseDVMGet(prog, "base:k")
			prog = phaseDVMGet(prog, "main:k")
			prog = phaseDVMPut(prog, "only:k", "ov")
			prog = phaseDVMGet(prog, "only:k")
			prog = append(prog, OpcodeExit)
			res, err := wasmHostNew(t, bin, h).Execute(prog, TimelineWASMEmptySnapshot(2))
			if err != nil {
				t.Fatalf("c host empty base: %v", err)
			}
			wasmHostCompareResults(t, res, []TimelineVMResult{
				{Op: OpcodeGet, Key: "base:k", Found: false},
				{Op: OpcodeGet, Key: "main:k", Found: false},
				{Op: OpcodeGet, Key: "only:k", Found: true, Value: []byte("ov")},
			})
			// Drain landed only the staged key; the
			// handle's own base view is untouched.
			if v, err := h.Get("only:k"); err != nil || string(v) != "ov" {
				t.Fatalf("drained only:k = %q err %v", v, err)
			}
			if v, err := h.Get("base:k"); err != nil || string(v) != "bv" {
				t.Fatalf("handle base:k = %q err %v", v, err)
			}
			if _, err := p.Get("only:k"); err == nil {
				t.Fatalf("main sees drained staged key")
			}
		})
	}
}

// Scan split: Go scans, C host refuses upfront and
// stages nothing (not even the PUT before the SCAN).
func TestTimelineWASMHostScanUnsupported(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"s:1": "v1", "s:2": "v2"})
			hGo := wasmHostHandle(t, p)
			hC := wasmHostHandle(t, p)
			prog := []byte{}
			prog = phaseDVMPut(prog, "s:3", "v3")
			prog = phaseDVMScan(prog, "s:")
			prog = append(prog, OpcodeExit)
			goRes, err := NewTimelineVM(hGo).Execute(prog)
			if err != nil {
				t.Fatalf("go scan: %v", err)
			}
			if len(goRes) != 1 || len(goRes[0].Rows) != 3 {
				t.Fatalf("go scan rows %+v", goRes)
			}
			snap, _ := TimelineWASMSnapshotFromHandle(hC)
			_, err = wasmHostNew(t, bin, hC).Execute(prog, snap)
			if !errors.Is(err, ErrTimelineWASMScanUnsupported) {
				t.Fatalf("c scan err %v, want ErrTimelineWASMScanUnsupported", err)
			}
			if _, err := hC.Get("s:3"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("c handle staged s:3 despite scan refusal (err %v)", err)
			}
			// The Go handle still scans its own view.
			wasmHostExpectHandle(t, hGo, map[string]string{"s:1": "v1", "s:2": "v2", "s:3": "v3"})
		})
	}
}

func TestTimelineWASMHostFutureSnapshot(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"f:k": "fv"})
			h := wasmHostHandle(t, p)
			if err := h.Put("pre:k", []byte("pv")); err != nil {
				t.Fatalf("pre-stage: %v", err)
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("vector: %v", err)
			}
			vec[0]++
			prog := []byte{}
			prog = phaseDVMPut(prog, "evil:k", "ev")
			prog = append(prog, OpcodeExit)
			_, err = wasmHostNew(t, bin, h).Execute(prog, TimelineWASMSnapshot{Vector: vec})
			if !errors.Is(err, ErrTimelineWASMFutureSnapshot) {
				t.Fatalf("future err %v, want ErrTimelineWASMFutureSnapshot", err)
			}
			if !errors.Is(err, ErrSnapshotInvalid) {
				t.Fatalf("future err %v, want it to wrap ErrSnapshotInvalid", err)
			}
			// Nothing drained, pre-existing staged write intact.
			if v, err := h.Get("pre:k"); err != nil || string(v) != "pv" {
				t.Fatalf("pre:k = %q err %v after future refusal", v, err)
			}
			if _, err := h.Get("evil:k"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("evil:k staged despite future refusal (err %v)", err)
			}
		})
	}
}

func TestTimelineWASMHostExpiredAndInvalidLease(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"l:k": "lv"})
			h := wasmHostHandle(t, p)
			snap, err := TimelineWASMSnapshotFromHandle(h)
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			ids, release, err := p.AcquireShardLeases(snap.Vector)
			if err != nil {
				t.Fatalf("acquire leases: %v", err)
			}
			if err := release(); err != nil {
				t.Fatalf("release leases: %v", err)
			}
			prog := []byte{}
			prog = phaseDVMPut(prog, "evil:k", "ev")
			prog = append(prog, OpcodeExit)
			expired := TimelineWASMSnapshot{Vector: snap.Vector, LeaseIDs: ids}
			_, err = wasmHostNew(t, bin, h).Execute(prog, expired)
			if !errors.Is(err, ErrTimelineWASMLeaseExpired) {
				t.Fatalf("expired lease err %v, want ErrTimelineWASMLeaseExpired", err)
			}
			if !errors.Is(err, ErrLeaseExpired) {
				t.Fatalf("expired lease err %v, want it to wrap ErrLeaseExpired", err)
			}
			bogus := TimelineWASMSnapshot{Vector: snap.Vector, LeaseIDs: []uint64{999999, 999999}}
			_, err = wasmHostNew(t, bin, h).Execute(prog, bogus)
			if !errors.Is(err, ErrTimelineWASMLeaseExpired) {
				t.Fatalf("bogus lease err %v, want ErrTimelineWASMLeaseExpired", err)
			}
			short := TimelineWASMSnapshot{Vector: snap.Vector[:1]}
			_, err = wasmHostNew(t, bin, h).Execute(prog, short)
			if !errors.Is(err, ErrTimelineWASMSnapshotInvalid) {
				t.Fatalf("short vector err %v, want ErrTimelineWASMSnapshotInvalid", err)
			}
			if _, err := h.Get("evil:k"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("evil:k staged despite lease/vector refusal (err %v)", err)
			}
		})
	}
}

func TestTimelineWASMHostClosedHandle(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"c:k": "cv"})
			h := wasmHostHandle(t, p)
			snap, err := TimelineWASMSnapshotFromHandle(h)
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			if err := h.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			prog := []byte{}
			prog = phaseDVMPut(prog, "x:k", "xv")
			prog = append(prog, OpcodeExit)
			_, err = wasmHostNew(t, bin, h).Execute(prog, snap)
			if !errors.Is(err, ErrTimelineHandleClosed) {
				t.Fatalf("closed err %v, want ErrTimelineHandleClosed", err)
			}
		})
	}
}

// Drain rollback: an oversized value executes fine in
// C but fails handle validation at drain time, so the
// whole drain — including valid entries before it —
// applies nothing and pre-existing staged writes survive.
// The direct Apply test proves the same atomicity for
// a multi-write set with a late invalid entry.
func TestTimelineWASMHostDrainRollback(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"d:k": "dv"})
			h := wasmHostHandle(t, p)
			if err := h.Put("pre:k", []byte("pv")); err != nil {
				t.Fatalf("pre-stage: %v", err)
			}
			prog := []byte{}
			prog = phaseDVMPut(prog, "good:k", "gv")
			prog = phaseDVMPut(prog, "big:k", strings.Repeat("x", 1500))
			prog = append(prog, OpcodeExit)
			snap, _ := TimelineWASMSnapshotFromHandle(h)
			_, err := wasmHostNew(t, bin, h).Execute(prog, snap)
			if err == nil {
				t.Fatalf("oversized drain must error")
			}
			// good:k (valid, before the bad entry) was
			// NOT applied, and pre:k survives.
			if _, err := h.Get("good:k"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("good:k applied despite failed drain (err %v)", err)
			}
			if _, err := h.Get("big:k"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("big:k applied despite failed drain (err %v)", err)
			}
			if v, err := h.Get("pre:k"); err != nil || string(v) != "pv" {
				t.Fatalf("pre:k = %q err %v after failed drain", v, err)
			}
			// Direct atomicity: valid + late invalid.
			if err := h.ApplyStagedWritesAtomic([]TimelineStagedWrite{
				{Key: "a:k", Val: []byte("av")},
				{Key: "bad:k", Val: []byte(strings.Repeat("y", 2000))},
			}); err == nil {
				t.Fatalf("direct apply with invalid entry must error")
			}
			if _, err := h.Get("a:k"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("a:k applied despite atomic apply failure (err %v)", err)
			}
			if err := h.ApplyStagedWritesAtomic([]TimelineStagedWrite{
				{Key: "a:k", Val: []byte("av")},
				{Key: "del:k", Deleted: true},
			}); err != nil {
				t.Fatalf("valid atomic apply: %v", err)
			}
			if v, err := h.Get("a:k"); err != nil || string(v) != "av" {
				t.Fatalf("a:k = %q err %v after valid apply", v, err)
			}
			// Closed handle refuses before mutation.
			h2 := wasmHostHandle(t, p)
			if err := h2.Close(); err != nil {
				t.Fatalf("close h2: %v", err)
			}
			if err := h2.ApplyStagedWritesAtomic([]TimelineStagedWrite{{Key: "z:k", Val: []byte("zv")}}); !errors.Is(err, ErrTimelineHandleClosed) {
				t.Fatalf("closed apply err %v, want ErrTimelineHandleClosed", err)
			}
		})
	}
}

// Failed C programs stage nothing (documented
// divergence from the Go VM, which keeps legacy puts
// staged before the failure). Both paths error; only
// the host's no-staging is asserted here.
func TestTimelineWASMHostFailedProgramStagesNothing(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			wasmHostBase(t, p, map[string]string{"e:k": "ev"})
			hGo := wasmHostHandle(t, p)
			hC := wasmHostHandle(t, p)
			prog := []byte{}
			prog = phaseDVMPut(prog, "before:k", "bv")
			prog = phaseDVMTxBegin(prog, 1)
			prog = phaseDVMPut(prog, "bad:k", "x")
			prog = append(prog, OpcodeExit)
			if _, err := NewTimelineVM(hGo).Execute(prog); err == nil {
				t.Fatalf("go legacy-in-tx must error")
			}
			snap, _ := TimelineWASMSnapshotFromHandle(hC)
			if _, err := wasmHostNew(t, bin, hC).Execute(prog, snap); err == nil {
				t.Fatalf("c legacy-in-tx must error")
			}
			if _, err := hC.Get("before:k"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("c handle staged before:k despite failed program (err %v)", err)
			}
			if hC.WriteCount() != 0 {
				t.Fatalf("c handle write count %d, want 0 after failed program", hC.WriteCount())
			}
		})
	}
}

func TestTimelineWASMHostConstructionErrors(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := wasmHostOpen(t, bin, 2)
			h := wasmHostHandle(t, p)
			if _, err := NewTimelineWASMHost(nil, h); !errors.Is(err, ErrTimelineWASMExportMissing) {
				t.Fatalf("empty wasm err %v, want ErrTimelineWASMExportMissing", err)
			}
			if _, err := NewTimelineWASMHost([]byte("not wasm"), h); err == nil {
				t.Fatalf("garbage wasm must fail construction")
			}
			if _, err := NewTimelineWASMHost(mustReadBin(t, bin), nil); err == nil {
				t.Fatalf("nil destination must fail construction")
			}
			_ = fmt.Sprintf
		})
	}
}

func mustReadBin(t *testing.T, bin string) []byte {
	t.Helper()
	b, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read %s: %v", bin, err)
	}
	return b
}
