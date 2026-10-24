package yowza

// Phase D tests: TimelineHandle (layered view over the
// Phase B snapshot + private write set), per-shard C
// timeline primitives, the Go TimelineVM, and
// manager-mode handles. Every test loops over both wasm
// binaries (persistTestBins), like the Phase B/C tests.

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
)

func phaseDOpen(t *testing.T, bin string, shards int) (*PersistedSegDB, string) {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read %s: %v", bin, err)
	}
	dir := t.TempDir()
	p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: shards, Durability: DurabilitySync})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, dir
}

func phaseDPut(t *testing.T, p *PersistedSegDB, k, v string) {
	t.Helper()
	if err := p.Put(k, []byte(v)); err != nil {
		t.Fatalf("put %s: %v", k, err)
	}
}

func phaseDHandleScanAll(t *testing.T, h *TimelineHandle, prefix string) map[string]string {
	t.Helper()
	it, err := h.Scan(prefix)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer it.Close()
	out := map[string]string{}
	for {
		page, err := it.NextBatch(64)
		if err != nil {
			t.Fatalf("scan page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			out[string(e.Key)] = string(e.Val)
		}
	}
	return out
}

func phaseDExpectMap(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("map size %d, want %d (got %v, want %v)", len(got), len(want), got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %q = %q, want %q (full got %v)", k, got[k], v, got)
		}
	}
}

// Basic handle: base snapshot + staged writes, main
// head untouched, main advance invisible to the handle.
func TestPhaseDTimelineHandleBasic(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseDOpen(t, bin, 2)
			for i := 0; i < 20; i++ {
				phaseDPut(t, p, fmt.Sprintf("k:%03d", i), fmt.Sprintf("v1-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := NewTimelineHandle(p)
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			defer h.Close()
			// Advance main + flush: handle must not see it.
			for i := 0; i < 20; i++ {
				phaseDPut(t, p, fmt.Sprintf("k:%03d", i), fmt.Sprintf("v2-%d", i))
			}
			phaseDPut(t, p, "k:new", "v2-new")
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			v, err := h.Get("k:005")
			if err != nil || string(v) != "v1-5" {
				t.Fatalf("handle get old = %q err %v, want v1-5", v, err)
			}
			if _, err := h.Get("k:new"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("handle get main-only key err %v, want ErrTimelineKeyNotFound", err)
			}
			// Staged writes: put + delete, main untouched.
			if err := h.Put("tl:new", []byte("tlv")); err != nil {
				t.Fatalf("handle put: %v", err)
			}
			if err := h.Delete("k:003"); err != nil {
				t.Fatalf("handle delete: %v", err)
			}
			if v, err := h.Get("tl:new"); err != nil || string(v) != "tlv" {
				t.Fatalf("handle staged get = %q err %v", v, err)
			}
			if _, err := h.Get("k:003"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("handle deleted get err %v, want ErrTimelineKeyNotFound", err)
			}
			mv, err := p.Get("k:003")
			if err != nil || string(mv) != "v2-3" {
				t.Fatalf("main get after handle delete = %q err %v, want v2-3", mv, err)
			}
			if _, err := p.Get("tl:new"); err == nil {
				t.Fatalf("main sees handle-staged key (must not)")
			}
			want := map[string]string{}
			for i := 0; i < 20; i++ {
				if i == 3 {
					continue
				}
				want[fmt.Sprintf("k:%03d", i)] = fmt.Sprintf("v1-%d", i)
			}
			want["tl:new"] = "tlv"
			phaseDExpectMap(t, phaseDHandleScanAll(t, h, ""), want)
		})
	}
}

// Per-shard C primitives: get-at, fork, cursor, and the
// fail-closed rules (future, expired lease).
func TestPhaseDPerShardTimelinePrimitives(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseDOpen(t, bin, 2)
			keys := []string{}
			for i := 0; i < 20; i++ {
				k := fmt.Sprintf("k:%03d", i)
				keys = append(keys, k)
				phaseDPut(t, p, k, fmt.Sprintf("v1-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("snapshot vector: %v", err)
			}
			leaseIDs, release, err := p.AcquireShardLeases(vec)
			if err != nil {
				t.Fatalf("acquire leases: %v", err)
			}
			defer release()
			// Advance main + flush.
			for i := 0; i < 20; i++ {
				phaseDPut(t, p, fmt.Sprintf("k:%03d", i), fmt.Sprintf("v2-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			curVec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("current vector: %v", err)
			}
			for _, k := range keys {
				idx := p.ShardIndex(k)
				sdb := p.shards[idx].sdb
				// Pinned get-at returns the OLD value.
				v, err := sdb.TimelineGetAt(k, vec[idx], leaseIDs[idx])
				if err != nil {
					t.Fatalf("timeline get-at %s: %v", k, err)
				}
				// Old value is v1-<i> for every key.
				if len(v) < 3 || string(v[:3]) != "v1-" {
					t.Fatalf("timeline get-at %s = %q, want v1-*", k, v)
				}
				// Lease-0 (caller-owned pinning) also
				// reads the pinned snapshot, never head.
				v0, err := sdb.TimelineGetAt(k, vec[idx], 0)
				if err != nil || string(v0) != string(v) {
					t.Fatalf("timeline get-at lease-0 %s = %q err %v, want %q", k, v0, err, v)
				}
				// Future sequence fails, never head.
				future := curVec[idx] + 100
				if _, err := sdb.TimelineGetAt(k, future, leaseIDs[idx]); !errors.Is(err, ErrSnapshotInvalid) {
					t.Fatalf("future get-at err %v, want ErrSnapshotInvalid", err)
				}
				// Fork: new lease on the same point.
				fk, err := sdb.TimelineFork(vec[idx], leaseIDs[idx])
				if err != nil || fk == 0 {
					t.Fatalf("fork: %v %d", err, fk)
				}
				fv, err := sdb.TimelineGetAt(k, vec[idx], fk)
				if err != nil || string(fv) != string(v) {
					t.Fatalf("fork get-at %s = %q err %v, want %q", k, fv, err, v)
				}
				_ = sdb.ReleaseLease(fk)
				// Expired lease fails (acquire + release
				// a throwaway lease, then read with it).
				tmp, err := sdb.TimelineFork(vec[idx], leaseIDs[idx])
				if err != nil {
					t.Fatalf("tmp fork: %v", err)
				}
				_ = sdb.ReleaseLease(tmp)
				if _, err := sdb.TimelineGetAt(k, vec[idx], tmp); !errors.Is(err, ErrLeaseExpired) {
					t.Fatalf("expired get-at err %v, want ErrLeaseExpired", err)
				}
			}
			// Cursor at the pinned point, per shard:
			// rows must equal ScanAt at the old vector.
			for idx, sh := range p.shards {
				want, err := sh.sdb.ScanAt("k:", vec[idx])
				if err != nil {
					t.Fatalf("shard %d scan-at: %v", idx, err)
				}
				var got []SegDBScanEntry
				var cursor []byte
				hasCursor := false
				for {
					page, next, more, err := sh.sdb.TimelineScanCursor("k:", vec[idx], leaseIDs[idx], cursor, hasCursor, 7)
					if err != nil {
						t.Fatalf("shard %d timeline cursor: %v", idx, err)
					}
					got = append(got, page...)
					if !more {
						break
					}
					cursor, hasCursor = next, true
				}
				if len(got) != len(want) {
					t.Fatalf("shard %d cursor rows %d, want %d", idx, len(got), len(want))
				}
				sort.Slice(got, func(i, j int) bool { return string(got[i].Key) < string(got[j].Key) })
				sort.Slice(want, func(i, j int) bool { return string(want[i].Key) < string(want[j].Key) })
				for i := range want {
					if string(got[i].Key) != string(want[i].Key) || string(got[i].Val) != string(want[i].Val) {
						t.Fatalf("shard %d row %d = %s/%s, want %s/%s", idx, i, got[i].Key, got[i].Val, want[i].Key, want[i].Val)
					}
				}
			}
		})
	}
}

// VM program builders (opcode encodings from vm.h).
func phaseDVMU32(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}
func phaseDVMU64(b []byte, v uint64) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24), byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}
func phaseDVMPut(b []byte, k, v string) []byte {
	b = append(b, OpcodePut)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	b = phaseDVMU32(b, uint32(len(v)))
	b = append(b, v...)
	return b
}
func phaseDVMGet(b []byte, k string) []byte {
	b = append(b, OpcodeGet)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	return b
}
func phaseDVMDel(b []byte, k string) []byte {
	b = append(b, OpcodeDel)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	return b
}
func phaseDVMScan(b []byte, prefix string) []byte {
	b = append(b, OpcodeScan)
	b = phaseDVMU32(b, uint32(len(prefix)))
	b = append(b, prefix...)
	b = phaseDVMU32(b, 1<<20)
	return b
}
func phaseDVMTxBegin(b []byte, id uint64) []byte {
	b = append(b, OpcodeTxBegin)
	b = phaseDVMU64(b, id)
	b = phaseDVMU64(b, 1)
	b = phaseDVMU32(b, 0)
	b = phaseDVMU64(b, 1)
	return b
}
func phaseDVMTxPut(b []byte, k, v string, flags byte) []byte {
	b = append(b, OpcodeTxPut)
	b = append(b, flags)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	b = phaseDVMU32(b, uint32(len(v)))
	b = append(b, v...)
	return b
}
func phaseDVMTxDel(b []byte, k string) []byte {
	b = append(b, OpcodeTxDel)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	return b
}
func phaseDVMTxGet(b []byte, k string) []byte {
	b = append(b, OpcodeTxGet)
	b = phaseDVMU32(b, uint32(len(k)))
	b = append(b, k...)
	return b
}

// VM program through a handle, read back through the VM
// path and the Go path with identical results; main
// head sees nothing.
func TestPhaseDVMProgramThroughHandle(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseDOpen(t, bin, 2)
			for i := 0; i < 5; i++ {
				phaseDPut(t, p, fmt.Sprintf("base:%d", i), fmt.Sprintf("b%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := NewTimelineHandle(p)
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			defer h.Close()
			vm := NewTimelineVM(h)
			prog := []byte{}
			prog = phaseDVMPut(prog, "vm:k", "vmv")
			prog = phaseDVMGet(prog, "vm:k")
			prog = phaseDVMTxBegin(prog, 1)
			prog = phaseDVMTxPut(prog, "tx:k", "txv", 0)
			prog = phaseDVMTxGet(prog, "tx:k")
			prog = append(prog, OpcodeTxCommit)
			prog = phaseDVMGet(prog, "tx:k")
			prog = phaseDVMScan(prog, "")
			prog = append(prog, OpcodeExit)
			results, err := vm.Execute(prog)
			if err != nil {
				t.Fatalf("vm execute: %v", err)
			}
			// Results in program order: GET vm:k,
			// TX_GET tx:k, GET tx:k, SCAN.
			if len(results) != 4 {
				t.Fatalf("vm results %d, want 4", len(results))
			}
			if !results[0].Found || string(results[0].Value) != "vmv" {
				t.Fatalf("vm get vm:k = %v %q", results[0].Found, results[0].Value)
			}
			if !results[1].Found || string(results[1].Value) != "txv" {
				t.Fatalf("vm tx-get tx:k = %v %q", results[1].Found, results[1].Value)
			}
			if !results[2].Found || string(results[2].Value) != "txv" {
				t.Fatalf("vm get tx:k = %v %q", results[2].Found, results[2].Value)
			}
			// Go path: identical values.
			if v, err := h.Get("vm:k"); err != nil || string(v) != "vmv" {
				t.Fatalf("go get vm:k = %q err %v", v, err)
			}
			if v, err := h.Get("tx:k"); err != nil || string(v) != "txv" {
				t.Fatalf("go get tx:k = %q err %v", v, err)
			}
			goMap := phaseDHandleScanAll(t, h, "")
			vmMap := map[string]string{}
			for _, e := range results[3].Rows {
				vmMap[string(e.Key)] = string(e.Val)
			}
			phaseDExpectMap(t, vmMap, goMap)
			// Main head sees neither staged key.
			if _, err := p.Get("vm:k"); err == nil {
				t.Fatalf("main sees vm:k")
			}
			if _, err := p.Get("tx:k"); err == nil {
				t.Fatalf("main sees tx:k")
			}
			// Abort discards; legacy op inside tx errors.
			prog2 := []byte{}
			prog2 = phaseDVMTxBegin(prog2, 2)
			prog2 = phaseDVMTxPut(prog2, "abort:k", "x", 0)
			prog2 = append(prog2, OpcodeTxAbort)
			prog2 = phaseDVMGet(prog2, "abort:k")
			prog2 = append(prog2, OpcodeExit)
			res2, err := vm.Execute(prog2)
			if err != nil {
				t.Fatalf("vm abort program: %v", err)
			}
			if len(res2) != 1 || res2[0].Found {
				t.Fatalf("aborted key visible: %+v", res2)
			}
			prog3 := []byte{}
			prog3 = phaseDVMTxBegin(prog3, 3)
			prog3 = phaseDVMPut(prog3, "bad:k", "x")
			prog3 = append(prog3, OpcodeExit)
			if _, err := vm.Execute(prog3); err == nil {
				t.Fatalf("legacy op inside tx must error")
			}
		})
	}
}

// Fork: same base vector, copied staged writes, later
// staged writes invisible across the fork.
func TestPhaseDTimelineHandleFork(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseDOpen(t, bin, 2)
			phaseDPut(t, p, "a", "1")
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := NewTimelineHandle(p)
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			defer h.Close()
			if err := h.Put("staged", []byte("s")); err != nil {
				t.Fatalf("put: %v", err)
			}
			f, err := h.Fork()
			if err != nil {
				t.Fatalf("fork: %v", err)
			}
			defer f.Close()
			if got, want := fmt.Sprint(f.BaseVector()), fmt.Sprint(h.BaseVector()); got != want {
				t.Fatalf("fork base vector %s != parent %s", got, want)
			}
			if v, err := f.Get("staged"); err != nil || string(v) != "s" {
				t.Fatalf("fork staged get = %q err %v", v, err)
			}
			if err := h.Put("later", []byte("l")); err != nil {
				t.Fatalf("parent later put: %v", err)
			}
			if _, err := f.Get("later"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("fork sees parent's later write: %v", err)
			}
			if err := f.Put("forkonly", []byte("f")); err != nil {
				t.Fatalf("fork put: %v", err)
			}
			if _, err := h.Get("forkonly"); !errors.Is(err, ErrTimelineKeyNotFound) {
				t.Fatalf("parent sees fork's write: %v", err)
			}
		})
	}
}

// Snapshot stable across Flush + Compact while pinned,
// via the handle (not just the raw snapshot handle).
func TestPhaseDTimelineHandleSnapshotStable(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseDOpen(t, bin, 2)
			for i := 0; i < 20; i++ {
				phaseDPut(t, p, fmt.Sprintf("s:%03d", i), fmt.Sprintf("old-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := NewTimelineHandle(p)
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			defer h.Close()
			for i := 0; i < 20; i++ {
				phaseDPut(t, p, fmt.Sprintf("s:%03d", i), fmt.Sprintf("new-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			if err := p.Compact(0); err != nil {
				t.Fatalf("compact under handle: %v", err)
			}
			got := phaseDHandleScanAll(t, h, "s:")
			want := map[string]string{}
			for i := 0; i < 20; i++ {
				want[fmt.Sprintf("s:%03d", i)] = fmt.Sprintf("old-%d", i)
			}
			phaseDExpectMap(t, got, want)
		})
	}
}

// Manager-mode handle + commit to the timeline.
func TestPhaseDTimelineHandleManagerMode(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseDOpen(t, bin, 2)
			mgr, err := NewTimelineManager(p)
			if err != nil {
				t.Fatalf("manager: %v", err)
			}
			t.Cleanup(func() { _ = mgr.Close() })
			if _, err := mgr.ForkFromDB("tl1"); err != nil {
				t.Fatalf("fork from db: %v", err)
			}
			if err := mgr.Put("tl1", "m:k", []byte("mv")); err != nil {
				t.Fatalf("mgr put: %v", err)
			}
			h, err := NewTimelineHandleFromManager(p, mgr, "tl1")
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			defer h.Close()
			if v, err := h.Get("m:k"); err != nil || string(v) != "mv" {
				t.Fatalf("handle mgr get = %q err %v", v, err)
			}
			if err := h.Put("m:staged", []byte("sv")); err != nil {
				t.Fatalf("handle put: %v", err)
			}
			if _, err := mgr.Get("tl1", "m:staged"); err == nil {
				t.Fatalf("manager sees uncommitted staged key")
			}
			if err := h.CommitToTimeline(); err != nil {
				t.Fatalf("commit to timeline: %v", err)
			}
			if v, err := mgr.Get("tl1", "m:staged"); err != nil || string(v) != "sv" {
				t.Fatalf("manager get after commit = %q err %v", v, err)
			}
			// ForkFrom through the handle.
			f, err := h.ForkFrom("tl2")
			if err != nil {
				t.Fatalf("fork from: %v", err)
			}
			defer f.Close()
			if v, err := f.Get("m:staged"); err != nil || string(v) != "sv" {
				t.Fatalf("forked handle get = %q err %v", v, err)
			}
		})
	}
}

// Large handle scan: 800 keys, paged, main advance
// invisible; the scan streams (batches of 64) rather
// than materialising at open.
func TestPhaseDTimelineHandleLargeScan(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read %s: %v", bin, err)
			}
			dir := t.TempDir()
			p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilityNoSync})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = p.Close() })
			for i := 0; i < 800; i++ {
				if err := p.Put(fmt.Sprintf("big:%04d", i), []byte(fmt.Sprintf("bv-%d", i))); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := NewTimelineHandle(p)
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			defer h.Close()
			for i := 0; i < 200; i++ {
				if err := p.Put(fmt.Sprintf("big:%04d", i), []byte(fmt.Sprintf("bv2-%d", i))); err != nil {
					t.Fatalf("put2: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			got := phaseDHandleScanAll(t, h, "big:")
			if len(got) != 800 {
				t.Fatalf("large scan rows %d, want 800", len(got))
			}
			for i := 0; i < 800; i++ {
				if got[fmt.Sprintf("big:%04d", i)] != fmt.Sprintf("bv-%d", i) {
					t.Fatalf("large scan key %d = %q, want bv-%d", i, got[fmt.Sprintf("big:%04d", i)], i)
				}
			}
		})
	}
}

// Recovery: merged timeline state + a durable pin's
// vector both survive close + reopen.
func TestPhaseDTimelineHandleRecovery(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read %s: %v", bin, err)
			}
			dir := t.TempDir()
			open := func() *PersistedSegDB {
				t.Helper()
				pp, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
				if err != nil {
					t.Fatalf("open: %v", err)
				}
				return pp
			}
			p := open()
			for i := 0; i < 10; i++ {
				phaseDPut(t, p, fmt.Sprintf("r:%02d", i), fmt.Sprintf("old-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			pin, err := p.CreateDurablePin("PhaseDTest", "tl-rec", "")
			if err != nil {
				t.Fatalf("pin: %v", err)
			}
			pinVec := append([]uint64(nil), pin.Vector...)
			h, err := NewTimelineHandle(p)
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			if err := h.Put("r:merged", []byte("mv")); err != nil {
				t.Fatalf("handle put: %v", err)
			}
			if err := h.MergeToMain(); err != nil {
				t.Fatalf("merge to main: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush merged: %v", err)
			}
			_ = h.Close()
			if err := p.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			p2 := open()
			t.Cleanup(func() { _ = p2.Close() })
			if v, err := p2.Get("r:merged"); err != nil || string(v) != "mv" {
				t.Fatalf("reopened merged get = %q err %v", v, err)
			}
			// The durable pin's vector still streams
			// the pre-merge old rows after reopen.
			it, err := p2.ScanStreaming("r:", pinVec)
			if err != nil {
				t.Fatalf("pin scan: %v", err)
			}
			rows := map[string]string{}
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("pin scan page: %v", err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					rows[string(e.Key)] = string(e.Val)
				}
			}
			_ = it.Close()
			if len(rows) != 10 {
				t.Fatalf("pin rows after reopen %d, want 10 (%v)", len(rows), rows)
			}
			for i := 0; i < 10; i++ {
				if rows[fmt.Sprintf("r:%02d", i)] != fmt.Sprintf("old-%d", i) {
					t.Fatalf("pin row %d = %q", i, rows[fmt.Sprintf("r:%02d", i)])
				}
			}
		})
	}
}

// Concurrency: parallel handle ops, no races (also
// run under -race by the gate command).
func TestPhaseDTimelineHandleConcurrent(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseDOpen(t, bin, 2)
			phaseDPut(t, p, "seed", "s")
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := NewTimelineHandle(p)
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			defer h.Close()
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 10; i++ {
						k := fmt.Sprintf("c:%d:%d", g, i)
						if err := h.Put(k, []byte("v")); err != nil {
							t.Errorf("put: %v", err)
							return
						}
						if _, err := h.Get(k); err != nil {
							t.Errorf("get: %v", err)
							return
						}
						if _, err := h.Get("seed"); err != nil {
							t.Errorf("get seed: %v", err)
							return
						}
					}
				}(g)
			}
			wg.Wait()
			got := phaseDHandleScanAll(t, h, "c:")
			if len(got) != 80 {
				t.Fatalf("concurrent scan rows %d, want 80", len(got))
			}
		})
	}
}
