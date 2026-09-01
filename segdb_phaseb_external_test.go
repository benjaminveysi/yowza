package yowza_test

// Phase B / O4 — external-caller test.
//
// This file is package yowza_test (not package yowza), so
// it proves AcquireShardLeases and the snapshot-handle
// API are usable by a caller that cannot see any private
// field or map: only exported methods are used. (The
// in-package tests live in segdb_phaseb_test.go; this
// file exists precisely because an in-package test could
// not prove the external point.)

import (
	"fmt"
	"os"
	"testing"

	yowza "github.com/yowza/yowza"
)

func TestPhaseBExternalAcquireShardLeases(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			dir := t.TempDir()
			p, err := yowza.OpenPersistedSegDB(dir, wasmBytes, yowza.PersistedOptions{NumShards: 2, Durability: yowza.DurabilitySync})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer p.Close()
			for i := 0; i < 20; i++ {
				if err := p.Put(fmt.Sprintf("e:%03d", i), []byte("old")); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("vector: %v", err)
			}
			// External multi-shard lease: no map, no
			// private field, just the public API.
			ids, release, err := p.AcquireShardLeases(vec)
			if err != nil {
				t.Fatalf("acquire shard leases: %v", err)
			}
			if len(ids) != 2 {
				t.Fatalf("lease ids %v, want 2", ids)
			}
			// Advance + flush + compact under the lease.
			for i := 0; i < 20; i++ {
				if err := p.Put(fmt.Sprintf("e:%03d", i), []byte("new")); err != nil {
					t.Fatalf("put2: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			if err := p.Compact(0); err != nil {
				t.Fatalf("compact under lease: %v", err)
			}
			// Old vector still streams fully under lease.
			it, err := p.ScanStreaming("e:", vec)
			if err != nil {
				t.Fatalf("scan at vector: %v", err)
			}
			n := 0
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("scan page: %v", err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					if string(e.Val) != "old" {
						t.Fatalf("row %s = %s, want old", e.Key, e.Val)
					}
				}
				n += len(page)
			}
			_ = it.Close()
			if n != 20 {
				t.Fatalf("rows %d, want 20", n)
			}
			// Release is idempotent and current data
			// survives afterwards.
			if err := release(); err != nil {
				t.Fatalf("release: %v", err)
			}
			if err := release(); err != nil {
				t.Fatalf("release idempotent: %v", err)
			}
			got, err := p.Get("e:003")
			if err != nil || string(got) != "new" {
				t.Fatalf("current get = %q err %v, want new", got, err)
			}
			// SnapshotHandle from outside the package.
			h, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("acquire snapshot: %v", err)
			}
			if len(h.Vector()) != 2 {
				t.Fatalf("handle vector len %d, want 2", len(h.Vector()))
			}
			v, err := h.Get([]byte("e:003"))
			if err != nil || string(v) != "new" {
				t.Fatalf("handle get = %q err %v, want new", v, err)
			}
			if err := h.Close(); err != nil {
				t.Fatalf("handle close: %v", err)
			}
			// A mismatched vector is refused, not
			// silently pinned.
			if _, _, err := p.AcquireShardLeases([]uint64{1}); err == nil {
				t.Fatalf("short vector accepted")
			}
		})
	}
}
