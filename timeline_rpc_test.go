package yowza

// Phase E, E3 tests: owner-process RPC —
// idempotent retry, lost-response recovery, stale
// head, restart durability, concurrent clients.

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestPhaseERPCIdempotent(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _ := phaseEGlobalSetup(t, bin, 1)
			srv, err := StartTimelineRPCServer(p, mgr, "127.0.0.1:0")
			if err != nil {
				t.Fatalf("start server: %v", err)
			}
			defer srv.Close()
			c := DialTimelineRPC(srv.Addr())
			if err := c.Ping(); err != nil {
				t.Fatalf("ping: %v", err)
			}
			// First apply.
			resp, err := c.Put("req-1", "rk", []byte("v1"), nil)
			if err != nil || resp.Error != "" || !resp.Applied {
				t.Fatalf("put req-1: resp %+v err %v", resp, err)
			}
			// Another writer changes the key in between.
			if err := p.Put("rk", []byte("intervening")); err != nil {
				t.Fatalf("intervening put: %v", err)
			}
			// Retry with the same ID: stored response, NO re-apply.
			resp2, err := c.Put("req-1", "rk", []byte("v1"), nil)
			if err != nil || resp2.Error != "" || !resp2.Applied {
				t.Fatalf("retry req-1: resp %+v err %v", resp2, err)
			}
			phaseEMustDBGet(t, p, "rk", "intervening")
			// Lost-response recovery: the original outcome is fetchable.
			rec, err := c.RecoverResponse("req-1")
			if err != nil || !rec.Applied {
				t.Fatalf("recover req-1: %+v err %v", rec, err)
			}
			// Unknown ID: typed error.
			if _, err := c.RecoverResponse("req-nope"); !errors.Is(err, ErrRPCUnknownID) {
				t.Fatalf("unknown recover err %v, want ErrRPCUnknownID", err)
			}
			// Stale expected head: refused, and the refusal is recorded
			// (a retry of the same ID stays refused, never a second chance).
			head, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("vector: %v", err)
			}
			stale := append([]uint64(nil), head...)
			if err := p.Put("other", []byte("x")); err != nil {
				t.Fatalf("move head: %v", err)
			}
			resp3, err := c.Put("req-stale", "rk2", []byte("v"), stale)
			if err != nil || resp3.Error == "" || resp3.Applied {
				t.Fatalf("stale put: resp %+v err %v, want refused", resp3, err)
			}
			resp4, err := c.Put("req-stale", "rk2", []byte("v"), stale)
			if err != nil || resp4.Error == "" || resp4.Applied {
				t.Fatalf("stale retry: resp %+v err %v, want refused", resp4, err)
			}
			phaseEMustDBMissing(t, p, "rk2")
			// Scan through RPC.
			scan, err := c.Scan("scan-1", "r", 64)
			if err != nil || scan.Error != "" {
				t.Fatalf("scan: %+v err %v", scan, err)
			}
			if len(scan.Rows) < 1 {
				t.Fatalf("scan rows %d, want >= 1", len(scan.Rows))
			}
			// Server restart: responses survive (reloaded from the log).
			if err := srv.Close(); err != nil {
				t.Fatalf("server close: %v", err)
			}
			srv2, err := StartTimelineRPCServer(p, mgr, "127.0.0.1:0")
			if err != nil {
				t.Fatalf("restart server: %v", err)
			}
			defer srv2.Close()
			c2 := DialTimelineRPC(srv2.Addr())
			rec2, err := c2.RecoverResponse("req-1")
			if err != nil || !rec2.Applied {
				t.Fatalf("post-restart recover: %+v err %v", rec2, err)
			}
			// Retry after restart still does not re-apply.
			if _, err := c2.Put("req-1", "rk", []byte("v1"), nil); err != nil {
				t.Fatalf("post-restart retry: %v", err)
			}
			phaseEMustDBGet(t, p, "rk", "intervening")
		})
	}
}

func TestPhaseERPCConcurrentClients(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _ := phaseEGlobalSetup(t, bin, 1)
			srv, err := StartTimelineRPCServer(p, mgr, "127.0.0.1:0")
			if err != nil {
				t.Fatalf("start server: %v", err)
			}
			defer srv.Close()
			c := DialTimelineRPC(srv.Addr())
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					resp, err := c.Put(fmt.Sprintf("conc-%d", i), fmt.Sprintf("ck%d", i), []byte("cv"), nil)
					if err != nil || resp.Error != "" || !resp.Applied {
						t.Errorf("concurrent put %d: %+v err %v", i, resp, err)
					}
				}(i)
			}
			wg.Wait()
			for i := 0; i < 8; i++ {
				phaseEMustDBGet(t, p, fmt.Sprintf("ck%d", i), "cv")
			}
		})
	}
}
