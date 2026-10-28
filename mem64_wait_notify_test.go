package yowza

import (
	"context"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/experimental"
)

// sharedMem64AtomicWasm is a minimal valid WebAssembly module with a
// shared 64-bit memory (flag 0x07, min=1 page, max=2 pages) and 3 exports:
//   - wait32(addr: i64, exp: i32, timeout: i64) -> i32
//   - wait64(addr: i64, exp: i64, timeout: i64) -> i32
//   - notify(addr: i64, count: i32) -> i32
//
// Section sizes verified by hand against the binary format (the guide's
// draft had three wrong sizes, fixed here): type section content is
// 1 (count) + 7 + 7 + 6 = 21 bytes (0x15, not 0x16); export section
// content is 1 (count) + 3*9 = 28 bytes (0x1c, not 0x1d); code section
// content is 1 (count) + 13 + 13 + 11 = 38 bytes (0x26, not 0x25).
// Function and memory section sizes (both 4) were already correct.
// Atomic opcodes (threads proposal): notify = 0xFE 0x00, wait32 =
// 0xFE 0x01, wait64 = 0xFE 0x02, each followed by memarg align+offset.
var sharedMem64AtomicWasm = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
	// Type section: 3 function types
	0x01, 0x15, 0x03,
	0x60, 0x03, 0x7e, 0x7f, 0x7e, 0x01, 0x7f, // (i64, i32, i64) -> i32
	0x60, 0x03, 0x7e, 0x7e, 0x7e, 0x01, 0x7f, // (i64, i64, i64) -> i32
	0x60, 0x02, 0x7e, 0x7f, 0x01, 0x7f, // (i64, i32) -> i32
	// Function section: 3 functions of types 0, 1, 2
	0x03, 0x04, 0x03, 0x00, 0x01, 0x02,
	// Memory section: 1 memory, flag 0x07 (shared + max + memory64), min=1, max=2
	0x05, 0x04, 0x01, 0x07, 0x01, 0x02,
	// Export section: wait32, wait64, notify
	0x07, 0x1c, 0x03,
	0x06, 'w', 'a', 'i', 't', '3', '2', 0x00, 0x00,
	0x06, 'w', 'a', 'i', 't', '6', '4', 0x00, 0x01,
	0x06, 'n', 'o', 't', 'i', 'f', 'y', 0x00, 0x02,
	// Code section:
	0x0a, 0x26, 0x03,
	// func 0 (wait32): local.get 0, local.get 1, local.get 2, memory.atomic.wait32 align=2 offset=0, end
	0x0c, 0x00, 0x20, 0x00, 0x20, 0x01, 0x20, 0x02, 0xfe, 0x01, 0x02, 0x00, 0x0b,
	// func 1 (wait64): local.get 0, local.get 1, local.get 2, memory.atomic.wait64 align=3 offset=0, end
	0x0c, 0x00, 0x20, 0x00, 0x20, 0x01, 0x20, 0x02, 0xfe, 0x02, 0x03, 0x00, 0x0b,
	// func 2 (notify): local.get 0, local.get 1, memory.atomic.notify align=2 offset=0, end
	0x0a, 0x00, 0x20, 0x00, 0x20, 0x01, 0xfe, 0x00, 0x02, 0x00, 0x0b,
}

func TestSharedMemory64WaitNotify(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  wazero.RuntimeConfig
	}{
		{"Interpreter", wazero.NewRuntimeConfigInterpreter()},
		{"Compiler", wazero.NewRuntimeConfigCompiler()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			features := api.CoreFeaturesV2 | experimental.CoreFeaturesThreads | api.CoreFeatureMemory64
			r := wazero.NewRuntimeWithConfig(ctx, tc.cfg.WithCoreFeatures(features))
			defer r.Close(ctx)

			compiled, err := r.CompileModule(ctx, sharedMem64AtomicWasm)
			if err != nil {
				t.Fatalf("CompileModule failed: %v", err)
			}

			modWait, err := r.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithName("waiter"))
			if err != nil {
				t.Fatalf("Instantiate waiter failed: %v", err)
			}
			// 1. Not-equal check: cell at offset 64 is 0, expected=42 -> returns 1 immediately
			res, err := modWait.ExportedFunction("wait64").Call(ctx, 64, 42, 10_000_000)
			if err != nil || res[0] != 1 {
				t.Fatalf("expected wait64 not-equal (1), got %v err=%v", res, err)
			}

			// 2. Wake-up check across goroutines on shared MemoryInstance:
			done := make(chan uint64, 1)
			go func() {
				out, werr := modWait.ExportedFunction("wait32").Call(ctx, 128, 0, uint64(2*time.Second))
				if werr != nil {
					done <- 999
					return
				}
				done <- out[0]
			}()

			// Spin until notify wakes the waiter (returns 1 woken waiter)
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				nres, nerr := modWait.ExportedFunction("notify").Call(ctx, 128, 1)
				if nerr != nil {
					t.Fatalf("notify failed: %v", nerr)
				}
				if nres[0] == 1 {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}

			status := <-done
			if status != 0 {
				t.Fatalf("expected wait32 status 0 (ok/woken), got %d", status)
			}

			// 3. Timeout check: no notifier for offset 192 (still 0, so
			// the waiter parks), 20 ms timeout must expire -> returns 2.
			tres, terr := modWait.ExportedFunction("wait32").Call(ctx, 192, 0, uint64(20*time.Millisecond))
			if terr != nil || tres[0] != 2 {
				t.Fatalf("expected wait32 timed-out (2), got %v err=%v", tres, terr)
			}

			// 4. Timeout check for wait64: offset 256 is 0, 20 ms timeout -> returns 2.
			tres64, terr64 := modWait.ExportedFunction("wait64").Call(ctx, 256, 0, uint64(20*time.Millisecond))
			if terr64 != nil || tres64[0] != 2 {
				t.Fatalf("expected wait64 timed-out (2), got %v err=%v", tres64, terr64)
			}
		})
	}
}
