package query

import (
	"bytes"
	"encoding/binary"
	"os"
	"strings"
	"testing"
)

// Unit tests for the ISA compiler (query/isa.go) and the C engine's
// rejection paths. Semantic equivalence across operators lives in
// isa_diff_test.go; these tests pin the gates: verification refusal,
// header encoding, and malformed/version rejection by the C engine.

func TestCompileISARefusesUnverified(t *testing.T) {
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Tampered plan version: the verifier must refuse it, and the
	// compiler must refuse it too (it verifies first).
	bad := *plan
	bad.Version = 99
	if _, err := CompileISA(&bad, Contract{}); err == nil {
		t.Fatalf("CompileISA accepted a plan with version 99")
	} else if ve, ok := err.(*VerificationError); !ok || ve.Code != "UNSUPPORTED_PLAN_VERSION" {
		t.Fatalf("version refusal = %v, want UNSUPPORTED_PLAN_VERSION", err)
	}
	// Disallowed table.
	if _, err := CompileISA(plan, Contract{AllowedTables: []string{"orders"}}); err == nil {
		t.Fatalf("CompileISA accepted a plan reading a disallowed table")
	} else if ve, ok := err.(*VerificationError); !ok || ve.Code != "PERMISSION_DENIED" {
		t.Fatalf("permission refusal = %v, want PERMISSION_DENIED", err)
	}
	// LIMIT over the result budget: refused statically by Verify.
	lim, err := Compile("SELECT name FROM users LIMIT 2", testSchema())
	if err != nil {
		t.Fatalf("compile limit: %v", err)
	}
	if _, err := CompileISA(lim, Contract{MaxResultRows: 1}); err == nil {
		t.Fatalf("CompileISA accepted LIMIT 2 over MaxResultRows 1")
	} else if ve, ok := err.(*VerificationError); !ok || ve.Code != "BUDGET_EXCEEDED" {
		t.Fatalf("budget refusal = %v, want BUDGET_EXCEEDED", err)
	}
	// Nil plan.
	if _, err := CompileISA(nil, Contract{}); err == nil {
		t.Fatalf("CompileISA accepted a nil plan")
	}
}

// parseISAHeader walks the header + table section of a compiled
// program and returns the offset of the op stream, for tests that
// assert on the encoding without reimplementing the whole decoder.
func parseISAHeader(t *testing.T, prog []byte) (opOff int, numTables uint32) {
	t.Helper()
	if len(prog) < 28 {
		t.Fatalf("program too short (%d bytes)", len(prog))
	}
	if !bytes.Equal(prog[:4], []byte{'Y', 'W', 'Q', 'I'}) {
		t.Fatalf("magic = %q, want YWQI", prog[:4])
	}
	if v := binary.LittleEndian.Uint32(prog[4:8]); v != ISAVersion {
		t.Fatalf("version = %d, want %d", v, ISAVersion)
	}
	// v2 header: magic(4) + version(4) + max_scanned(8) +
	// max_result(8) + max_memory(8) = 32 bytes, then num_tables.
	pos := 32
	numTables = binary.LittleEndian.Uint32(prog[pos:])
	pos += 4
	for i := uint32(0); i < numTables; i++ {
		if pos+4 > len(prog) {
			t.Fatalf("truncated table alias")
		}
		alen := int(binary.LittleEndian.Uint32(prog[pos:]))
		pos += 4 + alen
		if pos+4 > len(prog) {
			t.Fatalf("truncated table prefix")
		}
		plen := int(binary.LittleEndian.Uint32(prog[pos:]))
		pos += 4 + plen
		if pos+4 > len(prog) {
			t.Fatalf("truncated table columns")
		}
		ncol := int(binary.LittleEndian.Uint32(prog[pos:]))
		pos += 4
		for c := 0; c < ncol; c++ {
			if pos+4 > len(prog) {
				t.Fatalf("truncated column name")
			}
			nlen := int(binary.LittleEndian.Uint32(prog[pos:]))
			pos += 4 + nlen + 1 // name + type byte
			if pos+4 > len(prog) {
				t.Fatalf("truncated column source")
			}
			slen := int(binary.LittleEndian.Uint32(prog[pos:]))
			pos += 4 + slen
		}
	}
	return pos, numTables
}

func TestCompileISAHeaderEncoding(t *testing.T) {
	plan, err := Compile("SELECT name, age FROM users WHERE age >= 30 LIMIT 2", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	prog, err := CompileISA(plan, Contract{MaxScannedRows: 100, MaxResultRows: 50})
	if err != nil {
		t.Fatalf("CompileISA: %v", err)
	}
	if got := binary.LittleEndian.Uint64(prog[8:16]); got != 100 {
		t.Fatalf("max_scanned = %d, want 100", got)
	}
	if got := binary.LittleEndian.Uint64(prog[16:24]); got != 50 {
		t.Fatalf("max_result = %d, want 50", got)
	}
	if got := binary.LittleEndian.Uint64(prog[24:32]); got != 0 {
		t.Fatalf("max_memory = %d, want 0 (unlimited)", got)
	}
	opOff, numTables := parseISAHeader(t, prog)
	if numTables != 1 {
		t.Fatalf("num_tables = %d, want 1", numTables)
	}
	if prog[opOff] != isaOpScan {
		t.Fatalf("first op = 0x%02x, want SCAN (0x01)", prog[opOff])
	}
	if prog[len(prog)-1] != isaOpEnd {
		t.Fatalf("last byte = 0x%02x, want END (0xFF)", prog[len(prog)-1])
	}
	// Unlimited budgets encode as 0.
	prog2, err := CompileISA(plan, Contract{})
	if err != nil {
		t.Fatalf("CompileISA unlimited: %v", err)
	}
	if got := binary.LittleEndian.Uint64(prog2[8:16]); got != 0 {
		t.Fatalf("unlimited max_scanned = %d, want 0", got)
	}
}

func TestCompileISAJoinTables(t *testing.T) {
	plan, err := Compile("SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id", testSchema())
	if err != nil {
		t.Fatalf("compile join: %v", err)
	}
	prog, err := CompileISA(plan, Contract{})
	if err != nil {
		t.Fatalf("CompileISA join: %v", err)
	}
	_, numTables := parseISAHeader(t, prog)
	if numTables != 2 {
		t.Fatalf("num_tables = %d, want 2", numTables)
	}
	// An aggregate plan compiles (operator mapping is exercised by
	// the differential tests, which run it through the C engine).
	agg, err := Compile("SELECT city, COUNT(*) FROM users GROUP BY city", testSchema())
	if err != nil {
		t.Fatalf("compile agg: %v", err)
	}
	if _, err := CompileISA(agg, Contract{}); err != nil {
		t.Fatalf("CompileISA agg: %v", err)
	}
}

// isaHostForTest loads a wasm binary or skips the test explicitly
// when the binary is absent (the task's skip pattern).
func isaHostForTest(t *testing.T, bin string) *ISAHost {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", bin, err)
	}
	host, err := NewISAHost(wasmBytes)
	if err != nil {
		t.Fatalf("NewISAHost %s: %v", bin, err)
	}
	t.Cleanup(func() { host.Close() })
	return host
}

func TestISAEngineRejectsBadPrograms(t *testing.T) {
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	good, err := CompileISA(plan, Contract{})
	if err != nil {
		t.Fatalf("CompileISA: %v", err)
	}
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			host := isaHostForTest(t, bin)
			// Wrong version: patch the version field (offset 4).
			ver := append([]byte(nil), good...)
			binary.LittleEndian.PutUint32(ver[4:8], 99)
			if _, _, err := host.ExecuteISA(ver, plan); err == nil {
				t.Fatalf("engine accepted ISA version 99")
			} else if !strings.Contains(err.Error(), "version") {
				t.Fatalf("version rejection = %v, want a version error", err)
			}
			// Bad magic.
			mag := append([]byte(nil), good...)
			mag[0] = 'X'
			if _, _, err := host.ExecuteISA(mag, plan); err == nil {
				t.Fatalf("engine accepted bad magic")
			}
			// Truncated header.
			if _, _, err := host.ExecuteISA(good[:10], plan); err == nil {
				t.Fatalf("engine accepted a 10-byte program")
			}
			// Truncated mid-tables.
			opOff, _ := parseISAHeader(t, good)
			if opOff > 10 {
				if _, _, err := host.ExecuteISA(good[:opOff-2], plan); err == nil {
					t.Fatalf("engine accepted a program truncated mid-tables")
				}
			}
			// SCAN without PROJECT (hand-built minimal program).
			var b bytes.Buffer
			b.Write([]byte{'Y', 'W', 'Q', 'I'})
			var tmp [8]byte
			binary.LittleEndian.PutUint32(tmp[:4], ISAVersion)
			b.Write(tmp[:4])
			binary.LittleEndian.PutUint64(tmp[:], 0)
			b.Write(tmp[:])
			b.Write(tmp[:])
			b.Write(tmp[:])
			binary.LittleEndian.PutUint32(tmp[:4], 1)
			b.Write(tmp[:4])
			// Table: alias "users", prefix "user:", 1 column
			// (name, string, json:name).
			binary.LittleEndian.PutUint32(tmp[:4], uint32(len("users")))
			b.Write(tmp[:4])
			b.WriteString("users")
			binary.LittleEndian.PutUint32(tmp[:4], uint32(len("user:")))
			b.Write(tmp[:4])
			b.WriteString("user:")
			binary.LittleEndian.PutUint32(tmp[:4], 1)
			b.Write(tmp[:4])
			binary.LittleEndian.PutUint32(tmp[:4], uint32(len("name")))
			b.Write(tmp[:4])
			b.WriteString("name")
			b.WriteByte(byte(KindString))
			binary.LittleEndian.PutUint32(tmp[:4], uint32(len("json:name")))
			b.Write(tmp[:4])
			b.WriteString("json:name")
			b.WriteByte(isaOpScan)
			binary.LittleEndian.PutUint32(tmp[:4], 0)
			b.Write(tmp[:4])
			b.WriteByte(isaOpEnd)
			if _, _, err := host.ExecuteISA(b.Bytes(), plan); err == nil {
				t.Fatalf("engine accepted SCAN without PROJECT")
			}
			// A good program on an empty host runs and yields 0 rows.
			rows, scanned, err := host.ExecuteISA(good, plan)
			if err != nil {
				t.Fatalf("good program on empty host: %v", err)
			}
			if len(rows) != 0 || scanned != 0 {
				t.Fatalf("empty host: rows=%d scanned=%d, want 0/0", len(rows), scanned)
			}
		})
	}
}
