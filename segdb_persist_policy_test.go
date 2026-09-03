package yowza

// O1 POLICY VALIDATION — unknown Policy values must be
// rejected, not silently treated like PolicyDefault.
//
// resolvePolicy's default branch previously accepted any
// Policy value: Policy(42) or Policy(-1) resolved exactly
// like PolicyDefault (ON), inconsistent with the tri-state
// contract and with this code's own rejection of invalid
// DurabilityMode. OpenPersistedSegDB now validates BOTH
// policy fields BEFORE creating the directory and BEFORE
// taking the directory lock, and returns ErrInvalidPolicy
// for any value other than PolicyDefault, PolicyOn, or
// PolicyOff. Valid values, defaults (both ON), and the
// precedence rule are unchanged (see
// TestPersistedPolicyPrecedence).
//
// Every test runs against both wasm binaries
// (persistTestBins). Helpers (persistOpenForTest,
// persistMustGet) live in segdb_persist_test.go.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Negative — Policy(-1), Policy(3), and Policy(42) for EACH
// field must return the typed ErrInvalidPolicy and must NOT
// create the directory. The other field is left at its valid
// default in each case, so each failure isolates one field.
func TestPersistedInvalidPolicy(t *testing.T) {
	badValues := []Policy{Policy(-1), Policy(3), Policy(42)}
	for _, bin := range persistTestBins {
		for _, bad := range badValues {
			for _, field := range []string{"KeepPreviousGenerationPolicy", "MixedCheckpointPolicy"} {
				t.Run(fmt.Sprintf("%s/%s=%d", bin, field, int(bad)), func(t *testing.T) {
					wasmBytes, err := os.ReadFile(bin)
					if err != nil {
						t.Fatalf("read %s: %v", bin, err)
					}
					opts := PersistedOptions{NumShards: 1, Durability: DurabilitySync}
					if field == "KeepPreviousGenerationPolicy" {
						opts.KeepPreviousGenerationPolicy = bad
					} else {
						opts.MixedCheckpointPolicy = bad
					}
					dir := filepath.Join(t.TempDir(), "db")
					_, err = OpenPersistedSegDB(dir, wasmBytes, opts)
					if !errors.Is(err, ErrInvalidPolicy) {
						t.Fatalf("open with %s=%d: err %v, want ErrInvalidPolicy", field, int(bad), err)
					}
					if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
						t.Fatalf("invalid %s=%d created directory %s (stat err %v)", field, int(bad), dir, statErr)
					}
					// No lock was taken on the failed
					// open: a valid open of the same
					// path must succeed (it would fail
					// with ErrDatabaseLocked if the
					// failed open had leaked a lock).
					p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
					defer p.Close()
					if err := p.Put("k", []byte("v")); err != nil {
						t.Fatalf("Put after invalid-policy failure: %v", err)
					}
					persistMustGet(t, p, "k", "v")
				})
			}
		}
	}
}

// Negative on reopen — an existing directory cannot be
// reopened with an invalid policy, and the failed reopen
// must not lock the directory out: a valid reopen must
// still succeed and see the stored data.
func TestPersistedInvalidPolicyReopen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("k", []byte("v")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read %s: %v", bin, err)
			}
			for _, bad := range []Policy{Policy(-1), Policy(3), Policy(42)} {
				for _, field := range []string{"KeepPreviousGenerationPolicy", "MixedCheckpointPolicy"} {
					opts := PersistedOptions{NumShards: 1, Durability: DurabilitySync}
					if field == "KeepPreviousGenerationPolicy" {
						opts.KeepPreviousGenerationPolicy = bad
					} else {
						opts.MixedCheckpointPolicy = bad
					}
					_, err := OpenPersistedSegDB(dir, wasmBytes, opts)
					if !errors.Is(err, ErrInvalidPolicy) {
						t.Fatalf("reopen with %s=%d: err %v, want ErrInvalidPolicy", field, int(bad), err)
					}
				}
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			persistMustGet(t, p2, "k", "v")
		})
	}
}

// Positive — every valid Policy value still opens fine,
// and resolves as before: Default -> ON (the documented
// default), On -> ON, Off -> OFF, for BOTH fields.
func TestPersistedValidPolicy(t *testing.T) {
	cases := []struct {
		name string
		pol  Policy
		want bool
	}{
		{"Default", PolicyDefault, true},
		{"On", PolicyOn, true},
		{"Off", PolicyOff, false},
	}
	for _, bin := range persistTestBins {
		for _, tc := range cases {
			t.Run(bin+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				opts := PersistedOptions{NumShards: 1, Durability: DurabilitySync,
					KeepPreviousGenerationPolicy: tc.pol, MixedCheckpointPolicy: tc.pol}
				p := persistOpenForTest(t, bin, dir, opts)
				defer p.Close()
				if got := p.KeepPreviousGeneration(); got != tc.want {
					t.Fatalf("KeepPreviousGeneration() = %v, want %v (policy %s)", got, tc.want, tc.name)
				}
				if got := p.MixedCheckpoint(); got != tc.want {
					t.Fatalf("MixedCheckpoint() = %v, want %v (policy %s)", got, tc.want, tc.name)
				}
				if err := p.Put("k", []byte("v")); err != nil {
					t.Fatalf("Put with policy %s: %v", tc.name, err)
				}
				persistMustGet(t, p, "k", "v")
			})
		}
	}
}
