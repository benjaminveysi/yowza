package yowza

// Pins hardening tests (Item 2): checksum + backup
// descriptor for pins.json. Loader-matrix cases run
// without a database (loadDurablePinsWithFallback on
// temp dirs); open-behaviour cases run through real
// OpenPersistedSegDB instances on both wasm variants.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

func pinsHardOpen(t *testing.T, dir, bin string) *PersistedSegDB {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read %s: %v", bin, err)
	}
	p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return p
}

func pinsHardSetup(t *testing.T, dir, bin string, nPins int) []string {
	t.Helper()
	p := pinsHardOpen(t, dir, bin)
	for i := 0; i < 20; i++ {
		if err := p.Put(fmt.Sprintf("h:%03d", i), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var ids []string
	for i := 0; i < nPins; i++ {
		pin, err := p.CreateDurablePin("Timeline", fmt.Sprintf("tl-%d", i), "")
		if err != nil {
			t.Fatalf("create pin %d: %v", i, err)
		}
		ids = append(ids, pin.ID)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return ids
}

func pinsHardPinIDs(t *testing.T, p *PersistedSegDB) []string {
	t.Helper()
	pins, err := p.ListPins()
	if err != nil {
		t.Fatalf("list pins: %v", err)
	}
	var ids []string
	for _, pin := range pins {
		if pin.Status == PinStatusActive {
			ids = append(ids, pin.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

func pinsHardAssertSameStrings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ids %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("ids %v, want %v", got, want)
		}
	}
}

// pinsHardAssertVerifiedPair asserts primary and backup
// both exist, are byte-identical, and verify as v2.
func pinsHardAssertVerifiedPair(t *testing.T, dir string) {
	t.Helper()
	primary, err := os.ReadFile(filepath.Join(dir, "pins.json"))
	if err != nil {
		t.Fatalf("read pins.json: %v", err)
	}
	backup, err := os.ReadFile(filepath.Join(dir, "pins.bak"))
	if err != nil {
		t.Fatalf("read pins.bak: %v", err)
	}
	if string(primary) != string(backup) {
		t.Fatalf("pins.json and pins.bak differ:\nprimary: %s\nbackup: %s", primary, backup)
	}
	var f durablePinFile
	if err := json.Unmarshal(primary, &f); err != nil {
		t.Fatalf("primary unmarshal: %v", err)
	}
	if f.Version != 2 || f.Checksum == "" {
		t.Fatalf("primary not v2 checksummed: version=%d checksum=%q", f.Version, f.Checksum)
	}
	if parsed := parseDurablePinFile(primary); parsed.err != nil {
		t.Fatalf("primary does not verify: %v", parsed.err)
	}
}

func pinsHardCorruptFile(t *testing.T, path string, mutate func([]byte) []byte) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := os.WriteFile(path, mutate(data), 0644); err != nil {
		t.Fatalf("rewrite %s: %v", path, err)
	}
}

// TestPinsHardeningChecksumAndBackup: a normal save
// writes identical checksummed primary + backup, and a
// reopen reports the primary (no fallback).
func TestPinsHardeningChecksumAndBackup(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			ids := pinsHardSetup(t, dir, bin, 2)
			pinsHardAssertVerifiedPair(t, dir)
			p := pinsHardOpen(t, dir, bin)
			defer p.Close()
			pinsHardAssertSameStrings(t, pinsHardPinIDs(t, p), ids)
			rep := p.PinRecoveryReport()
			if rep.Source != "primary" || rep.UsedBackup || rep.LegacyFormat {
				t.Fatalf("report %+v, want primary/no-fallback/no-legacy", rep)
			}
		})
	}
}

// TestPinsHardeningCorruptPrimaryFallsBack: corrupt
// primary (valid JSON, wrong checksum) -> backup used,
// fallback reported, and the next pin write heals the
// primary so a later open reports primary again.
func TestPinsHardeningCorruptPrimaryFallsBack(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			ids := pinsHardSetup(t, dir, bin, 2)
			primary := filepath.Join(dir, "pins.json")
			pinsHardCorruptFile(t, primary, func(data []byte) []byte {
				if !strings.Contains(string(data), `"status":"active"`) {
					t.Fatalf("primary lacks active status marker: %s", data)
				}
				return []byte(strings.Replace(string(data), `"status":"active"`, `"status":"deleted"`, 1))
			})
			p := pinsHardOpen(t, dir, bin)
			pinsHardAssertSameStrings(t, pinsHardPinIDs(t, p), ids)
			rep := p.PinRecoveryReport()
			if rep.Source != "backup" || !rep.UsedBackup {
				t.Fatalf("report %+v, want backup fallback", rep)
			}
			// The recovered pins must still be ACTIVE
			// (from the backup), not the tampered
			// "deleted" value from the corrupt primary.
			for _, pin := range mustListPins(t, p) {
				if pin.Status != PinStatusActive {
					t.Fatalf("recovered pin %s status %q, want active", pin.ID, pin.Status)
				}
			}
			// Next write heals the primary.
			if _, err := p.CreateDurablePin("Timeline", "tl-heal", ""); err != nil {
				t.Fatalf("create after fallback: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			pinsHardAssertVerifiedPair(t, dir)
			p2 := pinsHardOpen(t, dir, bin)
			defer p2.Close()
			rep2 := p2.PinRecoveryReport()
			if rep2.Source != "primary" || rep2.UsedBackup {
				t.Fatalf("post-heal report %+v, want primary", rep2)
			}
			if got := pinsHardPinIDs(t, p2); len(got) != 3 {
				t.Fatalf("post-heal active pins %v, want 3", got)
			}
		})
	}
}

func mustListPins(t *testing.T, p *PersistedSegDB) []*DurableSnapshotPin {
	t.Helper()
	pins, err := p.ListPins()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return pins
}

// TestPinsHardeningCorruptBothFailsOpen: both files
// corrupt -> open fails loudly with
// ErrDurablePinsCorrupt (gate closed, no instance).
func TestPinsHardeningCorruptBothFailsOpen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			pinsHardSetup(t, dir, bin, 1)
			pinsHardCorruptFile(t, filepath.Join(dir, "pins.json"), func(data []byte) []byte {
				return []byte(strings.Replace(string(data), `"status":"active"`, `"status":"deleted"`, 1))
			})
			pinsHardCorruptFile(t, filepath.Join(dir, "pins.bak"), func(data []byte) []byte {
				return data[:len(data)/2]
			})
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read bin: %v", err)
			}
			p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			if err == nil {
				_ = p.Close()
				t.Fatalf("open succeeded with both pin files corrupt")
			}
			if !errors.Is(err, ErrDurablePinsCorrupt) {
				t.Fatalf("open err %v, want ErrDurablePinsCorrupt", err)
			}
		})
	}
}

// TestPinsHardeningTornWrite: a truncated primary falls
// back to the backup; truncated both fails loudly.
// Neither outcome silently accepts the torn bytes.
func TestPinsHardeningTornWrite(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			ids := pinsHardSetup(t, dir, bin, 2)
			truncate := func(data []byte) []byte { return data[:len(data)/2] }
			pinsHardCorruptFile(t, filepath.Join(dir, "pins.json"), truncate)
			p := pinsHardOpen(t, dir, bin)
			pinsHardAssertSameStrings(t, pinsHardPinIDs(t, p), ids)
			if rep := p.PinRecoveryReport(); !rep.UsedBackup {
				t.Fatalf("torn-primary report %+v, want backup fallback", rep)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			// Now truncate the backup too: loud failure.
			pinsHardCorruptFile(t, filepath.Join(dir, "pins.bak"), truncate)
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read bin: %v", err)
			}
			p2, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			if err == nil {
				_ = p2.Close()
				t.Fatalf("open succeeded with both pin files torn")
			}
			if !errors.Is(err, ErrDurablePinsCorrupt) {
				t.Fatalf("open err %v, want ErrDurablePinsCorrupt", err)
			}
		})
	}
}

// TestPinsHardeningMissingPrimary: primary deleted,
// backup intact -> backup used and reported; primary
// missing with a corrupt backup -> loud failure.
func TestPinsHardeningMissingPrimary(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			ids := pinsHardSetup(t, dir, bin, 1)
			if err := os.Remove(filepath.Join(dir, "pins.json")); err != nil {
				t.Fatalf("remove primary: %v", err)
			}
			p := pinsHardOpen(t, dir, bin)
			pinsHardAssertSameStrings(t, pinsHardPinIDs(t, p), ids)
			if rep := p.PinRecoveryReport(); !rep.UsedBackup || rep.Source != "backup" {
				t.Fatalf("missing-primary report %+v, want backup fallback", rep)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			// Primary still missing, backup now corrupt.
			pinsHardCorruptFile(t, filepath.Join(dir, "pins.bak"), func(data []byte) []byte {
				return []byte("not json at all")
			})
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read bin: %v", err)
			}
			p2, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			if err == nil {
				_ = p2.Close()
				t.Fatalf("open succeeded with primary missing and backup corrupt")
			}
			if !errors.Is(err, ErrDurablePinsCorrupt) {
				t.Fatalf("open err %v, want ErrDurablePinsCorrupt", err)
			}
		})
	}
}

// TestPinsHardeningLegacyLoadsAndMigrates: a version-1
// file (no checksum, no backup) loads, reports legacy,
// is NOT rewritten by open alone, and migrates on the
// next pin write — after which both files verify.
func TestPinsHardeningLegacyLoadsAndMigrates(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := pinsHardOpen(t, dir, bin)
			for i := 0; i < 5; i++ {
				if err := p.Put(fmt.Sprintf("l:%03d", i), []byte("v")); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			pin, err := p.CreateDurablePin("Timeline", "tl-legacy", "")
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			pins, err := p.ListPins()
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			// Rewrite the primary in the old format
			// and drop the backup: the legacy state.
			legacy, err := json.Marshal(durablePinFile{Version: 1, Pins: pins})
			if err != nil {
				t.Fatalf("marshal legacy: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "pins.json"), legacy, 0644); err != nil {
				t.Fatalf("write legacy: %v", err)
			}
			if err := os.Remove(filepath.Join(dir, "pins.bak")); err != nil {
				t.Fatalf("remove backup: %v", err)
			}
			p2 := pinsHardOpen(t, dir, bin)
			pinsHardAssertSameStrings(t, pinsHardPinIDs(t, p2), []string{pin.ID})
			if rep := p2.PinRecoveryReport(); rep.Source != "legacy-primary" || !rep.LegacyFormat || rep.UsedBackup {
				t.Fatalf("legacy report %+v, want legacy-primary", rep)
			}
			if err := p2.Close(); err != nil {
				t.Fatalf("close2: %v", err)
			}
			// Open alone must not rewrite the legacy
			// file: the only copy is untouched.
			after, err := os.ReadFile(filepath.Join(dir, "pins.json"))
			if err != nil {
				t.Fatalf("read after open: %v", err)
			}
			if string(after) != string(legacy) {
				t.Fatalf("open rewrote legacy pins.json")
			}
			if _, err := os.Stat(filepath.Join(dir, "pins.bak")); !os.IsNotExist(err) {
				t.Fatalf("open created a backup, err=%v", err)
			}
			// Next pin write migrates.
			p3 := pinsHardOpen(t, dir, bin)
			if _, err := p3.CreateDurablePin("Timeline", "tl-after", ""); err != nil {
				t.Fatalf("create after legacy: %v", err)
			}
			if err := p3.Close(); err != nil {
				t.Fatalf("close3: %v", err)
			}
			pinsHardAssertVerifiedPair(t, dir)
			p4 := pinsHardOpen(t, dir, bin)
			defer p4.Close()
			if rep := p4.PinRecoveryReport(); rep.Source != "primary" || rep.LegacyFormat {
				t.Fatalf("post-migration report %+v, want primary", rep)
			}
			if got := pinsHardPinIDs(t, p4); len(got) != 2 {
				t.Fatalf("post-migration pins %v, want 2", got)
			}
		})
	}
}

// TestPinsHardeningStrippedChecksumRejected: a version-2
// file with its checksum removed must NOT be accepted
// as legacy; with a valid backup present, the backup is
// used instead.
func TestPinsHardeningStrippedChecksumRejected(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			ids := pinsHardSetup(t, dir, bin, 1)
			data, err := os.ReadFile(filepath.Join(dir, "pins.json"))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var f durablePinFile
			if err := json.Unmarshal(data, &f); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			f.Checksum = ""
			stripped, err := json.Marshal(f)
			if err != nil {
				t.Fatalf("marshal stripped: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "pins.json"), stripped, 0644); err != nil {
				t.Fatalf("write stripped: %v", err)
			}
			p := pinsHardOpen(t, dir, bin)
			defer p.Close()
			pinsHardAssertSameStrings(t, pinsHardPinIDs(t, p), ids)
			if rep := p.PinRecoveryReport(); !rep.UsedBackup {
				t.Fatalf("stripped-checksum report %+v, want backup fallback", rep)
			}
		})
	}
}

// TestPinsHardeningConcurrentCreateDelete: concurrent
// pin create/delete under -race; the final on-disk
// pair must be identical and verified, and a reopen
// must load exactly the surviving set.
func TestPinsHardeningConcurrentCreateDelete(t *testing.T) {
	bin := persistTestBins[0]
	dir := t.TempDir()
	p := pinsHardOpen(t, dir, bin)
	for i := 0; i < 5; i++ {
		if err := p.Put(fmt.Sprintf("c:%03d", i), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var wg sync.WaitGroup
	// Four workers churn create+delete pairs; two
	// workers create pins that survive. Peak live pins
	// stay far below the 64-per-shard lease cap.
	survivors := make([][]string, 2)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				pin, err := p.CreateDurablePin("Timeline", "tl-churn", "")
				if err != nil {
					t.Errorf("churn create: %v", err)
					return
				}
				if err := p.DeletePin(pin.ID); err != nil {
					t.Errorf("churn delete: %v", err)
					return
				}
			}
		}()
	}
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				pin, err := p.CreateDurablePin("Timeline", "tl-keep", "")
				if err != nil {
					t.Errorf("keep create: %v", err)
					return
				}
				survivors[w] = append(survivors[w], pin.ID)
			}
		}(w)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	want := append(append([]string{}, survivors[0]...), survivors[1]...)
	sort.Strings(want)
	pinsHardAssertSameStrings(t, pinsHardPinIDs(t, p), want)
	pinsHardAssertVerifiedPair(t, dir)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	p2 := pinsHardOpen(t, dir, bin)
	defer p2.Close()
	pinsHardAssertSameStrings(t, pinsHardPinIDs(t, p2), want)
	if rep := p2.PinRecoveryReport(); rep.Source != "primary" {
		t.Fatalf("reopen report %+v, want primary", rep)
	}
}

// Crash-mid-write matrix, loader level: every
// (primary, backup) pair a crash or external fault can
// leave behind must resolve to the old set, the new
// set, the empty set (both missing only), or a loud
// failure — never a torn set, never a silent drop of a
// set that exists in a verified file.
func TestPinsHardeningCrashMatrix(t *testing.T) {
	oldPins := []*DurableSnapshotPin{{ID: "pin-1", Owner: "Timeline", Vector: []uint64{1, 1}, Status: PinStatusActive}}
	newPins := []*DurableSnapshotPin{
		{ID: "pin-1", Owner: "Timeline", Vector: []uint64{1, 1}, Status: PinStatusActive},
		{ID: "pin-2", Owner: "Timeline", Vector: []uint64{2, 2}, Status: PinStatusActive},
	}
	mkV2 := func(pins []*DurableSnapshotPin) []byte {
		data, err := json.Marshal(durablePinFile{Version: 2, Pins: pins, Checksum: durablePinChecksum(pins)})
		if err != nil {
			t.Fatalf("marshal v2: %v", err)
		}
		return data
	}
	mkLegacy := func(pins []*DurableSnapshotPin) []byte {
		data, err := json.Marshal(durablePinFile{Version: 1, Pins: pins})
		if err != nil {
			t.Fatalf("marshal legacy: %v", err)
		}
		return data
	}
	mkTorn := func(data []byte) []byte { return data[:len(data)/2] }
	mkCorruptSum := func(data []byte) []byte {
		return []byte(strings.Replace(string(data), `"status":"active"`, `"status":"deleted"`, 1))
	}
	mkStripped := func(data []byte) []byte {
		var f durablePinFile
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("unmarshal for strip: %v", err)
		}
		f.Checksum = ""
		out, err := json.Marshal(f)
		if err != nil {
			t.Fatalf("marshal stripped: %v", err)
		}
		return out
	}
	type state struct {
		name string
		data func() []byte // nil = file missing
	}
	oldV2 := state{"old-v2", func() []byte { return mkV2(oldPins) }}
	newV2 := state{"new-v2", func() []byte { return mkV2(newPins) }}
	legacyOld := state{"legacy-old", func() []byte { return mkLegacy(oldPins) }}
	tornOld := state{"torn", func() []byte { return mkTorn(mkV2(oldPins)) }}
	corruptOld := state{"corrupt-sum", func() []byte { return mkCorruptSum(mkV2(oldPins)) }}
	strippedNew := state{"stripped-v2", func() []byte { return mkStripped(mkV2(newPins)) }}
	missing := state{"missing", nil}
	type outcome struct {
		ids        []string // nil = expect loud failure
		usedBackup bool
		legacy     bool
	}
	cases := []struct {
		name    string
		primary state
		backup  state
		want    outcome
	}{
		{"primary-old-backup-new", oldV2, newV2, outcome{ids: []string{"pin-1"}}},
		{"primary-new-backup-old", newV2, oldV2, outcome{ids: []string{"pin-1", "pin-2"}}},
		{"primary-new-backup-missing", newV2, missing, outcome{ids: []string{"pin-1", "pin-2"}}},
		{"primary-old-backup-missing", oldV2, missing, outcome{ids: []string{"pin-1"}}},
		{"primary-missing-backup-new", missing, newV2, outcome{ids: []string{"pin-1", "pin-2"}, usedBackup: true}},
		{"primary-missing-backup-old", missing, oldV2, outcome{ids: []string{"pin-1"}, usedBackup: true}},
		{"primary-torn-backup-new", tornOld, newV2, outcome{ids: []string{"pin-1", "pin-2"}, usedBackup: true}},
		{"primary-torn-backup-torn", tornOld, tornOld, outcome{ids: nil}},
		{"primary-torn-backup-missing", tornOld, missing, outcome{ids: nil}},
		{"primary-missing-backup-torn", missing, tornOld, outcome{ids: nil}},
		{"primary-missing-backup-missing", missing, missing, outcome{ids: []string{}}},
		{"primary-new-backup-torn", newV2, tornOld, outcome{ids: []string{"pin-1", "pin-2"}}},
		{"primary-old-backup-torn", oldV2, tornOld, outcome{ids: []string{"pin-1"}}},
		{"primary-corrupt-backup-new", corruptOld, newV2, outcome{ids: []string{"pin-1", "pin-2"}, usedBackup: true}},
		{"primary-corrupt-backup-corrupt", corruptOld, corruptOld, outcome{ids: nil}},
		{"primary-legacy-backup-new-migration-crash", legacyOld, newV2, outcome{ids: []string{"pin-1"}, legacy: true}},
		{"primary-stripped-backup-new", strippedNew, newV2, outcome{ids: []string{"pin-1", "pin-2"}, usedBackup: true}},
		{"primary-stripped-backup-missing", strippedNew, missing, outcome{ids: nil}},
		{"primary-missing-backup-legacy", missing, legacyOld, outcome{ids: []string{"pin-1"}, usedBackup: true, legacy: true}},
		{"primary-corrupt-backup-legacy", corruptOld, legacyOld, outcome{ids: []string{"pin-1"}, usedBackup: true, legacy: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.primary.data != nil {
				if err := os.WriteFile(filepath.Join(dir, "pins.json"), tc.primary.data(), 0644); err != nil {
					t.Fatalf("write primary: %v", err)
				}
			}
			if tc.backup.data != nil {
				if err := os.WriteFile(filepath.Join(dir, "pins.bak"), tc.backup.data(), 0644); err != nil {
					t.Fatalf("write backup: %v", err)
				}
			}
			pins, rep, err := loadDurablePinsWithFallback(dir)
			if tc.want.ids == nil {
				if err == nil {
					t.Fatalf("expected loud failure, got pins %v report %+v", pins, rep)
				}
				if !errors.Is(err, ErrDurablePinsCorrupt) {
					t.Fatalf("err %v, want ErrDurablePinsCorrupt", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var got []string
			for _, pin := range pins {
				got = append(got, pin.ID)
			}
			sort.Strings(got)
			pinsHardAssertSameStrings(t, got, tc.want.ids)
			if rep.UsedBackup != tc.want.usedBackup {
				t.Fatalf("report %+v, want usedBackup=%v", rep, tc.want.usedBackup)
			}
			if rep.LegacyFormat != tc.want.legacy {
				t.Fatalf("report %+v, want legacy=%v", rep, tc.want.legacy)
			}
		})
	}
}
