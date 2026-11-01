package yowza

// Patch 2, Track 2 — BLOB WIRING: THE CONTRACT, THE
// HELPERS, AND THE RECOVERY RULE.
//
// THE DECISION (this is the contract; it is also
// written in TIMELINE_V2_PATCH_NOTES.md, Patch 2
// Track 2, so the parent and future readers find it
// in both places):
//
// A domain-write value is a blob reference ONLY when
// a TYPED, caller-supplied manifest says so. The
// manifest (BlobOpManifest below) lists, per entry:
// the target participant DB ID, the key, the BlobRef
// struct itself (SHA-256, total length, chunk count —
// a typed value the caller already holds because it
// wrote the blob with WriteBlob/PutBlob and got the
// BlobRef back), and the source directory the bytes
// live in. The wiring copies and stages EXACTLY the
// manifest's refs, and nothing else.
//
// What is NEVER the basis: parsing delta Value bytes
// to discover blob references. The pre-existing
// PrepareCrossDBBlobs (lifecycle.go) and CopyBlobDeltas
// (blob_store.go) do exactly that discovery
// (ParseBlobRef on every delta value), and the new
// wiring does NOT call them: a byte string that merely
// LOOKS like "blob:v1:..." is an ordinary inline value
// unless a manifest claims it, and a manifest entry
// whose delta value does not carry exactly that ref's
// string form is refused (fail loudly, never guessed).
// The one place a value string is compared to a
// manifest ref is that cross-check (manifest says key
// K is ref R; the delta for K must carry R.String(),
// else the manifest is inconsistent with the write
// set and the whole operation is refused before any
// staging). That is validation of typed metadata, not
// discovery from content: discovery is manifest-only.
// The Axis D GC full scan (ScanLiveBlobRefs) still
// parses values to find references — that is GC's
// conservative over-approximation for deletion safety
// (a missed reference deletes live data), the opposite
// direction from wiring, and it is NOT changed here.
//
// Mapping to target directories: a manifest entry's
// target directory is NEVER guessed from the ref or
// the value. It is the directory of the participant DB
// named by the entry's DBID in the domain (for domain
// ops) or the single target DB directory passed to the
// merge helper (for MergeToMain). The source directory
// is likewise explicit per entry. A manifest entry for
// an unknown DBID, for a key with no non-deleted delta
// in that DB's write set, for a deleted delta, or with
// a value that does not match the ref, refuses the
// whole operation before anything is staged or copied.
//
// ORDERING AND RECOVERY (unified root + staging rule):
//   - Blob staging (copy + BeginStaging, fsynced via
//     the staging file's tmp+sync+rename+dir-sync) is
//     durable BEFORE the domain stage, the coordinator
//     root, and the domain commit log. A crash at any
//     later point therefore finds the staging record.
//   - Commit: after apply + done log, ResolveStaging
//     (committed=true) drops the staging record; the
//     blobs are then protected by the new head
//     reference itself, so GC keeps them.
//   - Abort / stale / pre-commit failure: ResolveStaging
//     (committed=false) drops the staging record with
//     nothing applied, so the blobs become collectible
//     by the next GC pass.
//   - Committed-but-incomplete (ErrDomainOpCommitted):
//     staging is deliberately LEFT in place. Recovery
//     rolls the operation forward (landing the blobs
//     in the head) and then commit-resolves the
//     staging. Aborting here would be the dangling
//     bug: GC between abort-resolve and roll-forward
//     could delete bytes the roll-forward still needs.
//   - Recovery sweep (RecoverDomainOps,
//     RecoverCoordinatedDomainOps): staged blob ops
//     with a decision (domain commit, or coordinator
//     root) are owned by the recovery that applies
//     them; staged blob ops with NO decision are
//     abort-resolved — except one conservative skip in
//     plain domain recovery (see RecoverDomainOps):
//     an op that reached the domain stage file is
//     skipped on that run (its stage is reclaimed by
//     that same run, so a second, idempotent recovery
//     run resolves its staging). This keeps a
//     root-published coordinated operation whose
//     coordinator directory differs from the domain's
//     directory from ever being abort-resolved by plain
//     recovery before coordinated recovery runs. When
//     the two directories are the same (the normal
//     setup), the root check alone is sufficient and
//     one recovery run resolves everything. Ordering
//     rule, stated as a rule: when coordDir != the
//     domain directory, run coordinated recovery before
//     a second plain recovery, and do not run GC
//     between recoveries for the same operation.
//   - MergeToMain: same-dir, so the copy is a no-op
//     (source == target) and only the staging record
//     matters; the helper below takes its refs from
//     the typed key registry (RecordBlobKey), never
//     from parsing merge delta values. A key absent
//     from the registry stages nothing (legacy
//     PutBlob/PutLarge writes are not in the registry
//     — see the registry section); that is a stated
//     gap, not a silent sniff, and same-dir GC scans
//     (staged merge chunks, head after commit) remain
//     the fallback protection for committed state.
//
// TYPED KEY REGISTRY (for MergeToMain): blobs written
// through the new PutBlobRecorded / PutLargeRecorded
// APIs record (key -> BlobRef, source dir) in
// blob-key-registry.json in the DB/manager directory,
// written by the writer that knows the blob — typed
// metadata at write time, not content inspection at
// merge time. The registry is advisory for staging
// only: a stale entry (key later overwritten with an
// inline value) over-stages a blob that the merge
// does not reference, which is safe (staging resolves
// on commit/abort and GC collects the unreferenced
// blob); it never causes an under-stage of a blob the
// merge DOES reference through a recorded key, because
// the recorded ref is staged whenever the key is in
// the merge's applied set, regardless of the final
// value's bytes.

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// BlobManifestEntry is one typed blob claim in a
// domain operation: participant DB ID, key, the typed
// BlobRef, and the directory the bytes currently live
// in. SourceDir may equal the target directory (then
// the copy is skipped and only staging happens).
type BlobManifestEntry struct {
	DBID      string  `json:"db_id"`
	Key       string  `json:"key"`
	Ref       BlobRef `json:"ref"`
	SourceDir string  `json:"source_dir"`
}

// BlobOpManifest is the per-operation manifest: the
// complete, explicit list of blob references a domain
// operation carries. Empty (nil or no entries) means
// the operation carries no blob references — not "find
// them by looking at the values".
type BlobOpManifest struct {
	Entries []BlobManifestEntry `json:"entries"`
}

// blobManifestTarget groups manifest refs by target
// directory for one operation.
type blobManifestTarget struct {
	Dir  string
	Refs []BlobRef
}

// validateBlobManifest checks a manifest against a
// write set WITHOUT inspecting any value for blob-ness:
// the manifest is the sole source of blob claims, and
// each claim must match the write set exactly (known
// participant DBID, a non-deleted delta for that key
// in that DB's writes, carrying exactly Ref.String()).
// It returns the refs grouped by target directory
// (target dir resolved from the domain participant map
// passed in — never from the ref or the value).
func validateBlobManifest(man *BlobOpManifest, writes map[string][]globalMergeDelta, targetDirOf func(dbID string) (string, bool)) (map[string]*blobManifestTarget, error) {
	if man == nil || len(man.Entries) == 0 {
		return nil, nil
	}
	// Index the write set once: (dbID, key) -> delta.
	type wkey struct{ db, key string }
	index := map[wkey]globalMergeDelta{}
	for dbID, deltas := range writes {
		for _, d := range deltas {
			index[wkey{dbID, d.Key}] = d
		}
	}
	seen := map[wkey]bool{}
	grouped := map[string]*blobManifestTarget{}
	for _, e := range man.Entries {
		if e.DBID == "" || e.Key == "" || e.SourceDir == "" {
			return nil, fmt.Errorf("yowza blob wiring: manifest entry with empty db/key/source dir")
		}
		if _, err := hex.DecodeString(e.Ref.SHA256); err != nil || len(e.Ref.SHA256) != 64 {
			return nil, fmt.Errorf("yowza blob wiring: manifest entry for %s/%s has invalid blob sha", e.DBID, e.Key)
		}
		if e.Ref.TotalLen < 0 || e.Ref.ChunkCount < 0 || (e.Ref.TotalLen > 0 && e.Ref.ChunkCount == 0) {
			return nil, fmt.Errorf("yowza blob wiring: manifest entry for %s/%s has invalid blob length/chunks", e.DBID, e.Key)
		}
		k := wkey{e.DBID, e.Key}
		if seen[k] {
			return nil, fmt.Errorf("yowza blob wiring: duplicate manifest entry for %s/%s", e.DBID, e.Key)
		}
		seen[k] = true
		d, ok := index[k]
		if !ok {
			return nil, fmt.Errorf("yowza blob wiring: manifest entry for %s/%s has no delta in the write set", e.DBID, e.Key)
		}
		if d.Deleted {
			return nil, fmt.Errorf("yowza blob wiring: manifest entry for deleted delta %s/%s", e.DBID, e.Key)
		}
		if string(d.Value) != e.Ref.String() {
			return nil, fmt.Errorf("yowza blob wiring: manifest entry for %s/%s (ref %s) does not match the delta value", e.DBID, e.Key, e.Ref.String())
		}
		targetDir, ok := targetDirOf(e.DBID)
		if !ok {
			return nil, fmt.Errorf("yowza blob wiring: manifest entry for unknown participant %q", e.DBID)
		}
		g := grouped[targetDir]
		if g == nil {
			g = &blobManifestTarget{Dir: targetDir}
			grouped[targetDir] = g
		}
		g.Refs = append(g.Refs, e.Ref)
	}
	return grouped, nil
}

// prepareBlobsFromManifest copies every manifest ref
// into its target directory (skipping same-dir copies)
// and then stages it there under opID. Copy-then-stage
// per target, in sorted target-dir order for stable
// behaviour; on failure, already-staged targets are
// abort-resolved so a failed prepare leaves no staged
// blobs behind. Nothing is staged for a target whose
// copy failed. Callers must call this BEFORE the
// domain stage / coordinator staging for the op.
func prepareBlobsFromManifest(man *BlobOpManifest, opID string, writes map[string][]globalMergeDelta, targetDirOf func(dbID string) (string, bool)) ([]string, error) {
	grouped, err := validateBlobManifest(man, writes, targetDirOf)
	if err != nil {
		return nil, err
	}
	if len(grouped) == 0 {
		return nil, nil
	}
	// Per-entry source dirs: validateBlobManifest
	// grouped by target only, so rebuild the
	// (target, ref) -> source map for copying.
	sourceOf := map[string]map[string]string{} // targetDir -> sha -> sourceDir
	for _, e := range man.Entries {
		targetDir, _ := targetDirOf(e.DBID)
		if sourceOf[targetDir] == nil {
			sourceOf[targetDir] = map[string]string{}
		}
		sourceOf[targetDir][e.Ref.SHA256] = e.SourceDir
	}
	var dirs []string
	for dir := range grouped {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	var staged []string
	for _, dir := range dirs {
		g := grouped[dir]
		for _, ref := range g.Refs {
			src := sourceOf[dir][ref.SHA256]
			if src != "" && src != dir {
				if err := CopyBlob(src, dir, ref); err != nil {
					_ = resolveBlobStagingDirs(staged, opID, false)
					return nil, fmt.Errorf("yowza blob wiring: copy blob %s to %s: %w", ref.SHA256, dir, err)
				}
			}
		}
		if err := BeginStaging(dir, opID, g.Refs); err != nil {
			_ = resolveBlobStagingDirs(staged, opID, false)
			return nil, fmt.Errorf("yowza blob wiring: stage blobs in %s: %w", dir, err)
		}
		staged = append(staged, dir)
	}
	return staged, nil
}

// resolveBlobStagingDirs resolves opID's staging in
// every listed target directory (commit or abort),
// trying all directories and returning the first error
// (if any). It is the shared resolve used by the domain
// wrappers; recovery uses the lenient variant.
func resolveBlobStagingDirs(dirs []string, opID string, committed bool) error {
	var first error
	for _, dir := range dirs {
		if err := ResolveStaging(dir, opID, committed); err != nil {
			if first == nil {
				first = err
			}
		}
	}
	return first
}

// resolveBlobStagingLenient resolves opID's staging in
// dir, ignoring "unknown operation" (no staging was
// ever written, or it was already resolved — both are
// fine for recovery, which must be idempotent).
func resolveBlobStagingLenient(dir, opID string, committed bool) {
	if err := ResolveStaging(dir, opID, committed); err != nil && !errors.Is(err, ErrLifecycleStagedBlobUnknown) {
		// Recovery is best-effort for staging cleanup:
		// the blobs themselves are protected by the
		// head (commit) or collectible (abort) either
		// way, and a staging-file error must not fail
		// a recovery that already applied the data.
		_ = err
	}
}

// blobStagingTargetDirs returns the distinct target
// directories for a domain's participants (for recovery
// sweeps). Caller supplies the participant dirs.
func blobStagingTargetDirs(dirs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, d := range dirs {
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// --- MergeToMain helper (parent inserts the calls;
// this file must not edit global_merge.go) ---
//
// StageMergeBlobs stages, under opID "merge:<mergeID>",
// every blob the typed key registry records for a key
// in deltas, copying from the registry's source dir
// when it differs from dir (same-dir merges skip the
// copy). It stages NOTHING for keys absent from the
// registry — it never parses delta values to find
// blobs. ResolveMergeBlobs resolves that staging
// (lenient: unknown is fine, e.g. nothing was staged).
func StageMergeBlobs(dir, mergeID string, deltas []globalMergeDelta) error {
	if mergeID == "" {
		return fmt.Errorf("yowza blob wiring: empty merge id")
	}
	var keys []string
	for _, d := range deltas {
		if !d.Deleted {
			keys = append(keys, d.Key)
		}
	}
	reg := LookupBlobRefs(dir, keys)
	if len(reg) == 0 {
		return nil
	}
	var refs []BlobRef
	for _, k := range keys {
		ref, ok := reg[k]
		if !ok {
			continue
		}
		// Copy from the recorded source when it is a
		// different directory (same-dir: no-op).
		if src, ok := regSourceDir(dir, k); ok && src != "" && src != dir {
			if err := CopyBlob(src, dir, ref); err != nil {
				return fmt.Errorf("yowza blob wiring: merge copy blob %s: %w", ref.SHA256, err)
			}
		}
		refs = append(refs, ref)
	}
	if len(refs) == 0 {
		return nil
	}
	return BeginStaging(dir, "merge:"+mergeID, refs)
}

// ResolveMergeBlobs resolves merge staging for mergeID.
// committed=true after a successful apply (head now
// protects the blobs); committed=false after a
// no-decision failure (stale head, commit-publish
// failure — staged blobs become collectible). It must
// NOT be called with committed=false after an apply
// failure (ErrGlobalMergeCommitted): there the merge
// WILL stand via recovery, so staging stays until
// RecoverGlobalMerges resolves it (see the parent
// snippet for RecoverGlobalMerges).
func ResolveMergeBlobs(dir, mergeID string, committed bool) error {
	if mergeID == "" {
		return fmt.Errorf("yowza blob wiring: empty merge id")
	}
	if err := ResolveStaging(dir, "merge:"+mergeID, committed); err != nil && !errors.Is(err, ErrLifecycleStagedBlobUnknown) {
		return err
	}
	return nil
}

// --- Typed key registry ---
//
// blob-key-registry.json in a DB/manager directory:
// entries written by PutBlobRecorded/PutLargeRecorded
// at write time (key -> BlobRef + source dir). Read by
// StageMergeBlobs at merge time. The file is written
// tmp + sync + rename + dir sync, like other decision
// files. A corrupt or missing registry reads as empty
// (staging is advisory for same-dir merges; a missing
// registry stages nothing rather than failing a merge
// — the merge's data path is unaffected, only the
// pre-commit staging record is absent).

const blobKeyRegistryFile = "blob-key-registry.json"

type blobKeyRegistryEntry struct {
	Key        string `json:"key"`
	SHA256     string `json:"sha256"`
	TotalLen   int64  `json:"total_len"`
	ChunkCount int    `json:"chunk_count"`
	SourceDir  string `json:"source_dir"`
}

type blobKeyRegistryDoc struct {
	Version int                    `json:"version"`
	Entries []blobKeyRegistryEntry `json:"entries"`
}

func blobKeyRegistryPath(dir string) string {
	return filepath.Join(dir, blobKeyRegistryFile)
}

func readBlobKeyRegistry(dir string) blobKeyRegistryDoc {
	data, err := os.ReadFile(blobKeyRegistryPath(dir))
	if err != nil {
		return blobKeyRegistryDoc{Version: 1}
	}
	var doc blobKeyRegistryDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return blobKeyRegistryDoc{Version: 1}
	}
	return doc
}

// RecordBlobKey records (key -> ref) in dir's registry.
// Upsert by key: a later blob for the same key replaces
// the earlier entry (the latest write is what a merge
// stages; a stale entry over-stages at worst — see the
// file header). The recorded SourceDir is dir itself
// unless the caller passes a different source via
// RecordBlobKeyFrom.
func RecordBlobKey(dir, key string, ref BlobRef) error {
	return recordBlobKey(dir, key, ref, dir)
}

func recordBlobKey(dir, key string, ref BlobRef, sourceDir string) error {
	if key == "" {
		return fmt.Errorf("yowza blob wiring: cannot record a blob for an empty key")
	}
	doc := readBlobKeyRegistry(dir)
	kept := doc.Entries[:0]
	for _, e := range doc.Entries {
		if e.Key != key {
			kept = append(kept, e)
		}
	}
	doc.Entries = append(kept, blobKeyRegistryEntry{Key: key, SHA256: ref.SHA256, TotalLen: ref.TotalLen, ChunkCount: ref.ChunkCount, SourceDir: sourceDir})
	doc.Version = 1
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	tmp := blobKeyRegistryPath(dir) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	f.Close()
	if syncErr != nil {
		return syncErr
	}
	if err := os.Rename(tmp, blobKeyRegistryPath(dir)); err != nil {
		return err
	}
	return persistSyncDir(dir)
}

// LookupBlobRefs returns the registry's refs for keys,
// for keys present only. Absent keys are simply absent
// (never inferred from values).
func LookupBlobRefs(dir string, keys []string) map[string]BlobRef {
	doc := readBlobKeyRegistry(dir)
	byKey := map[string]BlobRef{}
	for _, e := range doc.Entries {
		byKey[e.Key] = BlobRef{SHA256: e.SHA256, TotalLen: e.TotalLen, ChunkCount: e.ChunkCount}
	}
	out := map[string]BlobRef{}
	for _, k := range keys {
		if ref, ok := byKey[k]; ok {
			out[k] = ref
		}
	}
	return out
}

func regSourceDir(dir, key string) (string, bool) {
	doc := readBlobKeyRegistry(dir)
	for _, e := range doc.Entries {
		if e.Key == key {
			return e.SourceDir, true
		}
	}
	return "", false
}

// PutBlobRecorded is PutBlob plus the typed registry
// record: bytes to blob files, reference through the
// facade, key -> ref recorded. If the registry write
// fails after the facade Put succeeded, the blob is
// still readable (head reference) but the error is
// returned loudly so the caller knows the registry is
// incomplete — it is never silently dropped.
func (p *PersistedSegDB) PutBlobRecorded(key string, r io.Reader) (BlobRef, error) {
	ref, err := WriteBlob(p.dir, r)
	if err != nil {
		return BlobRef{}, err
	}
	if err := p.Put(key, []byte(ref.String())); err != nil {
		return BlobRef{}, err
	}
	if err := RecordBlobKey(p.dir, key, ref); err != nil {
		return ref, fmt.Errorf("yowza blob wiring: blob stored under %q but registry record failed: %w", key, err)
	}
	return ref, nil
}

// PutLargeRecorded is PutLarge plus the typed registry
// record, for timeline values. Same failure semantics
// as PutBlobRecorded.
func (m *TimelineManager) PutLargeRecorded(id, key string, r io.Reader) (BlobRef, error) {
	ref, err := WriteBlob(m.dir, r)
	if err != nil {
		return BlobRef{}, err
	}
	if err := m.Put(id, key, []byte(ref.String())); err != nil {
		return BlobRef{}, err
	}
	if err := RecordBlobKey(m.dir, key, ref); err != nil {
		return ref, fmt.Errorf("yowza blob wiring: large value stored under %q but registry record failed: %w", key, err)
	}
	return ref, nil
}
