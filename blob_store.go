package yowza

// Phase E, E4 — VARIABLE-LENGTH VALUES + CHUNKED
// IMMUTABLE BLOBS.
//
// The Timeline/DB value limits (key 1..1023, value
// <=1024, key+value <=1014) become v1 COMPATIBILITY
// limits, not permanent caps. Small values keep the
// fast inline path (ordinary Put/Get, unchanged).
// Large values go through blobs:
//
//   PutBlob / PutLarge: the value is streamed (never
//     loaded whole) in blobChunkSize chunks into
//     immutable, content-addressed chunk files
//     (blobs/chunk-<sha256>.bin — the name IS the
//     checksum), plus one small manifest
//     (blobs/<blobsha>.json) listing the chunks, the
//     total length, and the blob SHA-256. Only the
//     short durable REFERENCE
//     ("blob:v1:<sha256>:<len>:<chunks>") is stored
//     through the facade / timeline overlay, so a
//     large value costs one short inline value in
//     the hot path and its bytes live in immutable
//     files. The reference is what merges, pins, and
//     WAL replay carry; the bytes follow by hash.
//   GetBlob / GetLarge: the reference is parsed and
//     the value is returned as a STREAMING reader:
//     chunk hashes are verified as chunks are read,
//     and the overall blob SHA is verified at EOF
//     (a mismatch fails the read with ErrBlobCorrupt
//     instead of returning corrupt bytes as EOF).
//
// Durability: chunk files and the manifest are
// written tmp + sync + rename (manifest last, so a
// manifest naming a chunk implies that chunk is
// durable). Blob files are never modified or deleted
// in this phase: content addressing dedups identical
// values, and a deleted reference leaves orphan
// chunks (harmless, reclaimable by a future GC that
// would need a full reference scan — listed as
// remaining work, not silently claimed). A blob
// reference merged to main by MergeToMain carries
// the same files (same directory), so the merged
// value stays readable; cross-DB blob copies are NOT
// done by the domain apply — a domain write set
// carrying a blob reference into another DB would
// dangle there (the bytes live in the source DB's
// blobs/ directory), so callers must copy the blob
// files first or PutBlob on the target DB. This is
// a documented caller rule, not a silent copy.
//
// Key cap honesty: the KEY limit (<=1023) and the
// facade budget (key+value <=1014) still bound the
// REFERENCE (about 85 bytes), so PutLarge keys are
// in practice <=929 bytes when the reference must
// also be DB-writable. The VALUE cap is what blobs
// lift, not the key cap.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ErrBlobCorrupt reports a blob chunk or manifest
// failing its hash check.
var ErrBlobCorrupt = errors.New("yowza blob: chunk or manifest hash mismatch")

// ErrBlobNotBlob reports a value that is not a blob
// reference being read as a blob.
var ErrBlobNotBlob = errors.New("yowza blob: value is not a blob reference")

// ErrBlobNotFound reports a missing blob manifest or chunk.
var ErrBlobNotFound = errors.New("yowza blob: blob not found")

const blobChunkSize = 8192

// BlobRef is the durable reference stored inline.
type BlobRef struct {
	SHA256     string
	TotalLen   int64
	ChunkCount int
}

func (r BlobRef) String() string {
	return fmt.Sprintf("blob:v1:%s:%d:%d", r.SHA256, r.TotalLen, r.ChunkCount)
}

// ParseBlobRef parses and validates a reference
// string (format + hex SHA length only; existence
// is checked by the reader).
func ParseBlobRef(s string) (BlobRef, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 5 || parts[0] != "blob" || parts[1] != "v1" {
		return BlobRef{}, fmt.Errorf("%w: %q", ErrBlobNotBlob, s)
	}
	sha, err := hex.DecodeString(parts[2])
	if err != nil || len(sha) != 32 {
		return BlobRef{}, fmt.Errorf("%w: bad sha in %q", ErrBlobNotBlob, s)
	}
	total, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || total < 0 {
		return BlobRef{}, fmt.Errorf("%w: bad length in %q", ErrBlobNotBlob, s)
	}
	chunks, err := strconv.Atoi(parts[4])
	if err != nil || chunks < 0 || (total > 0 && chunks == 0) {
		return BlobRef{}, fmt.Errorf("%w: bad chunk count in %q", ErrBlobNotBlob, s)
	}
	return BlobRef{SHA256: parts[2], TotalLen: total, ChunkCount: chunks}, nil
}

type blobManifest struct {
	Version   int      `json:"version"`
	SHA256    string   `json:"sha256"`
	TotalLen  int64    `json:"total_len"`
	ChunkSize int      `json:"chunk_size"`
	Chunks    []string `json:"chunks"`
	Checksum  uint32   `json:"checksum"`
}

func blobDir(dir string) string { return filepath.Join(dir, "blobs") }

func blobChunkPath(dir, chunkSHA string) string {
	return filepath.Join(blobDir(dir), "chunk-"+chunkSHA+".bin")
}

func blobManifestPath(dir, blobSHA string) string {
	return filepath.Join(blobDir(dir), blobSHA+".json")
}

func blobWriteFileSync(path string, data []byte) error {
	tmp := path + ".tmp"
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
	return os.Rename(tmp, path)
}

// WriteBlob streams r into chunk files + a manifest
// in dir/blobs and returns the reference. An existing
// manifest for the same SHA short-circuits the write
// (idempotent, deduped). It never loads the value.
func WriteBlob(dir string, r io.Reader) (BlobRef, error) {
	if err := os.MkdirAll(blobDir(dir), 0o755); err != nil {
		return BlobRef{}, err
	}
	var chunks []string
	hasher := sha256.New()
	var total int64
	buf := make([]byte, blobChunkSize)
	for {
		n, err := io.ReadFull(r, buf)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return BlobRef{}, err
		}
		if n > 0 {
			chunk := buf[:n]
			hasher.Write(chunk)
			total += int64(n)
			sum := sha256.Sum256(chunk)
			hexSum := hex.EncodeToString(sum[:])
			chunks = append(chunks, hexSum)
			path := blobChunkPath(dir, hexSum)
			if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
				if err := blobWriteFileSync(path, append([]byte(nil), chunk...)); err != nil {
					return BlobRef{}, err
				}
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}
	blobSHA := hex.EncodeToString(hasher.Sum(nil))
	manPath := blobManifestPath(dir, blobSHA)
	if _, err := os.Stat(manPath); err == nil {
		return BlobRef{SHA256: blobSHA, TotalLen: total, ChunkCount: len(chunks)}, nil
	}
	man := blobManifest{Version: 1, SHA256: blobSHA, TotalLen: total, ChunkSize: blobChunkSize, Chunks: chunks}
	body, err := json.Marshal(struct {
		Version   int      `json:"version"`
		SHA256    string   `json:"sha256"`
		TotalLen  int64    `json:"total_len"`
		ChunkSize int      `json:"chunk_size"`
		Chunks    []string `json:"chunks"`
	}{Version: man.Version, SHA256: man.SHA256, TotalLen: man.TotalLen, ChunkSize: man.ChunkSize, Chunks: man.Chunks})
	if err != nil {
		return BlobRef{}, err
	}
	man.Checksum = crc32.ChecksumIEEE(body)
	data, err := json.Marshal(man)
	if err != nil {
		return BlobRef{}, err
	}
	if err := blobWriteFileSync(manPath, data); err != nil {
		return BlobRef{}, err
	}
	_ = persistSyncDir(blobDir(dir))
	return BlobRef{SHA256: blobSHA, TotalLen: total, ChunkCount: len(chunks)}, nil
}

// blobReader streams a blob, verifying chunk hashes
// as it goes and the overall hash at EOF.
type blobReader struct {
	dir     string
	man     blobManifest
	chunk   int
	data    []byte
	off     int
	hasher  hash.Hash
	seen    int64
	eofDone bool
	closed  bool
}

func (br *blobReader) loadChunk() error {
	if br.chunk >= len(br.man.Chunks) {
		return io.EOF
	}
	hexSum := br.man.Chunks[br.chunk]
	data, err := os.ReadFile(blobChunkPath(br.dir, hexSum))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: chunk %s", ErrBlobNotFound, hexSum)
		}
		return err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != hexSum {
		return fmt.Errorf("%w: chunk %s", ErrBlobCorrupt, hexSum)
	}
	br.data = data
	br.off = 0
	br.chunk++
	return nil
}

func (br *blobReader) Close() error {
	br.closed = true
	br.data = nil
	return nil
}

// readChunked is the streaming read implementation
// (exposed through blobReadCloser, below).
func (br *blobReader) readChunked(p []byte) (int, error) {
	if br.closed {
		return 0, fmt.Errorf("yowza blob: read from closed reader")
	}
	if br.eofDone {
		return 0, io.EOF
	}
	for br.off >= len(br.data) {
		if err := br.loadChunk(); err != nil {
			if err == io.EOF {
				if br.seen != br.man.TotalLen {
					return 0, fmt.Errorf("%w: short blob (seen %d, want %d)", ErrBlobCorrupt, br.seen, br.man.TotalLen)
				}
				if hex.EncodeToString(br.hasher.Sum(nil)) != br.man.SHA256 {
					return 0, fmt.Errorf("%w: blob sha mismatch", ErrBlobCorrupt)
				}
				br.eofDone = true
				return 0, io.EOF
			}
			return 0, err
		}
	}
	n := copy(p, br.data[br.off:])
	br.off += n
	br.hasher.Write(p[:n])
	br.seen += int64(n)
	return n, nil
}

type blobReadCloser struct {
	br *blobReader
}

func (c *blobReadCloser) Read(p []byte) (int, error) { return c.br.readChunked(p) }
func (c *blobReadCloser) Close() error               { return c.br.Close() }

// ReadBlob opens a streaming reader for ref in dir.
// The manifest is validated (checksum + SHA format);
// chunk contents are verified during reading.
func ReadBlob(dir string, ref BlobRef) (io.ReadCloser, int64, error) {
	data, err := os.ReadFile(blobManifestPath(dir, ref.SHA256))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("%w: manifest %s", ErrBlobNotFound, ref.SHA256)
		}
		return nil, 0, err
	}
	var man blobManifest
	if err := json.Unmarshal(data, &man); err != nil {
		return nil, 0, fmt.Errorf("%w: manifest unparseable: %v", ErrBlobCorrupt, err)
	}
	if man.Version != 1 || man.SHA256 != ref.SHA256 || man.TotalLen != ref.TotalLen || len(man.Chunks) != ref.ChunkCount {
		return nil, 0, fmt.Errorf("%w: manifest does not match reference", ErrBlobCorrupt)
	}
	body, err := json.Marshal(struct {
		Version   int      `json:"version"`
		SHA256    string   `json:"sha256"`
		TotalLen  int64    `json:"total_len"`
		ChunkSize int      `json:"chunk_size"`
		Chunks    []string `json:"chunks"`
	}{Version: man.Version, SHA256: man.SHA256, TotalLen: man.TotalLen, ChunkSize: man.ChunkSize, Chunks: man.Chunks})
	if err != nil {
		return nil, 0, err
	}
	if crc32.ChecksumIEEE(body) != man.Checksum {
		return nil, 0, fmt.Errorf("%w: manifest checksum mismatch", ErrBlobCorrupt)
	}
	br := &blobReader{dir: dir, man: man, hasher: sha256.New()}
	return &blobReadCloser{br: br}, man.TotalLen, nil
}

// PutBlob stores a large value under key in the DB:
// bytes to blob files, reference through the facade.
// Small values should use Put (the inline path).
func (p *PersistedSegDB) PutBlob(key string, r io.Reader) (BlobRef, error) {
	ref, err := WriteBlob(p.dir, r)
	if err != nil {
		return BlobRef{}, err
	}
	if err := p.Put(key, []byte(ref.String())); err != nil {
		return BlobRef{}, err
	}
	return ref, nil
}

// GetBlob returns a streaming reader for the blob
// stored under key. The stored value must be a blob
// reference (inline values give ErrBlobNotBlob).
func (p *PersistedSegDB) GetBlob(key string) (io.ReadCloser, int64, error) {
	v, err := p.Get(key)
	if err != nil {
		return nil, 0, err
	}
	ref, err := ParseBlobRef(string(v))
	if err != nil {
		return nil, 0, err
	}
	return ReadBlob(p.dir, ref)
}

// --- Axis D: blob lifecycle (reference-counted by full
// reference scan, not by a counter that can drift) ---
//
// A blob is collectible ONLY when no live reference to it
// exists in ANY of these sources, all of which
// ScanLiveBlobRefs covers:
//   - the DB live head (PersistedSegDB.Scan),
//   - every ACTIVE durable pin's snapshot vector
//     (ScanStreaming at the pin vector: a blob readable
//     only from a pinned old version is still live),
//   - every live (non-deleted) timeline's overlay
//     (all entries, not just the latest per key: an
//     ancestor overlay entry below a fork head is
//     still a live reference for that branch),
//   - every live timeline's checkpoint segment and
//     every delta segment named by a committed
//     merge-ref record in a live timeline's WAL,
//   - every staged global-merge chunk in the DB
//     directory (a staged merge has no commit record
//     yet, but its blobs must survive until the merge
//     decision resolves),
//   - every staged domain-operation file in the
//     caller-supplied coordinator directories, and
//   - the lifecycle staged-blob file (see
//     lifecycle.go: blobs staged for a cross-DB apply
//     whose decision has not resolved yet).
// Deleted timelines, deleted (released/expired) pins,
// orphan deltas, and reclaimed global-merge stages
// protect NOTHING: their blobs are collectible.
// Chunk safety: chunk files are content-addressed and
// SHARED between blobs with identical chunk bytes, so
// GC deletes a chunk only when no live blob's manifest
// lists it — never merely because one unreferenced blob
// listed it. Deletion order is manifest-first, then
// chunks: a crash mid-GC leaves orphan chunks (harmless,
// re-collectable), never a live manifest pointing at a
// deleted chunk (and an unreferenced manifest is never
// read by a live path anyway). A corrupt or unreadable
// manifest is SKIPPED, never deleted on a guess.
// GC is an explicit maintenance operation, never a read
// or scan side effect.

// ErrBlobLiveReference reports an attempt to delete a blob
// that still has a live reference.
var ErrBlobLiveReference = errors.New("yowza blob: blob still has a live reference")

// BlobLiveSet is the result of a full reference scan.
type BlobLiveSet struct {
	Blobs  map[string]bool // blob SHA -> live
	Chunks map[string]bool // chunk SHA -> listed by a live blob manifest
}

func blobCollectValueRefs(set map[string]bool, v []byte) {
	if ref, err := ParseBlobRef(string(v)); err == nil {
		set[ref.SHA256] = true
	}
}

func blobScanStreamingRefs(db *PersistedSegDB, vec []uint64, set map[string]bool) error {
	it, err := db.ScanStreaming("", vec)
	if err != nil {
		return err
	}
	defer it.Close()
	for {
		batch, err := it.NextBatch(64)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, e := range batch {
			blobCollectValueRefs(set, e.Val)
		}
	}
}

// blobLiveChunks resolves live blob SHAs to their chunk
// SHAs via the manifests on disk. A live blob whose
// manifest is missing or corrupt keeps its SHA in the
// live set (so its manifest is never GC-deleted) but
// contributes no chunk protection — the chunk files
// themselves are only deleted when no live manifest
// lists them, so a corrupt-manifest blob cannot cause
// a live chunk deletion through this path either.
func blobLiveChunks(dir string, blobs map[string]bool) map[string]bool {
	chunks := map[string]bool{}
	for sha := range blobs {
		data, err := os.ReadFile(blobManifestPath(dir, sha))
		if err != nil {
			continue
		}
		var man blobManifest
		if err := json.Unmarshal(data, &man); err != nil {
			continue
		}
		for _, c := range man.Chunks {
			chunks[c] = true
		}
	}
	return chunks
}

// referencedDeltaFiles returns the delta-segment files a
// live timeline actually references: live timelines'
// checkpoint files plus delta files named by committed
// merge-ref records in live timelines' WALs. Orphan
// deltas and deleted timelines' checkpoints are NOT
// referenced, by construction.
func referencedDeltaFiles(dir string, mgr *TimelineManager) map[string]bool {
	out := map[string]bool{}
	if mgr == nil {
		if doc, exists, _, _, err := timelineLoadRegistryV2(dir); err == nil && exists {
			for _, rec := range doc.Timelines {
				if rec.Deleted {
					continue
				}
				if rec.CheckpointFile != "" {
					out[rec.CheckpointFile] = true
				}
				entries, _, _, _, err := timelineReplayWAL(timelineWALPath(dir, rec.ID))
				if err != nil {
					continue
				}
				for _, ent := range entries {
					if ent.MergeRef != nil && ent.MergeRef.DeltaFile != "" {
						out[ent.MergeRef.DeltaFile] = true
					}
				}
			}
		}
		return out
	}
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	for _, ts := range mgr.timelines {
		if ts.deleted {
			continue
		}
		if ts.checkpointFile != "" {
			out[ts.checkpointFile] = true
		}
		entries, _, _, _, err := timelineReplayWAL(ts.walPath)
		if err != nil {
			continue
		}
		for _, ent := range entries {
			if ent.MergeRef != nil && ent.MergeRef.DeltaFile != "" {
				out[ent.MergeRef.DeltaFile] = true
			}
		}
	}
	return out
}

// ScanLiveBlobRefs performs the full reference scan
// described above. db must be open; mgr may be nil (then
// timeline references come from the registry + WALs on
// disk). extraDirs are domain coordinator directories
// whose staged operation files are also scanned.
func ScanLiveBlobRefs(db *PersistedSegDB, mgr *TimelineManager, extraDirs ...string) (*BlobLiveSet, error) {
	if db == nil {
		return nil, fmt.Errorf("yowza blob: GC requires an open database")
	}
	dir := db.Dir()
	set := map[string]bool{}
	// DB live head.
	rows, err := db.Scan("")
	if err != nil {
		return nil, err
	}
	for _, e := range rows {
		blobCollectValueRefs(set, e.Val)
	}
	// Active pins' snapshot vectors. A pin whose vector
	// scan fails fails the whole GC loudly: silently
	// skipping a pin could delete a blob that pin
	// protects.
	pins, err := db.ListPins()
	if err != nil {
		return nil, err
	}
	pinnedVectors := map[string]bool{}
	for _, pin := range pins {
		if pin.Status != PinStatusActive {
			continue
		}
		key := fmt.Sprintf("%v", pin.Vector)
		pinnedVectors[key] = true
		if err := blobScanStreamingRefs(db, pin.Vector, set); err != nil {
			return nil, fmt.Errorf("yowza blob: pin %s vector scan: %w", pin.ID, err)
		}
	}
	// Live timelines: overlay entries + branch vectors
	// without an active pin (branch pins normally exist;
	// the scan is the fallback, not a second pin).
	collectTimeline := func(id string, overlay map[string][]timelineOverlayEntry, branchVector []uint64, checkpointFile string) error {
		for _, list := range overlay {
			for _, e := range list {
				blobCollectValueRefs(set, e.val)
			}
		}
		if len(branchVector) > 0 && !pinnedVectors[fmt.Sprintf("%v", branchVector)] {
			if err := blobScanStreamingRefs(db, branchVector, set); err != nil {
				return fmt.Errorf("yowza blob: timeline %s branch scan: %w", id, err)
			}
		}
		_ = checkpointFile
		return nil
	}
	if mgr != nil {
		mgr.mu.RLock()
		type snap struct {
			id string
			ov map[string][]timelineOverlayEntry
			bv []uint64
			cf string
		}
		var snaps []snap
		for _, ts := range mgr.timelines {
			if ts.deleted {
				continue
			}
			ts.mu.Lock()
			ov := make(map[string][]timelineOverlayEntry, len(ts.overlay))
			for k, list := range ts.overlay {
				ov[k] = append([]timelineOverlayEntry(nil), list...)
			}
			snaps = append(snaps, snap{id: ts.id, ov: ov, bv: append([]uint64(nil), ts.branchVector...), cf: ts.checkpointFile})
			ts.mu.Unlock()
		}
		mgr.mu.RUnlock()
		for _, s := range snaps {
			if err := collectTimeline(s.id, s.ov, s.bv, s.cf); err != nil {
				return nil, err
			}
		}
	} else if doc, exists, _, _, err := timelineLoadRegistryV2(dir); err == nil && exists {
		for _, rec := range doc.Timelines {
			if rec.Deleted {
				continue
			}
			entries, _, _, _, err := timelineReplayWAL(timelineWALPath(dir, rec.ID))
			if err != nil {
				continue
			}
			ov := map[string][]timelineOverlayEntry{}
			for _, ent := range entries {
				if ent.Type == timelineWALPut {
					ov[ent.Key] = append(ov[ent.Key], timelineOverlayEntry{seq: ent.Seq, val: append([]byte(nil), ent.Value...)})
				} else if ent.Type == timelineWALDelete {
					ov[ent.Key] = append(ov[ent.Key], timelineOverlayEntry{seq: ent.Seq, deleted: true})
				}
			}
			if err := collectTimeline(rec.ID, ov, rec.BranchVector, rec.CheckpointFile); err != nil {
				return nil, err
			}
		}
	}
	// Referenced delta segments (checkpoints + committed
	// merge deltas of live timelines).
	for name := range referencedDeltaFiles(dir, mgr) {
		dents, err := timelineLoadDeltaSegment(filepath.Join(dir, name), "", -1)
		if err != nil {
			continue // a corrupt referenced delta fails open elsewhere; GC never deletes on its account
		}
		for _, d := range dents {
			blobCollectValueRefs(set, d.Value)
		}
	}
	// Staged global merges in this DB's directory.
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		name := e.Name()
		if strings.HasPrefix(name, "global-merge-") && strings.HasSuffix(name, ".manifest.json") {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				continue
			}
			man, err := globalMergeDecodeManifest(data)
			if err != nil {
				continue
			}
			deltas, err := loadGlobalMergeDeltas(dir, man)
			if err != nil {
				continue
			}
			for _, d := range deltas {
				blobCollectValueRefs(set, d.Value)
			}
		}
	}
	// Staged domain operations in coordinator dirs.
	for _, cdir := range extraDirs {
		cents, err := os.ReadDir(cdir)
		if err != nil {
			continue
		}
		for _, e := range cents {
			name := e.Name()
			if !strings.HasPrefix(name, "domain-op-") || !strings.HasSuffix(name, ".stage.json") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(cdir, name))
			if err != nil {
				continue
			}
			stage, err := domainDecodeStage(data)
			if err != nil {
				continue
			}
			for _, deltas := range stage.Writes {
				for _, d := range deltas {
					blobCollectValueRefs(set, d.Value)
				}
			}
		}
	}
	// Lifecycle staged blobs (cross-DB apply staging).
	for _, ref := range readStagedBlobRefs(dir) {
		set[ref.SHA256] = true
	}
	return &BlobLiveSet{Blobs: set, Chunks: blobLiveChunks(dir, set)}, nil
}

// BlobGCReport describes one GC pass.
type BlobGCReport struct {
	ManifestsDeleted int
	ChunksDeleted    int
	ManifestsKept    int
	ChunksKept       int
	SkippedCorrupt   []string
}

// CollectUnreferencedBlobs lists, without deleting, the
// manifests and chunks GC would remove for live.
func CollectUnreferencedBlobs(dir string, live *BlobLiveSet) (manifests []string, chunks []string, skipped []string, err error) {
	ents, err := os.ReadDir(blobDir(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, err
	}
	for _, e := range ents {
		name := e.Name()
		if strings.HasSuffix(name, ".json") {
			sha := strings.TrimSuffix(name, ".json")
			if live != nil && live.Blobs[sha] {
				continue
			}
			data, err := os.ReadFile(filepath.Join(blobDir(dir), name))
			if err != nil {
				skipped = append(skipped, name)
				continue
			}
			var man blobManifest
			if err := json.Unmarshal(data, &man); err != nil || man.SHA256 != sha {
				skipped = append(skipped, name)
				continue
			}
			manifests = append(manifests, sha)
		}
	}
	manifestSet := map[string]bool{}
	for _, m := range manifests {
		manifestSet[m] = true
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "chunk-") || !strings.HasSuffix(name, ".bin") {
			continue
		}
		sha := strings.TrimSuffix(strings.TrimPrefix(name, "chunk-"), ".bin")
		if live != nil && live.Chunks[sha] {
			continue
		}
		chunks = append(chunks, sha)
	}
	return manifests, chunks, skipped, nil
}

// GCBlobs deletes every blob not in live (manifest-first,
// then chunks not listed by any live manifest). With
// dryRun it only reports. It never deletes a blob in
// live, and it never deletes a chunk listed by a live
// manifest, even if that chunk also belongs to a
// deleted blob.
func GCBlobs(dir string, live *BlobLiveSet, dryRun bool) (*BlobGCReport, error) {
	manifests, chunks, skipped, err := CollectUnreferencedBlobs(dir, live)
	if err != nil {
		return nil, err
	}
	rep := &BlobGCReport{SkippedCorrupt: skipped}
	if live != nil {
		rep.ManifestsKept = len(live.Blobs)
		rep.ChunksKept = len(live.Chunks)
	}
	if dryRun {
		rep.ManifestsDeleted = len(manifests)
		rep.ChunksDeleted = len(chunks)
		return rep, nil
	}
	for _, sha := range manifests {
		if err := os.Remove(blobManifestPath(dir, sha)); err != nil && !os.IsNotExist(err) {
			return rep, err
		}
		rep.ManifestsDeleted++
	}
	for _, sha := range chunks {
		if err := os.Remove(blobChunkPath(dir, sha)); err != nil && !os.IsNotExist(err) {
			return rep, err
		}
		rep.ChunksDeleted++
	}
	if rep.ManifestsDeleted > 0 || rep.ChunksDeleted > 0 {
		_ = persistSyncDir(blobDir(dir))
	}
	return rep, nil
}

// CopyBlob copies one blob (manifest + chunks) from
// srcDir to dstDir, verifying every chunk hash and the
// manifest against ref as it goes. This is the copy a
// cross-DB apply must perform BEFORE applying a delta
// carrying ref to the target DB: without it the
// reference dangles on the target (Phase E honest
// limit, now with the API that closes it). Copying is
// idempotent: existing identical chunks are skipped,
// and an existing manifest short-circuits after
// verification.
func CopyBlob(srcDir, dstDir string, ref BlobRef) error {
	if err := os.MkdirAll(blobDir(dstDir), 0o755); err != nil {
		return err
	}
	srcData, err := os.ReadFile(blobManifestPath(srcDir, ref.SHA256))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: manifest %s in source", ErrBlobNotFound, ref.SHA256)
		}
		return err
	}
	var man blobManifest
	if err := json.Unmarshal(srcData, &man); err != nil {
		return fmt.Errorf("%w: source manifest unparseable: %v", ErrBlobCorrupt, err)
	}
	if man.SHA256 != ref.SHA256 || man.TotalLen != ref.TotalLen || len(man.Chunks) != ref.ChunkCount {
		return fmt.Errorf("%w: source manifest does not match reference", ErrBlobCorrupt)
	}
	for _, chunkSHA := range man.Chunks {
		dstPath := blobChunkPath(dstDir, chunkSHA)
		if _, err := os.Stat(dstPath); err == nil {
			continue
		}
		data, err := os.ReadFile(blobChunkPath(srcDir, chunkSHA))
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("%w: chunk %s in source", ErrBlobNotFound, chunkSHA)
			}
			return err
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != chunkSHA {
			return fmt.Errorf("%w: source chunk %s", ErrBlobCorrupt, chunkSHA)
		}
		if err := blobWriteFileSync(dstPath, data); err != nil {
			return err
		}
	}
	if _, err := os.Stat(blobManifestPath(dstDir, ref.SHA256)); os.IsNotExist(err) {
		if err := blobWriteFileSync(blobManifestPath(dstDir, ref.SHA256), srcData); err != nil {
			return err
		}
	}
	// Verify the copy by streaming it (chunk hashes +
	// overall SHA), never by trusting the copy loop.
	rc, _, err := ReadBlob(dstDir, ref)
	if err != nil {
		return err
	}
	defer rc.Close()
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return err
	}
	_ = persistSyncDir(blobDir(dstDir))
	return nil
}

// CopyBlobDeltas copies every blob referenced by deltas
// from srcDir to dstDir. A delta whose value is not a
// blob reference is skipped. This is the exact call a
// cross-DB apply path makes before applying deltas to
// the target; see lifecycle.go's StagedBlobSet, which
// keeps the copied blobs alive until the apply decision
// (commit/abort) resolves.
func CopyBlobDeltas(srcDir, dstDir string, deltas []globalMergeDelta) error {
	for _, d := range deltas {
		if d.Deleted {
			continue
		}
		ref, err := ParseBlobRef(string(d.Value))
		if err != nil {
			continue
		}
		if err := CopyBlob(srcDir, dstDir, ref); err != nil {
			return err
		}
	}
	return nil
}

// PutLarge stores a large timeline value: bytes to
// blob files in the manager directory, reference
// through the timeline overlay (which stays within
// the v1 value caps because the reference is short).
func (m *TimelineManager) PutLarge(id, key string, r io.Reader) (BlobRef, error) {
	ref, err := WriteBlob(m.dir, r)
	if err != nil {
		return BlobRef{}, err
	}
	if err := m.Put(id, key, []byte(ref.String())); err != nil {
		return BlobRef{}, err
	}
	return ref, nil
}

// GetLarge returns a streaming reader for a large
// timeline value stored with PutLarge.
func (m *TimelineManager) GetLarge(id, key string) (io.ReadCloser, int64, error) {
	v, err := m.Get(id, key)
	if err != nil {
		return nil, 0, err
	}
	ref, err := ParseBlobRef(string(v))
	if err != nil {
		return nil, 0, err
	}
	return ReadBlob(m.dir, ref)
}
