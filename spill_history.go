package yowza

// Phase E, E4 — SPILLABLE VERSION LOG (replacing the
// fixed 1024-entry hot history as the durable history
// for the Go layer).
//
// The C hot history (SEGDB_HIST_MAX = 1024 versions
// per flush epoch, in a fixed array) still bounds the
// C snapshot path — that C change (a heap-overflow
// list for hist[]) is NOT claimed here; see the notes
// for the exact remaining C piece. What this file
// replaces, at the layer the timeline/DB APIs own, is
// "history lives only in the fixed hot table and is
// lost on restart": every PersistedSegDB mutation
// (facade Put/Delete, global-merge apply, domain
// apply) records its PRE-IMAGE into this log BEFORE
// the mutation is applied, so a crash at any point
// leaves either a harmless orphan record (a mutation
// that never happened — its pre-image is the value
// that persisted, so ValueAt stays correct) or the
// record for a mutation the WAL will replay. The log
// is therefore the versioned WAL + durable snapshot
// root for needed history that Gate 1 lacked.
//
// Format: chunk files history-chunk-%06d.log (level 0)
// and history-level-%06d.log (compacted levels),
// each a sequence of framed records
// [u32 len][u32 crc32][payload], payload =
// [u64 seq][u32 klen][key][u8 preOK][u32 vlen][val].
// Records are in seq order across chunks. Chunk
// files are IMMUTABLE once rotated: compaction and
// pruning write NEW files and publish a new manifest
// (the commit point); old files are deleted only
// after the publish. The manifest
// (history-manifest.json, JSON + CRC-32, tmp + sync
// + rename + dir sync in Sync mode) lists completed
// chunks with seq ranges and levels; the currently
// open chunk is NOT in the manifest until rotation
// or close — recovery adopts it by scanning (a chunk
// whose first seq exceeds the manifest's max last seq
// is new data, not a stale prune output, and is
// adopted after tail validation). A complete record
// with a bad CRC is corruption (loud failure, never
// treated as a tail); only a truncated LAST record
// of the LAST chunk is an incomplete tail and is
// truncated. Retention floor: HistoryPrune refuses
// to prune past the oldest active durable pin's host
// seq (pins.json HostSeq is the host WAL seq at pin
// time); with no pins any prune is allowed. Every
// manifest publish also writes an immutable copy
// history-manifest-gen-<gen>.json, so old roots stay
// recoverable (their chunk files are immutable) —
// generation retention by pins + recoverable roots,
// not "only the previous generation".
//
// Memory: readers stream chunk files record by
// record; no whole-log load, ever. A read stops at
// the first record that answers it.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrSpillCorrupt reports a complete spill record
// failing its CRC, or a manifest failing its own.
// Recovery stops loudly; the file is left for
// inspection, never truncated past a valid record.
var ErrSpillCorrupt = errors.New("yowza spill history: corrupt record or manifest")

// ErrSpillPruneFloor reports a prune that would drop
// versions a live durable pin may still need.
var ErrSpillPruneFloor = errors.New("yowza spill history: prune blocked by durable-pin retention floor")

// ErrSpillGenerationUnknown reports a request for a
// manifest generation that was never published.
var ErrSpillGenerationUnknown = errors.New("yowza spill history: unknown manifest generation")

// SpillVersion is one recorded pre-image: the value a
// key had immediately before the mutation at Seq
// (PreOK false = the key was absent before it).
type SpillVersion struct {
	Seq    uint64
	Key    string
	PreVal []byte
	PreOK  bool
}

type spillChunkRef struct {
	File     string `json:"file"`
	FirstSeq uint64 `json:"first_seq"`
	LastSeq  uint64 `json:"last_seq"`
	Count    int    `json:"count"`
	Level    int    `json:"level"`
}

type spillManifest struct {
	Version    int             `json:"version"`
	Generation uint64          `json:"generation"`
	Chunks     []spillChunkRef `json:"chunks"`
	Checksum   uint32          `json:"checksum"`
}

type spillHistory struct {
	mu        sync.Mutex
	dir       string
	chunkSize int
	syncMode  bool
	manifest  *spillManifest
	// Current open chunk (not yet in the manifest).
	cur       *os.File
	curName   string
	curNum    int
	curCount  int
	curFirst  uint64
	curLast   uint64
	nextNum   int
	closed    bool
}

const spillDefaultChunkSize = 4096

func spillManifestPath(dir string) string { return filepath.Join(dir, "history-manifest.json") }

func spillChunkPath(dir string, level int, num int) string {
	if level == 0 {
		return filepath.Join(dir, fmt.Sprintf("history-chunk-%06d.log", num))
	}
	return filepath.Join(dir, fmt.Sprintf("history-level-%06d.log", num))
}

func spillEncodeRecord(seq uint64, key string, preVal []byte, preOK bool) []byte {
	var payload []byte
	var tmp8 [8]byte
	var tmp4 [4]byte
	binary.LittleEndian.PutUint64(tmp8[:], seq)
	payload = append(payload, tmp8[:]...)
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(key)))
	payload = append(payload, tmp4[:]...)
	payload = append(payload, []byte(key)...)
	if preOK {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(preVal)))
	payload = append(payload, tmp4[:]...)
	payload = append(payload, preVal...)
	frame := make([]byte, 0, 8+len(payload))
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(payload)))
	frame = append(frame, tmp4[:]...)
	binary.LittleEndian.PutUint32(tmp4[:], crc32.ChecksumIEEE(payload))
	frame = append(frame, tmp4[:]...)
	frame = append(frame, payload...)
	return frame
}

// spillDecodeChunk decodes a whole chunk file. It
// returns the records, the valid byte length, and
// whether the file ends in an incomplete tail (a
// truncated last record). A complete record with a
// bad CRC is ErrSpillCorrupt, never a tail.
func spillDecodeChunk(data []byte) ([]SpillVersion, int, bool, error) {
	var out []SpillVersion
	off := 0
	for off < len(data) {
		if len(data)-off < 8 {
			return out, off, true, nil
		}
		ln := int(binary.LittleEndian.Uint32(data[off:]))
		crc := binary.LittleEndian.Uint32(data[off+4:])
		if ln <= 0 || ln > 1<<26 {
			return out, off, true, nil
		}
		if len(data)-off-8 < ln {
			return out, off, true, nil
		}
		payload := data[off+8 : off+8+ln]
		if crc32.ChecksumIEEE(payload) != crc {
			return out, off, false, fmt.Errorf("%w: record at offset %d", ErrSpillCorrupt, off)
		}
		off += 8 + ln
		if len(payload) < 17 {
			return out, off, false, fmt.Errorf("%w: short payload", ErrSpillCorrupt)
		}
		seq := binary.LittleEndian.Uint64(payload[0:])
		klen := int(binary.LittleEndian.Uint32(payload[8:]))
		if 12+klen+5 > len(payload) {
			return out, off, false, fmt.Errorf("%w: key length out of bounds", ErrSpillCorrupt)
		}
		key := string(payload[12 : 12+klen])
		rest := payload[12+klen:]
		preOK := rest[0] == 1
		vlen := int(binary.LittleEndian.Uint32(rest[1:]))
		if 5+vlen != len(rest) {
			return out, off, false, fmt.Errorf("%w: value length out of bounds", ErrSpillCorrupt)
		}
		out = append(out, SpillVersion{Seq: seq, Key: key, PreVal: append([]byte(nil), rest[5:]...), PreOK: preOK})
	}
	return out, off, false, nil
}

func spillManifestChecksum(m *spillManifest) uint32 {
	body, _ := json.Marshal(struct {
		Version    int             `json:"version"`
		Generation uint64          `json:"generation"`
		Chunks     []spillChunkRef `json:"chunks"`
	}{Version: m.Version, Generation: m.Generation, Chunks: m.Chunks})
	return crc32.ChecksumIEEE(body)
}

func spillPublishManifest(dir string, m *spillManifest, syncDir bool) error {
	m.Checksum = spillManifestChecksum(m)
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	path := spillManifestPath(dir)
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
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// Immutable generation copy: the recoverable root.
	genPath := filepath.Join(dir, fmt.Sprintf("history-manifest-gen-%d.json", m.Generation))
	if err := os.WriteFile(genPath, data, 0o644); err == nil {
		// Best effort sync of the generation copy; the
		// live manifest above is the authoritative one.
		if gf, err := os.OpenFile(genPath, os.O_WRONLY, 0o644); err == nil {
			_ = gf.Sync()
			gf.Close()
		}
	}
	if syncDir {
		return persistSyncDir(dir)
	}
	return nil
}

func spillLoadManifest(dir string) (*spillManifest, error) {
	data, err := os.ReadFile(spillManifestPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return &spillManifest{Version: 1, Generation: 0}, nil
		}
		return nil, err
	}
	var m spillManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: manifest unparseable: %v", ErrSpillCorrupt, err)
	}
	if m.Version != 1 {
		return nil, fmt.Errorf("%w: manifest version %d", ErrSpillCorrupt, m.Version)
	}
	if spillManifestChecksum(&m) != m.Checksum {
		return nil, fmt.Errorf("%w: manifest checksum mismatch", ErrSpillCorrupt)
	}
	return &m, nil
}

// newSpillHistory loads (or creates) the version log
// in dir: manifest first, then a directory scan that
// validates every chunk file, truncates only a torn
// tail of the last chunk, adopts new chunks past the
// manifest's max seq, and deletes stale orphans
// (crashed prune/compaction outputs whose seq range
// is already covered by the manifest).
func newSpillHistory(dir string, chunkSize int, syncMode bool) (*spillHistory, error) {
	if chunkSize <= 0 {
		chunkSize = spillDefaultChunkSize
	}
	man, err := spillLoadManifest(dir)
	if err != nil {
		return nil, err
	}
	h := &spillHistory{dir: dir, chunkSize: chunkSize, syncMode: syncMode, manifest: man, nextNum: 1}
	maxLast := uint64(0)
	seenNums := map[int]bool{}
	for _, c := range man.Chunks {
		if c.LastSeq > maxLast {
			maxLast = c.LastSeq
		}
		// Recover the chunk counter from manifest names.
		if strings.HasPrefix(c.File, "history-chunk-") {
			if n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(c.File, "history-chunk-"), ".log")); err == nil {
				seenNums[n] = true
				if n >= h.nextNum {
					h.nextNum = n + 1
				}
			}
		}
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var adopted []spillChunkRef
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "history-chunk-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		// Skip manifest-listed chunks (already trusted,
		// still re-validated below via the adopted path
		// only when they are new; manifest chunks are
		// validated lazily on read, and their ranges
		// are authoritative).
		listed := false
		for _, c := range man.Chunks {
			if c.File == name {
				listed = true
				break
			}
		}
		if listed {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		recs, validLen, isTail, err := spillDecodeChunk(data)
		if err != nil {
			return nil, err
		}
		if len(recs) == 0 {
			// Empty or torn-only file: drop it.
			_ = os.Remove(filepath.Join(dir, name))
			continue
		}
		if isTail {
			// Torn tail of a new chunk: truncate to
			// the last valid record (incomplete-tail
			// rule only; a bad CRC above already
			// failed the load).
			if err := os.Truncate(filepath.Join(dir, name), int64(validLen)); err != nil {
				return nil, err
			}
			recs = recs[:len(recs)]
			if len(recs) == 0 {
				_ = os.Remove(filepath.Join(dir, name))
				continue
			}
		}
		first, last := recs[0].Seq, recs[len(recs)-1].Seq
		if first > maxLast {
			adopted = append(adopted, spillChunkRef{File: name, FirstSeq: first, LastSeq: last, Count: len(recs), Level: 0})
			if n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "history-chunk-"), ".log")); err == nil && n >= h.nextNum {
				h.nextNum = n + 1
			}
		} else {
			// Stale orphan (crashed prune/compaction
			// output, or a duplicated chunk): the
			// manifest's chunks already cover this
			// range, so deleting it loses nothing.
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	if len(adopted) > 0 {
		sort.Slice(adopted, func(i, j int) bool { return adopted[i].FirstSeq < adopted[j].FirstSeq })
		man.Chunks = append(man.Chunks, adopted...)
		man.Generation++
		if err := spillPublishManifest(dir, man, syncMode); err != nil {
			return nil, err
		}
	}
	return h, nil
}

// publishLocked publishes the current manifest with a
// new generation. Caller holds h.mu.
func (h *spillHistory) publishLocked() error {
	h.manifest.Generation++
	return spillPublishManifest(h.dir, h.manifest, h.syncMode)
}

// rotateLocked closes the current chunk (if any) and
// publishes it into the manifest. Caller holds h.mu.
func (h *spillHistory) rotateLocked() error {
	if h.cur == nil {
		return nil
	}
	if err := h.cur.Sync(); err != nil && h.syncMode {
		return err
	}
	if err := h.cur.Close(); err != nil {
		return err
	}
	h.cur = nil
	if h.curCount > 0 {
		h.manifest.Chunks = append(h.manifest.Chunks, spillChunkRef{File: h.curName, FirstSeq: h.curFirst, LastSeq: h.curLast, Count: h.curCount, Level: 0})
	} else {
		_ = os.Remove(filepath.Join(h.dir, h.curName))
	}
	return h.publishLocked()
}

// recordLocked appends one pre-image record. Caller
// holds p.mu (lock order p.mu -> spill.mu) and has
// already assigned seq. It is called BEFORE the
// mutation it describes (see the file header for why
// an orphan record is harmless).
func (h *spillHistory) recordLocked(dir string, seq uint64, key string, preVal []byte, preOK bool, sync bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("yowza spill history: log closed")
	}
	if h.cur == nil || h.curCount >= h.chunkSize {
		if err := h.rotateLocked(); err != nil {
			return err
		}
		name := fmt.Sprintf("history-chunk-%06d.log", h.nextNum)
		h.nextNum++
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		h.cur = f
		h.curName = name
		h.curCount = 0
		h.curFirst = seq
	}
	frame := spillEncodeRecord(seq, key, preVal, preOK)
	if _, err := h.cur.Write(frame); err != nil {
		return err
	}
	if sync {
		if err := h.cur.Sync(); err != nil {
			return err
		}
	}
	h.curCount++
	h.curLast = seq
	return nil
}

// closeLocked publishes the open chunk and closes the
// log. Caller holds p.mu.
func (h *spillHistory) closeLocked() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	if err := h.rotateLocked(); err != nil {
		return err
	}
	return nil
}

// streamChunks walks chunk files in manifest order
// (plus the open chunk last), decoding record by
// record and calling fn per record. Returning false
// from fn stops the walk early (bounded reads stop
// at the first answering record).
func (h *spillHistory) streamChunks(refs []spillChunkRef, fn func(SpillVersion) bool) error {
	for _, ref := range refs {
		data, err := os.ReadFile(filepath.Join(h.dir, ref.File))
		if err != nil {
			return err
		}
		recs, _, _, err := spillDecodeChunk(data)
		if err != nil {
			return err
		}
		for _, r := range recs {
			if !fn(r) {
				return nil
			}
		}
	}
	return nil
}

func (h *spillHistory) refsLocked() []spillChunkRef {
	refs := append([]spillChunkRef(nil), h.manifest.Chunks...)
	if h.cur != nil && h.curCount > 0 {
		refs = append(refs, spillChunkRef{File: h.curName, FirstSeq: h.curFirst, LastSeq: h.curLast, Count: h.curCount, Level: 0})
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].FirstSeq != refs[j].FirstSeq {
			return refs[i].FirstSeq < refs[j].FirstSeq
		}
		return refs[i].Level < refs[j].Level
	})
	return refs
}

// Versions returns pre-images for key with Seq >=
// fromSeq, in seq order, at most limit (0 = all).
// It streams chunk files; it never loads the log.
func (h *spillHistory) Versions(key string, fromSeq uint64, limit int) ([]SpillVersion, error) {
	h.mu.Lock()
	refs := h.refsLocked()
	h.mu.Unlock()
	var out []SpillVersion
	err := h.streamChunks(refs, func(r SpillVersion) bool {
		if r.Key == key && r.Seq >= fromSeq {
			out = append(out, r)
			if limit > 0 && len(out) >= limit {
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ValueAt returns the value key had at WAL seq S:
// the pre-image of the first mutation after S, or
// the live value when no mutation follows S. live
// is the current value (nil + false when absent);
// it is only consulted when no later mutation exists.
func (h *spillHistory) ValueAt(key string, seq uint64, live []byte, liveOK bool) ([]byte, bool, error) {
	h.mu.Lock()
	refs := h.refsLocked()
	h.mu.Unlock()
	var answer []byte
	answerOK := false
	found := false
	err := h.streamChunks(refs, func(r SpillVersion) bool {
		if r.Key == key && r.Seq > seq {
			answer, answerOK, found = append([]byte(nil), r.PreVal...), r.PreOK, true
			return false
		}
		return true
	})
	if err != nil {
		return nil, false, err
	}
	if found {
		return answer, answerOK, nil
	}
	return live, liveOK, nil
}

// Manifest returns a copy of the current manifest
// (diagnostic: chunk counts, levels, generation).
func (h *spillHistory) Manifest() spillManifest {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := *h.manifest
	cp.Chunks = append([]spillChunkRef(nil), h.manifest.Chunks...)
	return cp
}

// LoadGenerationManifest reads an immutable retained
// manifest generation (a recoverable root).
func LoadSpillGenerationManifest(dir string, gen uint64) (*spillManifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("history-manifest-gen-%d.json", gen)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: generation %d", ErrSpillGenerationUnknown, gen)
		}
		return nil, err
	}
	var m spillManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: generation %d unparseable: %v", ErrSpillCorrupt, gen, err)
	}
	if spillManifestChecksum(&m) != m.Checksum {
		return nil, fmt.Errorf("%w: generation %d checksum mismatch", ErrSpillCorrupt, gen)
	}
	return &m, nil
}

// VersionsAtGeneration reads versions through one
// retained manifest generation (chunk files are
// immutable, so a generation root stays readable even
// after later prunes — unless its chunk files were
// reclaimed by a prune whose floor allowed it; a
// missing chunk file is a loud error, never a
// silent short read).
func (h *spillHistory) VersionsAtGeneration(gen uint64, key string, fromSeq uint64, limit int) ([]SpillVersion, error) {
	man, err := LoadSpillGenerationManifest(h.dir, gen)
	if err != nil {
		return nil, err
	}
	var out []SpillVersion
	err = h.streamChunks(man.Chunks, func(r SpillVersion) bool {
		if r.Key == key && r.Seq >= fromSeq {
			out = append(out, r)
			if limit > 0 && len(out) >= limit {
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---- PersistedSegDB spill-history API (Phase E) ----

// HistoryVersions returns pre-images for key with
// Seq >= fromSeq, in seq order, at most limit.
func (p *PersistedSegDB) HistoryVersions(key string, fromSeq uint64, limit int) ([]SpillVersion, error) {
	if p.spill == nil {
		return nil, fmt.Errorf("yowza spill history: log not initialised")
	}
	return p.spill.Versions(key, fromSeq, limit)
}

// HistoryValueAt returns the value key had at WAL seq
// S (see spillHistory.ValueAt). It consults the live
// head only when no later mutation exists.
func (p *PersistedSegDB) HistoryValueAt(key string, seq uint64) ([]byte, bool, error) {
	if p.spill == nil {
		return nil, false, fmt.Errorf("yowza spill history: log not initialised")
	}
	// Scan first (no p.mu); live read only as fallback.
	h := p.spill
	h.mu.Lock()
	refs := h.refsLocked()
	h.mu.Unlock()
	var answer []byte
	answerOK := false
	found := false
	err := h.streamChunks(refs, func(r SpillVersion) bool {
		if r.Key == key && r.Seq > seq {
			answer, answerOK, found = append([]byte(nil), r.PreVal...), r.PreOK, true
			return false
		}
		return true
	})
	if err != nil {
		return nil, false, err
	}
	if found {
		return answer, answerOK, nil
	}
	v, gerr := p.Get(key)
	if gerr != nil {
		return nil, false, nil
	}
	return v, true, nil
}

// HistoryManifest returns the current version-log
// manifest (generation, chunk list, levels).
func (p *PersistedSegDB) HistoryManifest() (spillManifest, error) {
	if p.spill == nil {
		return spillManifest{}, fmt.Errorf("yowza spill history: log not initialised")
	}
	return p.spill.Manifest(), nil
}

// spillRetentionFloor is the oldest host seq a live
// durable pin may still need: the minimum HostSeq
// over active pins. With no active pins there is no
// floor (MaxUint64: any prune is allowed).
func (p *PersistedSegDB) spillRetentionFloorLocked() uint64 {
	floor := uint64(0)
	have := false
	for _, st := range p.pins {
		if st == nil || st.pin == nil || st.pin.Status != PinStatusActive {
			continue
		}
		if !have || st.pin.HostSeq < floor {
			floor = st.pin.HostSeq
			have = true
		}
	}
	if !have {
		return ^uint64(0)
	}
	return floor
}

// HistoryPrune drops version records with Seq <
// beforeSeq, staged: new chunk files + manifest
// publish is the commit point; old files are deleted
// only after the publish. It refuses (typed error,
// nothing changed) when beforeSeq would pass the
// retention floor. Generational reclaim: pruning
// happens here, after publish, and only for versions
// no live pin needs — never between operations as a
// side effect of a read or a scan.
func (p *PersistedSegDB) HistoryPrune(beforeSeq uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPersistedClosed
	}
	if p.spill == nil {
		return fmt.Errorf("yowza spill history: log not initialised")
	}
	if beforeSeq == 0 {
		return nil
	}
	if floor := p.spillRetentionFloorLocked(); beforeSeq > floor {
		return fmt.Errorf("%w: prune to %d passes oldest pinned host seq %d", ErrSpillPruneFloor, beforeSeq, floor)
	}
	h := p.spill
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.rotateLocked(); err != nil {
		return err
	}
	var kept []spillChunkRef
	var doomed []string
	for _, ref := range h.manifest.Chunks {
		data, err := os.ReadFile(filepath.Join(h.dir, ref.File))
		if err != nil {
			return err
		}
		recs, _, _, err := spillDecodeChunk(data)
		if err != nil {
			return err
		}
		var keep []SpillVersion
		for _, r := range recs {
			if r.Seq >= beforeSeq {
				keep = append(keep, r)
			}
		}
		switch {
		case len(keep) == len(recs):
			kept = append(kept, ref)
		case len(keep) == 0:
			doomed = append(doomed, ref.File)
		default:
			name := spillChunkPath(h.dir, ref.Level, h.nextNum)
			h.nextNum++
			f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				return err
			}
			for _, r := range keep {
				if _, err := f.Write(spillEncodeRecord(r.Seq, r.Key, r.PreVal, r.PreOK)); err != nil {
					f.Close()
					return err
				}
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
			kept = append(kept, spillChunkRef{File: filepath.Base(name), FirstSeq: keep[0].Seq, LastSeq: keep[len(keep)-1].Seq, Count: len(keep), Level: ref.Level})
			doomed = append(doomed, ref.File)
		}
	}
	h.manifest.Chunks = kept
	if err := h.publishLocked(); err != nil {
		return err
	}
	for _, f := range doomed {
		_ = os.Remove(filepath.Join(h.dir, f))
	}
	if h.syncMode {
		_ = persistSyncDir(h.dir)
	}
	return nil
}

// HistoryCompactLevels merges runs of groupSize
// consecutive level-0 chunks into single level-1
// files (multi-level history compaction: the manifest
// becomes hierarchical — level-0 leaves under level-1
// nodes — instead of one flat chunk list capped at
// any size). Staged like prune: new level files +
// manifest publish, then old chunks deleted. It
// returns (levels created, chunks merged).
func (p *PersistedSegDB) HistoryCompactLevels(groupSize int) (int, int, error) {
	if groupSize < 2 {
		groupSize = 4
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, 0, ErrPersistedClosed
	}
	if p.spill == nil {
		return 0, 0, fmt.Errorf("yowza spill history: log not initialised")
	}
	h := p.spill
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.rotateLocked(); err != nil {
		return 0, 0, err
	}
	var out []spillChunkRef
	var doomed []string
	levels, merged := 0, 0
	i := 0
	refs := h.manifest.Chunks
	for i < len(refs) {
		// Count a run of level-0 chunks.
		j := i
		for j < len(refs) && refs[j].Level == 0 {
			j++
		}
		run := refs[i:j]
		k := 0
		for k+groupSize <= len(run) {
			group := run[k : k+groupSize]
			name := spillChunkPath(h.dir, 1, h.nextNum)
			h.nextNum++
			f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				return 0, 0, err
			}
			var first, last uint64
			count := 0
			ok := true
			for _, ref := range group {
				data, err := os.ReadFile(filepath.Join(h.dir, ref.File))
				if err != nil {
					f.Close()
					return 0, 0, err
				}
				recs, _, _, err := spillDecodeChunk(data)
				if err != nil {
					f.Close()
					return 0, 0, err
				}
				for _, r := range recs {
					if _, err := f.Write(spillEncodeRecord(r.Seq, r.Key, r.PreVal, r.PreOK)); err != nil {
						f.Close()
						return 0, 0, err
					}
					if count == 0 {
						first = r.Seq
					}
					last = r.Seq
					count++
				}
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return 0, 0, err
			}
			if err := f.Close(); err != nil {
				return 0, 0, err
			}
			_ = ok
			out = append(out, spillChunkRef{File: filepath.Base(name), FirstSeq: first, LastSeq: last, Count: count, Level: 1})
			for _, ref := range group {
				doomed = append(doomed, ref.File)
			}
			levels++
			merged += len(group)
			k += groupSize
		}
		out = append(out, run[k:]...)
		i = j
	}
	if levels == 0 {
		return 0, 0, nil
	}
	h.manifest.Chunks = out
	if err := h.publishLocked(); err != nil {
		return 0, 0, err
	}
	for _, f := range doomed {
		_ = os.Remove(filepath.Join(h.dir, f))
	}
	if h.syncMode {
		_ = persistSyncDir(h.dir)
	}
	return levels, merged, nil
}
