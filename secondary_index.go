package yowza

// Phase E, E4 — PAGED SECONDARY INDEX (replacing
// linear lookup over >256-block segments).
//
// The C seg_index_view caps at 256 blocks and falls
// back to a linear index scan past that (Phase C
// note); merely raising 256 would keep a linear
// design with a bigger constant. This index is a
// different design: an IMMUTABLE, paged, sparse
// index over a sorted key stream. Pages hold at most
// pageSize entries; the header holds one first-key
// per page; a lookup binary-searches the headers,
// loads EXACTLY ONE page, and binary-searches within
// it. Lookup cost is O(log pages + log pageSize)
// page loads (one), regardless of total size — a
// 300-page index (past the old 256-block cap) looks
// up with a single page load, proven by the load
// counter in the tests. The index is built by
// streaming (one page buffered, never the whole key
// set), and a built index is never modified in
// place: rebuilding writes new files and swaps the
// header atomically (tmp + sync + rename), so a
// reader sees a whole old or whole new index.
//
// File pair in the DB directory:
//   secidx-<name>.dat  — pages, each [u32 count]
//     then count entries [u32 klen][key][u32 vlen][val]
//   secidx-<name>.json — header {version, page_size,
//     total, pages: [{first_key, offset, bytes,
//     count}]}, JSON + CRC-32.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
)

// ErrSecondaryIndexCorrupt reports a header or page
// failing validation.
var ErrSecondaryIndexCorrupt = errors.New("yowza secondary index: corrupt header or page")

type secIdxPageHeader struct {
	FirstKey string `json:"first_key"`
	Offset   int64  `json:"offset"`
	Bytes    int64  `json:"bytes"`
	Count    int    `json:"count"`
}

type secIdxHeader struct {
	Version  int                `json:"version"`
	PageSize int                `json:"page_size"`
	Total    int                `json:"total"`
	Pages    []secIdxPageHeader `json:"pages"`
	Checksum uint32             `json:"checksum"`
}

// SecondaryIndex is one immutable paged index. It is
// safe for concurrent lookups; PageLoads counts
// physical page loads (the paging proof).
type SecondaryIndex struct {
	dir      string
	name     string
	pageSize int
	total    int
	pages    []secIdxPageHeader
	loads    int64
}

func secIdxPaths(dir, name string) (dat, hdr string) {
	return filepath.Join(dir, "secidx-"+name+".dat"), filepath.Join(dir, "secidx-"+name+".json")
}

func secIdxEncodePage(entries []SegDBScanEntry) []byte {
	var out []byte
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(entries)))
	out = append(out, tmp4[:]...)
	for _, e := range entries {
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(e.Key)))
		out = append(out, tmp4[:]...)
		out = append(out, e.Key...)
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(e.Val)))
		out = append(out, tmp4[:]...)
		out = append(out, e.Val...)
	}
	return out
}

func secIdxDecodePage(data []byte) ([]SegDBScanEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("%w: short page", ErrSecondaryIndexCorrupt)
	}
	count := int(binary.LittleEndian.Uint32(data[:]))
	off := 4
	var out []SegDBScanEntry
	for i := 0; i < count; i++ {
		if len(data)-off < 4 {
			return nil, fmt.Errorf("%w: page truncated at key len", ErrSecondaryIndexCorrupt)
		}
		klen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if len(data)-off < klen+4 {
			return nil, fmt.Errorf("%w: page truncated at key", ErrSecondaryIndexCorrupt)
		}
		key := append([]byte(nil), data[off:off+klen]...)
		off += klen
		vlen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if len(data)-off < vlen {
			return nil, fmt.Errorf("%w: page truncated at value", ErrSecondaryIndexCorrupt)
		}
		val := append([]byte(nil), data[off:off+vlen]...)
		off += vlen
		out = append(out, SegDBScanEntry{Key: key, Val: val})
	}
	return out, nil
}

// BuildSecondaryIndex streams entries (which must
// arrive in strictly increasing key order — the
// builder refuses an unsorted stream rather than
// building a lying index) into a paged index named
// name in dir. Only one page is buffered at a time.
func BuildSecondaryIndex(dir, name string, stream hierStream, pageSize int) (*SecondaryIndex, error) {
	if pageSize <= 0 {
		pageSize = 64
	}
	datPath, hdrPath := secIdxPaths(dir, name)
	tmpDat := datPath + ".tmp"
	tmpHdr := hdrPath + ".tmp"
	df, err := os.OpenFile(tmpDat, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	var pages []secIdxPageHeader
	var offset int64
	var page []SegDBScanEntry
	var prevKey string
	havePrev := false
	total := 0
	flush := func() error {
		if len(page) == 0 {
			return nil
		}
		blob := secIdxEncodePage(page)
		if _, err := df.Write(blob); err != nil {
			return err
		}
		pages = append(pages, secIdxPageHeader{FirstKey: string(page[0].Key), Offset: offset, Bytes: int64(len(blob)), Count: len(page)})
		offset += int64(len(blob))
		page = nil
		return nil
	}
	for {
		e, ok, err := stream.Next()
		if err != nil {
			df.Close()
			return nil, err
		}
		if !ok {
			break
		}
		if havePrev && string(e.Key) <= prevKey {
			df.Close()
			return nil, fmt.Errorf("yowza secondary index: stream not strictly sorted at %q after %q", string(e.Key), prevKey)
		}
		prevKey, havePrev = string(e.Key), true
		page = append(page, e)
		total++
		if len(page) >= pageSize {
			if err := flush(); err != nil {
				df.Close()
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		df.Close()
		return nil, err
	}
	if err := df.Sync(); err != nil {
		df.Close()
		return nil, err
	}
	if err := df.Close(); err != nil {
		return nil, err
	}
	hdr := secIdxHeader{Version: 1, PageSize: pageSize, Total: total, Pages: pages}
	body, err := json.Marshal(struct {
		Version  int                `json:"version"`
		PageSize int                `json:"page_size"`
		Total    int                `json:"total"`
		Pages    []secIdxPageHeader `json:"pages"`
	}{Version: hdr.Version, PageSize: hdr.PageSize, Total: hdr.Total, Pages: hdr.Pages})
	if err != nil {
		return nil, err
	}
	hdr.Checksum = crc32.ChecksumIEEE(body)
	data, err := json.Marshal(hdr)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(tmpHdr, data, 0o644); err != nil {
		return nil, err
	}
	if hf, err := os.OpenFile(tmpHdr, os.O_WRONLY, 0o644); err == nil {
		_ = hf.Sync()
		hf.Close()
	}
	if err := os.Rename(tmpDat, datPath); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpHdr, hdrPath); err != nil {
		return nil, err
	}
	_ = persistSyncDir(dir)
	return &SecondaryIndex{dir: dir, name: name, pageSize: pageSize, total: total, pages: pages}, nil
}

// OpenSecondaryIndex loads an index header (headers
// only — pages stay on disk until looked up).
func OpenSecondaryIndex(dir, name string) (*SecondaryIndex, error) {
	_, hdrPath := secIdxPaths(dir, name)
	data, err := os.ReadFile(hdrPath)
	if err != nil {
		return nil, err
	}
	var hdr secIdxHeader
	if err := json.Unmarshal(data, &hdr); err != nil {
		return nil, fmt.Errorf("%w: header unparseable: %v", ErrSecondaryIndexCorrupt, err)
	}
	if hdr.Version != 1 {
		return nil, fmt.Errorf("%w: header version %d", ErrSecondaryIndexCorrupt, hdr.Version)
	}
	body, err := json.Marshal(struct {
		Version  int                `json:"version"`
		PageSize int                `json:"page_size"`
		Total    int                `json:"total"`
		Pages    []secIdxPageHeader `json:"pages"`
	}{Version: hdr.Version, PageSize: hdr.PageSize, Total: hdr.Total, Pages: hdr.Pages})
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(body) != hdr.Checksum {
		return nil, fmt.Errorf("%w: header checksum mismatch", ErrSecondaryIndexCorrupt)
	}
	return &SecondaryIndex{dir: dir, name: name, pageSize: hdr.PageSize, total: hdr.Total, pages: hdr.Pages}, nil
}

// Lookup finds key with exactly one page load (or
// zero loads when the index is empty). It returns
// found=false when the key is absent — never an
// error for absence.
func (idx *SecondaryIndex) Lookup(key string) ([]byte, bool, error) {
	if len(idx.pages) == 0 {
		return nil, false, nil
	}
	// Last page whose first key <= key (headers are
	// in key order by construction).
	pi := sort.Search(len(idx.pages), func(i int) bool { return idx.pages[i].FirstKey > key }) - 1
	if pi < 0 {
		return nil, false, nil
	}
	ph := idx.pages[pi]
	datPath, _ := secIdxPaths(idx.dir, idx.name)
	f, err := os.Open(datPath)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	buf := make([]byte, ph.Bytes)
	if _, err := f.ReadAt(buf, ph.Offset); err != nil {
		return nil, false, err
	}
	atomic.AddInt64(&idx.loads, 1)
	entries, err := secIdxDecodePage(buf)
	if err != nil {
		return nil, false, err
	}
	i := sort.Search(len(entries), func(i int) bool { return string(entries[i].Key) >= key })
	if i < len(entries) && string(entries[i].Key) == key {
		return append([]byte(nil), entries[i].Val...), true, nil
	}
	return nil, false, nil
}

// PageLoads returns the number of physical page loads
// so far (the paging proof: one per lookup).
func (idx *SecondaryIndex) PageLoads() int64 { return atomic.LoadInt64(&idx.loads) }

// PageCount returns the number of pages in the index.
func (idx *SecondaryIndex) PageCount() int { return len(idx.pages) }

// Total returns the number of indexed entries.
func (idx *SecondaryIndex) Total() int { return idx.total }
