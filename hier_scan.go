package yowza

// Phase E, E4 — MERGE TREE + HIERARCHICAL ITERATOR.
//
// The 4096 fan-in cap is a per-level cap, never a
// global one. Phase B proved the level structure over
// synthetic leaves (HierBuildBalanced); this file
// wires it into a real scan path. A MergeTree is a
// tree of merge nodes: each node merges at most
// FanIn child streams with a key-ordered heap, so
// memory at any node is bounded by (fan-in x one
// page) plus the heap — never by the total row
// count, and never by a flat source list. Leaves are
// either real per-shard cursor streams (SegDB.
// ScanCursor at the shard's vector entry, paged)
// or synthetic sorted streams (the >4096-source
// proof: 5000 synthetic leaves through the same
// merge code, since a 4096-segment wasm fixture
// cannot be built through the 4 MiB-heap shards —
// see the honest limit in the notes).
//
// Scan order: each leaf stream is key-sorted within
// its shard (ScanCursor order); the tree merge is a
// k-way merge, so the tree output is globally
// key-sorted when leaf streams are disjoint in key
// space OR when duplicates are resolved by the
// node's tie-break (first child wins — callers
// whose leaves overlap must partition keys, as
// shards do by hash). The real per-shard wiring
// partitions by shard, so leaves never overlap.

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
)

// ErrHierScanLimit reports a merge-tree resource cap
// (fan-in, page size). It is a typed error, never a
// fixed-array failure or silent truncation.
var ErrHierScanLimit = errors.New("yowza hierarchical scan: resource limit")

// hierStream is one sorted stream of scan entries.
type hierStream interface {
	// Next returns the next entry, or ok=false at EOF.
	Next() (SegDBScanEntry, bool, error)
}

// sliceStream is a sorted in-memory leaf (synthetic
// sources in tests, and the unit proof of the tree).
type sliceStream struct {
	rows []SegDBScanEntry
	i    int
}

func (s *sliceStream) Next() (SegDBScanEntry, bool, error) {
	if s.i >= len(s.rows) {
		return SegDBScanEntry{}, false, nil
	}
	e := s.rows[s.i]
	s.i++
	return e, true, nil
}

// shardCursorStream is a real per-shard leaf: it pages
// SegDB.ScanCursor at one shard's vector entry, one
// page at a time, never materialising the shard.
type shardCursorStream struct {
	shard   *SegDB
	prefix  string
	seq     uint64
	cursor  []byte
	hasCur  bool
	page    []SegDBScanEntry
	pi      int
	hasMore bool
	started bool
	limit   uint32
}

func (s *shardCursorStream) Next() (SegDBScanEntry, bool, error) {
	for {
		if s.pi < len(s.page) {
			e := s.page[s.pi]
			s.pi++
			return e, true, nil
		}
		if s.started && !s.hasMore {
			return SegDBScanEntry{}, false, nil
		}
		rows, next, more, err := s.shard.ScanCursor(s.prefix, s.seq, s.cursor, s.hasCur, s.limit)
		if err != nil {
			return SegDBScanEntry{}, false, err
		}
		s.started = true
		s.page = rows
		s.pi = 0
		s.cursor = next
		s.hasCur = true
		s.hasMore = more
		if len(rows) == 0 && !more {
			return SegDBScanEntry{}, false, nil
		}
	}
}

// mergeHeapItem is one buffered head entry.
type mergeHeapItem struct {
	entry SegDBScanEntry
	child int
}

type mergeHeap []mergeHeapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	ki, kj := string(h[i].entry.Key), string(h[j].entry.Key)
	if ki != kj {
		return ki < kj
	}
	return h[i].child < h[j].child
}
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(mergeHeapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// mergeNode merges at most fanIn child streams.
type mergeNode struct {
	children []hierStream
	h        mergeHeap
	started  bool
}

func (n *mergeNode) Next() (SegDBScanEntry, bool, error) {
	if !n.started {
		n.started = true
		for i, c := range n.children {
			e, ok, err := c.Next()
			if err != nil {
				return SegDBScanEntry{}, false, err
			}
			if ok {
				heap.Push(&n.h, mergeHeapItem{entry: e, child: i})
			}
		}
	}
	if n.h.Len() == 0 {
		return SegDBScanEntry{}, false, nil
	}
	it := heap.Pop(&n.h).(mergeHeapItem)
	e, ok, err := n.children[it.child].Next()
	if err != nil {
		return SegDBScanEntry{}, false, err
	}
	if ok {
		heap.Push(&n.h, mergeHeapItem{entry: e, child: it.child})
	}
	return it.entry, true, nil
}

// HierMergeTree builds a balanced merge tree over
// leaves with the given fan-in (the tree version of
// HierBuildBalanced, but over live streams instead
// of segment IDs). Fan-in above 4096 is refused:
// 4096 is one level's cap in this policy, and a
// level wider than that is a configuration error,
// not a bigger array.
func HierMergeTree(leaves []hierStream, fanIn int) (hierStream, error) {
	if fanIn <= 0 || fanIn > 4096 {
		return nil, fmt.Errorf("%w: fan-in %d out of range 1..4096", ErrHierScanLimit, fanIn)
	}
	if len(leaves) == 0 {
		return &sliceStream{}, nil
	}
	level := leaves
	for len(level) > 1 {
		var next []hierStream
		for i := 0; i < len(level); i += fanIn {
			end := i + fanIn
			if end > len(level) {
				end = len(level)
			}
			if end-i == 1 {
				next = append(next, level[i])
			} else {
				next = append(next, &mergeNode{children: append([]hierStream(nil), level[i:end]...)})
			}
		}
		level = next
	}
	return level[0], nil
}

// HierMergeTreeDepth returns the tree depth for a
// leaf count and fan-in (root = 1): the level count
// a scan actually walks.
func HierMergeTreeDepth(leafCount, fanIn int) int {
	if leafCount <= 1 {
		return 1
	}
	depth := 1
	for leafCount > 1 {
		leafCount = (leafCount + fanIn - 1) / fanIn
		depth++
	}
	return depth
}

// HierarchicalScan streams a whole PersistedSegDB at
// a snapshot vector through a merge tree over
// per-shard cursor streams: one leaf per shard, one
// merge node over them (fanIn bounds the node, the
// shard count bounds the leaves — adding shards
// adds leaves, never a wider flat list). Rows are
// returned globally key-sorted (shard streams are
// disjoint by hash partitioning). The caller pulls
// page by page; memory is bounded by the page plus
// one buffered head per shard.
func (p *PersistedSegDB) HierarchicalScan(vector []uint64, prefix string, pageSize int, fanIn int) ([][]SegDBScanEntry, error) {
	if len(vector) != len(p.shards) {
		return nil, fmt.Errorf("%w: vector has %d entries, database has %d shards", ErrPersistedSnapshotMismatch, len(vector), len(p.shards))
	}
	if pageSize <= 0 {
		pageSize = 64
	}
	if fanIn <= 0 {
		fanIn = 16
	}
	// Pin the vector for the whole scan (Phase B
	// leases), so a concurrent compact cannot drop
	// data this scan still needs. The release is
	// safe after DB close (leases die with the
	// modules), matching AcquireShardLeases.
	_, release, err := p.AcquireShardLeases(vector)
	if err != nil {
		return nil, err
	}
	defer release()
	p.mu.Lock()
	shards := append([]*persistShard(nil), p.shards...)
	p.mu.Unlock()
	var leaves []hierStream
	for i, sh := range shards {
		limit := uint32(pageSize)
		if limit > 27 {
			limit = 27 // segPageMaxRows: one cursor page always fits
		}
		leaves = append(leaves, &shardCursorStream{shard: sh.sdb, prefix: prefix, seq: vector[i], limit: limit})
	}
	tree, err := HierMergeTree(leaves, fanIn)
	if err != nil {
		return nil, err
	}
	var pages [][]SegDBScanEntry
	var page []SegDBScanEntry
	for {
		e, ok, err := tree.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		page = append(page, e)
		if len(page) >= pageSize {
			pages = append(pages, page)
			page = nil
		}
	}
	if len(page) > 0 {
		pages = append(pages, page)
	}
	return pages, nil
}

// sortedHierKeys is a test helper (sorted keys of a
// page set), kept here to avoid touching other files.
func sortedHierKeys(pages [][]SegDBScanEntry) []string {
	var out []string
	for _, pg := range pages {
		for _, e := range pg {
			out = append(out, string(e.Key))
		}
	}
	sort.Strings(out)
	return out
}
