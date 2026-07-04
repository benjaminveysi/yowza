// Host spill (Gate 4.1) — the disk-backed spill contract for ISA
// execution at the host layer.
//
// WHY THIS LIVES ON THE HOST. The wasm ISA engine has no
// filesystem and no host file-spill imports: its spill is a
// LOGICAL spill inside the host-provided 32 MiB work buffer
// (see include/query_isa.h). Enlarging that buffer would just
// move the ceiling, and adding wasm imports for file spill
// would break every other host that instantiates the module
// with an empty env module (notably the root package's
// NewYowzaDB) — so Gate 4 does NOT add wasm imports. Instead,
// spill to real temporary files happens here, on the host,
// where the coordinator's global operators (JOIN build, SORT
// runs, aggregation groups) live. A query whose working set
// exceeds the 32 MiB work buffer is executed by the
// coordinator (query/isa_multishard.go): per-shard scans still
// run inside the ISA engines, and the retaining operators run
// here, spilling to disk. The monolithic C engine's in-buffer
// spill limit is unchanged and is documented as such in
// GATE4_NOTES.md — it is not re-described as disk spill.
//
// CONTRACT (each bullet is enforced and tested):
//   - Temporary run creation: one temp file per run, created
//     under the manager's directory (os.CreateTemp, prefix
//     "yowza-spill-"), one JSON value per line (the same
//     line format the Go executor's spill files use).
//   - Sequential writing: rows are appended in order; a run
//     is write-then-read (writers close before readers open,
//     or the reader re-opens the finished file).
//   - Paged reading: readers stream one page at a time and
//     never load a whole run into memory.
//   - Deletion: Close on a run removes its file; Close on
//     the manager removes every live run's file. Close is
//     idempotent.
//   - Sync where durability is required: when the manager
//     was created with sync=true, each finished run is
//     fsynced before it is handed to a reader. (Query spill
//     is scratch, not durable state, so sync defaults to
//     false; a caller that treats spill as durable state
//     must opt in explicitly.)
//   - Per-query scratch quota: the manager tracks bytes
//     written across all its live runs; a write that would
//     exceed the quota fails with ErrSpillQuotaExceeded and
//     the run is left in a clean, closable state. Quota
//     accounting is released when a run closes.
//   - Cleanup on error or cancellation: every coordinator
//     path defers run/manager Close, and Write checks
//     ctx.Err() first, so a cancelled query leaves no temp
//     files behind. A run whose writer errors is closed
//     (deleted) by the caller; the error is typed, never a
//     partial apparently-successful result.
//   - Errors are typed: quota exhaustion is
//     ErrSpillQuotaExceeded; I/O failures wrap ErrSpillIO;
//     cancellation propagates ctx.Err(). A spill failure
//     aborts the query with no rows, exactly like a C
//     engine OOM: never a partial result.
package query

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ErrSpillQuotaExceeded is returned when a spill write would
// exceed the manager's scratch quota. It is a resource error,
// not a data error: the query may succeed with a larger quota
// or a smaller working set, but it must not be retried
// against the same quota expecting a different result.
var ErrSpillQuotaExceeded = errors.New("query: host spill quota exceeded")

// ErrSpillIO wraps host spill I/O failures (create, write,
// read, sync, delete). The wrapped cause is preserved with %w.
var ErrSpillIO = errors.New("query: host spill I/O failure")

// DefaultSpillQuotaBytes is the default per-manager scratch
// quota when a ResourcePolicy leaves SpillQuotaBytes at 0:
// 512 MiB. It is a policy default, not a hard engine cap —
// callers set an explicit quota for tighter control.
const DefaultSpillQuotaBytes = int64(512) << 20

// SpillManager owns a set of temporary spill runs under one
// directory and enforces one byte quota across them.
type SpillManager struct {
	mu     sync.Mutex
	dir    string
	ownDir bool
	quota  int64 // <0 = unlimited
	sync   bool
	used   int64
	runs   map[*SpillRun]struct{}
	closed bool
}

// NewSpillManager creates a manager. dir == "" creates a fresh
// temp directory (removed on Close); a caller-supplied dir is
// kept on Close (only run files inside it are removed).
// quota < 0 means unlimited; quota == 0 means the default
// quota (DefaultSpillQuotaBytes); quota > 0 is used as-is.
func NewSpillManager(dir string, quota int64, syncDurable bool) (*SpillManager, error) {
	own := false
	if dir == "" {
		d, err := os.MkdirTemp("", "yowza-spill-*")
		if err != nil {
			return nil, fmt.Errorf("%w: mkdirtemp: %v", ErrSpillIO, err)
		}
		dir = d
		own = true
	} else if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("%w: mkdir %s: %v", ErrSpillIO, dir, err)
	}
	if quota == 0 {
		quota = DefaultSpillQuotaBytes
	}
	return &SpillManager{dir: dir, ownDir: own, quota: quota, sync: syncDurable, runs: map[*SpillRun]struct{}{}}, nil
}

// Dir returns the manager's spill directory.
func (m *SpillManager) Dir() string { return m.dir }

// UsedBytes returns the bytes currently accounted to live runs.
func (m *SpillManager) UsedBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.used
}

// Quota returns the configured quota (<0 = unlimited).
func (m *SpillManager) Quota() int64 { return m.quota }

// NewRun creates one temporary spill run. The caller must
// Close the run (which deletes its file) even on error.
func (m *SpillManager) NewRun(ctx context.Context) (*SpillRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("%w: manager closed", ErrSpillIO)
	}
	f, err := os.CreateTemp(m.dir, "yowza-spill-*.jsonl")
	if err != nil {
		return nil, fmt.Errorf("%w: create run: %v", ErrSpillIO, err)
	}
	r := &SpillRun{mgr: m, path: f.Name(), f: f, enc: json.NewEncoder(f)}
	m.runs[r] = struct{}{}
	return r, nil
}

func (m *SpillManager) account(r *SpillRun, delta int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.quota >= 0 && delta > 0 && m.used+delta > m.quota {
		return ErrSpillQuotaExceeded
	}
	m.used += delta
	return nil
}

func (m *SpillManager) release(r *SpillRun) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.used -= r.bytes
	if m.used < 0 {
		m.used = 0
	}
	delete(m.runs, r)
}

// Close deletes every live run's file and, when the manager
// owns its directory, the directory itself. Idempotent.
func (m *SpillManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	runs := make([]*SpillRun, 0, len(m.runs))
	for r := range m.runs {
		runs = append(runs, r)
	}
	dir, own := m.dir, m.ownDir
	m.mu.Unlock()
	var firstErr error
	for _, r := range runs {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if own {
		if err := os.RemoveAll(dir); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("%w: remove spill dir: %v", ErrSpillIO, err)
		}
	}
	return firstErr
}

// SpillTrace records what a spill-backed operation did, in the
// same vocabulary as ISATrace's spilled flag plus byte counts.
type SpillTrace struct {
	Runs    int
	Bytes   int64
	Spilled bool // true iff at least one run was created
}

// SpillRun is one sequential spill file: rows are written in
// order, then read back paged, then the file is deleted by
// Close. A run is single-writer, single-reader-sequential:
// Finish must be called before NewReader, and no Write may
// follow Finish.
type SpillRun struct {
	mgr      *SpillManager
	path     string
	f        *os.File
	enc      *json.Encoder
	bytes    int64
	rows     int64
	finished bool
	closed   bool
	mu       sync.Mutex
}

// Path returns the run's temp file path (for tests and for
// verifying cleanup; callers must not open it directly).
func (r *SpillRun) Path() string { return r.path }

// RowsWritten returns the number of rows written so far.
func (r *SpillRun) RowsWritten() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rows
}

// WriteRow appends one row. It checks ctx first (cancellation
// aborts the write) and enforces the manager's quota: a write
// that would exceed the quota returns ErrSpillQuotaExceeded
// and writes nothing for that row.
func (r *SpillRun) WriteRow(ctx context.Context, row Row) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("%w: write to closed spill run", ErrSpillIO)
	}
	if r.finished {
		return fmt.Errorf("%w: write after spill run finished", ErrSpillIO)
	}
	// Encode to bytes first so the quota check and the write
	// are consistent (the encoder's byte count is not
	// observable, so measure the marshalled form).
	b, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("%w: marshal spill row: %v", ErrSpillIO, err)
	}
	// +1 for the newline the encoder appends.
	if err := r.mgr.account(r, int64(len(b))+1); err != nil {
		return err
	}
	if _, err := r.f.Write(append(b, '\n')); err != nil {
		r.mgr.account(r, -(int64(len(b)) + 1))
		return fmt.Errorf("%w: write spill row: %v", ErrSpillIO, err)
	}
	r.bytes += int64(len(b)) + 1
	r.rows++
	return nil
}

// Finish flushes the run and, when the manager requires sync,
// fsyncs it. After Finish the run is readable and no longer
// writable.
func (r *SpillRun) Finish(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("%w: finish closed spill run", ErrSpillIO)
	}
	if r.finished {
		return nil
	}
	if err := r.f.Sync(); err != nil && r.mgr.sync {
		return fmt.Errorf("%w: sync spill run: %v", ErrSpillIO, err)
	}
	// Sync errors in non-durable mode are ignored deliberately:
	// scratch spill does not need durability, and a sync
	// failure must not fail a query that never promised it.
	// (In durable mode the error above already returned.)
	if err := r.f.Close(); err != nil {
		return fmt.Errorf("%w: close spill run for reading: %v", ErrSpillIO, err)
	}
	r.f = nil
	r.finished = true
	return nil
}

// NewReader returns a paged reader over the finished run.
// The reader does not delete the file; Close on the run does.
func (r *SpillRun) NewReader() (*SpillReader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("%w: read closed spill run", ErrSpillIO)
	}
	if !r.finished {
		return nil, fmt.Errorf("%w: read unfinished spill run", ErrSpillIO)
	}
	f, err := os.Open(r.path)
	if err != nil {
		return nil, fmt.Errorf("%w: open spill run: %v", ErrSpillIO, err)
	}
	return &SpillReader{f: f, dec: json.NewDecoder(bufio.NewReader(f))}, nil
}

// Close deletes the run's file and releases its quota
// accounting. Idempotent; safe to call on an unfinished run.
func (r *SpillRun) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	f, path := r.f, r.path
	r.f = nil
	r.mu.Unlock()
	var err error
	if f != nil {
		err = f.Close()
	}
	if rerr := os.Remove(path); rerr != nil && !os.IsNotExist(rerr) && err == nil {
		err = fmt.Errorf("%w: remove spill run: %v", ErrSpillIO, rerr)
	}
	r.mgr.release(r)
	return err
}

// SpillReader streams one spill run, one page at a time.
type SpillReader struct {
	f   *os.File
	dec *json.Decoder
	eof bool
}

// NextPage reads up to pageSize rows. An empty slice with a
// nil error means end of input. A decoding failure is a
// typed spill I/O error, never a truncated page presented
// as success.
func (sr *SpillReader) NextPage(ctx context.Context, pageSize int) ([]Row, error) {
	if sr.eof {
		return nil, nil
	}
	if pageSize <= 0 {
		pageSize = 64
	}
	rows := make([]Row, 0, pageSize)
	for len(rows) < pageSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var row Row
		if err := sr.dec.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				sr.eof = true
				break
			}
			return nil, fmt.Errorf("%w: decode spill row: %v", ErrSpillIO, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Close closes the reader's file handle (not the run file).
func (sr *SpillReader) Close() error {
	if sr.f == nil {
		return nil
	}
	err := sr.f.Close()
	sr.f = nil
	return err
}

// spillDirEntries is a test helper: the number of spill files
// currently present in dir.
func spillDirEntries(dir string) (int, error) {
	ents, err := filepath.Glob(filepath.Join(dir, "yowza-spill-*.jsonl"))
	if err != nil {
		return 0, err
	}
	return len(ents), nil
}
