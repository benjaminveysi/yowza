/* Query ISA v1 — YowzaQL -> Typed IR -> Verified Physical Plan ->
 * Query ISA -> WASM Execution -> SegDB.
 *
 * This header is the complete, normative specification of the binary
 * ISA format. The Go compiler (query/isa.go) produces exactly these
 * bytes from a verified Plan, and the C engine (src/query_isa.c)
 * decodes exactly these bytes. Nothing outside this file defines
 * the encoding: if the two sides disagree, this file is the arbiter.
 *
 * DESIGN RULES (load-bearing, do not relax silently):
 *  - The ISA program is produced ONLY from a Plan that passed the
 *    existing verifier (query/verifier.go Verify). The Go compiler
 *    refuses any plan that fails verification. The C engine does
 *    NOT re-run the full verifier (it has no contract/role table),
 *    but it DOES check the ISA format version itself: a program
 *    whose version is not QISA_VERSION is rejected with
 *    QISA_ERR_VERSION before any scan or execution happens.
 *  - The C engine reads data ONLY through the SegDB scan APIs
 *    (segdb_scan_cursor at one caller-supplied snapshot, see
 *    UNIFIED EXECUTION CONTRACT below). It never touches ART,
 *    segments, or the manifest directly, and it never reimplements
 *    storage.
 *  - The C engine is read-only and side-effect free with respect to
 *    the database: re-running a program (e.g. after growing the
 *    host output buffer) yields identical output.
 *  - On ANY error (malformed program, version mismatch, type error,
 *    budget failure, OOM, scan failure, output overflow) the engine
 *    returns a negative code and NO rows: partial results are never
 *    delivered. The host discards the output buffer on error.
 *  - Semantics are the Track B reference executor's semantics
 *    (query/executor.go), not a second opinion: SQL three-valued
 *    logic for FILTER, NULL join keys never match, stable sort
 *    with NULLs last in either direction, ungrouped aggregation
 *    over empty input yields one row (COUNT = 0, other aggregates
 *    NULL), COUNT(col) counts non-null values only, SUM keeps int
 *    for all-int input and float otherwise, AVG is always float.
 *    Any divergence found by differential testing is a bug in one
 *    of the two engines, never a documented difference — except
 *    the two explicitly documented limits in DEEP_ISA_NOTES.md
 *    (number parsing beyond ~15 significant digits, and int64
 *    overflow of a JSON number, neither reachable from ordinary
 *    test data).
 *  - No libc, no malloc: the C engine uses only manual byte loops
 *    and a host-provided work buffer. This matters on wasm32,
 *    where a compiler-emitted memcpy/memset import broke this
 *    codebase before (Track A). Do not add string.h, do not call
 *    memcpy/memset/strcmp/strlen/malloc, and do not copy structs
 *    by assignment (use qi_vcopy for values); keep every copy an
 *    explicit loop or field-by-field copy.
 *
 * ALL INTEGERS ARE LITTLE-ENDIAN. All lengths/counts are u32 unless
 * noted. All strings are raw bytes (not NUL-terminated); a length
 * of 0 is a legal empty string.
 *
 * ------------------------------------------------------------------
 * UNIFIED EXECUTION CONTRACT (Reliability stage item 02, ISA v2)
 * ------------------------------------------------------------------
 * ISA v2 is the same Track B pipeline as v1, plus the three
 * guarantees that make the Go executor and the ISA engine
 * interchangeable for a plan (same rows, same error class, same
 * scanned count, same memory peak, same spilled flag):
 *   SNAPSHOT: one query execution reads every table at ONE
 *     snapshot sequence. The host takes the sequence once at
 *     query start (yowza_segdb_snapshot_seq) and passes it to
 *     the engine (see HOST CALLING CONVENTION); the engine
 *     passes that same sequence to every segdb_scan_cursor
 *     call for both tables. A write committed after the
 *     snapshot is invisible to both tables, including a write
 *     landing "between" the two table reads (the v1 bug: each
 *     segdb_scan call re-read "current" at its own moment).
 *     Snapshot sequences are LITERAL here (as in
 *     segdb_scan_cursor): sequence 0 yields no rows, so the
 *     host must pass a real snapshot sequence, never 0, for a
 *     non-empty database. A caller that wants "current" passes
 *     the sequence it just took, not 0.
 *   STREAMING SCAN: tables are read through the item-03 cursor
 *     scan (segdb_scan_cursor), in pages of 64 rows, never
 *     through the capped segdb_scan. The streaming (no
 *     AGGREGATE, no SORT) path processes one page at a time
 *     and never materializes a table: its capacity is the
 *     cursor scan's capacity (bounded by the 128-segment
 *     source cap, i.e. 524288 rows at 4096-row flushes, and
 *     by the host output buffer for the result), not by any
 *     ISA row cap. AGGREGATE/SORT/JOIN-build still retain
 *     data by semantics (groups, sorted rows, build side)
 *     and keep their documented caps below.
 *   MEMORY BUDGET AND SPILL: the program carries
 *     Contract.MaxMemoryBytes as max_memory (0 = unlimited).
 *     The engine enforces it with EXACTLY the Go executor's
 *     accounting (see query/executor.go, reproduced here so
 *     the two sides cannot drift):
 *       rowSize(row) = 32 + sum over columns of
 *         (len(column name) + valueSize(value))
 *       valueSize: NULL 0, INT/FLOAT 8, BOOL 1,
 *         STRING len, BYTES len.
 *     Column names are the Go row names: pipeline rows use
 *     alias.column (the program's per-table alias plus the
 *     column name, which is why v2 carries the alias), and
 *     projected (sort-input) rows use the PROJECT output
 *     names (which is why v2 carries them). Per retaining
 *     operator, in pipeline order (JOIN build, then
 *     AGGREGATE groups, then SORT buffer):
 *       - A single incoming batch (64 rows, or one join
 *         batch) whose measured size exceeds the budget
 *         fails with QISA_ERR_BUDGET and no rows. This is
 *         the "budget cannot hold one batch" rule, identical
 *         in both engines.
 *       - Otherwise, when retained data would exceed the
 *         budget, the operator SPILLS (sets the spilled
 *         flag) instead of failing: JOIN build batches and
 *         SORT chunks move to the spill store and the
 *         in-memory counter resets; AGGREGATE groups flush
 *         to spill records and the in-memory group set
 *         resets, merging back at the end in
 *         first-appearance order (deduplicated: see the
 *         note on the Go repeat-group spill bug in
 *         RELIABILITY_PARITY_PERF_NOTES.md — the ISA
 *         engine does NOT reproduce that bug).
 *       - The reported peak (mem_peak cell) is the largest
 *         per-operator retained size, using the same
 *         note points as Go: JOIN/SORT peaks never exceed
 *         the budget (they note after a spill-and-append);
 *         AGGREGATE peaks note group size BEFORE the spill
 *         check, so an aggregate peak may exceed the budget
 *         by one batch's group growth, exactly as in Go.
 *       - The final result collection is NOT budget-counted
 *         in either engine (the budget governs operator
 *         working sets, not the returned result set).
 *     WASM SPILL, STATED PLAINLY: the wasm module has no
 *     filesystem and no host file-spill imports, so ISA
 *     spill is a LOGICAL spill inside the host-provided
 *     work buffer: spilled batches/chunks/groups move to a
 *     separate spill region of that buffer (still wasm
 *     memory, freed when the call returns), and the
 *     reported peak/spilled flag follow the Go accounting
 *     above. Spill therefore reproduces Go's rows, error
 *     decisions, peak, and spilled flag for every input
 *     that fits the work buffer, but it does NOT bound
 *     total wasm memory to the budget the way Go's disk
 *     spill bounds host memory: an input whose retained +
 *     spilled data exceeds the work buffer fails with
 *     QISA_ERR_OOM, where Go would spill to disk and
 *     succeed. That is a documented wasm limit, not a
 *     silent parity claim. Host cooperation is explicit:
 *     the host supplies the work buffer (32 MiB in
 *     query/isa.go) and reads the mem_peak/spilled cells;
 *     there are no implicit host calls.
 *   A plan whose memory contract the ISA engine cannot
 *   execute is never silently approximated: it either
 *   executes with the accounting above, or fails with
 *   QISA_ERR_BUDGET / QISA_ERR_OOM and no rows. The
 *   compiler (query/isa.go) still runs Verify first, so
 *   the verifier's static estimate check (LIMIT plans:
 *   EstimatedRows x 256 bytes) applies to ISA programs
 *   exactly as to Go plans.
 *
 * ------------------------------------------------------------------
 * PROGRAM LAYOUT (ISA v2)
 * ------------------------------------------------------------------
 *   Header:
 *     magic           4 bytes  "YWQI" (0x59 0x57 0x51 0x49)
 *     version         u32      must equal QISA_VERSION (2)
 *     max_scanned     u64      runtime scanned-row budget, 0 = unlimited
 *                              (from Contract.MaxScannedRows; the
 *                              engine counts left + right scanned
 *                              rows against ONE shared budget, as
 *                              the reference engine does)
 *     max_result      u64      runtime result-row budget, 0 = unlimited
 *                              (from Contract.MaxResultRows; checked
 *                              after LIMIT, as the reference engine
 *                              checks it as batches arrive)
 *     max_memory      u64      runtime memory budget in bytes,
 *                              0 = unlimited
 *                              (from Contract.MaxMemoryBytes;
 *                              enforced per retaining operator
 *                              with the accounting above)
 *     num_tables      u32      1 or 2 (FROM table, then JOIN table)
 *   Table section, num_tables times, in plan Bindings order
 *   (Bindings[0] is always the FROM table; Bindings[1], when
 *   present, is the JOIN table):
 *     alias_len       u32
 *     alias           bytes    binding alias (for qualified
 *                              column names alias.column in
 *                              memory accounting; the engine
 *                              still addresses columns by
 *                              INDEX, never by name)
 *     prefix_len      u32
 *     prefix          bytes    SegDB key prefix for this table
 *     num_cols        u32
 *     per column, in binding order:
 *       name_len      u32
 *       name          bytes    column name (informational for
 *                              addressing: the C engine
 *                              addresses columns by INDEX,
 *                              never by name; the name IS
 *                              load-bearing for memory
 *                              accounting, via alias.name)
 *       type          u8       QISA_KIND_* (see below; == query.Kind)
 *       source_len    u32
 *       source        bytes    "key", "suffix", "value", or
 *                              "json:<field>" (see MATERIALIZATION)
 *   Op stream, in pipeline order (see OP STREAM below), ending with
 *   OP_END. Nothing may follow OP_END.
 *
 * GLOBAL COLUMN INDEX: columns are numbered across tables in
 * order: table 0's columns are 0..n0-1, table 1's columns are
 * n0..n0+n1-1. Every column reference in expressions, joins,
 * group-by lists, and (for non-aggregate plans) projections is a
 * global column index. Aggregate-plan projections do NOT use
 * global indices: they use PIPELINE indices (see AGGREGATE).
 *
 * VALUE ENCODING (literals, IN lists, and result rows):
 *   kind              u8       QISA_KIND_NULL 0, INT 1, FLOAT 2,
 *                              STRING 3, BOOL 4, BYTES 5
 *   payload, by kind:
 *     NULL            (none)
 *     INT             i64
 *     FLOAT           u64, IEEE-754 double bits
 *     STRING          u32 len + bytes
 *     BOOL            u8 (0 = false, nonzero = true)
 *     BYTES           u32 len + bytes
 *   In result rows the kind byte is written for every cell, even
 *   NULL (which then has no payload). The Go decoder validates
 *   every length against the output buffer before reading.
 *
 * ------------------------------------------------------------------
 * OP STREAM
 * ------------------------------------------------------------------
 * Opcodes are one byte. Operands follow immediately, in the order
 * listed. The expected sequence is:
 *   SCAN [JOIN] [FILTER] [AGGREGATE] PROJECT [SORT] [LIMIT] END
 * Any other order, a missing PROJECT, a second PROJECT, an unknown
 * opcode, or bytes after END is QISA_ERR_MALFORMED. (The compiler
 * always emits exactly this order, mirroring Plan.Operators.)
 *
 *   OP_SCAN  0x01
 *     table   u32      must be 0. Scans table 0 through
 *                      segdb_scan_cursor at the query snapshot
 *                      (see UNIFIED EXECUTION CONTRACT), in
 *                      pages of 64 rows. The streaming path
 *                      processes pages as they arrive and never
 *                      materializes the table; retaining paths
 *                      (JOIN build, AGGREGATE, SORT) retain only
 *                      what their semantics require.
 *   OP_JOIN  0x02
 *     left_col  u32    global column index of the FROM-side key
 *     right_table u32  must be 1. Scans table 1 through
 *                      segdb_scan_cursor at the query snapshot
 *                      and builds a hash table on its key.
 *     right_col u32    global column index of the JOIN-side key
 *     Inner equijoin, hash-based: a pipeline row is produced for
 *     every (left, right) pair whose keys are equal. Rows with a
 *     NULL key on EITHER side never match. Numeric keys compare
 *     numerically across int/float (as Compare does); any other
 *     kind mismatch between the two keys is QISA_ERR_TYPE.
 *     Pipeline rows after a join are left columns followed by
 *     right columns (width n0 + n1); without a join the width is
 *     n0. Hash collisions are resolved by chaining; the hash is
 *     over a canonical key encoding (see the C source), never over
 *     a raw pointer.
 *   OP_FILTER 0x03
 *     expr             one expression tree (see EXPRESSIONS).
 *     Keeps only rows whose expression evaluates to TRUE under
 *     SQL three-valued logic (see EXPRESSIONS).
 *   OP_AGGREGATE 0x04
 *     num_group u32
 *     group_cols num_group x u32   global column indices, in
 *                      GROUP BY order
 *     num_agg   u32
 *     per aggregate, in Projection order (aggregate items only):
 *       agg    u8       QISA_AGG_* (COUNT 0, SUM 1, AVG 2, MIN 3, MAX 4)
 *       star   u8       1 = COUNT(*) (col ignored), else 0
 *       col    u32      global column index (ignored when star)
 *     Groups are emitted in first-appearance order. A grouped
 *     plan over empty input emits no rows. An UNGROUPED plan
 *     (num_group == 0) over empty input emits exactly one row
 *     (COUNT = 0, SUM/AVG/MIN/MAX = NULL), per SQL semantics.
 *     After aggregation a pipeline row is: group keys in GROUP BY
 *     order, followed by aggregate results in the order the
 *     aggregates were listed here. PROJECTIONS of aggregate plans
 *     therefore use PIPELINE indices into that row: group key k
 *     is pipeline index k; aggregate j (0-based among aggregates)
 *     is pipeline index num_group + j. The compiler computes
 *     these indices; the C engine just selects them.
 *     COUNT(col) counts non-null values only; COUNT(*) counts
 *     rows. SUM/AVG skip NULLs and fail with QISA_ERR_TYPE on a
 *     non-numeric non-null value. SUM is int when every non-null
 *     input is int, else float; AVG is always float; MIN/MAX keep
 *     the input value (skipping NULLs; all-NULL input -> NULL).
 *   OP_PROJECT 0x05
 *     num_out   u32
 *     cols      num_out x u32   pipeline column indices, in
 *                      output order. For non-aggregate plans
 *                      these are global column indices; for
 *                      aggregate plans they are the pipeline
 *                      indices described under AGGREGATE.
 *     out_names num_out x (len u32 + bytes)   the plan's
 *                      projection OutputNames, in output order.
 *                      The C engine returns values only, in this
 *                      order, and the Go side reattaches the
 *                      names; the names are also load-bearing
 *                      for SORT memory accounting (sort-input
 *                      rows are projected rows, named by
 *                      OutputName in Go).
 *     The output column NAMES and their order are the plan's
 *     OutputColumns; the C engine returns values only, in this
 *     order, and the Go side reattaches the names.
 *   OP_SORT  0x06
 *     num_keys  u32
 *     per key:
 *       col    u32      PROJECTED column position (0-based into
 *                      the projected row, NOT a global index:
 *                      ORDER BY resolves against output columns)
 *       desc   u8       1 = descending, else ascending
 *     Stable sort (equal keys keep pipeline order). NULLs sort
 *     LAST in either direction. Other values compare with the
 *     same rules as FILTER comparisons; a comparison type error
 *     inside SORT is treated as "equal for this key" (matching
 *     the reference executor, which ignores Compare errors in
 *     its sort comparator and moves to the next key).
 *   OP_LIMIT 0x07
 *     limit     u64      result cap. LIMIT 0 yields zero rows
 *                      (and, in the streaming path, pulls no left
 *                      rows at all — see SCANNED COUNT below).
 *   OP_END   0xFF
 *     (no operands)
 *
 * ------------------------------------------------------------------
 * EXPRESSIONS (FILTER operand, and JOIN is NOT an expression)
 * ------------------------------------------------------------------
 * One node, encoded prefix-style (node tag first, then children):
 *   tag             u8       QISA_EXPR_* (below)
 *   AND 0x10 / OR 0x11:
 *     count         u32      number of children (>= 1)
 *     children      count x expr
 *   NOT 0x12:
 *     child         expr
 *   CMP 0x13:
 *     op            u8       QISA_CMP_EQ 0, NE 1, LT 2, LE 3, GT 4, GE 5
 *                              (the compiler maps both "!=" and
 *                              "<>" to NE)
 *     left          expr     (COL or LIT only)
 *     right         expr     (COL or LIT only)
 *   IN 0x14:
 *     left          expr     (COL or LIT only)
 *     negated       u8       1 = NOT IN, else IN
 *     count         u32      number of list literals
 *     list          count x value
 *   LIKE 0x15:
 *     left          expr     (COL or LIT only)
 *     negated       u8       1 = NOT LIKE, else LIKE
 *     pattern_len   u32
 *     pattern       bytes    SQL LIKE pattern: % = any sequence,
 *                              _ = exactly one character,
 *                              case-sensitive, anchored both ends
 *   ISNULL 0x16:
 *     left          expr     (COL or LIT only)
 *     negated       u8       1 = IS NOT NULL, else IS NULL
 *   COL 0x17:
 *     col           u32      global column index
 *   LIT 0x18:
 *     value         value encoding (see above)
 * Evaluation is SQL three-valued logic, exactly as the reference
 * executor: a comparison involving NULL is UNKNOWN; AND/OR/NOT
 * follow Kleene logic; IS NULL / IS NOT NULL are never UNKNOWN;
 * FILTER keeps only TRUE rows. IN: NULL left -> UNKNOWN; a match
 * -> TRUE (or FALSE when negated); no match with a NULL in the
 * list -> UNKNOWN; no match without NULL -> FALSE (or TRUE when
 * negated). A comparison or IN-equality between incompatible
 * types is QISA_ERR_TYPE, not FALSE (the binder normally prevents
 * this, but the engine must not silently coerce). LIKE on a
 * non-string non-null value is QISA_ERR_TYPE. LIKE matching is
 * over Unicode code points (so _ matches one character, not one
 * byte), using the same backtracking algorithm as the reference.
 *
 * ------------------------------------------------------------------
 * MATERIALIZATION (scan entry -> pipeline row)
 * ------------------------------------------------------------------
 * One scan entry is one KV pair [key][value] from a
 * segdb_scan_cursor page at the query snapshot.
 * For each column of the table, in binding order:
 *   source "key":    the full key, as a string.
 *   source "suffix": the key with the table prefix removed (if the
 *                    key does not start with the prefix, the full
 *                    key — matching Go strings.TrimPrefix).
 *   source "value":  the whole value; string columns take it as a
 *                    string, bytes columns as raw bytes; any other
 *                    column type is QISA_ERR_TYPE.
 *   source "json:<field>": the value parsed as a JSON object.
 *     - If the value is not a JSON object, or is malformed JSON
 *       (including trailing data after the object), every JSON
 *       column of the row is NULL and no error is raised (this
 *       matches Go: a failed Unmarshal leaves the field map nil).
 *     - A missing field, or a field whose JSON value is null,
 *       yields NULL.
 *     - A present field of the wrong type is QISA_ERR_TYPE (the
 *       query fails; a type violation is never hidden as NULL):
 *       string columns need a JSON string; bool columns need
 *       true/false; int columns need a JSON number whose value
 *       is integral (30.0 and 1e3 count as integral, as in Go);
 *       float columns need a JSON number; bytes columns need a
 *       JSON string (taken as raw bytes).
 *     - Duplicate field names: the LAST occurrence wins (Go map
 *       semantics).
 *
 * ------------------------------------------------------------------
 * RESULT ENCODING (host output buffer)
 * ------------------------------------------------------------------
 *   row_count       u32
 *   per row, per projected column, in order:
 *     value encoding (kind byte + payload, see above)
 * The row count is patched in after all rows are encoded (the
 * streaming path does not know the total up front). The host
 * reads row_count and then decodes exactly that many rows; any
 * length overrun while decoding is a host-side error, never a
 * partial row set. Two out cells accompany the buffer:
 *   out_len   u32    total bytes written to the output buffer
 *   scanned   u64    simulated scanned-row count (see below)
 *
 * SCANNED COUNT (what the host reports as ScannedRows, and what
 * differential tests compare): the reference engine pulls scans in
 * batches of 64 and its scanned count is the number of rows pulled
 * so far, not the table size. The C engine reproduces that count
 * directly, because its cursor pages ARE 64 rows: each page
 * pulled counts as one batch.
 *   - The right (join build) side is always fully counted, in
 *     64-row pages, before any left row is pulled — even when
 *     LIMIT is 0 (the reference engine builds the join before
 *     the limit stage exists in the pipeline).
 *   - When AGGREGATE or SORT is present, all left rows are counted
 *     (those stages consume their whole input).
 *   - Otherwise (streaming path) left rows are counted page by
 *     page (64 rows per page) and pulling STOPS as soon as the
 *     LIMIT is satisfied or the input is exhausted; pages that
 *     filter to empty still count, because they were pulled.
 *   - Exceeding max_scanned during a page pull (right build or
 *     left pull) fails with QISA_ERR_BUDGET and no rows.
 * The Go differential tests run the reference with its default
 * batch size (64), so these counts agree exactly. If the reference
 * batch size ever changes, the page size must change with it.
 *
 * ------------------------------------------------------------------
 * HOST CALLING CONVENTION (wasm exports)
 * ------------------------------------------------------------------
 * Following yowza_segdb.c: state pointers are passed by the host,
 * never held in C statics (the SegDB struct lives in Go-managed
 * wasm memory); out buffers and length/count cells are host
 * regions; all copies are manual byte loops.
 *   yowza_queryisa_shard_safe(prog, prog_len, work, work_cap) -> int
 *     Reliability stage item 04, shard-coordinator input: parses
 *     the program with the engine's own parser and classifies it
 *     for per-shard execution. Returns 1 when the program has no
 *     JOIN op (safe to run per shard and concatenate for
 *     SCAN+FILTER+PROJECT programs), 0 when it has a JOIN op
 *     (per-shard execution would drop cross-shard matches, so
 *     the host coordinator must refuse it), or a negative
 *     QISA_ERR_* code on malformed program, version mismatch,
 *     or work-buffer exhaustion. Classifies only; never scans
 *     or executes. work is host scratch for the parse bump
 *     allocator. The coordinator additionally refuses
 *     AGGREGATE, SORT, and LIMIT programs on the host side
 *     (they do not distribute by concatenation either); this
 *     export answers only the JOIN question, which is the one
 *     that must be decided from the program bytes themselves.
 *   yowza_queryisa_version() -> u64 (always QISA_VERSION)
 *   yowza_queryisa_execute(state, prog, prog_len,
 *       out, out_cap, out_len, scanned, work, work_cap) -> int
 *     Legacy wrapper: takes the current snapshot itself
 *     (segdb_snapshot_seq) and runs with unlimited reporting
 *     of memory (mem_peak/spilled discarded). Kept so old
 *     callers keep linking; new callers use _execute_at.
 *   yowza_queryisa_execute_at(state, prog, prog_len,
 *       out, out_cap, out_len, scanned, mem_peak, spilled,
 *       snapshot_seq, work, work_cap) -> int
 *     state    SegDB* (the same pointer the segdb exports take)
 *     prog     ISA program bytes (host region, read-only)
 *     prog_len program length in bytes (u32)
 *     out      result buffer (host region)
 *     out_cap  result buffer capacity in bytes (u32)
 *     out_len  u32* cell: bytes written, valid only on success
 *     scanned  u64* cell: scanned-row count (see above)
 *     mem_peak u64* cell: largest per-operator retained
 *              memory in bytes, Go accounting (see UNIFIED
 *              EXECUTION CONTRACT); set on success AND on
 *              error (peak so far), 0 when no retaining
 *              operator ran
 *     spilled  u32* cell: 1 when any retaining operator
 *              spilled during this execution, else 0; set
 *              on success AND on error (flag so far)
 *     snapshot_seq u64: the ONE query snapshot; every table
 *              is read at this literal sequence. The host
 *              takes it once at query start with
 *              yowza_segdb_snapshot_seq. (Passing 0 reads
 *              at literal 0, i.e. no rows — never pass 0
 *              for "current"; take a snapshot first.)
 *     work     scratch region owned by this call (host region)
 *     work_cap scratch size in bytes (u64); the first 1 MiB holds
 *              the two cursor page buffers (512 KiB each);
 *              the next 4 MiB is the per-page transient region
 *              (reset after each page); the rest is a bump
 *              allocator for retained rows, groups, hash
 *              tables, spill records, and string pools.
 *              Below 1 MiB + 4 MiB + 64 KiB the call fails
 *              with QISA_ERR_OOM.
 *   Return: 0 on success; otherwise one of the QISA_ERR_* codes.
 *   On QISA_ERR_OUT_SMALL the host may grow `out` and retry: the
 *   call is deterministic and read-only, so retrying is safe.
 *   C int is 32-bit on both wasm targets; the host reads the
 *   return as int32 (see query/isa.go).
 *
 * CAPS (fail cleanly with QISA_ERR_OOM, never truncate silently):
 *   - Cursor scans cap segment sources at 128 per page call
 *     (see src/segdb.c); the ISA engine's page buffers are
 *     512 KiB per side, which covers a 64-row page of
 *     worst-case rows (64 x 2055 = 131,520 bytes).
 *   - At most 4096 columns per table is not a real constraint; the
 *     practical caps are: pipeline width <= 64 columns, JOIN
 *     build rows <= 262144, SORT stored rows <= 262144,
 *     groups <= 65536, expression depth <= 64, LIKE
 *     pattern/string code points bounded by their byte
 *     lengths. The STREAMING path has no ISA row cap: its
 *     capacity is the cursor scan's (128 segments) and the
 *     host output buffer's. Ordinary queries are orders of
 *     magnitude below all of these; see DEEP_ISA_NOTES.md
 *     and RELIABILITY_PARITY_PERF_NOTES.md for the honest list.
 *
 * ------------------------------------------------------------------
 * GATE 4 ADDENDUM (ISA Resource Parity + Distributed Query)
 * ------------------------------------------------------------------
 *  RESOURCE POLICY (§5b): the fixed caps above are now the
 *    DEFAULTS of QisaPolicy (see below). yowza_queryisa_run
 *    keeps them; yowza_queryisa_run_policy and the wasm
 *    export yowza_queryisa_execute_policy take explicit
 *    caps. A cap exceeded by a retaining operator returns
 *    QISA_ERR_LIMIT with no rows (previously QISA_ERR_OOM
 *    for the same cases — the Go host maps both, and
 *    QISA_ERR_LIMIT additionally maps to the typed
 *    ErrResourceLimit). Width is configurable downward
 *    only (transient arrays are sized for 64 columns);
 *    join/sort/groups caps may be raised, bounded in
 *    practice by work-buffer exhaustion (QISA_ERR_OOM).
 *    The Go coordinator (query/isa_multishard.go) treats
 *    these caps as chunk caps in spill mode and as hard
 *    errors in hard-cap mode; see GATE4_NOTES.md.
 *  NUMERIC EXACTNESS (§5a): int/int and int/float
 *    comparisons are now EXACT (no float64 routing of the
 *    int), and join keys canonicalize exactly (integral
 *    floats in int64 range share the int key, so int 1
 *    still matches float 1.0). SUM int accumulation is
 *    checked: overflow returns QISA_ERR_OVERFLOW, never a
 *    silent wrap (the Go executor still wraps — that
 *    divergence is documented in GATE4_NOTES.md, not
 *    hidden). JSON numbers: Go converts via
 *    correctly-rounded float64 (int-exact to 2^53), while
 *    this engine's decimal parser accumulates 15
 *    significant digits exactly and folds the rest into
 *    the exponent — so beyond 15 significant digits the
 *    two engines can differ (observed: 9007199254740993
 *    -> Go 9007199254740992, C 9007199254740990).
 *    Query literals and SUM results are exact to int64
 *    in both engines. A correctly-rounded C decimal
 *    parser is the remaining §5a work.
 *  HOST SPILL (§4.1): this engine's spill remains a
 *    LOGICAL spill inside the host work buffer — it has
 *    no filesystem and no host file-spill imports, by
 *    design (adding imports would break hosts that
 *    instantiate the module with an empty env module).
 *    Disk-backed host spill lives in the Go host
 *    (query/hostspill.go) and is used by the Go
 *    multi-shard coordinator for its global operators.
 *    A monolithic C execution whose retained+spilled
 *    data exceeds the work buffer still fails with
 *    QISA_ERR_OOM; the host fallback
 *    (ExecuteISAWithHostSpill) then re-executes through
 *    the coordinator. See GATE4_NOTES.md for the exact
 *    boundary.
 *  MULTI-SHARD (§4.2) and CONSISTENCY (§4.3) live in Go
 *    (query/isa_multishard.go, query/consistency.go):
 *    global JOIN (broadcast-build or spill-backed
 *    chunked build with ordered merge), global
 *    AGGREGATE (merged groups, never per-shard counts),
 *    global SORT (sorted-run merge), LIMIT after global
 *    semantics, partition contracts with reject-or-dedup,
 *    and consistency domains/sessions/tokens. The old
 *    shard_safe classification export is unchanged.
 */
#ifndef YOWZA_QUERY_ISA_H
#define YOWZA_QUERY_ISA_H

#include <stdint.h>

#define QISA_MAGIC_0 'Y'
#define QISA_MAGIC_1 'W'
#define QISA_MAGIC_2 'Q'
#define QISA_MAGIC_3 'I'
#define QISA_VERSION 2u

/* Return / error codes (negative = error, as with the VM codes). */
#define QISA_OK 0
#define QISA_ERR_MALFORMED (-1)
#define QISA_ERR_VERSION (-2)
#define QISA_ERR_TYPE (-3)
#define QISA_ERR_BUDGET (-4)
#define QISA_ERR_OOM (-5)
#define QISA_ERR_OUT_SMALL (-6)
#define QISA_ERR_SCAN (-7)
#define QISA_ERR_OVERFLOW (-8) /* exact int64 overflow (SUM), Gate 4 */
#define QISA_ERR_LIMIT (-9)    /* resource-policy cap exceeded, Gate 4 */

/* Value kinds (== query.Kind codes). */
#define QISA_KIND_NULL 0u
#define QISA_KIND_INT 1u
#define QISA_KIND_FLOAT 2u
#define QISA_KIND_STRING 3u
#define QISA_KIND_BOOL 4u
#define QISA_KIND_BYTES 5u

/* Opcodes. */
#define QISA_OP_SCAN 0x01u
#define QISA_OP_JOIN 0x02u
#define QISA_OP_FILTER 0x03u
#define QISA_OP_AGGREGATE 0x04u
#define QISA_OP_PROJECT 0x05u
#define QISA_OP_SORT 0x06u
#define QISA_OP_LIMIT 0x07u
#define QISA_OP_END 0xFFu

/* Expression tags. */
#define QISA_EXPR_AND 0x10u
#define QISA_EXPR_OR 0x11u
#define QISA_EXPR_NOT 0x12u
#define QISA_EXPR_CMP 0x13u
#define QISA_EXPR_IN 0x14u
#define QISA_EXPR_LIKE 0x15u
#define QISA_EXPR_ISNULL 0x16u
#define QISA_EXPR_COL 0x17u
#define QISA_EXPR_LIT 0x18u

/* Comparison operators. */
#define QISA_CMP_EQ 0u
#define QISA_CMP_NE 1u
#define QISA_CMP_LT 2u
#define QISA_CMP_LE 3u
#define QISA_CMP_GT 4u
#define QISA_CMP_GE 5u

/* Aggregate functions. */
#define QISA_AGG_COUNT 0u
#define QISA_AGG_SUM 1u
#define QISA_AGG_AVG 2u
#define QISA_AGG_MIN 3u
#define QISA_AGG_MAX 4u

#ifdef __cplusplus
extern "C" {
#endif

/* Resource policy (Gate 4 §5b): the configurable form of the
 * old fixed caps. A zero field means the engine default for
 * that field (width 64, join/sort rows 262144, groups 65536).
 * max_width above 64 is rejected with QISA_ERR_LIMIT: the
 * engine's transient row arrays are sized for 64 columns, so
 * width is configurable downward only; join/sort/groups may
 * be raised, bounded in practice by the work buffer (bump
 * exhaustion still reports QISA_ERR_OOM). In spill mode the
 * Go coordinator treats these as chunk caps and spills the
 * rest; in hard-cap mode the C engine reports
 * QISA_ERR_LIMIT with no rows. The wasm export
 * yowza_queryisa_execute_policy takes the four fields as
 * trailing u32 arguments (0 = default). */
typedef struct {
    uint32_t max_width;
    uint32_t max_join_rows;
    uint32_t max_sort_rows;
    uint32_t max_groups;
} QisaPolicy;

#define QISA_DEFAULT_WIDTH 64u
#define QISA_DEFAULT_ROWS 262144u
#define QISA_DEFAULT_GROUPS 65536u

/* Decode and execute one ISA program against a SegDB. See the
 * header comment above for the full contract. Implemented in
 * src/query_isa.c; declared here so native harnesses can link it
 * (the wasm exports wrap this function). */
int yowza_queryisa_run(void* segdb_state, const uint8_t* prog,
                       uint32_t prog_len, uint8_t* out,
                       uint32_t out_cap, uint32_t* out_len,
                       uint64_t* scanned, uint64_t* mem_peak,
                       uint32_t* spilled, uint64_t snapshot_seq,
                       uint8_t* work, uint64_t work_cap);

/* Same as yowza_queryisa_run, under an explicit resource
 * policy (NULL = defaults). */
int yowza_queryisa_run_policy(void* segdb_state, const uint8_t* prog,
                              uint32_t prog_len, uint8_t* out,
                              uint32_t out_cap, uint32_t* out_len,
                              uint64_t* scanned, uint64_t* mem_peak,
                              uint32_t* spilled, uint64_t snapshot_seq,
                              uint8_t* work, uint64_t work_cap,
                              const QisaPolicy* policy);

/* ------------------------------------------------------------------
 * QBATCH ADDENDUM (Track A, ISA v2 batch operators)
 * ------------------------------------------------------------------
 * The monolithic ISA engine above executes a whole verified plan
 * inside wasm against SegDB. The QBATCH API below is the
 * complementary batch-at-a-time surface: the Go coordinator
 * streams row batches (from ANY Source, not only SegDB) into
 * wasm, and wasm executes one operator per call. Go stays the
 * planner/coordinator (pipeline order, scanned/result budgets,
 * LIMIT, snapshot discipline); wasm executes operator batches.
 * Nothing here re-derives plan semantics: the Go coordinator
 * computes column positions, group columns, aggregate specs, and
 * sort keys from the verified plan, and the C code executes
 * exactly what it is given, with the Track B reference
 * executor's semantics (query/executor.go).
 *
 * BATCH WIRE FORMAT (all integers little-endian):
 *   u32 nrows, then nrows * width values in ISA value encoding
 *   (kind byte + payload, see VALUE ENCODING above). A batch
 *   with width 0 or with trailing bytes after the last value
 *   is QISA_ERR_MALFORMED. The host sizes the output buffer
 *   and retries on QISA_ERR_OUT_SMALL (calls are pure and
 *   deterministic for stateless ops; stateful finish calls
 *   re-read state that the failed attempt did not mutate).
 *
 * STATE BUFFERS (join/sort/agg): the host allocates one state
 * buffer per operator instance and passes the same pointer on
 * every call for that instance. The *_init call writes the
 * header; later calls validate magic/kind/width and fail
 * with QISA_ERR_MALFORMED on mismatch. State header layout
 * (fixed offsets, read by the Go host after each call):
 *   0  u32 magic "QBST"   4  u32 kind (1 join, 2 sort, 3 agg)
 *   8  u32 width          12 u32 nrows (join build rows /
 *      sort rows; agg: distinct groups merged so far, set at
 *      finish time to the emitted group count)
 *   16 u64 budget        24 u64 retained bytes (Go accounting)
 *   32 u64 peak          40 u32 spilled (0/1)
 *   44 u32 flags (reserved, 0)
 * On ANY error the state buffer is unusable and the host must
 * discard it (re-init for a new attempt): a failed call may
 * have partially appended a batch. This is loud by design —
 * never a silently partial operator state. EXCEPTION (High
 * Capacity, see below): QISA_ERR_OOM returned by an add /
 * probe / finish call's capacity preflight leaves the state
 * byte-identical (only the diagnostic last_need field, read
 * via yowza_qbatch_state_info, changes). The host may then
 * grow the state region (relocating it with
 * yowza_qbatch_state_relocate) and retry the same batch;
 * the retry appends exactly once. Any other error, and any
 * OOM with last_need == 0 (not a capacity failure, e.g. an
 * overlong join key or a work-buffer exhaustion), still
 * follows the discard rule above.
 *
 * BUDGET AND SPILL (per stateful operator, Go accounting):
 *   rowSize(row) = 32 + sum(len(column name) + valueSize(value))
 *   over the operator's input rows (join: right rows named by
 *   their qualified names; sort: projected rows named by
 *   output names; agg: pipeline rows named alias.column for
 *   the single-batch check, groups measured as 48 + key value
 *   sizes + 24 per accumulator + min/max sizes when present).
 *   - A single batch whose bytes exceed the budget fails with
 *     QISA_ERR_BUDGET and no rows (the "budget cannot hold
 *     one batch" rule, identical in both engines).
 *   - Otherwise, when retained data would exceed the budget,
 *     the operator SPILLS (sets spilled=1) and resets its
 *     retained counter exactly as the Go spillStore /
 *     aggregate epoch does: join/sort keep the new batch as
 *     the retained set; agg flushes the epoch groups into
 *     the merged set and starts a fresh epoch. Peak is noted
 *     after the spill-and-append (join/sort) or before the
 *     spill check (agg), exactly as Go notes it.
 *   - SPILL IS LOGICAL inside the state buffer, as in the
 *     ISA v2 wasm spill: spilled rows/groups stay in the
 *     buffer (so finish still sees all of them and rows
 *     stay correct), and the reported peak/spilled flag
 *     follow Go accounting. It does NOT bound total wasm
 *     memory to the budget the way Go's disk spill bounds
 *     host memory: input whose retained + spilled data
 *     exceeds the state buffer fails with QISA_ERR_OOM.
 *     That is a documented wasm limit, not a silent parity
 *     claim. The Go coordinator reports it as a loud error.
 *   - Caps that fail cleanly, never truncate: pipeline width
 *     <= 64 columns; join build / sort rows <= 262144;
 *     distinct aggregate groups <= 65536; join keys
 *     (string/bytes) <= 2030 bytes (canonical-key buffer).
 *     Exceeding a cap returns QISA_ERR_LIMIT (groups/rows/
 *     width) or QISA_ERR_OOM (key buffer / state buffer).
 *     The Go executor has no group/row caps, so a query
 *     over the cap succeeds in Go and fails loudly here —
 *     a documented batch-API cap, never a silent cut.
 *     HIGH CAPACITY (runtime caps): the *_init2 variants
 *     take the row/group caps as host parameters (same
 *     pattern as state_cap), stored in the state header
 *     extension. The plain *_init variants keep the
 *     compile-time defaults (262144 rows / 65536 groups)
 *     exactly, so Standard behaviour is unchanged. Width
 *     stays a compile-time cap (64): it drives stack arrays
 *     and is not a host parameter.
 *
 * HIGH-CAPACITY STATE GROWTH (host-owned, wasm64 only):
 *   Operator state is NOT position-independent: the state
 *   bump holds absolute pointers (bump base, retained-row
 *   chunk pointers, string payload pointers, join hash
 *   arrays, aggregate group/key/accumulator pointers).
 *   A raw byte copy of a state region to a new address is
 *   therefore INVALID — every such pointer would dangle.
 *   Relocation is only valid through
 *   yowza_qbatch_state_relocate, which walks the state
 *   (C knows the layout on each target) and adjusts every
 *   pointer that falls inside the old region by the
 *   relocation delta, and re-derives bump.cap from the new
 *   region size. The host growth policy is: on a preflight
 *   OOM, new_cap = max(2 * old_cap, last_need) rounded up
 *   to a page, capped by the host budget; growth beyond
 *   the budget is a loud host error, never a silent cut
 *   and never an unbounded host allocation. last_need is
 *   the region cap (header size + bump bytes) the failed
 *   batch's preflight simulation requires; it is 0 when
 *   the OOM is not a capacity failure. The preflight
 *   simulations are exact for join/sort/probe (same
 *   allocation order and counts as the real append, so a
 *   passing preflight cannot OOM mid-append) and an upper
 *   bound for aggregate (MIN/MAX string copies and merge
 *   copies are counted at their maximum, plus an
 *   8-bytes-per-possible-allocation padding margin, so a
 *   passing aggregate preflight also cannot OOM
 *   mid-append; a failing one may over-estimate need by
 *   at most that margin, which growth absorbs).
 *
 * SEMANTIC NOTES (each mirrors query/executor.go):
 *   - FILTER: SQL three-valued logic via the shared
 *     qi_eval_tri; only TRUE rows pass; type errors abort.
 *   - JOIN: inner equijoin, hash build on the right side,
 *     probe left-major in build order per left row (chain
 *     order is reversed back to build order). NULL keys on
 *     either side never match. A left key whose kind is
 *     incompatible with any non-null build key kind fails
 *     with QISA_ERR_TYPE (Go's Equals errors on the same
 *     pair); numeric int/float keys compare numerically.
 *   - SORT: stable merge sort, NULLs last in either
 *     direction, comparison type error treated as equal
 *     for that key (as the reference does).
 *   - AGGREGATE: groups in first-appearance order across
 *     spill epochs (merged set, never re-emitted); ungrouped
 *     over empty input yields one row (COUNT = 0, others
 *     NULL); COUNT(col) counts non-null only; SUM is int
 *     for all-int input else float, with checked int64
 *     accumulation (QISA_ERR_OVERFLOW, where Go still
 *     wraps — pre-existing divergence, documented in
 *     GATE4_NOTES.md §5a, not introduced here).
 *   - LIMIT is NOT a batch op: the Go coordinator applies
 *     it after global semantics (after sort/agg finish on
 *     barrier paths; by early-stopping left pulls on
 *     streaming paths, exactly as the reference limitIter
 *     does), so LIMIT-after-global-semantics holds.
 *   - SNAPSHOT: batch calls receive rows, never storage
 *     pointers, so they cannot re-read "current" state,
 *     cannot see post-Open writes, and cannot express a
 *     snapshot at all — snapshot discipline lives entirely
 *     in the Go coordinator's Source Open (frozen at Open),
 *     which is stronger than a literal sequence the engine
 *     must be trusted to honour. The single-shard literal
 *     snapshot + lease discipline of ExecuteISAAtTimeline
 *     is unchanged and still covers the SegDB path.
 * WORK BUFFER: stateless calls decode one batch into it
 * (nrows * width values); sort_finish additionally holds
 * two index arrays (2 * nrows * 4 bytes); probe holds one
 * match-index array (build rows * 4 bytes). Below 64 KiB
 * the call fails with QISA_ERR_OOM. The Go host supplies
 * 4 MiB by default, which covers batch sizes up to 64
 * rows x 64 columns and sort/join states up to the
 * 262144-row cap.
 */
#define QBATCH_MAGIC 0x51425354u /* 'Q','B','S','T' */
#define QBATCH_KIND_JOIN 1u
#define QBATCH_KIND_SORT 2u
#define QBATCH_KIND_AGG 3u
#define QBATCH_MAX_WIDTH 64u
#define QBATCH_MAX_ROWS 262144u
#define QBATCH_MAX_GROUPS 65536u
#define QBATCH_OFF_MAGIC 0u
#define QBATCH_OFF_KIND 4u
#define QBATCH_OFF_WIDTH 8u
#define QBATCH_OFF_NROWS 12u
#define QBATCH_OFF_BUDGET 16u
#define QBATCH_OFF_RETAINED 24u
#define QBATCH_OFF_PEAK 32u
#define QBATCH_OFF_SPILLED 40u

/* Stateless batch operators. in/out are batch-format buffers;
 * work is call scratch (see above). expr is one ISA expression
 * tree (see EXPRESSIONS above); cols is ncols u32 column
 * positions into the input row. */
int yowza_qbatch_filter(const uint8_t* in, uint32_t in_len,
                        uint32_t width, const uint8_t* expr,
                        uint32_t expr_len, uint8_t* out,
                        uint32_t out_cap, uint32_t* out_len,
                        uint8_t* work, uint64_t work_cap);
int yowza_qbatch_project(const uint8_t* in, uint32_t in_len,
                         uint32_t width, const uint32_t* cols,
                         uint32_t ncols, uint8_t* out,
                         uint32_t out_cap, uint32_t* out_len);

/* Stateful operators. state/state_cap is the host-provided
 * operator state buffer (see above). name_lens is width
 * u32 column-name lengths for budget accounting, or 0 when
 * the call needs no accounting (join probe). keys for
 * sort_finish are nkeys pairs of (col u32, desc u32).
 * agg_specs for agg_init are nagg triples of
 * (code u32, star u32, col u32) with QISA_AGG_* codes. */
int yowza_qbatch_join_init(uint8_t* state, uint64_t state_cap,
                           uint32_t left_width, uint32_t left_key,
                           uint32_t right_width, uint32_t right_key,
                           uint64_t budget);
int yowza_qbatch_join_build(uint8_t* state, uint64_t state_cap,
                            const uint8_t* in, uint32_t in_len,
                            const uint32_t* name_lens);
int yowza_qbatch_join_probe(uint8_t* state, uint64_t state_cap,
                            const uint8_t* in, uint32_t in_len,
                            uint8_t* out, uint32_t out_cap,
                            uint32_t* out_len, uint8_t* work,
                            uint64_t work_cap);
int yowza_qbatch_sort_init(uint8_t* state, uint64_t state_cap,
                           uint32_t width, uint64_t budget);
int yowza_qbatch_sort_add(uint8_t* state, uint64_t state_cap,
                          const uint8_t* in, uint32_t in_len,
                          const uint32_t* name_lens);
int yowza_qbatch_sort_finish(uint8_t* state, uint64_t state_cap,
                             const uint32_t* keys, uint32_t nkeys,
                             uint8_t* out, uint32_t out_cap,
                             uint32_t* out_len, uint8_t* work,
                             uint64_t work_cap);
int yowza_qbatch_agg_init(uint8_t* state, uint64_t state_cap,
                          uint32_t width, uint32_t ngroup,
                          const uint32_t* group_cols, uint32_t nagg,
                          const uint32_t* agg_specs, uint64_t budget);
int yowza_qbatch_agg_add(uint8_t* state, uint64_t state_cap,
                         const uint8_t* in, uint32_t in_len,
                         const uint32_t* name_lens);
int yowza_qbatch_agg_finish(uint8_t* state, uint64_t state_cap,
                            uint8_t* out, uint32_t out_cap,
                            uint32_t* out_len);

/* High-capacity variants: *_init2 passes the row/group caps
 * the host chose (0 is rejected with QISA_ERR_LIMIT; the
 * plain *_init variants pass the compile-time defaults).
 * yowza_qbatch_state_relocate moves a state region copied
 * by the host from old_base (old_cap bytes) to the state
 * pointer given, adjusting every internal pointer; regions
 * that overlap are rejected. yowza_qbatch_state_info
 * reports the bump bytes currently used and the last
 * preflight need (0 when the last failure was not a
 * capacity failure, or when no failure occurred). */
int yowza_qbatch_join_init2(uint8_t* state, uint64_t state_cap,
                            uint32_t left_width, uint32_t left_key,
                            uint32_t right_width, uint32_t right_key,
                            uint64_t budget, uint32_t max_rows);
int yowza_qbatch_sort_init2(uint8_t* state, uint64_t state_cap,
                            uint32_t width, uint64_t budget,
                            uint32_t max_rows);
int yowza_qbatch_agg_init2(uint8_t* state, uint64_t state_cap,
                           uint32_t width, uint32_t ngroup,
                           const uint32_t* group_cols, uint32_t nagg,
                           const uint32_t* agg_specs, uint64_t budget,
                           uint32_t max_groups);
int yowza_qbatch_state_relocate(uint8_t* state, uint64_t state_cap,
                                uint64_t old_base, uint64_t old_cap);
int yowza_qbatch_state_info(uint8_t* state, uint64_t state_cap,
                            uint64_t* bump_used, uint64_t* last_need);

#ifdef __cplusplus
}
#endif

#endif /* YOWZA_QUERY_ISA_H */
