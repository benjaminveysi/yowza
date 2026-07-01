// Package query is Track B (Phase 3, first slice): YowzaQL, the typed
// query IR, the batch execution engine, the Query Contract verifier,
// and the unified trace.
//
// Pipeline (nothing is executed from raw text):
//
//	YowzaQL text -> Parse (AST) -> Bind against Schema (typed Plan/IR)
//	  -> Verify against Contract -> Execute as batch pipeline
//	  -> rows + unified Trace
//
// The engine reads only through the Source interface (source.go).
// Today that means MemorySource (tests/reference) and YowzaDBSource
// (a marked, temporary adapter over the public ScanEntries API that
// freezes one scan at Open). The real layered read path needs the
// SegDBSnapshotSource interface defined in source.go, which Track A
// has not yet exposed in Go; until then, segment-layer tombstones
// are not visible to queries, and join snapshots are per-source
// rather than one shared sequence. Both limits are stated in
// TRACKB_NOTES.md, not hidden.
//
// First-slice scope (full statement in TRACKB_NOTES.md): SELECT with
// combined WHERE conditions (AND/OR/NOT, comparisons, IN, LIKE,
// IS NULL), projection, ORDER BY, LIMIT, GROUP BY with COUNT/SUM/AVG/
// MIN/MAX, and inner equijoin. Subqueries, CTEs, window functions,
// HAVING, UNION, and all DML are rejected, not approximated.
package query
