package yowza

import (
	"fmt"
	"strings"
)

// indexScanInitialCap is the INITIAL output capacity requested for
// internal index scans (reverse-lookup and exact index search). It is
// deliberately NOT a hard cap: ScanEntries grows the yowza_scan buffer
// dynamically when the result does not fit (see ScanEntries), so a scan
// larger than this value no longer fails with the old 64 KiB VM cap.
// On binaries without yowza_scan the legacy OP_SCAN path still has its
// 64 KiB static buffer, so this value is also the practical cap there.
const indexScanInitialCap uint32 = 65536

// Secondary index layout (step 3 redesign):
//
//	Primary:  {primaryKey} -> {val}
//	Index:    idx:{namespace}:{indexValue}:{primaryKey} -> {primaryKey}
//	Reverse:  idxrev:{primaryKey}:{namespace}:{indexValue} -> {idxKey}
//
// The old layout used idx:{namespace}:{indexValue} -> {primaryKey} as a
// single key, so a second primary sharing an index value overwrote the
// first primary's mapping and the first record became unfindable by
// index. With the primary key as the last segment, each (index value,
// primary key) pair gets its own entry, so duplicate index values coexist
// and are all found by a prefix scan over idx:{namespace}:{indexValue}:.
//
// The reverse mapping is what makes deletion work: deleting a primary
// record must also delete its index entries, but the index entries cannot
// be enumerated from the primary key alone (the index values are not
// stored in the primary value). The reverse entries are enumerable by
// prefix idxrev:{primaryKey}:, and each one names the index entry to
// remove. See Delete and indexKeysForPrimary.

// indexKey returns the index entry key for one (namespace, value, primary).
func indexKey(indexNamespace, indexValue, primaryKey string) string {
	return fmt.Sprintf("idx:%s:%s:%s", indexNamespace, indexValue, primaryKey)
}

// reverseIndexKey returns the reverse-mapping key for one index entry.
func reverseIndexKey(primaryKey, indexNamespace, indexValue string) string {
	return fmt.Sprintf("idxrev:%s:%s:%s", primaryKey, indexNamespace, indexValue)
}

// indexKeysForPrimary enumerates, for a primary key, the index entry keys
// and reverse-mapping keys that Delete must remove. It returns empty
// slices (not an error) if the primary has no indexes or the scan fails:
// Delete treats a failed reverse lookup as "no known indexes" and still
// deletes the primary itself, rather than failing a delete because an
// auxiliary lookup failed. The trailing ":" in the prefix keeps a primary
// "user:1" from matching reverse entries of "user:12".
func (db *YowzaDB) indexKeysForPrimary(primaryKey string) (idxKeys []string, revKeys []string) {
	prefix := fmt.Sprintf("idxrev:%s:", primaryKey)
	entries, err := db.ScanEntries(prefix, indexScanInitialCap)
	if err != nil || len(entries) == 0 {
		return nil, nil
	}
	for _, e := range entries {
		// Reverse entry value is the index entry key; its own key is
		// the reverse-mapping key to remove.
		idxKeys = append(idxKeys, string(e.Value))
		revKeys = append(revKeys, e.Key)
	}
	return idxKeys, revKeys
}

// PutWithIndex saves a primary key-value pair and also registers it against a secondary index.
// This allows fetching the primary key quickly using the index_value.
// Format natively maps to:
// Primary: {key} -> {val}
// Index: idx:{namespace}:{index_value}:{key} -> {key} (see layout comment above)
// Reverse: idxrev:{key}:{namespace}:{index_value} -> {idxKey}
//
// Phase 1: the whole operation is ONE transaction (single bytecode
// program, single lock, single WAL frame). The stale-index scan still
// happens before the transaction, so it can be stale by commit time;
// that window is closed by optimistic asserts inside the transaction:
// the primary's value and every stale reverse/forward entry it is
// about to delete are asserted against COMMITTED state at validation
// time. If a concurrent PutWithIndex changed this primary's index
// set in between, validation fails (-8/-9), nothing is applied, and
// the caller must retry. A retry is deliberately NOT automatic: an
// automatic retry would hide a genuine conflict from the caller.
// Namespace/value ':' note: the reverse prefix is built with the
// exact namespace and old values are recovered with TrimPrefix on
// that exact prefix (no Split on ':'), so ':' inside a namespace or
// a value does not break this cleanup. The joined key format itself
// is still theoretically ambiguous (ns "b:c" + value "d" collides
// with ns "b" + value "c:d"); changing the format would break
// existing keys, so it is documented, not changed.
func (db *YowzaDB) PutWithIndex(primaryKey string, val []byte, indexNamespace string, indexValue string) error {
	// Scan stale entries for this primary in the SAME namespace
	// (e.g. a user changing email). Entries with the same value are
	// left alone: they are re-put below with identical contents.
	nsRevPrefix := fmt.Sprintf("idxrev:%s:%s:", primaryKey, indexNamespace)
	type staleEntry struct {
		revKey string
		idxKey string
	}
	var stale []staleEntry
	if oldEntries, err := db.ScanEntries(nsRevPrefix, indexScanInitialCap); err == nil {
		for _, e := range oldEntries {
			oldVal := strings.TrimPrefix(e.Key, nsRevPrefix)
			if oldVal == indexValue {
				continue
			}
			stale = append(stale, staleEntry{revKey: e.Key, idxKey: string(e.Value)})
		}
	}
	// Committed primary value at scan time, for the optimistic assert.
	// Get returns (nil, nil) for "not found" and a non-nil (possibly
	// empty) slice for a found key, so nil is a reliable "absent".
	oldPrimary, err := db.Get(primaryKey)
	if err != nil {
		return err
	}

	idxKey := indexKey(indexNamespace, indexValue, primaryKey)
	revKey := reverseIndexKey(primaryKey, indexNamespace, indexValue)

	ops := make([]TxOp, 0, 2+2*len(stale)+3)
	if oldPrimary == nil {
		ops = append(ops, TxOp{Type: TxOpAssert, Key: primaryKey, AssertKind: TxAssertAbsent})
	} else {
		ops = append(ops, TxOp{Type: TxOpAssert, Key: primaryKey, Value: oldPrimary, AssertKind: TxAssertValueEquals})
	}
	for _, s := range stale {
		ops = append(ops, TxOp{Type: TxOpAssert, Key: s.revKey, Value: []byte(s.idxKey), AssertKind: TxAssertValueEquals})
		ops = append(ops, TxOp{Type: TxOpAssert, Key: s.idxKey, Value: []byte(primaryKey), AssertKind: TxAssertValueEquals})
		ops = append(ops, TxOp{Type: TxOpDelete, Key: s.idxKey})
		ops = append(ops, TxOp{Type: TxOpDelete, Key: s.revKey})
	}
	ops = append(ops, TxOp{Type: TxOpPut, Key: primaryKey, Value: val})
	ops = append(ops, TxOp{Type: TxOpPut, Key: idxKey, Value: []byte(primaryKey)})
	ops = append(ops, TxOp{Type: TxOpPut, Key: revKey, Value: []byte(idxKey)})

	return db.ExecuteTransaction(0, ops)
}

// DeleteWithIndex deletes a primary record together with all of its
// secondary index entries. It is exactly db.Delete, which performs the
// same reverse-mapping cleanup; it exists as a named entry point for
// callers that think in index terms.
func (db *YowzaDB) DeleteWithIndex(primaryKey string) error {
	return db.Delete(primaryKey)
}

// SearchByIndex performs a fast lookup using the secondary index.
// It finds the mapped primary key, and then fetches the actual data.
// If several primaries share the index value, the first one in scan order
// is returned; use ScanByIndex to get all of them.
func (db *YowzaDB) SearchByIndex(indexNamespace string, indexValue string) ([]byte, error) {
	// Prefix scan over idx:{ns}:{value}: -- the trailing ":" keeps
	// "alice@example.com" from matching "alice@example.com.evil".
	entries, err := db.ScanEntries(indexKey(indexNamespace, indexValue, ""), indexScanInitialCap)
	if err != nil {
		return nil, fmt.Errorf("index search failed: %v", err)
	}

	if len(entries) == 0 {
		return nil, nil // Not found
	}

	primaryKey := string(entries[0].Value)

	// Fetch the actual data
	return db.Get(primaryKey)
}

// ScanByIndex returns all primary values that match a partial index prefix.
func (db *YowzaDB) ScanByIndex(indexNamespace string, indexPrefix string, limitBufferBytes uint32) ([][]byte, error) {
	idxPrefix := fmt.Sprintf("idx:%s:%s", indexNamespace, indexPrefix)

	// 1. Scan the index namespace to get the mapped primary keys.
	// ScanEntries (not Scan) is used so the index keys themselves are
	// available; the primary key is the entry value either way.
	indexResults, err := db.ScanEntries(idxPrefix, limitBufferBytes)
	if err != nil {
		return nil, fmt.Errorf("index scan failed: %v", err)
	}

	if len(indexResults) == 0 {
		return nil, nil
	}

	var finalResults [][]byte

	// 2. Fetch the actual values for those primary keys
	for _, entry := range indexResults {
		pk := string(entry.Value)
		val, err := db.Get(pk)
		if err != nil {
			continue // Skip missing primaries (soft deleted)
		}
		if val != nil {
			finalResults = append(finalResults, val)
		}
	}

	return finalResults, nil
}
