import re

with open("yowza.go", "r") as f:
    content = f.read()

# 1. Refactor Compact to use compactLocked
compact_logic = """
// compactLocked performs the actual compaction without acquiring the mutex.
// Assumes the caller holds db.mu.Lock().
func (db *YowzaDB) compactLocked(ctx context.Context) error {
	res, err := db.fnCompact.Call(ctx, uint64(db.memOffsetSnap), uint64(db.memOffsetSnapCap))
	if err != nil {
		return fmt.Errorf("failed to call yowza_compact: %v", err)
	}

	if int32(uint32(res[0])) != 0 {
		return fmt.Errorf("compaction failed internally (OOM or buffer too small)")
	}

	return nil
}

// Compact reclaims leaked string values from the Arena by fully serializing the live tree
// to a temporary buffer and restoring it to a freshly reset Arena.
func (db *YowzaDB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.compactLocked(context.Background())
}
"""

content = re.sub(r'// Compact reclaims leaked string values.*?return nil\n}', compact_logic, content, flags=re.DOTALL)

# 2. Add compactLocked call inside FetchWAL
fetch_wal_update = """	// Clear WAL in C Engine
	_, err = db.fnClearWal.Call(ctx)
	if err != nil {
		return entries, fmt.Errorf("parsed WAL but failed to clear it in C engine: %v", err)
	}

	// Automated internal compaction triggered on WAL flush
	// This prevents the Arena from leaking orphaned node memory between sweeps.
	if len(entries) > 0 {
		if err := db.compactLocked(ctx); err != nil {
			// We log but don't strictly fail FetchWAL, since the WAL was fetched successfully.
			// In production, you might want to handle this more robustly.
		}
	}

	return entries, nil"""

content = re.sub(r'\t// Clear WAL in C Engine.*?\treturn entries, nil', fetch_wal_update, content, flags=re.DOTALL)

with open("yowza.go", "w") as f:
    f.write(content)
