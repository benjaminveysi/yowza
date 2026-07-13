package cloud

import (
	"fmt"
	"log"
	"sync"

	"github.com/yowza/yowza"
)

// Manager orchestrates syncing Yowza! state to an external storage backend.
type Manager struct {
	db      *yowza.YowzaDB
	backend StorageBackend

	// syncMu serializes SyncWAL calls: concurrent HTTP handlers must not
	// interleave Peek -> Save -> Ack sequences, or WAL batches would
	// reach the backend out of order and Ack could clear entries that
	// another in-flight sync had not saved yet.
	syncMu sync.Mutex
}

func NewManager(db *yowza.YowzaDB, backend StorageBackend) *Manager {
	return &Manager{
		db:      db,
		backend: backend,
	}
}

// replayWAL applies persisted WAL entries to the local DB without
// re-entering the local WAL for the operations that bypass it: PUT via
// PutRemote and DEL via DeleteRemote do not append to the Wasm WAL, and
// CRDT via PutCRDT is applied with its original timestamp so last-write-
// wins ordering is preserved. Every entry type here is idempotent
// (PutRemote overwrites, DeleteRemote deletes, PutCRDT keeps the newest
// timestamp), so replaying a batch twice -- e.g. because a previous
// bootstrap crashed between replay and Ack -- is safe.
func (m *Manager) replayWAL(entries []yowza.WalEntry) error {
	for _, entry := range entries {
		var err error
		switch entry.Opcode {
		case yowza.OpcodePut, yowza.OpcodePutRemote:
			err = m.db.PutRemote(string(entry.Key), entry.Value)
		case yowza.OpcodeDel, yowza.OpcodeDelRemote:
			err = m.db.DeleteRemote(string(entry.Key))
		case yowza.OpcodePutCRDT:
			err = m.db.PutCRDT(string(entry.Key), entry.Value, entry.Timestamp)
		default:
			return fmt.Errorf("bootstrap replay: unknown WAL opcode %d for key %q", entry.Opcode, string(entry.Key))
		}
		if err != nil {
			return fmt.Errorf("bootstrap replay failed for key %q (opcode %d): %v", string(entry.Key), entry.Opcode, err)
		}
	}
	return nil
}

// Bootstrap simulates a cold-start recovery. It downloads the latest snapshot
// and injects it directly into the Wasm memory, then replays the persisted
// WAL on top of it, so mutations that were synced but never compacted into
// a snapshot are not lost. The WAL is replayed even when no snapshot
// exists: a WAL-only recovery is still a recovery.
func (m *Manager) Bootstrap() error {
	log.Println("[Cloud Manager] Checking for existing snapshot...")
	data, err := m.backend.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("bootstrap failed to load snapshot: %v", err)
	}

	if len(data) == 0 {
		log.Println("[Cloud Manager] No snapshot found. Starting fresh.")
	} else {
		log.Printf("[Cloud Manager] Found snapshot (%d bytes). Restoring into Wasm...", len(data))
		if err := m.db.RestoreSnapshot(data); err != nil {
			return fmt.Errorf("bootstrap failed to restore snapshot: %v", err)
		}
	}

	log.Println("[Cloud Manager] Checking for persisted WAL to replay...")
	entries, err := m.backend.LoadWAL()
	if err != nil {
		return fmt.Errorf("bootstrap failed to load WAL: %v", err)
	}

	if len(entries) > 0 {
		log.Printf("[Cloud Manager] Replaying %d persisted WAL entries...", len(entries))
		if err := m.replayWAL(entries); err != nil {
			return err
		}
		// Clear the in-memory (Wasm) WAL after replay. PutRemote and
		// DeleteRemote did not append to it, but PutCRDT did, so without
		// this the replayed CRDT records would be published again by the
		// next SyncWAL. The backend WAL itself is left alone: it is
		// cleared by the next SaveSnapshot, as before.
		if err := m.db.AckWAL(); err != nil {
			return fmt.Errorf("bootstrap replayed WAL but failed to clear in-memory WAL: %v", err)
		}
		log.Printf("[Cloud Manager] Replayed %d WAL entries.", len(entries))
	}

	log.Println("[Cloud Manager] Bootstrapped successfully.")
	return nil
}

// SyncWAL reads the Wasm Shadow WAL, parses it, and pushes it to the backend.
// It is serialized by syncMu and transactional: Peek, then Save, then Ack
// only on a successful Save. A failed Save leaves the WAL intact for the
// next SyncWAL; the old FetchWAL-based version cleared on read, so a
// failed save silently lost records.
func (m *Manager) SyncWAL() error {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	entries, err := m.db.PeekWAL()
	if err != nil {
		return fmt.Errorf("failed to peek WAL: %v", err)
	}

	if len(entries) == 0 {
		return nil // Nothing to sync
	}

	if err := m.backend.SaveWAL(entries); err != nil {
		return fmt.Errorf("failed to push WAL to backend: %v", err)
	}

	if err := m.db.AckWAL(); err != nil {
		return fmt.Errorf("saved WAL to backend but failed to ack it: %v", err)
	}

	log.Printf("[Cloud Manager] Synced %d WAL entries to backend.", len(entries))

	// Automatic Compaction Trigger
	if len(entries) >= 10 { // Arbitrary small threshold for demo
		log.Println("[Cloud Manager] WAL threshold reached. Triggering cloud snapshot compaction...")
		if err := m.CompactSnapshot(); err != nil {
			log.Printf("[Cloud Manager] Warning: Cloud Snapshot compaction failed: %v", err)
		}
	}

	return nil
}

// CompactSnapshot freezes the current Wasm state, creates a compact snapshot,
// and pushes it to the backend, discarding the old WAL.
func (m *Manager) CompactSnapshot() error {
	log.Println("[Cloud Manager] Starting snapshot compaction...")

	data, err := m.db.CreateSnapshot()
	if err != nil {
		return fmt.Errorf("compaction failed to create snapshot: %v", err)
	}

	if err := m.backend.SaveSnapshot(data); err != nil {
		return fmt.Errorf("compaction failed to upload snapshot: %v", err)
	}

	log.Printf("[Cloud Manager] Compaction complete. New snapshot size: %d bytes.", len(data))
	return nil
}
