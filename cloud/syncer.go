package cloud

import "github.com/yowza/yowza"

// StorageBackend defines the interface for an external persistent storage system
// (e.g., AWS S3, Redis, Local Disk, another remote Yowza! node).
type StorageBackend interface {
	// SaveWAL appends a batch of WAL entries to the persistent storage.
	SaveWAL(entries []yowza.WalEntry) error

	// SaveSnapshot overwrites the existing snapshot with a compacted state of the database.
	// Implementing this usually allows clearing the remote WAL logs up to this point.
	SaveSnapshot(data []byte) error

	// LoadSnapshot retrieves the latest compacted snapshot for cold-start bootstrapping.
	// Returns nil, nil if no snapshot exists.
	LoadSnapshot() ([]byte, error)

	// LoadWAL retrieves the WAL batches not yet covered by a snapshot, in
	// apply order, for cold-start bootstrapping. Returns nil, nil if there
	// is no persisted WAL. Manager.Bootstrap replays these after restoring
	// the snapshot; a backend must not delete its WAL here, only report it.
	LoadWAL() ([]yowza.WalEntry, error)
}
