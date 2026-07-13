package cloud

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/yowza/yowza"
)

// LocalFileBackend implements StorageBackend by saving data to local files.
// This is primarily for testing the sync architecture locally or using a simple mounted volume.
type LocalFileBackend struct {
	mu           sync.Mutex
	walPath      string
	snapshotPath string
}

func NewLocalFileBackend(walPath, snapshotPath string) *LocalFileBackend {
	return &LocalFileBackend{
		walPath:      walPath,
		snapshotPath: snapshotPath,
	}
}

func (b *LocalFileBackend) SaveWAL(entries []yowza.WalEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	f, err := os.OpenFile(b.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %v", err)
	}
	defer f.Close()

	// Using JSON for the WAL representation just to make it easily inspectable.
	// In production, protobuf or raw binary would be faster.
	encoder := json.NewEncoder(f)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("failed to encode WAL entry: %v", err)
		}
	}

	return nil
}

func (b *LocalFileBackend) SaveSnapshot(data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Write to a temporary file first, then rename for atomicity
	tmpPath := b.snapshotPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp snapshot: %v", err)
	}

	if err := os.Rename(tmpPath, b.snapshotPath); err != nil {
		return fmt.Errorf("failed to commit snapshot file: %v", err)
	}

	// We can safely truncate the WAL since a new snapshot encompasses all previous state
	_ = os.Remove(b.walPath)

	return nil
}

func (b *LocalFileBackend) LoadSnapshot() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	data, err := os.ReadFile(b.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No snapshot yet
		}
		return nil, fmt.Errorf("failed to read snapshot file: %v", err)
	}

	return data, nil
}

// LoadWAL reads the NDJSON WAL file in append order and returns its
// entries. A missing file means no persisted WAL (nil, nil); this is the
// normal state after a SaveSnapshot, which deletes the WAL file.
func (b *LocalFileBackend) LoadWAL() ([]yowza.WalEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	f, err := os.Open(b.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No WAL yet
		}
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}
	defer f.Close()

	var entries []yowza.WalEntry
	decoder := json.NewDecoder(f)
	for decoder.More() {
		var entry yowza.WalEntry
		if err := decoder.Decode(&entry); err != nil {
			return nil, fmt.Errorf("failed to decode WAL entry: %v", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
