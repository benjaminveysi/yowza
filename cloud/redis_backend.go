package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yowza/yowza"
	"github.com/yowza/yowza/security"
)

// RedisBackend implements StorageBackend using a Redis cluster.
// WAL is stored in a Redis Stream or List, and Snapshots are stored as string keys.
type RedisBackend struct {
	client        *redis.Client
	walKey        string
	snapshotKey   string
	encryptionKey string
	ctx           context.Context
}

func NewRedisBackend(redisURL, prefix, encryptionKey string) (*RedisBackend, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %v", err)
	}

	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %v", err)
	}

	return &RedisBackend{
		client:        client,
		walKey:        prefix + ":wal",
		snapshotKey:   prefix + ":snapshot",
		encryptionKey: encryptionKey,
		ctx:           context.Background(), // Typically would inject this per call, keeping simple
	}, nil
}

func (b *RedisBackend) SaveWAL(entries []yowza.WalEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// We append to a Redis List for WAL
	pipeline := b.client.Pipeline()
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		if b.encryptionKey != "" {
			data, err = security.EncryptData(b.encryptionKey, data)
			if err != nil {
				return fmt.Errorf("failed to encrypt WAL entry: %v", err)
			}
		}

		pipeline.RPush(b.ctx, b.walKey, data)
	}

	_, err := pipeline.Exec(b.ctx)
	return err
}

func (b *RedisBackend) SaveSnapshot(data []byte) error {
	var err error
	if b.encryptionKey != "" {
		data, err = security.EncryptData(b.encryptionKey, data)
		if err != nil {
			return fmt.Errorf("failed to encrypt snapshot: %v", err)
		}
	}

	// Set snapshot
	if err := b.client.Set(b.ctx, b.snapshotKey, data, 0).Err(); err != nil {
		return err
	}

	// Safely clear the WAL since the snapshot covers it
	return b.client.Del(b.ctx, b.walKey).Err()
}

func (b *RedisBackend) LoadSnapshot() ([]byte, error) {
	data, err := b.client.Get(b.ctx, b.snapshotKey).Bytes()
	if err == redis.Nil {
		return nil, nil // No snapshot
	}
	if err != nil {
		return nil, err
	}

	if b.encryptionKey != "" {
		data, err = security.DecryptData(b.encryptionKey, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt snapshot: %v", err)
		}
	}

	return data, nil
}

// LoadWAL reads the Redis WAL list in push order (oldest first, matching
// SaveWAL's RPush order) and decodes each entry, decrypting first when an
// encryption key is configured. A missing list returns nil, nil.
func (b *RedisBackend) LoadWAL() ([]yowza.WalEntry, error) {
	items, err := b.client.LRange(b.ctx, b.walKey, 0, -1).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var entries []yowza.WalEntry
	for _, item := range items {
		data := []byte(item)
		if b.encryptionKey != "" {
			data, err = security.DecryptData(b.encryptionKey, data)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt WAL entry: %v", err)
			}
		}
		var entry yowza.WalEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, fmt.Errorf("failed to decode WAL entry: %v", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
