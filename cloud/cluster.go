package cloud

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/yowza/yowza"
)

// ClusterCoordinator handles Pub/Sub syncing across multiple Yowza nodes.
// When one node updates its WAL, it publishes the WalEntries.
// Other nodes subscribe to the channel, receive the WalEntries, and apply them
// directly to their local Wasm memory using standard db.Put / db.Delete.
type ClusterCoordinator struct {
	db     *yowza.YowzaDB
	client *redis.Client
	pubSub *redis.PubSub
	nodeID string
	topic  string
}

func NewClusterCoordinator(db *yowza.YowzaDB, redisURL string, nodeID string, topic string) (*ClusterCoordinator, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)

	return &ClusterCoordinator{
		db:     db,
		client: client,
		nodeID: nodeID,
		topic:  topic,
	}, nil
}

// PublishSync reads the local WAL, publishes it to the cluster, and clears
// the local WAL only after a successful publish. Transactional pattern:
// Peek -> Publish -> Ack. The old FetchWAL-based version cleared on read,
// so a failed publish silently lost records; a failed publish here leaves
// the WAL intact for the next PublishSync.
func (c *ClusterCoordinator) PublishSync() error {
	entries, err := c.db.PeekWAL()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil // No changes
	}

	payload := struct {
		NodeID  string           `json:"node_id"`
		Entries []yowza.WalEntry `json:"entries"`
	}{
		NodeID:  c.nodeID,
		Entries: entries,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := c.client.Publish(ctx, c.topic, data).Err(); err != nil {
		return err
	}

	return c.db.AckWAL()
}

// ListenAndApply starts a blocking goroutine that listens for other nodes' WAL updates
// and applies them locally to ensure eventual consistency without heavy Paxos/Raft overhead.
func (c *ClusterCoordinator) ListenAndApply(ctx context.Context) {
	c.pubSub = c.client.Subscribe(ctx, c.topic)
	ch := c.pubSub.Channel()

	log.Printf("[Cluster %s] Listening for remote WAL syncs on %s...", c.nodeID, c.topic)

	for msg := range ch {
		var payload struct {
			NodeID  string           `json:"node_id"`
			Entries []yowza.WalEntry `json:"entries"`
		}

		if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
			log.Printf("[Cluster %s] Failed to parse cluster msg: %v", c.nodeID, err)
			continue
		}

		// Don't apply our own broadcasted WALs
		if payload.NodeID == c.nodeID {
			continue
		}

		// Apply the remote mutations. Remote PUT/DEL use the REMOTE
		// opcodes so they do not re-enter the local WAL; a remote CRDT
		// record keeps its original timestamp via PutCRDT.
		for _, entry := range payload.Entries {
			switch entry.Opcode {
			case yowza.OpcodePut, yowza.OpcodePutRemote:
				_ = c.db.PutRemote(string(entry.Key), entry.Value)
			case yowza.OpcodeDel, yowza.OpcodeDelRemote:
				_ = c.db.DeleteRemote(string(entry.Key))
			case yowza.OpcodePutCRDT:
				_ = c.db.PutCRDT(string(entry.Key), entry.Value, entry.Timestamp)
			}
		}

		log.Printf("[Cluster %s] Applied %d remote mutations from node %s", c.nodeID, len(payload.Entries), payload.NodeID)
	}
}

func (c *ClusterCoordinator) Close() error {
	if c.pubSub != nil {
		c.pubSub.Close()
	}
	return c.client.Close()
}
