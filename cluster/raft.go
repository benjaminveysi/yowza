package cluster

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/yowza/yowza"
)

// yowzaFSM implements the raft.FSM interface to apply Raft log entries to the Yowza Wasm Engine.
type yowzaFSM struct {
	db *yowza.YowzaDB
}

// Apply is called by Raft once a log entry is committed by a majority of the cluster.
func (f *yowzaFSM) Apply(l *raft.Log) interface{} {
	var entry yowza.WalEntry
	if err := json.Unmarshal(l.Data, &entry); err != nil {
		log.Printf("[Raft FSM] Failed to unmarshal log data: %v", err)
		return err
	}

	// Apply securely to the Wasm Engine. Raft log entries for CRDT
	// operations carry their timestamp in entry.Timestamp (see
	// yowza.WalEntry); it must be preserved, so they are applied with
	// PutCRDT, not PutRemote (which would drop the timestamp and break
	// last-write-wins ordering).
	switch entry.Opcode {
	case yowza.OpcodePut, yowza.OpcodePutRemote:
		if err := f.db.PutRemote(string(entry.Key), entry.Value); err != nil {
			log.Printf("[Raft FSM] Failed to apply PUT_REMOTE: %v", err)
			return err
		}
	case yowza.OpcodeDel, yowza.OpcodeDelRemote:
		if err := f.db.DeleteRemote(string(entry.Key)); err != nil {
			log.Printf("[Raft FSM] Failed to apply DEL_REMOTE: %v", err)
			return err
		}
	case yowza.OpcodePutCRDT:
		if err := f.db.PutCRDT(string(entry.Key), entry.Value, entry.Timestamp); err != nil {
			log.Printf("[Raft FSM] Failed to apply PUT_CRDT: %v", err)
			return err
		}
	}

	// Because this operation arrived via Raft (consensus), we do NOT want
	// to re-propose it to the Raft cluster (that would be a feedback
	// loop). PUT/DEL use OP_PUT_REMOTE and OP_DEL_REMOTE, which means the
	// C engine bypassed the WAL array entirely. A CRDT record is different:
	// PutCRDT DOES append to the local WAL in the C engine, so a Raft-
	// applied CRDT entry will be picked up by the cloud SyncWAL and
	// published to the cloud backends -- its WAL is published in cloud
	// SyncWAL, not in the Raft loop. That is the intended path (cloud
	// replicas learn the CRDT write from the WAL), and it is safe because
	// PutCRDT is idempotent under its timestamp.

	return nil
}

// Snapshot is used to support log compaction.
func (f *yowzaFSM) Snapshot() (raft.FSMSnapshot, error) {
	// Instruct the Wasm engine to dump its memory tree into a compressed snapshot.
	data, err := f.db.CreateSnapshot()
	if err != nil {
		return nil, err
	}
	return &yowzaSnapshot{data: data}, nil
}

// Restore is used to restore an FSM from a snapshot.
func (f *yowzaFSM) Restore(rc io.ReadCloser) error {
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	// Inject the snapshot byte array directly into the Wasm memory, rebuilding the ART tree instantly.
	return f.db.RestoreSnapshot(data)
}

// yowzaSnapshot implements the raft.FSMSnapshot interface.
type yowzaSnapshot struct {
	data []byte
}

func (s *yowzaSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *yowzaSnapshot) Release() {}

// Helper to cast strings for HTTP handler
func ServerID(id string) raft.ServerID {
	return raft.ServerID(id)
}

func ServerAddress(addr string) raft.ServerAddress {
	return raft.ServerAddress(addr)
}

// SetupRaftNode initializes a Raft node for strong consensus, returning the Raft object.
func SetupRaftNode(nodeID, bindAddr, dataDir string, isBootstrap bool, db *yowza.YowzaDB) (*raft.Raft, error) {
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(nodeID)

	// Setup Raft communication
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create directories
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}

	// Setup Snapshots
	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Setup Log Store (BoltDB)
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}

	// Setup Stable Store (BoltDB)
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}

	// Create FSM
	fsm := &yowzaFSM{db: db}

	// Instantiate Raft system
	r, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	// Bootstrap if instructed (only needed for the very first node in a cluster)
	if isBootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      conf.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return r, nil
}
