# Distributed Clustering

## Real-time Synchronization
While S3 is excellent for persistent snapshots, it's too slow for real-time edge synchronization. For deployments requiring sub-second state propagation across multiple edge regions, Yowza! uses Redis Pub/Sub (`cloud/cluster.go`).

When `OP_PUT` is executed, the `RedisBackend` publishes the raw Opcode buffer to a global Redis channel. All other edge nodes subscribed to this channel instantly receive the buffer and execute it locally using `OP_PUT_REMOTE`.

## Raft Consensus
For mission-critical deployments requiring strong consistency rather than eventual consistency, Yowza! integrates HashiCorp's Raft implementation (`cluster/raft.go`).

In this mode, edge nodes form a Raft cluster. One node is elected the Leader. All writes must be forwarded to the Leader, which appends the operation to the Raft log and ensures it is replicated to a quorum of followers before returning success. This guarantees no split-brain scenarios and strict linearizability for edge operations.
