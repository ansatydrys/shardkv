# shardkv

A small distributed key-value store in Go. Built to explore shard placement, durable replication, and recovery after process crashes.

- gRPC `Put`, `Get`, and `Delete`, with a command-line client
- Consistent hashing with 64 virtual nodes per physical node
- 16 logical shards by default, each assigned to three distinct replicas
- Fixed leader per shard; a successful write requires two durable acknowledgments
- Checksummed write-ahead log, `fsync` before acknowledgment, and replay on restart
- Concurrent reads with per-shard read/write locks
- Subprocess fault tests that terminate nodes with `SIGKILL`

This is a learning project, not a production database. It does **not** implement Raft, automatic leader election, or linearizable quorum reads.

## Run locally

Requires Go 1.27, Linux or macOS. No protobuf compiler is needed: the service uses the standard protobuf `Struct` message and a small hand-written gRPC binding.

```sh
go build -o bin/shardkv ./cmd/shardkv
```

Start one command in each of three terminals:

```sh
./bin/shardkv serve -id node1 -data data/node1
./bin/shardkv serve -id node2 -data data/node2
./bin/shardkv serve -id node3 -data data/node3
```

From a fourth terminal (flags must precede positional arguments):

```sh
./bin/shardkv put user:42 'Ansat'
./bin/shardkv get user:42
./bin/shardkv delete user:42
```

The client loads `cluster.local.json` and routes each key to its shard's leader. To inspect a specific replica, use `get -addr 127.0.0.1:7002 user:42`. Replica reads can be stale.

## Run with Docker

```sh
docker compose up --build -d
docker compose exec node1 /app/shardkv put -config cluster.docker.json user:42 Ansat
docker compose exec node1 /app/shardkv get -config cluster.docker.json user:42
docker compose down
```

Named volumes preserve each node's WAL across restarts. Services stay on the Compose network; no host ports are published. The gRPC service has no TLS or authentication and must stay on a trusted local network.

## Write path

1. Hash the key into a logical shard; use the hash ring to choose its three owners.
2. The fixed leader serializes writes for that shard and appends a checksummed WAL record.
3. The leader calls `fsync`, applies the record locally, then sends the shard's log prefix to its two followers in parallel.
4. Followers validate the prefix, append missing records, `fsync`, and acknowledge.
5. Return success if the leader plus at least one follower persisted the record. Each follower RPC has a 750 ms deadline; the leader waits for both results.

With a stable configuration and intact disks, losing any one node leaves at least one durable copy of every acknowledged log record. This is a durability property, not an availability guarantee: losing a shard's leader stops routed reads and writes for that shard until it restarts. Later updates or deletes naturally replace earlier values.

## Tests

```sh
go test -race -count=1 ./...
go vet ./...
```

The integration tests use real loopback gRPC connections and separate OS processes. They cover leader termination after a successful write, WAL recovery on restart, follower outage with a functioning majority, follower catch-up on the next write, refusal to acknowledge without a majority, concurrent requests, and deletes. Storage tests cover incomplete WAL tails, checksum corruption, duplicate prefixes, sequence gaps, and exclusive directory locking. Ring tests cover deterministic placement and ownership movement when adding a node.

See [architecture and tradeoffs](docs/architecture.md) and the [RPC contract](docs/protocol.md).

## Deliberate limits

- **Static topology.** No failover, membership change, or data migration. A persisted manifest rejects a changed cluster configuration.
- **Local reads.** A read sees that node's applied log, including potentially unacknowledged writes. Follower reads may lag. No linearizability or committed-only visibility is promised.
- **Unknown write outcomes.** An error or timeout does not roll back an appended record. It may become visible or replicated later; clients must reconcile ambiguous outcomes.
- **Small workloads.** Each replication sends the full shard prefix. The demo caps each shard at 256 mutations (including deletes and unsuccessful quorum attempts), keys at 1 KiB, and values at 64 KiB. No compaction, snapshots, or background repair.
- **Trusted peers.** Replication validates the claimed leader ID but does not authenticate it. Process failure tests do not simulate total power loss, disk loss, or Byzantine behavior.

Potential next steps: incremental replication, periodic repair, snapshots/compaction, generated typed protobuf messages, and consensus-based failover.
