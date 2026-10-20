# Architecture and failure model

## Placement

Keys map to a fixed number of logical shards using the first 64 bits of SHA-256. Shard IDs are placed on a sorted ring of virtual node hashes. Walking clockwise selects three distinct physical nodes; the first is the fixed leader. Sorting node IDs and breaking hash ties by ID keeps placement deterministic.

The included deployment has three physical nodes, so all data is replicated to all three, while shard leadership is distributed. A configuration with more nodes still assigns exactly three replicas per shard. Ring tests demonstrate placement movement, but online rebalancing is not implemented.

## Concurrency and durable storage

Each shard has an `RWMutex`: writes hold an exclusive lock through replication, and reads share a read lock. Shards can progress independently. A store-level mutex protects the append-only WAL and in-memory maps, so disk appends remain serialized per node.

WAL frames have an 8-byte header (big-endian payload length and CRC32) followed by a JSON record. Records carry a shard ID, contiguous per-shard sequence number, key, value, and delete flag. The file is synced before the map is changed. Newly created directory entries are synced too. The WAL is exclusively locked to prevent two processes from writing the same directory.

Recovery replays complete, valid frames in sequence. A partial final header or payload is truncated to the last complete frame. Invalid lengths, noncontiguous sequences, or checksum failures fail startup instead of silently discarding potentially acknowledged data. A write or sync error poisons the store until restart.

## Replication

Each mutation first persists at the leader, then transfers the full shard prefix to two followers concurrently. A follower rejects a divergent prefix, a sequence gap, or an unexpected target/leader ID. Missing suffix records are individually appended and synced before acknowledgment. Replaying the same prefix is idempotent.

A majority means the leader plus one follower, not a consensus commit. There is no election, epoch, commit index, rollback, or quorum read protocol. A node that missed updates catches up only when the next write for that shard arrives. The log prefix and memory footprint grow with mutations; the 256-record cap keeps this intentionally simple transport bounded.

## Failure table

| Event | Behavior |
|---|---|
| One follower unavailable | Leader may still acknowledge with the other follower |
| Both followers unavailable | No success response; local append may already exist |
| Leader killed after success | At least one follower retains the record; routed operations wait for leader restart |
| Follower restarts | Replays its WAL; catches up on the next shard write |
| Client deadline expires | Outcome unknown; inspect state before retrying |
| WAL tail incomplete | Truncate incomplete tail, replay preceding frames |
| Complete frame corrupt | Refuse startup |
| Topology differs from persisted manifest | Refuse startup; migration required |

All guarantees assume one process per data directory, identical static configurations, trusted nodes, and a filesystem that honors `fsync`. Disk destruction, power-loss behavior, malicious traffic, and network-partition consistency are outside the tested model.
