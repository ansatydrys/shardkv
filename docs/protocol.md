# RPC contract

Service: `shardkv.KV` in [`api.proto`](../api.proto). Every request and response is `google.protobuf.Struct`. The wire encoding is standard protobuf over gRPC/HTTP2; no custom JSON codec is used. JSON below describes the fields inside a Struct.

| Method | Request | Success response |
|---|---|---|
| Put | `{"key":"user:42","value":"Ansat"}` | `{"sequence":"1","shard":"3","acknowledgments":3}` |
| Get | `{"key":"user:42"}` | `{"value":"Ansat","node":"node1"}` |
| Delete | `{"key":"user:42"}` | Same shape as Put |
| Status | `{}` | `{"node":"node1"}` |
| Replicate | `{"leader":"node1","shard":"3","entries":[{"seq":"1","key":"user:42","value":"Ansat","delete":false}]}` | `{"node":"node2"}` |

Example shard numbers are illustrative; compute actual routing through the ring. Sequence numbers and shard IDs are decimal strings to avoid floating-point precision problems with Struct numbers. Keys and values are UTF-8 strings; values may be empty. Deleting an absent key is permitted and still appends a tombstone.

`Get` returns `NOT_FOUND` for an absent key. Invalid input returns `INVALID_ARGUMENT`. A write sent to a follower or an invalid replication target returns `FAILED_PRECONDITION`. A missing durable majority returns `UNAVAILABLE`, which leaves an ambiguous write outcome. The demo mutation cap returns `RESOURCE_EXHAUSTED`.

The server and client allow 32 MiB RPC messages. Use this API only on a trusted local network: leader IDs are claimed metadata, not authenticated identities.
