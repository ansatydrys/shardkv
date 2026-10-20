// Package cluster implements static-leader, three-replica shards.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ansatydrys/shardkv/internal/protocol"
	"github.com/ansatydrys/shardkv/internal/ring"
	"github.com/ansatydrys/shardkv/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const MaxKey = 1024
const MaxValue = 64 * 1024

// Limit full-prefix replication to a bounded demo workload (no compaction yet).
const MaxShardRecords = 256
const MaxRPC = 32 << 20

type Config struct {
	Shards int               `json:"shards"`
	Nodes  map[string]string `json:"nodes"`
}

func Load(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	if err == nil {
		_, err = c.Ring()
	}
	return c, err
}
func (c Config) Ring() (*ring.Ring, error) {
	if c.Shards < 1 || c.Shards > 1024 {
		return nil, fmt.Errorf("shards must be 1..1024")
	}
	ids := []string{}
	addresses := map[string]bool{}
	for id, addr := range c.Nodes {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("invalid node address: %w", err)
		}
		if addresses[addr] {
			return nil, fmt.Errorf("duplicate node address")
		}
		addresses[addr] = true
		ids = append(ids, id)
	}
	return ring.New(ids)
}
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.3, Jitter: 0.2, MaxDelay: 250 * time.Millisecond}, MinConnectTimeout: 500 * time.Millisecond}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxRPC), grpc.MaxCallSendMsgSize(MaxRPC)))
}

type Node struct {
	ID     string
	Config Config
	ring   *ring.Ring
	store  *store.Store
	locks  []sync.RWMutex
	peers  map[string]*grpc.ClientConn
}

func New(id, dir string, c Config) (*Node, error) {
	r, err := c.Ring()
	if err != nil {
		return nil, err
	}
	if _, ok := c.Nodes[id]; !ok {
		return nil, fmt.Errorf("unknown node %q", id)
	}
	// Persist immutable topology; changing it without data migration is unsafe.
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	manifest, _ := json.Marshal(struct {
		ID      string `json:"id"`
		Cluster Config `json:"cluster"`
	}{id, c})
	manifestPath := filepath.Join(dir, "cluster.json")
	existing, readErr := os.ReadFile(manifestPath)
	if readErr == nil && !bytes.Equal(existing, manifest) {
		return nil, fmt.Errorf("topology differs from data directory manifest; migration is not implemented")
	}
	if readErr != nil && !os.IsNotExist(readErr) {
		return nil, readErr
	}
	s, err := store.Open(dir)
	if err != nil {
		return nil, err
	}
	if os.IsNotExist(readErr) {
		f, e := os.OpenFile(manifestPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			s.Close()
			return nil, e
		}
		_, e = f.Write(manifest)
		if e == nil {
			e = f.Sync()
		}
		f.Close()
		if e != nil {
			s.Close()
			return nil, e
		}
		d, e := os.Open(dir)
		if e != nil {
			s.Close()
			return nil, e
		}
		e = d.Sync()
		d.Close()
		if e != nil {
			s.Close()
			return nil, e
		}
	}
	n := &Node{ID: id, Config: c, ring: r, store: s, locks: make([]sync.RWMutex, c.Shards), peers: map[string]*grpc.ClientConn{}}
	for peer, addr := range c.Nodes {
		if peer == id {
			continue
		}
		conn, err := Dial(addr)
		if err != nil {
			n.Close()
			return nil, err
		}
		n.peers[peer] = conn
	}
	return n, nil
}
func (n *Node) Close() {
	for _, c := range n.peers {
		c.Close()
	}
	n.store.Close()
}
func (n *Node) Server() *grpc.Server {
	g := grpc.NewServer(grpc.MaxRecvMsgSize(MaxRPC), grpc.MaxSendMsgSize(MaxRPC))
	protocol.Register(g, n)
	return g
}
func text(m *protocol.Message, key string) string { return m.GetFields()[key].GetStringValue() }
func member(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}
func (n *Node) Call(ctx context.Context, method string, in *protocol.Message) (*protocol.Message, error) {
	if method == "Status" {
		return protocol.MessageOf(map[string]any{"node": n.ID}), nil
	}
	if method == "Replicate" {
		return n.replicate(in)
	}
	key := text(in, "key")
	if _, ok := in.GetFields()["key"].GetKind().(*structpb.Value_StringValue); !ok {
		return nil, status.Error(codes.InvalidArgument, "key must be a string")
	}
	if key == "" || len(key) > MaxKey {
		return nil, status.Error(codes.InvalidArgument, "key must be 1..1024 bytes")
	}
	shard := ring.Shard(key, n.Config.Shards)
	owners := n.ring.Owners(shard)
	if !member(owners, n.ID) {
		return nil, status.Error(codes.FailedPrecondition, "node is not a replica for key")
	}
	switch method {
	case "Get":
		n.locks[shard].RLock()
		defer n.locks[shard].RUnlock()
		value, ok := n.store.Get(shard, key)
		if !ok {
			return nil, status.Error(codes.NotFound, "key not found")
		}
		return protocol.MessageOf(map[string]any{"value": value, "node": n.ID}), nil
	case "Put", "Delete":
		if owners[0] != n.ID {
			return nil, status.Errorf(codes.FailedPrecondition, "fixed leader is %s", owners[0])
		}
		value := text(in, "value")
		if method == "Put" {
			if _, ok := in.GetFields()["value"].GetKind().(*structpb.Value_StringValue); !ok {
				return nil, status.Error(codes.InvalidArgument, "value must be a string")
			}
		}
		if len(value) > MaxValue {
			return nil, status.Error(codes.InvalidArgument, "value exceeds 64 KiB")
		}
		n.locks[shard].Lock()
		defer n.locks[shard].Unlock()
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		log := n.store.Log(shard)
		if len(log) >= MaxShardRecords {
			return nil, status.Error(codes.ResourceExhausted, "demo shard capacity reached")
		}
		rec := store.Record{Shard: shard, Seq: uint64(len(log) + 1), Key: key, Value: value, Delete: method == "Delete"}
		if err := n.store.Append(rec); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		log = append(log, rec)
		// Each follower merges the complete prefix; a missed write catches up on the next write.
		entries := make([]any, 0, len(log))
		for _, r := range log {
			entries = append(entries, map[string]any{"seq": strconv.FormatUint(r.Seq, 10), "key": r.Key, "value": r.Value, "delete": r.Delete})
		}
		request := protocol.MessageOf(map[string]any{"leader": n.ID, "shard": strconv.Itoa(shard), "entries": entries})
		results := make(chan error, 2)
		for _, id := range owners[1:] {
			conn := n.peers[id]
			go func() {
				callCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
				defer cancel()
				_, err := protocol.CallReady(callCtx, conn, "Replicate", request)
				results <- err
			}()
		}
		acks := 1
		for i := 0; i < 2; i++ {
			if <-results == nil {
				acks++
			}
		}
		if acks < 2 {
			return nil, status.Error(codes.Unavailable, "no durable majority; write outcome is unknown")
		}
		return protocol.MessageOf(map[string]any{"sequence": strconv.FormatUint(rec.Seq, 10), "shard": strconv.Itoa(shard), "acknowledgments": acks}), nil
	default:
		return nil, status.Error(codes.Unimplemented, "unknown method")
	}
}
func (n *Node) replicate(in *protocol.Message) (*protocol.Message, error) {
	shard, err := strconv.Atoi(text(in, "shard"))
	if err != nil || shard < 0 || shard >= n.Config.Shards {
		return nil, status.Error(codes.InvalidArgument, "invalid shard")
	}
	owners := n.ring.Owners(shard)
	if text(in, "leader") != owners[0] || !member(owners[1:], n.ID) {
		return nil, status.Error(codes.FailedPrecondition, "invalid replication target or leader")
	}
	list := in.GetFields()["entries"].GetListValue()
	if list == nil || len(list.Values) == 0 || len(list.Values) > MaxShardRecords {
		return nil, status.Error(codes.InvalidArgument, "invalid entries")
	}
	records := make([]store.Record, 0, len(list.Values))
	for _, item := range list.Values {
		m := item.GetStructValue()
		seq, e := strconv.ParseUint(text(m, "seq"), 10, 64)
		key := text(m, "key")
		value := text(m, "value")
		if e != nil || key == "" || len(key) > MaxKey || len(value) > MaxValue || ring.Shard(key, n.Config.Shards) != shard {
			return nil, status.Error(codes.InvalidArgument, "invalid record")
		}
		records = append(records, store.Record{Shard: shard, Seq: seq, Key: key, Value: value, Delete: m.GetFields()["delete"].GetBoolValue()})
	}
	n.locks[shard].Lock()
	defer n.locks[shard].Unlock()
	if err := n.store.Merge(shard, records); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return protocol.MessageOf(map[string]any{"node": n.ID}), nil
}

// Request routes to a key's fixed leader. An explicit address reads a local replica.
func Request(ctx context.Context, c Config, addr, method, key, value string) (*protocol.Message, error) {
	if addr == "" {
		r, err := c.Ring()
		if err != nil {
			return nil, err
		}
		addr = c.Nodes[r.Owners(ring.Shard(key, c.Shards))[0]]
	}
	conn, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return protocol.Call(ctx, conn, method, protocol.MessageOf(map[string]any{"key": key, "value": value}))
}
