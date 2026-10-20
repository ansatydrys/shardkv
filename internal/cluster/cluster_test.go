package cluster

import (
	"context"
	"fmt"
	"github.com/ansatydrys/shardkv/internal/protocol"
	"github.com/ansatydrys/shardkv/internal/ring"
	"github.com/ansatydrys/shardkv/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Real subprocesses are killed without graceful shutdown to exercise WAL durability.
func TestNodeProcess(t *testing.T) {
	if os.Getenv("SHARDKV_HELPER") != "1" {
		return
	}
	c, e := Load(os.Getenv("SHARDKV_CONFIG"))
	if e != nil {
		panic(e)
	}
	id := os.Getenv("SHARDKV_ID")
	n, e := New(id, os.Getenv("SHARDKV_DATA"), c)
	if e != nil {
		panic(e)
	}
	l, e := net.Listen("tcp", c.Nodes[id])
	if e != nil {
		panic(e)
	}
	if e = n.Server().Serve(l); e != nil {
		panic(e)
	}
}

type processCluster struct {
	t           *testing.T
	c           Config
	config, dir string
	processes   map[string]*exec.Cmd
}

func newProcesses(t *testing.T) *processCluster {
	t.Helper()
	p := &processCluster{t: t, c: Config{Shards: 16, Nodes: map[string]string{}}, dir: t.TempDir(), processes: map[string]*exec.Cmd{}}
	listeners := []net.Listener{}
	for i := 1; i <= 3; i++ {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		listeners = append(listeners, l)
		p.c.Nodes[fmt.Sprintf("n%d", i)] = l.Addr().String()
	}
	for _, l := range listeners {
		l.Close()
	}
	p.config = filepath.Join(p.dir, "cluster.json")
	b := fmt.Sprintf(`{"shards":16,"nodes":{"n1":%q,"n2":%q,"n3":%q}}`, p.c.Nodes["n1"], p.c.Nodes["n2"], p.c.Nodes["n3"])
	if e := os.WriteFile(p.config, []byte(b), 0600); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		for id := range p.processes {
			p.kill(id)
		}
	})
	for id := range p.c.Nodes {
		p.start(id)
	}
	return p
}
func (p *processCluster) start(id string) {
	p.t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNodeProcess$")
	cmd.Env = append(os.Environ(), "SHARDKV_HELPER=1", "SHARDKV_CONFIG="+p.config, "SHARDKV_ID="+id, "SHARDKV_DATA="+filepath.Join(p.dir, id))
	cmd.Stderr = os.Stderr
	if e := cmd.Start(); e != nil {
		p.t.Fatal(e)
	}
	p.processes[id] = cmd
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		conn, e := Dial(p.c.Nodes[id])
		if e == nil {
			_, e = protocol.Call(ctx, conn, "Status", protocol.MessageOf(map[string]any{}))
			conn.Close()
		}
		cancel()
		if e == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.t.Fatal("node failed to start", id)
}
func (p *processCluster) kill(id string) {
	if cmd := p.processes[id]; cmd != nil {
		cmd.Process.Kill()
		cmd.Wait()
		delete(p.processes, id)
	}
}
func (p *processCluster) request(addr, method, key, value string) (*protocol.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	return Request(ctx, p.c, addr, method, key, value)
}
func TestAcknowledgedWriteSurvivesLeaderCrash(t *testing.T) {
	p := newProcesses(t)
	key := "durable"
	out, e := p.request("", "Put", key, "persisted")
	if e != nil {
		t.Fatal(e)
	}
	if out.Fields["acknowledgments"].GetNumberValue() < 2 {
		t.Fatal(out)
	}
	// Derive the leader through the same routing function used by clients.
	leader := p.leader(key)
	p.kill(leader)
	found := 0
	for id, addr := range p.c.Nodes {
		if id == leader {
			continue
		}
		out, e = p.request(addr, "Get", key, "")
		if e == nil && out.Fields["value"].GetStringValue() == "persisted" {
			found++
		}
	}
	if found < 1 {
		t.Fatal("acknowledged value absent from surviving replicas")
	}
	p.start(leader)
	out, e = p.request("", "Get", key, "")
	if e != nil || out.Fields["value"].GetStringValue() != "persisted" {
		t.Fatal("leader WAL recovery failed", e, out)
	}
}
func (p *processCluster) leader(key string) string {
	r, _ := p.c.Ring()
	return r.Owners(ring.Shard(key, p.c.Shards))[0]
}
func TestFollowerFailureQuorumCatchupAndNoMajority(t *testing.T) {
	p := newProcesses(t)
	key := "quorum"
	leader := p.leader(key)
	followers := []string{}
	for id := range p.c.Nodes {
		if id != leader {
			followers = append(followers, id)
		}
	}
	p.kill(followers[0])
	if _, e := p.request("", "Put", key, "one"); e != nil {
		t.Fatal(e)
	}
	p.start(followers[0])
	if _, e := p.request("", "Put", key, "two"); e != nil {
		t.Fatal(e)
	}
	out, e := p.request(p.c.Nodes[followers[0]], "Get", key, "")
	if e != nil || out.Fields["value"].GetStringValue() != "two" {
		t.Fatal("catchup failed", e)
	}
	p.kill(followers[0])
	p.kill(followers[1])
	if _, e := p.request("", "Put", key, "unknown"); status.Code(e) != codes.Unavailable {
		t.Fatal("write succeeded without majority", e)
	}
}
func TestConcurrentRPCAndDelete(t *testing.T) {
	p := newProcesses(t)
	var wg sync.WaitGroup
	errs := make(chan error, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key-" + strconv.Itoa(i)
			if _, e := p.request("", "Put", key, key); e != nil {
				errs <- e
				return
			}
			out, e := p.request("", "Get", key, "")
			if e != nil {
				errs <- e
			} else if out.Fields["value"].GetStringValue() != key {
				errs <- fmt.Errorf("wrong value")
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if _, e := p.request("", "Delete", "key-0", ""); e != nil {
		t.Fatal(e)
	}
	if _, e := p.request("", "Get", "key-0", ""); status.Code(e) != codes.NotFound {
		t.Fatal(e)
	}
	if _, e := p.request("", "Put", "", ""); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
}
func TestMalformedReplication(t *testing.T) {
	p := newProcesses(t)
	for id, addr := range p.c.Nodes {
		_ = id
		conn, e := Dial(addr)
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, e = protocol.Call(ctx, conn, "Replicate", protocol.MessageOf(map[string]any{"shard": "0", "leader": "wrong", "entries": []any{nil}}))
		cancel()
		conn.Close()
		if status.Code(e) != codes.FailedPrecondition {
			t.Fatal(e)
		}
		break
	}
}

func TestConfigAndManifest(t *testing.T) {
	c := Config{Shards: 16, Nodes: map[string]string{"a": "127.0.0.1:8001", "b": "127.0.0.1:8002", "c": "127.0.0.1:8003"}}
	dir := t.TempDir()
	n, e := New("a", dir, c)
	if e != nil {
		t.Fatal(e)
	}
	n.Close()
	if n, e = New("b", dir, c); e == nil {
		n.Close()
		t.Fatal("node identity change accepted")
	}
	c.Shards = 32
	if n, e = New("a", dir, c); e == nil {
		n.Close()
		t.Fatal("topology change accepted")
	}
	c.Shards = 0
	if _, e = c.Ring(); e == nil {
		t.Fatal("invalid shard count")
	}
}
func TestInputValidationAndCapacity(t *testing.T) {
	c := Config{Shards: 1, Nodes: map[string]string{"a": "127.0.0.1:8101", "b": "127.0.0.1:8102", "c": "127.0.0.1:8103"}}
	r, _ := c.Ring()
	id := r.Owners(0)[0]
	n, e := New(id, t.TempDir(), c)
	if e != nil {
		t.Fatal(e)
	}
	defer n.Close()
	for _, m := range []map[string]any{{"key": "k", "value": 12}, {"key": 12, "value": "v"}, {"key": "k", "value": string(make([]byte, MaxValue+1))}} {
		if _, e = n.Call(context.Background(), "Put", protocol.MessageOf(m)); status.Code(e) != codes.InvalidArgument {
			t.Fatal(e)
		}
	}
	for i := 1; i <= MaxShardRecords; i++ {
		if e = n.store.Append(store.Record{Shard: 0, Seq: uint64(i), Key: "k", Value: "v"}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = n.Call(context.Background(), "Put", protocol.MessageOf(map[string]any{"key": "k", "value": "v"})); status.Code(e) != codes.ResourceExhausted {
		t.Fatal(e)
	}
}
