// Package ring implements deterministic consistent hashing with virtual nodes.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

type point struct {
	hash uint64
	node string
}
type Ring struct {
	points []point
	nodes  []string
}

func hash(s string) uint64 { h := sha256.Sum256([]byte(s)); return binary.BigEndian.Uint64(h[:8]) }
func New(nodes []string) (*Ring, error) {
	if len(nodes) < 3 {
		return nil, fmt.Errorf("at least three nodes required")
	}
	nodes = append([]string(nil), nodes...)
	sort.Strings(nodes)
	r := &Ring{nodes: nodes}
	for i, n := range nodes {
		if n == "" || (i > 0 && nodes[i-1] == n) {
			return nil, fmt.Errorf("node IDs must be nonempty and unique")
		}
		for v := 0; v < 64; v++ {
			r.points = append(r.points, point{hash(fmt.Sprintf("%s/%d", n, v)), n})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash == r.points[j].hash {
			return r.points[i].node < r.points[j].node
		}
		return r.points[i].hash < r.points[j].hash
	})
	return r, nil
}

// Shard maps a key to one of a fixed number of logical shards.
func Shard(key string, count int) int { return int(hash(key) % uint64(count)) }

// Owners returns three distinct physical nodes; the first is the fixed leader.
func (r *Ring) Owners(shard int) []string {
	h := hash(fmt.Sprintf("shard/%d", shard))
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	out := []string{}
	seen := map[string]bool{}
	for j := 0; len(out) < 3; j++ {
		n := r.points[(i+j)%len(r.points)].node
		if !seen[n] {
			out = append(out, n)
			seen[n] = true
		}
	}
	return out
}
