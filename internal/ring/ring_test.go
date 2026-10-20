package ring

import (
	"reflect"
	"testing"
)

func TestDeterministicDistinctOwners(t *testing.T) {
	a, e := New([]string{"c", "a", "b", "d"})
	if e != nil {
		t.Fatal(e)
	}
	b, _ := New([]string{"d", "b", "a", "c"})
	for s := 0; s < 256; s++ {
		owners := a.Owners(s)
		if !reflect.DeepEqual(owners, b.Owners(s)) {
			t.Fatal("nondeterministic")
		}
		if len(owners) != 3 || owners[0] == owners[1] || owners[1] == owners[2] || owners[0] == owners[2] {
			t.Fatal(owners)
		}
	}
}
func TestMembershipMovement(t *testing.T) {
	a, _ := New([]string{"a", "b", "c"})
	b, _ := New([]string{"a", "b", "c", "d"})
	moved := 0
	for s := 0; s < 1000; s++ {
		before, after := a.Owners(s)[0], b.Owners(s)[0]
		if before != after {
			moved++
			if after != "d" {
				t.Fatal("existing ownership moved to unrelated node")
			}
		}
	}
	if moved == 0 || moved == 1000 {
		t.Fatal(moved)
	}
}
func TestInvalidNodes(t *testing.T) {
	for _, nodes := range [][]string{{"a", "b"}, {"a", "a", "b"}, {"", "b", "c"}} {
		if _, e := New(nodes); e == nil {
			t.Fatal(nodes)
		}
	}
}
