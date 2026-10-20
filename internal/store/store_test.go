package store

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRecoveryAndDelete(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range []Record{{Shard: 0, Seq: 1, Key: "a", Value: "x"}, {Shard: 0, Seq: 2, Key: "a", Delete: true}, {Shard: 1, Seq: 1, Key: "b", Value: "y"}} {
		if e = s.Append(r); e != nil {
			t.Fatal(e)
		}
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, ok := s.Get(0, "a"); ok {
		t.Fatal("deleted key restored")
	}
	if v, _ := s.Get(1, "b"); v != "y" {
		t.Fatal(v)
	}
}
func TestTornTail(t *testing.T) {
	for _, tail := range [][]byte{{0, 0, 0}, {0, 0, 0, 10, 0, 0, 0, 0, 'x'}} {
		dir := t.TempDir()
		s, e := Open(dir)
		if e != nil {
			t.Fatal(e)
		}
		if e = s.Append(Record{Seq: 1, Key: "a", Value: "safe"}); e != nil {
			t.Fatal(e)
		}
		s.Close()
		f, e := os.OpenFile(filepath.Join(dir, "data.wal"), os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		f.Write(tail)
		f.Close()
		s, e = Open(dir)
		if e != nil {
			t.Fatal(e)
		}
		if v, _ := s.Get(0, "a"); v != "safe" {
			t.Fatal(v)
		}
		if e = s.Append(Record{Seq: 2, Key: "b", Value: "next"}); e != nil {
			t.Fatal(e)
		}
		s.Close()
		s, e = Open(dir)
		if e != nil {
			t.Fatal(e)
		}
		s.Close()
	}
}
func TestCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	s.Append(Record{Seq: 1, Key: "a", Value: "x"})
	s.Close()
	p := filepath.Join(dir, "data.wal")
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	b[len(b)-1] ^= 1
	os.WriteFile(p, b, 0600)
	if s, e = Open(dir); e == nil {
		s.Close()
		t.Fatal("corruption accepted")
	}
}
func TestPrefixAndDirectoryLock(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r := Record{Seq: 1, Key: "a", Value: "x"}
	if e = s.Merge(0, []Record{r}); e != nil {
		t.Fatal(e)
	}
	if e = s.Merge(0, []Record{r}); e != nil {
		t.Fatal(e)
	}
	r.Value = "other"
	if s.Merge(0, []Record{r}) == nil {
		t.Fatal("divergent prefix accepted")
	}
	if s.Append(Record{Seq: 4}) == nil {
		t.Fatal("gap accepted")
	}
	if second, e := Open(filepath.Dir(s.f.Name())); e == nil {
		second.Close()
		t.Fatal("double open allowed")
	}
}
func TestConcurrentReaders(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Get(0, "key")
				s.Log(0)
			}
		}()
	}
	for i := 1; i <= 50; i++ {
		if e = s.Append(Record{Seq: uint64(i), Key: "key", Value: "value"}); e != nil {
			t.Fatal(e)
		}
	}
	wg.Wait()
}
