// Package store provides an fsync-backed, checksummed append-only WAL.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const MaxRecord = 1 << 20

// Record sequences are contiguous per shard, starting at one.
type Record struct {
	Shard  int    `json:"shard"`
	Seq    uint64 `json:"seq"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	Delete bool   `json:"delete,omitempty"`
}
type Store struct {
	mu     sync.RWMutex
	f      *os.File
	logs   map[int][]Record
	data   map[int]map[string]string
	failed error
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "data.wal"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data directory already in use: %w", err)
	}
	s := &Store{f: f, logs: map[int][]Record{}, data: map[int]map[string]string{}}
	if err = s.recover(); err != nil {
		s.Close()
		return nil, err
	}
	// Persist the directory entry before accepting any writes.
	d, err := os.Open(dir)
	if err != nil {
		s.Close()
		return nil, err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) recover() error {
	var offset int64
	for {
		var h [8]byte
		n, err := io.ReadFull(s.f, h[:])
		if errors.Is(err, io.EOF) && n == 0 {
			break
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return s.truncate(offset)
			}
			return err
		}
		size := binary.BigEndian.Uint32(h[:4])
		if size == 0 || size > MaxRecord {
			return fmt.Errorf("invalid WAL frame at %d", offset)
		}
		body := make([]byte, size)
		_, err = io.ReadFull(s.f, body)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return s.truncate(offset)
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(h[4:]) {
			return fmt.Errorf("WAL checksum mismatch at %d", offset)
		}
		var r Record
		if err = json.Unmarshal(body, &r); err != nil {
			return err
		}
		if r.Shard < 0 || r.Seq != uint64(len(s.logs[r.Shard])+1) {
			return fmt.Errorf("invalid WAL sequence")
		}
		s.apply(r)
		offset += int64(8 + size)
	}
	_, err := s.f.Seek(0, io.SeekEnd)
	return err
}
func (s *Store) truncate(offset int64) error {
	if err := s.f.Truncate(offset); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	_, err := s.f.Seek(offset, io.SeekStart)
	return err
}
func (s *Store) apply(r Record) {
	s.logs[r.Shard] = append(s.logs[r.Shard], r)
	if s.data[r.Shard] == nil {
		s.data[r.Shard] = map[string]string{}
	}
	if r.Delete {
		delete(s.data[r.Shard], r.Key)
	} else {
		s.data[r.Shard][r.Key] = r.Value
	}
}

// Append writes and fsyncs before publishing the record in memory.
func (s *Store) Append(r Record) error { s.mu.Lock(); defer s.mu.Unlock(); return s.append(r) }
func (s *Store) append(r Record) error {
	if s.failed != nil {
		return s.failed
	}
	if r.Shard < 0 || r.Seq != uint64(len(s.logs[r.Shard])+1) {
		return fmt.Errorf("noncontiguous sequence")
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > MaxRecord {
		return fmt.Errorf("record too large")
	}
	frame := make([]byte, 8+len(b))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(b)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(b))
	copy(frame[8:], b)
	n, err := s.f.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = s.f.Sync()
	}
	if err != nil {
		s.failed = fmt.Errorf("WAL unavailable; restart required: %w", err)
		return s.failed
	}
	s.apply(r)
	return nil
}

// Merge accepts an identical prefix and fsyncs missing records. Divergence fails closed.
func (s *Store) Merge(shard int, records []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	local := s.logs[shard]
	for i, r := range records {
		if r.Shard != shard || r.Seq != uint64(i+1) {
			return fmt.Errorf("invalid replication sequence")
		}
		if i < len(local) && local[i] != r {
			return fmt.Errorf("divergent WAL prefix")
		}
	}
	for i := len(local); i < len(records); i++ {
		if err := s.append(records[i]); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) Log(shard int) []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Record(nil), s.logs[shard]...)
}
func (s *Store) Get(shard int, key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[shard][key]
	return v, ok
}
func (s *Store) Close() error { s.mu.Lock(); defer s.mu.Unlock(); return s.f.Close() }
