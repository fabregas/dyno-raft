package dynoraft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

var (
	// Bucket names we perform transactions in
	dbKV = []byte("kv")
)

type Fsm struct {
	mu    sync.Mutex
	m     map[string]string // The key-value store for the system.
	nodes string            // raw nodes info

	conn *bolt.DB
}

func NewFsm(dbpath string) (*Fsm, error) {
	handle, err := bolt.Open(dbpath, 0600, nil)
	if err != nil {
		return nil, err
	}

	fsm := &Fsm{
		m:     make(map[string]string),
		nodes: "",
		conn:  handle}
	if err := fsm.initialize(); err != nil {
		return nil, err
	}
	return fsm, nil
}

func (f *Fsm) initialize() error {
	tx, err := f.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Create the bucket
	if _, err := tx.CreateBucketIfNotExists(dbKV); err != nil {
		return err
	}

	c := tx.Bucket(dbKV).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		f.m[string(k)] = string(v)
	}
	return tx.Commit()
}

func (f *Fsm) Close() error {
	return f.conn.Close()
}

// Apply applies a Raft log entry to the key-value store.
func (f *Fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.setKV(c.Key, c.Value)
	case "delete":
		return f.deleteKV(c.Key)
	case "save_nodes":
		f.mu.Lock()
		f.nodes = c.Value
		f.mu.Unlock()
		return nil
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

func (f *Fsm) GetNodes() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return []byte(f.nodes)
}

func (f *Fsm) setKV(key, value string) error {
	tx, err := f.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbKV)
	if err := bucket.Put([]byte(key), []byte(value)); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	f.mu.Lock()
	f.m[key] = value
	f.mu.Unlock()
	return nil
}

func (f *Fsm) deleteKV(key string) error {
	tx, err := f.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbKV)
	if err := bucket.Delete([]byte(key)); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	f.mu.Lock()
	delete(f.m, key)
	f.mu.Unlock()
	return nil
}

func (f *Fsm) Get(key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m[key], nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *Fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
