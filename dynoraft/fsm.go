package dynoraft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type Fsm struct {
	mu sync.Mutex
	m  map[string]string // The key-value store for the system.
}

func NewFsm() *Fsm {
	return &Fsm{m: make(map[string]string)}
}

// Apply applies a Raft log entry to the key-value store.
func (f *Fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		f.mu.Lock()
		f.m[c.Key] = c.Value
		f.mu.Unlock()
		return nil
	case "delete":
		f.mu.Lock()
		delete(f.m, c.Key)
		f.mu.Unlock()
		return nil
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
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
