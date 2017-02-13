// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type NodeInfo struct {
	HttpAddr string
	Name     string
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir         string
	RaftBind        string
	HttpBind        string
	NodeName        string
	MinimumNodesCnt int

	mu sync.Mutex
	m  map[string]string // The key-value store for the system.

	raft       *raft.Raft // The consensus mechanism
	peerStore  raft.PeerStore
	nodesCache map[string]NodeInfo

	logger *log.Logger
}

// New returns a new Store.
func New(raftDir, raftAddr, httpAddr, nodeName string, minimumNodesCnt int) *Store {
	return &Store{
		RaftDir:         raftDir,
		RaftBind:        raftAddr,
		HttpBind:        httpAddr,
		NodeName:        nodeName,
		MinimumNodesCnt: minimumNodesCnt,
		m:               make(map[string]string),
		logger:          log.New(NewLogWriter(1), fmt.Sprintf("[%s] ", nodeName), log.LstdFlags),
	}
}

func (s *Store) Open(enableSingleNode bool) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.SnapshotInterval = 60 * time.Second
	config.SnapshotThreshold = 60
	config.ShutdownOnRemove = false
	config.Logger = s.logger

	// setup nodes cache
	s.nodesCache = map[string]NodeInfo{}
	s.nodesCache[s.RaftBind] = NodeInfo{s.HttpBind, s.NodeName}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransportWithLogger(s.RaftBind, addr, 3, 10*time.Second, s.logger)
	if err != nil {
		return err
	}

	// Create peer storage.
	s.peerStore = &raft.StaticPeers{}

	if enableSingleNode && s.MinimumNodesCnt == 1 {
		s.logger.Println("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, logStore, snapshots, s.peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	go s.syncLeader()
	go s.checkPeers()
	return nil
}

func (s *Store) SetPeers(peers []string) {
	s.raft.SetPeers(peers)
}

func (s *Store) checkPeers() {
	failCnt := map[string]int{}
	for {
		time.Sleep(2 * time.Second)
		peers, _ := s.peerStore.Peers()
		//fmt.Println(">>>>>> PEERS: ", peers)
		for _, peer := range peers {
			if peer == s.RaftBind {
				continue
			}
			conn, err := net.Dial("tcp", peer)
			//fmt.Println(">>> DIAL RESP", conn, err)
			if err != nil {
				failCnt[peer] += 1
				if failCnt[peer] == 3 {
					delete(failCnt, peer)
					if len(peers) <= s.MinimumNodesCnt {
						s.logger.Printf("[WARN] Peer %s does not respond! But we can't remove it...!\n", peer)
						continue
					}

					s.logger.Printf("[WARN] Peer %s does not respond! Removing it from cluster\n", peer)
					var fut raft.Future
					if s.raft.State() == raft.Leader {
						fut = s.raft.RemovePeer(peer)
					} else {
						curPeers, _ := s.peerStore.Peers()
						fut = s.raft.SetPeers(raft.ExcludePeer(curPeers, peer))
					}
					if fut.Error() == nil {
						s.logger.Printf("[WARN] Peer %s is removed!", peer)
					} else {
						s.logger.Printf("[ERROR] Peer %s is not removed: %s", peer, fut.Error())
					}
				}
			} else {
				failCnt[peer] = 0
				conn.Close()
			}
		}
	}
}

func (s *Store) syncLeader() {
	for leader := range s.raft.LeaderCh() {
		if !leader {
			continue
		}

		// get stored nodes info and update it by current nodes cache
		nodes := s.getNodesInfo()
		s.mu.Lock()
		for k, v := range s.nodesCache {
			nodes[k] = v
		}
		s.nodesCache = nodes
		s.mu.Unlock()
		s.saveNodes()
	}
}

func (s *Store) getNodesInfo() map[string]NodeInfo {
	v, err := s.Get("__internal_nodes__")
	if err != nil {
		s.logger.Printf("[DEBUG] get nodes info error: %s", err)
		return s.nodesCache
	}
	if v != "" {
		nodes := make(map[string]NodeInfo)
		if err := json.Unmarshal([]byte(v), &nodes); err != nil {
			s.logger.Printf("[error] nodes info corrupted: %s", err)
		} else {
			return nodes
		}
	}
	return s.nodesCache

}

// Get returns the value for the given key.
func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) saveNodes() {
	s.mu.Lock()
	v, err := json.Marshal(s.nodesCache)
	s.mu.Unlock()
	if err != nil {
		s.logger.Printf("[error] can't json marshal nodes info: %s", err)
		return
	}
	if err := s.Set("__internal_nodes__", string(v)); err != nil {
		s.logger.Printf("[DEBUG] can't store node info: %s", err)
		return
	}
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr, haddr, name string) ([]string, error) {
	s.logger.Printf("[INFO] received join request for remote node as %s", addr)

	curPeers, _ := s.peerStore.Peers()
	newPeers := raft.AddUniquePeer(curPeers, addr)
	var fut raft.Future
	if s.raft.State() == raft.Leader {
		fut = s.raft.AddPeer(addr)
	} else if s.raft.Leader() == "" {
		fut = s.raft.SetPeers(newPeers)
	} else {
		return nil, errors.New("not leader!")
	}

	if fut.Error() != nil {
		return nil, fut.Error()
	}

	s.mu.Lock()
	s.nodesCache[addr] = NodeInfo{haddr, name}
	s.mu.Unlock()
	s.saveNodes()

	s.logger.Printf("[INFO] node at %s joined successfully", addr)
	// return all known peers
	return raft.AddUniquePeer(newPeers, s.RaftBind), nil
}

func (s *Store) RaftStats() map[string]string {
	return s.raft.Stats()
}

func (s *Store) Leader() string {
	raftLeader := s.raft.Leader()

	for raftAddr, nodeInfo := range s.getNodesInfo() {
		if raftAddr == raftLeader {
			return nodeInfo.HttpAddr
		}
	}
	return ""
}

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
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
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
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
