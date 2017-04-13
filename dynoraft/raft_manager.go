package dynoraft

import (
	"encoding/json"
	"errors"
	"fmt"
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

var (
	errNotLeader = errors.New("not leader!")
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

type RaftManager struct {
	raftDir         string
	raftBind        string
	httpBind        string
	nodeName        string
	minimumNodesCnt int

	mu  sync.Mutex
	fsm *Fsm

	raft       *raft.Raft // The consensus mechanism
	peerStore  raft.PeerStore
	nodesCache map[string]NodeInfo

	logger *log.Logger
}

// New returns a new RaftManager.
func NewRaftManager(raftDir, raftAddr, httpAddr, nodeName string, minimumNodesCnt int, logger *log.Logger) *RaftManager {
	return &RaftManager{
		raftDir:         raftDir,
		raftBind:        raftAddr,
		httpBind:        httpAddr,
		nodeName:        nodeName,
		minimumNodesCnt: minimumNodesCnt,
		logger:          logger,
	}
}

func (s *RaftManager) Open(enableSingleNode bool) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.SnapshotInterval = 60 * time.Second
	config.SnapshotThreshold = 60
	config.ShutdownOnRemove = false
	config.Logger = s.logger

	// setup nodes cache
	s.nodesCache = map[string]NodeInfo{}
	s.nodesCache[s.raftBind] = NodeInfo{s.httpBind, s.nodeName}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransportWithLogger(s.raftBind, addr, 3, 10*time.Second, s.logger)
	if err != nil {
		return err
	}

	// Create peer storage.
	s.peerStore = &raft.StaticPeers{}

	if enableSingleNode && s.minimumNodesCnt == 1 {
		s.logger.Println("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	store, err := raftboltdb.NewBoltStore(filepath.Join(s.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	s.fsm, err = NewFsm(filepath.Join(s.raftDir, "data.db"))
	if err != nil {
		return fmt.Errorf("new fsm store: %s", err)
	}
	logStoreCache, _ := raft.NewLogCache(100, store)
	ra, err := raft.NewRaft(
		config,
		s.fsm,
		logStoreCache,
		store,
		snapshots,
		s.peerStore,
		transport,
	)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	go s.syncLeader()
	go s.checkPeers()
	return nil
}

func (s *RaftManager) SetPeers(peers []string) {
	s.raft.SetPeers(peers)
}

func (s *RaftManager) checkPeers() {
	failCnt := map[string]int{}
	for {
		time.Sleep(2 * time.Second)
		peers, _ := s.peerStore.Peers()
		for _, peer := range peers {
			if peer == s.raftBind {
				// this is I am
				continue
			}
			conn, err := net.Dial("tcp", peer)
			//fmt.Println(">>> DIAL RESP", conn, err)
			if err != nil {
				failCnt[peer] += 1
				if failCnt[peer] == 3 {
					delete(failCnt, peer)
					if len(peers) <= s.minimumNodesCnt {
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

						s.mu.Lock()
						delete(s.nodesCache, peer)
						s.mu.Unlock()
						s.saveNodes()
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

func (s *RaftManager) syncLeader() {
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
		s.saveNodes() //FIXME, update nodes only if changed
	}
}

func (s *RaftManager) getNodesInfo() map[string]NodeInfo {
	v := s.fsm.GetNodes()
	if len(v) != 0 {
		nodes := make(map[string]NodeInfo)
		if err := json.Unmarshal(v, &nodes); err != nil {
			s.logger.Printf("[error] nodes info corrupted: %s", err)
		} else {
			return nodes
		}
	}
	return s.nodesCache

}

// Get returns the value for the given key.
func (s *RaftManager) Get(key string) (string, error) {
	return s.fsm.Get(key)
}

// Set sets the value for the given key.
func (s *RaftManager) Set(key, value string) error {
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
func (s *RaftManager) Delete(key string) error {
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

func (s *RaftManager) saveNodes() {
	if s.raft.State() != raft.Leader {
		// cant save nodes if not leader
		return
	}
	s.mu.Lock()
	v, err := json.Marshal(s.nodesCache)
	s.mu.Unlock()
	if err != nil {
		s.logger.Printf("[error] can't json marshal nodes info: %s", err)
		return
	}

	c := &command{
		Op:    "save_nodes",
		Value: string(v),
	}
	b, err := json.Marshal(c)
	if err != nil {
		s.logger.Printf("[error] can't json marshal save_nodes command: %s", err)
		return
	}

	f := s.raft.Apply(b, raftTimeout)
	if f.Error() != nil {
		s.logger.Printf("[DEBUG] can't store node info: %s", err)
		return
	}
}

// Joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *RaftManager) Join(addr, haddr, name string) ([]string, error) {
	s.logger.Printf("[INFO] received join request for remote node as %s", addr)
	if addr == s.raftBind {
		// self join?
		// return all known peers
		return s.peerStore.Peers()
	}

	curPeers, _ := s.peerStore.Peers()
	newPeers := raft.AddUniquePeer(curPeers, addr)
	var fut raft.Future
	if s.raft.State() == raft.Leader {
		fut = s.raft.AddPeer(addr)
	} else if s.raft.Leader() == "" {
		if len(curPeers) == 0 {
			// no one peer known, try to add this one
			fut = s.raft.SetPeers(newPeers)
		} else {
			return nil, errNotLeader
		}
	} else {
		return nil, errNotLeader
	}

	if fut.Error() != nil && fut.Error() != raft.ErrKnownPeer {
		return nil, fut.Error()
	}

	s.mu.Lock()
	s.nodesCache[addr] = NodeInfo{haddr, name}
	s.mu.Unlock()
	s.saveNodes()

	s.logger.Printf("[INFO] node at %s joined successfully", addr)
	// return all known peers
	return raft.AddUniquePeer(newPeers, s.raftBind), nil
}

func (s *RaftManager) RaftStats() map[string]string {
	return s.raft.Stats()
}

func (s *RaftManager) Leader() string {
	raftLeader := s.raft.Leader()
	if raftLeader == "" {
		return ""
	}

	for raftAddr, nodeInfo := range s.getNodesInfo() {
		if raftAddr == raftLeader {
			return nodeInfo.HttpAddr
		}
	}
	return ""
}
