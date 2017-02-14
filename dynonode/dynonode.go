package dynonode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"dyno-raft/dynoraft"
)

// DynoNode provides node based on raft with dynamic join/remove
type DynoNode struct {
	addr     string
	raftAddr string
	ln       net.Listener

	raftMgr *dynoraft.RaftManager
	logger  *log.Logger
}

// New returns an uninitialized DynoNode.
func NewDynoNode(addr, raftAddr, raftDir, nodeName string, minNodes int, logger *log.Logger) *DynoNode {
	raftMgr := dynoraft.NewRaftManager(raftDir, raftAddr, addr, nodeName, minNodes, logger)

	return &DynoNode{
		addr:     addr,
		raftAddr: raftAddr,
		raftMgr:  raftMgr,
		logger:   logger,
	}
}

// Start starts the node.
func (s *DynoNode) Start(enableSingleNode bool) error {
	if err := s.raftMgr.Open(enableSingleNode); err != nil {
		return err
	}
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

// Close closes the node.
func (s *DynoNode) Close() {
	s.ln.Close()
	return
}

// join node to the raft
func (s *DynoNode) JoinToRaft(joinAddr string) error {
	// get leader of Raft ring
	leader := ""
	for {
		resp, err := http.Get(fmt.Sprintf("http://%s/leader", joinAddr))
		if err != nil {
			s.logger.Printf("[WARN] failed to get leader at %s: %s", joinAddr, err.Error())
			time.Sleep(2 * time.Second) //FIXME
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		leader = string(body)
		resp.Body.Close()
		if leader == "" {
			// no leader in cluster, trying join to known node
			leader = joinAddr
		}
		break
	}

	// send join request to leader
	b, err := json.Marshal(map[string]string{
		"addr":     s.raftAddr,
		"httpAddr": s.addr,
	})
	if err != nil {
		return err
	}
	resp, err := http.Post(
		fmt.Sprintf("http://%s/join", joinAddr),
		"application-type/json", bytes.NewReader(b),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		s.logger.Printf("[WARN] Join request failed: %s", string(body))
		return nil
	}
	// parse foreign peers list and setup it
	peers := []string{}
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return err
	}

	s.raftMgr.SetPeers(peers)
	return nil

}

// ServeHTTP allows DynoNode to serve HTTP requests.
func (s *DynoNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyRequest(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else if r.URL.Path == "/leader" {
		s.handleLeader(w, r)
	} else if r.URL.Path == "/raft-stats" {
		s.handleRaftStats(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *DynoNode) handleRaftStats(w http.ResponseWriter, r *http.Request) {
	b, err := json.Marshal(s.raftMgr.RaftStats())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func (s *DynoNode) handleLeader(w http.ResponseWriter, r *http.Request) {
	addr := s.raftMgr.Leader()
	io.WriteString(w, addr)
}

func (s *DynoNode) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	peers, err := s.raftMgr.Join(remoteAddr, m["httpAddr"], "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	b, err := json.Marshal(peers)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func (s *DynoNode) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func() string {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			return ""
		}
		return parts[2]
	}

	switch r.Method {
	case "GET":
		leader := s.raftMgr.Leader()
		if leader != "" && leader != s.addr {
			http.Redirect(w, r, fmt.Sprintf("http://%s%s", leader, r.URL.Path), 301)
			return
		}

		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		v, err := s.raftMgr.Get(k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		b, err := json.Marshal(map[string]string{k: v})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		io.WriteString(w, string(b))

	case "POST":
		leader := s.raftMgr.Leader()
		if leader == "" {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, "No leader in Raft")
			return
		}
		if leader != s.addr {
			http.Redirect(w, r, fmt.Sprintf("http://%s/key", leader), 301)
			return
		}

		// Read the value from the POST body.
		m := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for k, v := range m {
			if err := s.raftMgr.Set(k, v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, err.Error())
				return
			}
		}

	case "DELETE":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := s.raftMgr.Delete(k); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.raftMgr.Delete(k)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

// Addr returns the address on which the Service is listening
func (s *DynoNode) Addr() net.Addr {
	return s.ln.Addr()
}
