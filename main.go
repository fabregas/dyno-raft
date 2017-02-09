package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"

	"dyno-raft/http"
	"dyno-raft/store"
)

// Command line defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
)

// Command line parameters
var httpAddr string
var raftAddr string
var joinAddr string
var raftDir string

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&raftDir, "dir", "", "Set the Raft data path")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func formBindAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	if host == "" {
		return fmt.Sprintf("127.0.0.1:%s", port), nil
	}

	if len(host) > 2 && host[0] == '_' {
		reqIface := host[1 : len(host)-1]

		ifaces, err := net.Interfaces()
		if err != nil {
			return "", err
		}
		for _, iface := range ifaces {
			if iface.Name != reqIface {
				continue
			}
			addrs, _ := iface.Addrs()
			if len(addrs) == 0 {
				return "", errors.New(fmt.Sprintf("No IP address on interface %s", reqIface))
			}
			host := strings.Split(addrs[0].String(), "/")[0] //FIXME
			return fmt.Sprintf("%s:%s", host, port), nil
		}
		return "", errors.New(fmt.Sprintf("Interface %s does not found", reqIface))

	}

	return addr, nil
}

func main() {
	flag.Parse()

	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	minimumNodesCnt := 1
	s := store.New(minimumNodesCnt)
	s.RaftDir = raftDir
	var err error
	raftAddr, err = formBindAddr(raftAddr)
	if err != nil {
		log.Fatalf("failed to parse raft addr: %s", err.Error())
	}
	httpAddr, err = formBindAddr(httpAddr)
	if err != nil {
		log.Fatalf("failed to parse http addr: %s", err.Error())
	}
	s.RaftBind = raftAddr
	s.HttpBind = httpAddr
	if err := s.Open(joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := httpd.New(httpAddr, raftAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	if joinAddr != "" {
		if err := s.JoinToRaft(joinAddr); err != nil {
			log.Fatalf("failed to join to Raft: %s", err.Error())
		}
	}

	log.Println("hraft started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("hraftd exiting")
}
