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
	"syscall"
	"time"

	"dyno-raft/dynonode"
)

// Command line defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
	BroadcastPort   = 13000
)

// Command line parameters
var httpAddr string
var raftAddr string
var joinAddr string
var token string
var raftDir string
var nodeName string
var minNodes int

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&raftDir, "dir", "", "Set the Raft data path")
	flag.StringVar(&joinAddr, "joinaddr", "", "Set join address, if any")
	flag.StringVar(&token, "jointoken", "", "Join Raft by UDP discovery mechamism using token")
	flag.IntVar(&minNodes, "quorum", 1, "Set the minimum nodes in raft cluster for quorum")
	flag.StringVar(&nodeName, "name", "", "Set the node name (label)")
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

	raftAddr, err := formBindAddr(raftAddr)
	if err != nil {
		log.Fatalf("failed to parse raft addr: %s", err.Error())
	}
	httpAddr, err = formBindAddr(httpAddr)
	if err != nil {
		log.Fatalf("failed to parse http addr: %s", err.Error())
	}

	logger := log.New(dynonode.NewLogWriter(0), fmt.Sprintf("[%s] ", nodeName), log.LstdFlags)

	node := dynonode.NewDynoNode(httpAddr, raftAddr, raftDir, nodeName, minNodes, logger)
	if err := node.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	if token != "" {
		discovery := NewDiscovery(BroadcastPort, token, httpAddr)
		joinAddr, err = discoveryPeer(discovery)
		if err != nil {
			logger.Println("Can't discovery peer over UDP broadcast")
		} else {
			logger.Printf("[INFO] discovered peer at %s", joinAddr)
		}
	}

	if joinAddr != "" {
		for {
			if err := node.JoinToRaft(joinAddr); err != nil {
				//log.Fatalf("failed to join Raft: %s", err.Error())
				time.Sleep(1 * time.Second) //FIXME
				continue
			}
			break
		}
	}

	logger.Println("[INFO] dyno-raft started successfully")

	terminate := make(chan os.Signal, 1)
	//signal.Notify(terminate, os.Interrupt)
	signal.Notify(terminate, syscall.SIGINT, syscall.SIGTERM)
	<-terminate
	logger.Println("[INFO] dyno-raft exiting")
}

func discoveryPeer(d *Discovery) (string, error) {
	var err error
	var peer string
	for i := 0; i < 10; i++ {
		peer, err = d.PeerDiscover()
		if err != nil {
			fmt.Println("failed peer discovery:", err.Error())
		} else {
			return peer, nil
		}
		time.Sleep(1 * time.Second)
	}
	return "", err
}
