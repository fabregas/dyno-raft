package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"time"
)

type Discovery struct {
	port  int64
	token string
	peer  string
}

type DiscoveryRequest struct {
	Version int
	Peer    string
	Token   string
}

type DiscoveryResponse struct {
	Version int
	Peer    string
}

func NewDiscovery(bindPort int64, token, peer string) *Discovery {
	d := Discovery{bindPort, token, peer}
	err := d.bind()
	if err != nil {
		panic(fmt.Sprintf("UDP bind error: %s", err.Error()))
	}
	return &d
}

func (d *Discovery) PeerDiscover() (string, error) {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return "", err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return "", err
	}

	raddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("255.255.255.255:%d", d.port))
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	req := DiscoveryRequest{1, d.peer, d.token}
	err = encoder.Encode(req)
	if err != nil {
		return "", err
	}

	_, err = conn.WriteToUDP(buf.Bytes(), raddr)
	if err != nil {
		return "", err
	}

	raw := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err = conn.ReadFromUDP(raw)
	if err != nil {
		return "", err
	}
	decoder := gob.NewDecoder(bytes.NewBuffer(raw))
	var resp DiscoveryResponse
	err = decoder.Decode(&resp)
	if err != nil {
		return "", err
	}

	return resp.Peer, nil
}

func (d *Discovery) bind() error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", d.port))
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	go d.udpReader(conn)
	return nil
}

func (d *Discovery) udpReader(conn *net.UDPConn) {
	resp := DiscoveryResponse{1, d.peer}
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(resp)
	if err != nil {
		panic(fmt.Sprintf("resonse encoding error: %s", err.Error()))
	}

	raw := make([]byte, 1024)
	for {
		n, raddr, err := conn.ReadFromUDP(raw)
		if err != nil {
			fmt.Printf("Error occured while read UDP socket: %s\n", err.Error())
		} else {
			decoder := gob.NewDecoder(bytes.NewBuffer(raw[:n]))
			var req DiscoveryRequest
			err = decoder.Decode(&req)
			if err != nil {
				fmt.Printf("Error occured while parse request: %s\n", err.Error())
				continue
			}

			if req.Token != d.token {
				fmt.Println("Received discovery request, but tokens are mismatch")
				continue
			}
			if req.Peer == d.peer {
				// self-request
				continue
			}

			conn.WriteToUDP(buf.Bytes(), raddr)
		}
	}
}
