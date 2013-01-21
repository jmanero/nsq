package main

import (
	"../nsq"
	"bufio"
	"net"
	"sync"
)

type Client struct {
	sync.RWMutex
	net.Conn
	State    int
	Version  string
	peerInfo *PeerInfo
	Reader   *bufio.Reader
	ExitChan chan int
}

func NewClient(conn net.Conn, version string) *Client {
	return &Client{
		Reader:   bufio.NewReaderSize(conn, 16*1024),
		State:    nsq.StateInit,
		Conn:     conn,
		Version:  version,
		ExitChan: make(chan int, 0),
	}
}

func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}
