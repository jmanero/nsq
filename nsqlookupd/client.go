package main

import (
	"bufio"
	"net"
	"sync"
)

type Client struct {
	sync.RWMutex
	net.Conn
	Version  string
	peerInfo *PeerInfo
	Reader   *bufio.Reader
	ExitChan chan int
}

func NewClient(conn net.Conn, version string) *Client {
	return &Client{
		Reader:   bufio.NewReaderSize(conn, 16*1024),
		Conn:     conn,
		Version:  version,
		ExitChan: make(chan int, 0),
	}
}

func (c *Client) String() string {
	return c.RemoteAddr().String()
}
