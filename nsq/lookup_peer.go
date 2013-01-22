package nsq

import (
	"container/list"
	"log"
	"net"
	"sync"
	"time"
)

// LookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A LookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
type LookupPeer struct {
	sync.Mutex
	addr            string
	conn            net.Conn
	state           int32
	connectCallback func(*LookupPeer)
	Info            PeerInfo
	dataChan        chan []byte
	transactionChan chan *LookupTransaction
	exitChan        chan int
	transactions    *list.List
}

type LookupTransaction struct {
	doneChan  chan int
	cmd       *Command
	frameType int32
	data      []byte
}

// PeerInfo contains metadata for a LookupPeer instance (and is JSON marshalable)
type PeerInfo struct {
	TcpPort          int    `json:"tcp_port"`
	HttpPort         int    `json:"http_port"`
	Version          string `json:"version"`
	Address          string `json:"address"` //TODO: remove for 1.0
	BroadcastAddress string `json:"broadcast_address"`
}

// NewLookupPeer creates a new LookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
func NewLookupPeer(addr string, connectCallback func(*LookupPeer)) *LookupPeer {
	return &LookupPeer{
		addr:            addr,
		state:           StateDisconnected,
		connectCallback: connectCallback,
		exitChan:        make(chan int),
		dataChan:        make(chan []byte),
		transactionChan: make(chan *LookupTransaction),
	}
}

// Connect will Dial the specified address, with timeouts
func (lp *LookupPeer) Connect() error {
	log.Printf("LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	lp.Write(MagicV2)
	lp.state = StateConnected
	lp.connectCallback(lp)
	return nil
}

func (lp *LookupPeer) readLoop() {
	for {
		lp.conn.SetReadDeadline(time.Now().Add(DefaultClientTimeout))
		data, err := ReadResponse(lp)
		if err != nil {
			lp.Close()
			return
		}

		select {
		case lp.dataChan <- data:
		case <-lp.exitChan:
			return
		}
	}
}

func (lp *LookupPeer) router() {
	var err error

	for {
		select {
		case transaction := <-lp.transactionChan:
			err = transaction.cmd.Write(lp)
			if err != nil {
				lp.Close()
				goto exit
			}
			lp.transactions.PushBack(transaction)
		case buf := <-lp.dataChan:
			frameType, data := UnpackResponse(buf)

			if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
				// send NOP
				continue
			}

			el := lp.transactions.Front()
			lp.transactions.Remove(el)
			transaction := el.Value.(*LookupTransaction)
			transaction.frameType = frameType
			transaction.data = data
			transaction.doneChan <- 1
		case <-lp.exitChan:
			goto exit
		}
	}

exit:
	// flush all pending transactions
	for e := lp.transactions.Front(); e != nil; e = e.Next() {
		transaction := e.Value.(*LookupTransaction)
		transaction.frameType = -1
		transaction.data = nil
		transaction.doneChan <- 1
	}
	lp.transactions.Init()
}

// String returns the specified address
func (lp *LookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
func (lp *LookupPeer) Read(data []byte) (int, error) {
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
func (lp *LookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close implements the io.Closer interface
func (lp *LookupPeer) Close() error {
	close(lp.exitChan)
	lp.state = StateDisconnected
	return lp.conn.Close()
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd
func (lp *LookupPeer) Command(cmd *Command) (int32, []byte, error) {
	lp.Lock()
	defer lp.Unlock()

	if lp.state == StateDisconnected {
		err := lp.Connect()
		if err != nil {
			return -1, nil, err
		}
	}

	if cmd == nil {
		return -1, nil, nil
	}

	doneChan := make(chan int)
	transaction := &LookupTransaction{
		doneChan: doneChan,
		cmd:      cmd,
	}
	lp.transactionChan <- transaction
	<-doneChan

	return transaction.frameType, transaction.data, nil
}
