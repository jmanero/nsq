package nsq

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"regexp"
	"time"
)

var MagicV1 = []byte("  V1")
var MagicV2 = []byte("  V2")

// The maximum value a client can specify via RDY
const MaxReadyCount = 2500

const (
	// when successful
	FrameTypeResponse int32 = 0
	// when an error occurred
	FrameTypeError int32 = 1
	// when it's a serialized message
	FrameTypeMessage int32 = 2
)

// The amount of time nsqd will allow a client to idle, can be overriden
const DefaultClientTimeout = 60 * time.Second

var validTopicNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+$`)
var validChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName checks a topic name for correctness
func IsValidTopicName(name string) bool {
	if len(name) > 32 || len(name) < 1 {
		return false
	}
	return validTopicNameRegex.MatchString(name)
}

// IsValidChannelName checks a channel name for correctness
func IsValidChannelName(name string) bool {
	if len(name) > 32 || len(name) < 1 {
		return false
	}
	return validChannelNameRegex.MatchString(name)
}

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}

func ReadResponse(r io.Reader) ([]byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func UnpackResponse(buf []byte) (int32, []byte) {
	return int32(binary.BigEndian.Uint32(buf)), buf[4:]
}

// ReadUnpackedResponse is a client-side utility function that reads
// and unpacks serialized data according to NSQ protocol spec:
//
//    [x][x][x][x][x][x][x][x][x][x][x][x]...
//    |  (int32) ||  (int32) || (binary)
//    |  4-byte  ||  4-byte  || N-byte
//    ------------------------------------...
//        size      frame ID     data
//
// Returns a triplicate of: frame type, data ([]byte), error
func ReadUnpackedResponse(r io.Reader) (int32, []byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return -1, nil, err
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return -1, nil, err
	}

	if len(buf) < 4 {
		return -1, nil, errors.New("length of response is too small")
	}

	return int32(binary.BigEndian.Uint32(buf)), buf[4:], nil
}
