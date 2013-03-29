package main

import (
	"encoding/binary"
	"github.com/bitly/nsq/nsq"
	"io"
	"log"
	"net"
	"strings"
)

type LookupProtocolV1 struct {
	nsq.Protocol
}

func init() {
	protocols[string(nsq.MagicV1)] = &LookupProtocolV1{}
}

func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClient(conn, "V1")
	for {
		line, err = client.Reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		response, err := Exec(client, params)
		if err != nil {
			context := ""
			if parentErr := err.(nsq.ChildError).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("ERROR: [%s] - %s%s", client, err.Error(), context)

			_, err = SendResponse(client, []byte(err.Error()))
			if err != nil {
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*nsq.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			_, err = SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	log.Printf("CLIENT(%s): closing", client)
	CleanupClientRegistrations(client)
	return err
}

func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 4, err
	}

	return (n + 4), nil
}
