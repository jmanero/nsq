package main

import (
	"../nsq"
	"log"
	"net"
	"strings"
	"time"
)

type LookupProtocolV2 struct {
	nsq.Protocol
}

func init() {
	protocols[string(nsq.MagicV2)] = &LookupProtocolV2{}
}

func (p *LookupProtocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClient(conn, "V2")
	go p.heartbeatLoop(client)
	for {
		client.SetReadDeadline(time.Now().Add(lookupd.clientTimeout))
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

			err = p.Send(client, nsq.FrameTypeError, []byte(err.Error()))
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
			err = p.Send(client, nsq.FrameTypeResponse, response)
			if err != nil {
				break
			}
		}
	}

	log.Printf("CLIENT(%s): closing", client)

	conn.Close()
	close(client.ExitChan)

	CleanupClientRegistrations(client)
	return err
}

func (p *LookupProtocolV2) Send(client *Client, frameType int32, data []byte) error {
	client.Lock()
	defer client.Unlock()

	client.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := nsq.SendFramedResponse(client, frameType, data)
	if err != nil {
		return err
	}

	return err
}

func (p *LookupProtocolV2) heartbeatLoop(client *Client) {
	var err error

	timer := time.NewTimer(lookupd.clientTimeout / 2)
	for {
		select {
		case <-timer.C:
			err = p.Send(client, nsq.FrameTypeResponse, []byte("_heartbeat_"))
			if err != nil {
				goto exit
			}
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	log.Printf("PROTOCOL(V2): [%s] exiting heartbeatLoop", client)
	timer.Stop()
	if err != nil {
		log.Printf("ERROR: [%s] - %s", client, err.Error())
	}
}
