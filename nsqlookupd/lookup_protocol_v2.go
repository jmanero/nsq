package main

import (
	"../nsq"
	"bufio"
	"log"
	"net"
	"strings"
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
	client.State = nsq.StateInit
	err = nil
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		response, err := Exec(client, reader, params)
		if err != nil {
			log.Printf("ERROR: CLIENT(%s) - %s", client, err.(*nsq.ClientErr).Description())
			_, err = nsq.SendFramedResponse(client, nsq.FrameTypeError, []byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			_, err = nsq.SendFramedResponse(client, nsq.FrameTypeResponse, response)
			if err != nil {
				break
			}
		}
	}

	log.Printf("CLIENT(%s): closing", client)
	if client.Producer != nil {
		lookupd.DB.RemoveProducer(Registration{"client", "", ""}, client.Producer)
		registrations := lookupd.DB.LookupRegistrations(client.Producer)
		for _, r := range registrations {
			lookupd.DB.RemoveProducer(*r, client.Producer)
		}
	}
	return err
}
