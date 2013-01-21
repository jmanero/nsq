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
			context := ""
			if parentErr := err.(nsq.ChildError).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("ERROR: [%s] - %s%s", client, err.Error(), context)

			_, err = nsq.SendFramedResponse(client, nsq.FrameTypeError, []byte(err.Error()))
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
