package main

import (
	"bytes"
	"encoding/json"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

func (n *NSQd) lookupLoop() {
	syncTopicChan := make(chan *nsq.LookupPeer)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: failed to get hostname - %s", err.Error())
	}

	for _, host := range n.lookupdTCPAddrs {
		log.Printf("LOOKUP: adding peer %s", host)
		lookupPeer := nsq.NewLookupPeer(host, func(lp *nsq.LookupPeer) error {
			ci := make(map[string]interface{})
			ci["version"] = util.BINARY_VERSION
			ci["tcp_port"] = n.tcpAddr.Port
			ci["http_port"] = n.httpAddr.Port
			ci["address"] = hostname //TODO: drop for 1.0
			ci["hostname"] = hostname
			ci["broadcast_address"] = n.options.broadcastAddress

			cmd, err := nsq.Identify(ci)
			if err != nil {
				return err
			}

			frameType, resp, err := lp.Command(cmd)
			if err != nil {
				log.Printf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err.Error())
				return err
			} else if frameType == nsq.FrameTypeError {
				log.Printf("LOOKUPD(%s): returned error %s", lp, resp)
			} else {
				err = json.Unmarshal(resp, &lp.Info)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR parsing response - %v", lp, resp)
				} else {
					log.Printf("LOOKUPD(%s): peer info %+v", lp, lp.Info)
				}
			}

			go func() {
				syncTopicChan <- lp
			}()
		})
		lookupPeer.Command(nil) // start the connection
		n.lookupPeers = append(n.lookupPeers, lookupPeer)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	for {
		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range n.lookupPeers {
				log.Printf("LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, _, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
				}
			}
		case val := <-n.notifyChan:
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() == true {
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() == true {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}

			for _, lookupPeer := range n.lookupPeers {
				log.Printf("LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				frameType, resp, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
				} else if frameType == nsq.FrameTypeError {
					log.Printf("LOOKUPD(%s): returned error %s", lookupPeer, resp)
				}
			}
		case lookupPeer := <-syncTopicChan:
			commands := make([]*nsq.Command, 0)
			// build all the commands first so we exit the lock(s) as fast as possible
			nsqd.RLock()
			for _, topic := range nsqd.topicMap {
				topic.RLock()
				if len(topic.channelMap) == 0 {
					commands = append(commands, nsq.Register(topic.name, ""))
				} else {
					for _, channel := range topic.channelMap {
						commands = append(commands, nsq.Register(channel.topicName, channel.name))
					}
				}
				topic.RUnlock()
			}
			nsqd.RUnlock()

			for _, cmd := range commands {
				log.Printf("LOOKUPD(%s): %s", lookupPeer, cmd)
				frameType, resp, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
					break
				} else if frameType == nsq.FrameTypeError {
					log.Printf("LOOKUPD(%s): returned error %s", lookupPeer, resp)
				}
			}
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("LOOKUP: closing")
}

func (n *NSQd) lookupHttpAddrs() []string {
	var lookupHttpAddrs []string
	for _, lp := range n.lookupPeers {

		//TODO: remove for 1.0
		if len(lp.Info.BroadcastAddress) <= 0 {
			lp.Info.BroadcastAddress = lp.Info.Address
		}

		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HttpPort))
		lookupHttpAddrs = append(lookupHttpAddrs, addr)
	}
	return lookupHttpAddrs
}
