package main

import (
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"log"
	"net"
	"os"
	"strconv"
)

func (n *NSQd) lookupLoop() {
	syncTopicChan := make(chan *nsq.LookupPeer, 1)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: failed to get hostname - %s", err.Error())
	}

	heartbeatChan := make(chan *nsq.LookupPeer, 1)
	messageChan := make(chan *nsq.Message)
	for _, host := range n.lookupdTCPAddrs {
		log.Printf("LOOKUP: adding peer %s", host)
		lookupPeer := nsq.NewLookupPeer(host, heartbeatChan, messageChan, func(lp *nsq.LookupPeer) error {
			log.Printf("LOOKUPD(%s): identifying...", lp)
			ci := make(map[string]interface{})
			ci["version"] = util.BINARY_VERSION
			ci["tcp_port"] = n.tcpAddr.Port
			ci["http_port"] = n.httpAddr.Port
			ci["address"] = hostname //TODO: drop for 1.0
			ci["hostname"] = hostname
			ci["broadcast_address"] = n.options.broadcastAddress
			err := lp.Identify(ci)
			if err != nil {
				log.Printf("LOOKUPD(%s): ERROR failed to IDENTIFY - %s", lp, err.Error())
				return err
			}
			log.Printf("LOOKUPD(%s): peer info %+v", lp, lp.Info)

			select {
			case syncTopicChan <- lp:
			default:
			}

			return nil
		})

		log.Printf("LOOKUP(%s): connecting...", lookupPeer)
		err := lookupPeer.Connect()
		if err != nil {
			log.Printf("LOOKUPD(%s): ERROR failed to connect - %s", lookupPeer, err.Error())
		}

		n.lookupPeers = append(n.lookupPeers, lookupPeer)
	}

	// for announcements, lookupd determines the host automatically
	for {
		select {
		case lp := <-heartbeatChan:
			log.Printf("LOOKUPD(%s): heartbeat", lp)
			cmd := nsq.Ping()
			_, _, err := lp.Command(cmd)
			if err != nil {
				log.Printf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err.Error())
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
				_, _, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
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
				log.Printf("LOOKUPD(%s): re-sync %s", lookupPeer, cmd)
				_, _, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
					break
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
