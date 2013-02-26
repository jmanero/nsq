package main

import (
	"../nsq"
	"../util"
	"../util/semver"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

func getLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allTopics := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/topics", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			// {"data":{"topics":["test"]}}
			// TODO: convert this to a StringArray() function in simplejson
			topics, _ := data.Get("topics").Array()
			allTopics = util.StringUnion(allTopics, topics)
		}(endpoint)
	}
	wg.Wait()
	sort.Strings(allTopics)
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allTopics, nil
}

func getLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allChannels := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			// {"data":{"channels":["test"]}}
			// TODO: convert this to a StringArray() function in simplejson
			channels, _ := data.Get("channels").Array()
			allChannels = util.StringUnion(allChannels, channels)
		}(endpoint)
	}
	wg.Wait()
	sort.Strings(allChannels)
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allChannels, nil
}

func getLookupdProducers(lookupdHTTPAddrs []string) ([]*Producer, error) {
	success := false
	allProducers := make(map[string]*Producer, 0)
	output := make([]*Producer, 0)
	maxVersion, _ := semver.Parse("0.0.0")
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/nodes", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)
		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true

			producers := data.Get("producers")
			producersArray, _ := producers.Array()
			for i, _ := range producersArray {
				producer := producers.GetIndex(i)
				address := producer.Get("address").MustString() //TODO: remove for 1.0
				hostname := producer.Get("hostname").MustString()
				broadcastAddress := producer.Get("broadcast_address").MustString()
				if broadcastAddress == "" {
					broadcastAddress = address
				}
				httpPort := producer.Get("http_port").MustInt()
				tcpPort := producer.Get("tcp_port").MustInt()
				key := fmt.Sprintf("%s:%d:%d", broadcastAddress, httpPort, tcpPort)
				_, ok := allProducers[key]
				if !ok {
					topicList, _ := producer.Get("topics").Array()
					var topics []string
					for _, t := range topicList {
						topics = append(topics, t.(string))
					}
					sort.Strings(topics)
					version := producer.Get("version").MustString("unknown")
					versionObj, err := semver.Parse(version)
					if err != nil {
						versionObj = maxVersion
					}
					if maxVersion.Less(versionObj) {
						maxVersion = versionObj
					}
					p := &Producer{
						Address:          address, //TODO: remove for 1.0
						Hostname:         hostname,
						BroadcastAddress: broadcastAddress,
						TcpPort:          tcpPort,
						HttpPort:         httpPort,
						Version:          version,
						VersionObj:       versionObj,
						Topics:           topics,
					}
					allProducers[key] = p
					output = append(output, p)
				}
			}
		}(endpoint)
	}
	wg.Wait()
	for _, producer := range allProducers {
		if producer.VersionObj.Less(maxVersion) {
			producer.OutOfDate = true
		}
	}
	sort.Sort(ProducersByHost{output})
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return output, nil
}

func getLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allSources := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)

		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			producers := data.Get("producers")
			producersArray, _ := producers.Array()
			for i, _ := range producersArray {
				producer := producers.GetIndex(i)
				address := producer.Get("address").MustString() //TODO: remove for 1.0
				broadcastAddress := producer.Get("broadcast_address").MustString()
				if broadcastAddress == "" {
					broadcastAddress = address
				}
				httpPort := producer.Get("http_port").MustInt()
				key := fmt.Sprintf("%s:%d", broadcastAddress, httpPort)
				allSources = util.StringAdd(allSources, key)
			}
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allSources, nil
}

func getNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	topics := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			topicList, _ := data.Get("topics").Array()
			for _, topicInfo := range topicList {
				topicInfo := topicInfo.(map[string]interface{})
				topicName := topicInfo["topic_name"].(string)
				topics = util.StringAdd(topics, topicName)
			}
		}(endpoint)
	}
	wg.Wait()
	sort.Strings(topics)
	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return topics, nil
}

func getNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) ([]string, error) {
	addresses := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			topicList, _ := data.Get("topics").Array()
			for _, topicInfo := range topicList {
				topicInfo := topicInfo.(map[string]interface{})
				topicName := topicInfo["topic_name"].(string)
				if topicName == topic {
					addresses = append(addresses, addr)
					return
				}
			}
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return addresses, nil
}

// if given no selectedTopic, this will return stats for all topic/channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func getNSQDStats(nsqdHTTPAddrs []string, selectedTopic string) ([]*TopicHostStats, map[string]*ChannelStats, error) {
	topicHostStats := make([]*TopicHostStats, 0)
	channelStats := make(map[string]*ChannelStats)
	success := false
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string, addr string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			topics, _ := data.Get("topics").Array()
			for _, topicInfo := range topics {
				topicInfo := topicInfo.(map[string]interface{})
				topicName := topicInfo["topic_name"].(string)
				if selectedTopic != "" && topicName != selectedTopic {
					continue
				}
				depth := int64(topicInfo["depth"].(float64))
				backendDepth := int64(topicInfo["backend_depth"].(float64))
				h := &TopicHostStats{
					HostAddress:  addr,
					Depth:        depth,
					BackendDepth: backendDepth,
					MemoryDepth:  depth - backendDepth,
					MessageCount: int64(topicInfo["message_count"].(float64)),
					ChannelCount: len(topicInfo["channels"].([]interface{})),
					Topic:        topicName,
				}
				topicHostStats = append(topicHostStats, h)

				channels := topicInfo["channels"].([]interface{})
				for _, c := range channels {
					c := c.(map[string]interface{})
					channelName := c["channel_name"].(string)
					channelStatsKey := channelName
					if selectedTopic == "" {
						channelStatsKey = fmt.Sprintf("%s:%s", topicName, channelName)
					}
					channel, ok := channelStats[channelStatsKey]
					if !ok {
						channel = &ChannelStats{
							ChannelName: channelName,
							Topic:       topicName,
						}
						channelStats[channelStatsKey] = channel
					}
					hcs := &ChannelStats{HostAddress: addr, ChannelName: channelName, Topic: topicName}
					depth := int64(c["depth"].(float64))
					backendDepth := int64(c["backend_depth"].(float64))
					hcs.Depth = depth
					var paused bool
					pausedInterface, ok := c["paused"]
					if ok {
						paused = pausedInterface.(bool)
					}
					hcs.Paused = paused
					hcs.BackendDepth = backendDepth
					hcs.MemoryDepth = depth - backendDepth
					hcs.InFlightCount = int64(c["in_flight_count"].(float64))
					hcs.DeferredCount = int64(c["deferred_count"].(float64))
					hcs.MessageCount = int64(c["message_count"].(float64))
					hcs.RequeueCount = int64(c["requeue_count"].(float64))
					hcs.TimeoutCount = int64(c["timeout_count"].(float64))
					clients := c["clients"].([]interface{})
					// TODO: this is sort of wrong; client's should be de-duped
					// client A that connects to NSQD-a and NSQD-b should only be counted once. right?
					hcs.ClientCount = len(clients)
					channel.AddHostStats(hcs)
					h.Channels = append(h.Channels, hcs)

					// "clients": [
					//   {
					//     "version": "V2",
					//     "remote_address": "127.0.0.1:49700",
					//     "name": "jehiah-air",
					//     "state": 3,
					//     "ready_count": 1000,
					//     "in_flight_count": 0,
					//     "message_count": 0,
					//     "finish_count": 0,
					//     "requeue_count": 0,
					//     "connect_ts": 1347150965
					//   }
					// ]
					for _, client := range clients {
						client := client.(map[string]interface{})
						connected := time.Unix(int64(client["connect_ts"].(float64)), 0)
						connectedDuration := time.Now().Sub(connected).Seconds()
						clientInfo := &ClientInfo{
							HostAddress:       addr,
							ClientVersion:     client["version"].(string),
							ClientIdentifier:  fmt.Sprintf("%s:%s", client["name"].(string), strings.Split(client["remote_address"].(string), ":")[1]),
							ConnectedDuration: time.Duration(int64(connectedDuration)) * time.Second, // truncate to second
							InFlightCount:     int(client["in_flight_count"].(float64)),
							ReadyCount:        int(client["ready_count"].(float64)),
							FinishCount:       int64(client["finish_count"].(float64)),
							RequeueCount:      int64(client["requeue_count"].(float64)),
							MessageCount:      int64(client["message_count"].(float64)),
						}
						channel.Clients = append(channel.Clients, clientInfo)
					}
					sort.Sort(ClientsByHost{channel.Clients})
				}
			}
			sort.Sort(TopicHostStatsByHost{topicHostStats})
		}(endpoint, addr)
	}
	wg.Wait()
	if success == false {
		return nil, nil, errors.New("unable to query any lookupd")
	}
	return topicHostStats, channelStats, nil
}
