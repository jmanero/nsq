package main

import (
	"bytes"
	"errors"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"log"
	"sync"
	"sync/atomic"
)

type Topic struct {
	sync.RWMutex
	name               string
	channelMap         map[string]*Channel
	backend            BackendQueue
	incomingMsgChan    chan *nsq.Message
	memoryMsgChan      chan *nsq.Message
	messagePumpStarter *sync.Once
	exitChan           chan int
	channelUpdateChan  chan int
	waitGroup          util.WaitGroupWrapper
	exitFlag           int32
	messageCount       uint64
	notifier           Notifier
	options            *nsqdOptions
}

// Topic constructor
func NewTopic(topicName string, options *nsqdOptions, notifier Notifier) *Topic {
	topic := &Topic{
		name:               topicName,
		channelMap:         make(map[string]*Channel),
		backend:            NewDiskQueue(topicName, options.dataPath, options.maxBytesPerFile, options.syncEvery),
		incomingMsgChan:    make(chan *nsq.Message, 1),
		memoryMsgChan:      make(chan *nsq.Message, options.memQueueSize),
		notifier:           notifier,
		options:            options,
		exitChan:           make(chan int),
		channelUpdateChan:  make(chan int),
		messagePumpStarter: new(sync.Once),
	}

	topic.waitGroup.Wrap(func() { topic.router() })

	go notifier.Notify(topic)

	return topic
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()
	if isNew {
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}
	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.options, t.notifier, deleteCallback)
		t.channelMap[channelName] = channel
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		// start the topic message pump lazily using a `once` on the first channel creation
		t.messagePumpStarter.Do(func() { t.waitGroup.Wrap(func() { t.messagePump() }) })
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	t.Unlock()

	log.Printf("TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

// PutMessage writes to the appropriate incoming message channel
func (t *Topic) PutMessage(msg *nsq.Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	t.incomingMsgChan <- msg
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

func (t *Topic) PutMessages(messages []*nsq.Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	for _, m := range messages {
		t.incomingMsgChan <- m
		atomic.AddUint64(&t.messageCount, 1)
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
func (t *Topic) messagePump() {
	var msg *nsq.Message
	var buf []byte
	var err error
	var chans []*Channel

	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	for {
		select {
		case msg = <-t.memoryMsgChan:
		case buf = <-t.backend.ReadChan():
			msg, err = nsq.DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
		case <-t.channelUpdateChan:
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			continue
		case <-t.exitChan:
			goto exit
		}

		// check if all the channels have been deleted
		if len(chans) == 0 {
			// put this message back on the queue
			t.PutMessage(msg)
			// reset the sync.Once
			t.messagePumpStarter = new(sync.Once)
			goto exit
		}

		for _, channel := range chans {
			// copy the message because each channel
			// needs a unique instance
			chanMsg := nsq.NewMessage(msg.Id, msg.Body)
			chanMsg.Timestamp = msg.Timestamp
			err := channel.PutMessage(chanMsg)
			if err != nil {
				log.Printf("TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s", t.name, msg.Id, channel.name, err.Error())
			}
		}
	}

exit:
	log.Printf("TOPIC(%s): closing ... messagePump", t.name)
}

// router handles muxing of Topic messages including
// proxying messages to memory or backend
func (t *Topic) router() {
	var msgBuf bytes.Buffer
	for msg := range t.incomingMsgChan {
		select {
		case t.memoryMsgChan <- msg:
		default:
			err := WriteMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
				// theres not really much we can do at this point, you're certainly
				// going to lose messages...
			}
		}
	}

	log.Printf("TOPIC(%s): closing ... router", t.name)
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	err := t.exit(true)
	// since we are explicitly deleting a topic (not just at system exit time)
	// de-register this from the lookupd
	go t.notifier.Notify(t)
	return err
}

func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	log.Printf("TOPIC(%s): closing", t.name)

	// initiate exit
	atomic.StoreInt32(&t.exitFlag, 1)

	close(t.exitChan)
	t.Lock()
	close(t.incomingMsgChan)
	t.Unlock()

	// synchronize the close of router() and messagePump()
	t.waitGroup.Wait()

	if deleted {
		// empty the queue (deletes the backend files, too)
		t.Empty()

		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()
	} else {
		// close all the channels
		for _, channel := range t.channelMap {
			err := channel.Close()
			if err != nil {
				// we need to continue regardless of error to close all the channels
				log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
			}
		}

		// write anything leftover to disk
		t.flush()
	}

	return t.backend.Close()
}

func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		log.Printf("TOPIC(%s): flushing %d memory messages to backend", t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := WriteMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}
