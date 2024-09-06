package main

import (
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type messageInfo struct {
	message     float64
	msg_id      float64
	destination string
	sender      string
}

var messageQueue = make(chan messageInfo, 1000)

var bufferMessages = make(map[string][]messageInfo)
var bufferMessageLock = &sync.Mutex{}

const (
	BROADCAST_INTERVAL = 500 * time.Millisecond
)

func manageQueue() {
	for message := range messageQueue {
		logger.Infof("recived a message from the queue: %v", message)

		// append the message received from the queue to the buffer array
		bufferMessageLock.Lock()
		if _, ok := bufferMessages[message.destination]; !ok {
			bufferMessages[message.destination] = make([]messageInfo, 0)
		}
		bufferMessages[message.destination] = append(bufferMessages[message.destination], message)
		bufferMessageLock.Unlock()
	}
}

func broadcastFromBuffer(n *maelstrom.Node) {
	// send the messages in the buffer to the neighbours
	ticker := time.NewTicker(BROADCAST_INTERVAL)

	for range ticker.C {
		// we lock the bufferMessageLock
		// we read the bufferMessages array and store it in a temporary variable
		// we reset the bufferMessages array to an empty array
		// we unlock the bufferMessageLock and let it accept new messages from the queue
		bufferMessageLock.Lock()
		messagesToSend := bufferMessages
		bufferMessages = make(map[string][]messageInfo)
		bufferMessageLock.Unlock()

		for destination, messages := range messagesToSend {
			if len(messages) > 0 {
				go sendMessagesToDestination(messages, destination, n)
			}
		}
	}
}
