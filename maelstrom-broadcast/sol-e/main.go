package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var logger *log.Logger

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("/Users/burnerlee/Projects/random/gossip-gloomers/maelstrom-broadcast/sol-e/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	logger.SetFormatter(&log.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})
	logger.SetOutput(logFile)
	logger.SetLevel(log.DebugLevel)
}

func sendMessagesToDestination(messages []messageInfo, desitnation string, node *maelstrom.Node) {
	for {
		messageValues := make([]float64, len(messages))
		for i, message := range messages {
			messageValues[i] = message.message
		}
		body := map[string]any{
			"msg_id":   rand.Int63n(1000000000000000000),
			"type":     "share",
			"messages": messageValues,
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
		defer cancel()
		if _, err := node.SyncRPC(ctx, desitnation, body); err != nil {
			logger.Errorf("Failed to send messages to %s: %s", desitnation, err)
			continue
		}
		logger.Infof("Messages %v sent to neighbour %s from %s", messageValues, desitnation, node.ID())
		break
	}
}

func sendMessageWithRetry(source, destination string, message, msg_id float64) {
	messageQueue <- messageInfo{
		message:     message,
		msg_id:      msg_id,
		destination: destination,
		sender:      source,
	}
	logger.Infof("sent message to the queue: %v", message)
}

func main() {
	store := messagesStorage{
		messages: make(map[float64]bool),
		lock:     sync.Mutex{},
	}

	initLogger()

	n := maelstrom.NewNode()

	n.Handle("topology", func(msg maelstrom.Message) error {

		body := map[string]any{
			"type": "topology_ok",
		}
		n.Reply(msg, body)
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"].(float64)

		logger.Infof("Received message from maelstrom %f on node %s", message, n.ID())

		// harcoding this here, but this should be generalised
		centerNode := "n12"

		msg_id := body["msg_id"].(float64)
		go sendMessageWithRetry(n.ID(), centerNode, message, msg_id)

		body["type"] = "broadcast_ok"
		delete(body, "message")
		n.Reply(msg, body)
		return nil
	})

	n.Handle("share", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messagesReceived := body["messages"].([]interface{})

		logger.Infof("Received messages from network %v on node %s", messagesReceived, n.ID())

		for _, message := range messagesReceived {
			message := message.(float64)
			alreadyExists := store.checkMessage(message)
			if alreadyExists {
				logger.Infof("Message %f already exists", message)
				body["type"] = "share_ok"
				delete(body, "message")
				n.Reply(msg, body)
				return nil
			}

			logger.Infof("saving message %f on node %s", message, n.ID())
			store.addMessage(message)

			logger.Infof("Broadcasting message %f to neighbours %s from node %s", message, getTopologyNeighbours(n.ID()), n.ID())

			for _, neighbour := range getTopologyNeighbours(n.ID()) {
				msg_id := body["msg_id"].(float64)
				go sendMessageWithRetry(n.ID(), neighbour, message, msg_id)
			}
		}

		body["type"] = "share_ok"
		delete(body, "message")

		n.Reply(msg, body)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		messages := store.getMessages()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages

		n.Reply(msg, body)
		return nil
	})

	go manageQueue()
	go broadcastFromBuffer(n)

	if err := n.Run(); err != nil {
		panic(err)
	}

}
