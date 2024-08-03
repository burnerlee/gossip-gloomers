package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var logger *log.Logger

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("/Users/burnerlee/Projects/random/gossip-gloomers/maelstrom-broadcast/sol-c/maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	logger.SetOutput(logFile)
	logger.SetLevel(log.DebugLevel)
}

func sendMessageWithRetry(n *maelstrom.Node, neighbour string, message, msg_id float64) {
	body := map[string]any{
		// assign a random message id
		"msg_id":  msg_id,
		"type":    "broadcast",
		"message": message,
	}
	for {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(100*time.Millisecond))
		defer cancel()
		if _, err := n.SyncRPC(ctx, neighbour, body); err != nil {
			logger.Errorf("Failed to send message to %s: %s", neighbour, err)
			continue
		}
		return
	}
}

func main() {
	initLogger()

	store := messagesStorage{
		messages: make(map[float64]bool),
		lock:     sync.Mutex{},
	}

	neighbourStore := nodeNeighbours{
		neighbours: make(map[string]bool),
	}

	logger.Info("Starting node")

	n := maelstrom.NewNode()

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topologyInterface := body["topology"].(map[string]interface{})
		topologyForNode := topologyInterface[n.ID()].([]interface{})
		for _, neighbour := range topologyForNode {
			neighbourStore.addNeighbour(neighbour.(string))
		}

		body["type"] = "topology_ok"
		delete(body, "topology")

		n.Reply(msg, body)
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"].(float64)

		logger.Infof("Received message %f", message)
		store.addMessage(message)

		for _, neighbour := range neighbourStore.getNeighbours() {
			if neighbour == msg.Src {
				// skip sending back to the sender
				continue
			}
			msg_id := body["msg_id"].(float64)
			go sendMessageWithRetry(n, neighbour, message, msg_id)
		}

		body["type"] = "broadcast_ok"
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

	if err := n.Run(); err != nil {
		panic(err)
	}

}
