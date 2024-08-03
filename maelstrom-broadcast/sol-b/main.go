package main

import (
	"encoding/json"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var logger *log.Logger

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	logger.SetOutput(logFile)
	logger.SetLevel(log.DebugLevel)
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

			body["type"] = "broadcast"
			body["message"] = message

			n.Send(neighbour, body)
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
