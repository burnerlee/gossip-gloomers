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
	logFile, err := os.OpenFile("/Users/burnerlee/Projects/random/gossip-gloomers/maelstrom-broadcast/sol-d/maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
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
		ctx, cancel := context.WithDeadline(context.TODO(), time.Now().Add(1*time.Second))
		defer cancel()
		_, err := n.SyncRPC(ctx, neighbour, body)
		if err != nil {
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

	logger.Info("Starting node")

	n := maelstrom.NewNode()

	neighbourStore := &nodeNeighbours{
		neighbours: make(map[string]bool),
		lock:       sync.Mutex{},
	}

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topologyInterface := body["topology"].(map[string]interface{})

		for nodeID, topology := range topologyInterface {
			neighbours := make([]string, 0)
			for _, neighbour := range topology.([]interface{}) {
				neighbours = append(neighbours, neighbour.(string))
			}

			if nodeID == n.ID() {
				for _, neighbour := range neighbours {
					neighbourStore.addNeighbour(neighbour)
				}
			} else {
				n.Send(nodeID, map[string]any{
					"type":       "neighbour_share",
					"neighbours": neighbours,
				})
			}
		}

		body["type"] = "topology_ok"
		delete(body, "topology")

		n.Reply(msg, body)
		return nil
	})

	n.Handle("neighbour_share", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		neighbours := body["neighbours"].([]interface{})
		for _, neighbour := range neighbours {
			neighbourStore.addNeighbour(neighbour.(string))
		}

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"].(float64)

		logger.Infof("Received message %f", message)

		alreadyExists := store.checkMessage(message)
		if alreadyExists {
			logger.Infof("Message %f already exists", message)
			body["type"] = "broadcast_ok"
			delete(body, "message")
			n.Reply(msg, body)
			return nil
		}
		store.addMessage(message)

		neighbours := neighbourStore.getNeighbours()
		logger.Infof("neighbours for %s is %v", n.ID(), neighbours)
		for _, neighbour := range neighbours {
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
