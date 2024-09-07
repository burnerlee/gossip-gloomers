package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var logger *log.Logger

var counter int = 0
var counterLock = &sync.Mutex{}

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("/Users/burnerlee/Projects/random/gossip-gloomers/maelstrom-counter/sol-2/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	logger.SetFormatter(&log.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})
	logger.SetOutput(logFile)
	logger.SetLevel(log.DebugLevel)
}

func main() {
	initLogger()
	n := maelstrom.NewNode()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := body["delta"].(float64)

		counterLock.Lock()
		counter += int(delta)
		counterLock.Unlock()

		body["type"] = "add_ok"
		delete(body, "delta")

		return n.Reply(msg, body)
	})

	n.Handle("get_counter", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		counterLock.Lock()
		body["counter"] = counter
		counterLock.Unlock()
		body["type"] = "get_counter_ok"
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// fetching the counter from the local node
		counterLock.Lock()
		counterValue := counter
		counterLock.Unlock()

		// fetching the counter from all other nodes
		for _, nodeID := range n.NodeIDs() {
			if nodeID == n.ID() {
				continue
			}
			resp, err := n.SyncRPC(context.Background(), nodeID, map[string]any{
				"type": "get_counter",
			})
			if err != nil {
				// if the node is not responding, we skip adding its counter
				// effectively, the totalCounterValue will be lower than the actual value
				// but it's okay since we can send a stale value
				// the correct value will eventually be sent when the node comes back online
				logger.Warnf("node %s not responding, skipping", nodeID)
				continue
			}
			var respBody map[string]any
			if err := json.Unmarshal(resp.Body, &respBody); err != nil {
				return err
			}

			counterValue += int(respBody["counter"].(float64))
		}

		body["value"] = counterValue
		body["type"] = "read_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
