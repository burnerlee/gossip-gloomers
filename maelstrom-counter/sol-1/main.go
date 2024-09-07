package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var logger *log.Logger

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("/Users/burnerlee/Projects/random/gossip-gloomers/maelstrom-counter/sol-1/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := body["delta"].(float64)

		counterKey := fmt.Sprintf("%s_counter", n.ID())
		currentCounterValue, err := readCounterFromKV(kv, counterKey)
		if err != nil {
			return err
		}

		newCounterValue := currentCounterValue + int(delta)
		kv.Write(context.Background(), counterKey, newCounterValue)

		body["type"] = "add_ok"
		delete(body, "delta")
		return n.Reply(msg, body)

	})

	n.Handle("get_counter", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		counterKey := fmt.Sprintf("%s_counter", n.ID())
		counterValue, err := readCounterFromKV(kv, counterKey)
		if err != nil {
			return err
		}

		body["counter"] = counterValue
		body["type"] = "get_counter_ok"
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// fetching the counter from the local node
		counterKey := fmt.Sprintf("%s_counter", n.ID())
		counterValue, err := readCounterFromKV(kv, counterKey)
		if err != nil {
			return err
		}

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

func readCounterFromKV(kv *maelstrom.KV, key string) (int, error) {
	counterValue, err := kv.ReadInt(context.Background(), key)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, nil
		} else {
			return 0, err
		}
	}
	return counterValue, nil
}
