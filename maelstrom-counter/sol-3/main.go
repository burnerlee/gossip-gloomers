package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"math/big"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var logger *log.Logger

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("/Users/burnerlee/Projects/random/gossip-gloomers/maelstrom-counter/sol-3/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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

		for {
			counter, err := kv.ReadInt(context.Background(), "counter")
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					counter = 0
				} else {
					return err
				}
			}
			updatedCounter := counter + int(delta)

			if err := kv.CompareAndSwap(context.Background(), "counter", counter, updatedCounter, true); err == nil {
				logger.Infof("Updated counter: %d on node %s", updatedCounter, n.ID())
				break
			}
		}

		body["type"] = "add_ok"
		delete(body, "delta")
		return n.Reply(msg, body)

	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// write random number to kv to make a transaction not possible
		// in the past
		// this forces the sequential kv for the node to be updated
		randomInt, _ := rand.Int(rand.Reader, big.NewInt(100))
		kv.Write(context.Background(), "rand", randomInt)

		counter, err := kv.ReadInt(context.Background(), "counter")
		if err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				counter = 0
			} else {
				return err
			}
		}

		logger.Infof("Read counter: %d on node %s", counter, n.ID())

		body["type"] = "read_ok"
		body["value"] = counter
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
