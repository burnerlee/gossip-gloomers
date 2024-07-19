package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var logger *log.Logger
var lock = sync.Mutex{}

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("/Users/burnerlee/Projects/random/gossip-gloomers/maelstrom-unique-ids/maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	logger.SetOutput(logFile)
}

func getCounter(kv *maelstrom.KV) (int, error) {
	val, err := kv.ReadInt(context.TODO(), "counter")
	if err != nil {
		if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return 0, err
		} else {
			val = 0
		}
	}

	for {
		newValue := val + 1
		if err := kv.CompareAndSwap(context.TODO(), "counter", val, newValue, true); err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				return 0, err
			}
		} else {
			return newValue, nil
		}
		val++
	}
}

func main() {
	initLogger()

	logger.Info("Starting node")

	n := maelstrom.NewNode()

	kv := maelstrom.NewLinKV(n)

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		lock.Lock()
		newID, err := getCounter(kv)
		if err != nil {
			logger.Errorf("error getting counter: %v", err)
			return err
		}
		body["id"] = newID
		body["type"] = "generate_ok"
		lock.Unlock()
		n.Reply(msg, body)
		return nil
	})

	if err := n.Run(); err != nil {
		panic(err)
	}

}
