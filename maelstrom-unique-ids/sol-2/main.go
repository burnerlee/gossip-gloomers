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
var lock = sync.Mutex{}
var COUNTER = 0

func initLogger() {
	logger = log.New()
	logFile, err := os.OpenFile("maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	logger.SetOutput(logFile)
	logger.SetLevel(log.DebugLevel)
}

func getCounter(n *maelstrom.Node) (int, error) {
	lock.Lock()
	defer lock.Unlock()
	if n.ID() == "n0" {
		return COUNTER, nil
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Microsecond*100)
	resp, err := n.SyncRPC(ctx, "n0", map[string]any{
		"type": "get_counter",
	})
	if err != nil {
		return 0, err
	}

	var body map[string]any
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return 0, err
	}
	return int(body["id"].(float64)), nil
}

func CASCounter(n *maelstrom.Node, existing, newValue int) error {
	lock.Lock()
	defer lock.Unlock()
	if n.ID() == "n0" {
		COUNTER = newValue
		return nil
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*1)
	resp, err := n.SyncRPC(ctx, "n0", map[string]any{
		"type":     "cas_counter",
		"id":       newValue,
		"existing": existing,
	})
	if err != nil {
		return err
	}
	if resp.Type() == "cas_counter_failed" {
		return maelstrom.NewRPCError(maelstrom.PreconditionFailed, "precondition failed")
	}
	return nil
}

func main() {
	initLogger()

	logger.Info("Starting node")

	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		counterValue, err := getCounter(n)
		if err != nil {
			logger.Errorf("error getting counter: %v", err)
			return err
		}
		for {
			if err := CASCounter(n, counterValue, counterValue+1); err != nil {
				logger.Errorf("error cas counter: %v", err)
				counterValue++
				continue
			} else {
				break
			}
		}
		body["id"] = counterValue + 1
		body["type"] = "generate_ok"
		n.Reply(msg, body)
		return nil
	})

	n.Handle("get_counter", func(msg maelstrom.Message) error {
		body := map[string]any{
			"id": COUNTER,
		}
		n.Reply(msg, body)
		return nil
	})

	n.Handle("cas_counter", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		existingValue := int(body["existing"].(float64))
		if COUNTER != existingValue {
			body["type"] = "cas_counter_failed"
			n.Reply(msg, body)
			return maelstrom.NewRPCError(maelstrom.PreconditionFailed, "precondition failed")
		}
		COUNTER = int(body["id"].(float64))
		return nil
	})

	if err := n.Run(); err != nil {
		panic(err)
	}

}
