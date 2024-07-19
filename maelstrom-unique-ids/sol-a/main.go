package main

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var COUNTER = 1
var logger *log.Logger
var lock = sync.Mutex{}

func initLogger() {
	logger = log.New()
	logFile, _ := os.OpenFile("./maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger.SetOutput(logFile)
}

func getCounter(n *maelstrom.Node) int {
	return COUNTER*len(n.NodeIDs()) + getIDFromNodeID(n.ID())
}

func incrementCounter() {
	COUNTER++
}

func getIDFromNodeID(nodeID string) int {
	id, _ := strconv.Atoi(nodeID[1:])
	return id
}

func main() {
	initLogger()

	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		lock.Lock()
		body["id"] = getCounter(n)
		incrementCounter()
		lock.Unlock()

		body["type"] = "generate_ok"
		n.Reply(msg, body)
		return nil
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
