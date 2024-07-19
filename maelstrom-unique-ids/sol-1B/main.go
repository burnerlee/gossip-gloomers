package main

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var logger *log.Logger
var lock = sync.Mutex{}

func initLogger() {
	logger = log.New()
	logFile, _ := os.OpenFile("./maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger.SetOutput(logFile)
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
		// adding nodeID to time to mitigate the possibility of generating the same ID
		// across different nodes at the same time
		body["id"] = time.Now().String() + " " + n.ID()
		lock.Unlock()

		body["type"] = "generate_ok"
		n.Reply(msg, body)
		return nil
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
