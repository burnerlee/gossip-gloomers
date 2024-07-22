package main

import (
	"encoding/json"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var logger *log.Logger

type messagesStorage struct {
	messages map[float64]bool
	lock     sync.Mutex
}

func (m *messagesStorage) addMessage(msg float64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.messages[msg] = true

}

func (m *messagesStorage) getMessages() []float64 {
	m.lock.Lock()
	defer m.lock.Unlock()

	var messages []float64
	for msg := range m.messages {
		messages = append(messages, msg)
	}

	return messages
}

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

	logger.Info("Starting node")

	n := maelstrom.NewNode()

	n.Handle("topology", func(msg maelstrom.Message) error {
		// topology doesn't matter in a single node system
		body := map[string]any{
			"type": "topology_ok",
		}
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

		delete(body, "message")
		body["type"] = "broadcast_ok"

		n.Reply(msg, body)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		messages := store.getMessages()

		body := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}

		n.Reply(msg, body)
		return nil
	})

	if err := n.Run(); err != nil {
		panic(err)
	}

}
