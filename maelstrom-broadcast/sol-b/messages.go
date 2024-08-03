package main

import "sync"

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
