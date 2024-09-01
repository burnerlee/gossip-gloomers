package main

import "sync"

type nodeNeighbours struct {
	neighbours map[string]bool
	lock       sync.Mutex
}

func (n *nodeNeighbours) addNeighbour(neighbour string) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.neighbours[neighbour] = true
}

func (n *nodeNeighbours) getNeighbours() []string {
	n.lock.Lock()
	defer n.lock.Unlock()

	var neighbours []string
	for neighbour := range n.neighbours {
		neighbours = append(neighbours, neighbour)
	}

	return neighbours
}
