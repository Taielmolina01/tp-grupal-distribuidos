package clientregistry

import (
	"net"
	"sync"
)

type ClientState struct {
	ID   int
	Conn net.Conn
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients []ClientState
}

func (registry *ClientRegistry) Add(client ClientState) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.clients = append(registry.clients, client)
}

func (registry *ClientRegistry) RemoveByID(id int) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for i, c := range registry.clients {
		if c.ID == id {
			registry.clients = append(registry.clients[:i], registry.clients[i+1:]...)
			return
		}
	}
}

func (registry *ClientRegistry) RemoveByConn(id int, conn net.Conn) bool {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	for i, c := range registry.clients {
		if c.ID == id && c.Conn == conn {
			registry.clients = append(registry.clients[:i], registry.clients[i+1:]...)
			return true
		}
	}
	return false
}

func (registry *ClientRegistry) WithLock(action func([]ClientState)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}
