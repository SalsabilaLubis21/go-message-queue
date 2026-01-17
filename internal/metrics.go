package internal

import (
	"encoding/json"
	"net/http"
	"sync"
)

type Metrics struct {
	mu         sync.RWMutex
	Queues     map[string]int `json:"queues"`
	ActiveConn int            `json:"active_connections"`
}

func NewMetrics() *Metrics {
	return &Metrics{
		Queues: make(map[string]int),
	}
}

func (m *Metrics) SetQueueDepth(topic string, depth int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Queues[topic] = depth
}

func (m *Metrics) IncActiveConn() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ActiveConn++
}

func (m *Metrics) DecActiveConn() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ActiveConn > 0 {
		m.ActiveConn--
	}
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	// Create a copy of the data to avoid holding the lock during JSON encoding.
	data := struct {
		Queues     map[string]int `json:"queues"`
		ActiveConn int            `json:"active_connections"`
	}{
		Queues:     make(map[string]int),
		ActiveConn: m.ActiveConn,
	}
	for k, v := range m.Queues {
		data.Queues[k] = v
	}
	m.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
	}
}