package internal

import (
	"sync"
	"time"
)

type Broker struct {
	mu      sync.RWMutex
	topics  map[string]*Topic
	dataDir string
	timeout time.Duration
	Metrics *Metrics
}

func NewBroker(dataDir string, timeout time.Duration, metrics *Metrics) *Broker {
	return &Broker{
		topics:  make(map[string]*Topic),
		dataDir: dataDir,
		timeout: timeout,
		Metrics: metrics,
	}
}

func (b *Broker) GetTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, ok := b.topics[name]; ok {
		return t
	}

	t := NewTopic(name, b.dataDir, b.timeout, b.Metrics)
	b.topics[name] = t
	return t
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.topics {
		t.Close()
	}
}

func (b *Broker) RequeueOrphaned() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, t := range b.topics {
		t.RequeueOrphaned()
	}
}