package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Topic struct {
	mu         sync.RWMutex
	name       string
	partitions map[int]*Queue
	dataDir    string
	timeout    time.Duration
	metrics    *Metrics
}

func NewTopic(name, dataDir string, timeout time.Duration, metrics *Metrics) *Topic {
	return &Topic{
		name:       name,
		partitions: make(map[int]*Queue),
		dataDir:    dataDir,
		timeout:    timeout,
		metrics:    metrics,
	}
}

func (t *Topic) GetPartition(id int) (*Queue, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if p, ok := t.partitions[id]; ok {
		return p, nil
	}

	
	partitionDir := filepath.Join(t.dataDir, t.name)
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}
	walPath := filepath.Join(partitionDir, fmt.Sprintf("%d.wal.log", id))

	q, err := NewQueue(fmt.Sprintf("%s-%d", t.name, id), t.timeout, walPath, t.metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue for partition %d: %w", id, err)
	}

	t.partitions[id] = q
	return q, nil
}

func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range t.partitions {
		p.Close()
	}
}

func (t *Topic) RequeueOrphaned() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.partitions {
		p.RequeueOrphaned()
	}
}