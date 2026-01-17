package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"message-queue/internal/wal"

	"github.com/google/uuid"
)

type Message struct {
	ID        string    `json:"id"`
	Body      []byte    `json:"body"`
	Timestamp time.Time `json:"-"`
}

type Queue struct {
	mu              sync.RWMutex
	topic           string
	messages        map[string]*Message
	pending         []string
	inFlight        map[string]time.Time
	inFlightTimeout time.Duration
	wal             *wal.WAL
	metrics         *Metrics
}

func NewQueue(topic string, inFlightTimeout time.Duration, walPath string, metrics *Metrics) (*Queue, error) {
	w, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	q := &Queue{
		topic:           topic,
		messages:        make(map[string]*Message),
		pending:         make([]string, 0),
		inFlight:        make(map[string]time.Time),
		inFlightTimeout: inFlightTimeout,
		wal:             w,
		metrics:         metrics,
	}

	if err := q.loadFromWAL(walPath); err != nil {
		return nil, fmt.Errorf("failed to load from WAL: %w", err)
	}

	q.metrics.SetQueueDepth(q.topic, len(q.pending))
	return q, nil
}

func (q *Queue) loadFromWAL(walPath string) error {
	records, err := wal.ReadAll(walPath)
	if err != nil {
		return err
	}

	log.Printf("Loaded %d messages from WAL for %s", len(records), walPath)

	for _, record := range records {
		var msg Message
		if err := json.Unmarshal(record, &msg); err != nil {
			log.Printf("WARN: Failed to unmarshal message from WAL, skipping: %s", err)
			continue
		}
		q.messages[msg.ID] = &msg
		q.pending = append(q.pending, msg.ID)
	}
	return nil
}

func (q *Queue) Enqueue(body []byte) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	msgID := uuid.New().String()
	msg := &Message{
		ID:   msgID,
		Body: body,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("failed to marshal message: %w", err)
	}

	if err := q.wal.Write(data); err != nil {
		return "", fmt.Errorf("failed to write to WAL: %w", err)
	}

	q.messages[msgID] = msg
	q.pending = append(q.pending, msgID)
	q.metrics.SetQueueDepth(q.topic, len(q.pending))
	return msgID, nil
}

func (q *Queue) DequeueForProcessing() *Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return nil
	}

	msgID := q.pending[0]
	q.pending = q.pending[1:]

	q.inFlight[msgID] = time.Now()

	return q.messages[msgID]
}

func (q *Queue) AckMessage(msgID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.inFlight, msgID)
	delete(q.messages, msgID)
	q.metrics.SetQueueDepth(q.topic, len(q.pending))

	// Rewrite the WAL with the remaining messages
	var remainingRecords [][]byte
	for _, msg := range q.messages {
		data, err := json.Marshal(msg)
		if err != nil {
			log.Printf("ERROR: Failed to marshal message for WAL rewrite: %s", err)
			continue
		}
		remainingRecords = append(remainingRecords, data)
	}

	log.Printf("INFO: Rewriting WAL for queue. %d messages remaining.", len(remainingRecords))
	if err := q.wal.Rewrite(remainingRecords); err != nil {
		log.Printf("ERROR: Failed to rewrite WAL after ACK: %s", err)
	} else {
		log.Printf("INFO: WAL rewrite successful.")
	}
}

func (q *Queue) RequeueOrphaned() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	for msgID, ts := range q.inFlight {
		if now.Sub(ts) > q.inFlightTimeout {
			log.Printf("Re-queuing orphaned message %s", msgID)
			delete(q.inFlight, msgID)
			q.pending = append([]string{msgID}, q.pending...)
		}
	}
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.wal.Close()
}