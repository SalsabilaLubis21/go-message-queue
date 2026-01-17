package internal

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestQueue_DurabilityAndMetrics(t *testing.T) {
	// Create a temporary directory for the WAL file
	dir, err := ioutil.TempDir("", "queue_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	walPath := dir + "/wal.log"
	topic := "test-topic"
	metrics := NewMetrics()

	// 1. Create a new queue
	q, err := NewQueue(topic, 1*time.Minute, walPath, metrics)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	if metrics.Queues[topic] != 0 {
		t.Errorf("Expected initial queue depth to be 0, got %d", metrics.Queues[topic])
	}

	// 2. Enqueue a message
	msgBody := []byte("test-message")
	msgID, err := q.Enqueue(msgBody)
	if err != nil {
		t.Fatalf("Failed to enqueue message: %v", err)
	}

	if metrics.Queues[topic] != 1 {
		t.Errorf("Expected queue depth to be 1 after enqueue, got %d", metrics.Queues[topic])
	}

	// 3. Dequeue the message
	dequeuedMsg := q.DequeueForProcessing()
	if dequeuedMsg == nil {
		t.Fatal("Expected to dequeue a message, but got nil")
	}
	if dequeuedMsg.ID != msgID {
		t.Errorf("Expected dequeued message ID to be '%s', got '%s'", msgID, dequeuedMsg.ID)
	}
	if string(dequeuedMsg.Body) != string(msgBody) {
		t.Errorf("Expected dequeued message body to be '%s', got '%s'", msgBody, dequeuedMsg.Body)
	}

	// 4. ACK the message, which should trigger a WAL rewrite
	q.AckMessage(dequeuedMsg.ID)

	if metrics.Queues[topic] != 0 {
		t.Errorf("Expected queue depth to be 0 after ack, got %d", metrics.Queues[topic])
	}

	// 5. Close the queue
	q.Close()

	// 6. Re-create the queue from the same WAL file
	q2, err := NewQueue(topic, 1*time.Minute, walPath, metrics)
	if err != nil {
		t.Fatalf("Failed to re-create queue: %v", err)
	}
	defer q2.Close()

	if metrics.Queues[topic] != 0 {
		t.Errorf("Expected queue depth to be 0 after restart, got %d", metrics.Queues[topic])
	}

	// 7. Try to dequeue again
	shouldBeNil := q2.DequeueForProcessing()
	if shouldBeNil != nil {
		t.Errorf("Expected queue to be empty after restart, but got message with ID %s", shouldBeNil.ID)
	}
}