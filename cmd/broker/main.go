package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"message-queue/internal"
)

var broker *internal.Broker

func main() {
	metrics := internal.NewMetrics()
	broker = internal.NewBroker("data", 10*time.Second, metrics)
	defer broker.Close()

	// Start metrics server
	go func() {
		http.Handle("/metrics", metrics)
		log.Println("Metrics server listening on :8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Fatalf("Failed to start metrics server: %s", err)
		}
	}()

	ln, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatalf("failed to listen on port 3000: %s", err)
	}
	defer ln.Close()

	fmt.Println("Broker listening on port 3000")

	go func() {
		for {
			time.Sleep(2 * time.Second)
			broker.RequeueOrphaned()
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("failed to accept connection: %s", err)
			continue
		}
		broker.Metrics.IncActiveConn()
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	defer broker.Metrics.DecActiveConn()
	fmt.Printf("New connection from %s\n", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Printf("Error reading from connection: %s\n", err)
			}
			return
		}

		command = strings.TrimSpace(command)
		parts := strings.Split(command, " ")
		cmd := parts[0]

		switch cmd {
		case "PRODUCE":
			if len(parts) < 4 {
				conn.Write([]byte("ERROR: PRODUCE command requires a topic, partition, and a message\n"))
				continue
			}
			topicName := parts[1]
			partitionID, err := strconv.Atoi(parts[2])
			if err != nil {
				conn.Write([]byte("ERROR: Invalid partition ID\n"))
				continue
			}
			message := strings.Join(parts[3:], " ")
			topic := broker.GetTopic(topicName)
			partition, err := topic.GetPartition(partitionID)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("ERROR: Failed to get partition %d for topic %s: %s\n", partitionID, topicName, err)))
				continue
			}
			if _, err := partition.Enqueue([]byte(message)); err != nil {
				conn.Write([]byte(fmt.Sprintf("ERROR: Failed to produce message: %s\n", err)))
			} else {
				conn.Write([]byte("OK\n"))
			}
		case "CONSUME":
			if len(parts) < 3 {
				conn.Write([]byte("ERROR: CONSUME command requires a topic and partition\n"))
				continue
			}
			topicName := parts[1]
			partitionID, err := strconv.Atoi(parts[2])
			if err != nil {
				conn.Write([]byte("ERROR: Invalid partition ID\n"))
				continue
			}
			topic := broker.GetTopic(topicName)
			partition, err := topic.GetPartition(partitionID)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("ERROR: Failed to get partition %d for topic %s: %s\n", partitionID, topicName, err)))
				continue
			}
			msg := partition.DequeueForProcessing()
			if msg == nil {
				conn.Write([]byte("No messages in queue\n"))
			} else {
				response := fmt.Sprintf("%s %s\n", msg.ID, string(msg.Body))
				conn.Write([]byte(response))
			}
		case "ACK":
			if len(parts) < 4 {
				conn.Write([]byte("ERROR: ACK command requires a topic, partition, and a message ID\n"))
				continue
			}
			topicName := parts[1]
			partitionID, err := strconv.Atoi(parts[2])
			if err != nil {
				conn.Write([]byte("ERROR: Invalid partition ID\n"))
				continue
			}
			msgID := parts[3]
			topic := broker.GetTopic(topicName)
			partition, err := topic.GetPartition(partitionID)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("ERROR: Failed to get partition %d for topic %s: %s\n", partitionID, topicName, err)))
				continue
			}
			partition.AckMessage(msgID)
			conn.Write([]byte("ACK_OK\n"))
		default:
			conn.Write([]byte(fmt.Sprintf("ERROR: Unknown command '%s'\n", cmd)))
		}
	}
}