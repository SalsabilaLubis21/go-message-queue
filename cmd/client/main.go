package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <PRODUCE|CONSUME> ...", os.Args[0])
	}

	conn, err := net.Dial("tcp", "localhost:3000")
	if err != nil {
		log.Fatalf("Failed to connect to broker: %s", err)
	}
	defer conn.Close()

	cmd := os.Args[1]
	switch cmd {
	case "PRODUCE":
		if len(os.Args) < 5 {
			log.Fatalf("Usage: %s PRODUCE <topic> <partition> <message>", os.Args[0])
		}
		topic := os.Args[2]
		partition := os.Args[3]
		message := strings.Join(os.Args[4:], " ")
		fmt.Fprintf(conn, "PRODUCE %s %s %s\n", topic, partition, message)
		response, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print(response)
	case "CONSUME":
		if len(os.Args) < 4 {
			log.Fatalf("Usage: %s CONSUME <topic> <partition>", os.Args[0])
		}
		topic := os.Args[2]
		partition := os.Args[3]
		reader := bufio.NewReader(conn)

		// Send CONSUME command
		fmt.Fprintf(conn, "CONSUME %s %s\n", topic, partition)

		// Read response
		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalf("Failed to read from connection: %s", err)
		}
		response = strings.TrimSpace(response)

		if response == "No messages in queue" {
			fmt.Println(response)
			return
		}

		// Parse message ID and body
		parts := strings.SplitN(response, " ", 2)
		if len(parts) < 2 {
			log.Fatalf("Invalid message format from server: %s", response)
		}
		msgID := parts[0]
		body := parts[1]

		fmt.Println(body)

		// Send ACK
		fmt.Fprintf(conn, "ACK %s %s %s\n", topic, partition, msgID)

		// Wait for ACK confirmation
		ackResponse, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalf("Failed to read ACK confirmation: %s", err)
		}
		if strings.TrimSpace(ackResponse) != "ACK_OK" {
			log.Fatalf("Did not receive ACK_OK. Got: %s", ackResponse)
		}

	default:
		log.Fatalf("Unknown command: %s", cmd)
	}
}