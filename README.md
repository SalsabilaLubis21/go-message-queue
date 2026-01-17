# A Simple Message Queue in Go

This project is a straightforward message queue built with Go. This is like a digital post office for your software applications. It allows different programs to communicate with each other by sending and receiving messages, ensuring that no message gets lost along the way.

## Core Features

- **Reliable Message Saving**: Your messages are always saved safely to disk. This is achieved using a technique called a **Write-Ahead Log (WAL)**. Before any message is processed, it's first written down in a journal. If the server crashes, it can read this journal upon restart to recover all the messages, ensuring nothing is lost.

- **Organized Message Channels (Topics & Partitions)**: You can create different channels, called **topics**, for different kinds of messages (e.g., `user-signups`, `order-processing`). To handle a large volume of messages efficiently, each topic can be split into multiple logs, known as **partitions**.

- **Health & Status Monitoring**: A simple web page (an HTTP endpoint) is available to show you the system's status in real-time. You can see how many messages are waiting in each queue and how many applications are currently connected.

- **Simple Communication Protocol**: Applications connect and talk to the queue using a basic, text-based language over a standard network (TCP) connection.

## Technology Used

- **Language**: [Go](https://golang.org/)
- **How it Handles Multiple Tasks**: It's built to manage many connections and messages at once using Go's powerful built-in features for concurrency (**goroutines** and **channels**).
- **How it Saves Data**: It uses a custom-built **Write-Ahead Log (WAL)** system to write messages to files on the disk, guaranteeing data durability.

## How to Use It

### Prerequisites

- You'll need Go (version 1.18 or newer) installed on your machine.

### 1. Start the Message Broker

The broker is the central server that manages all the messages. Open your terminal and run:

```bash
go run cmd/broker/main.go
```

The broker will start listening for messages on port `3000`, and the monitoring server will be available on port `8081`.

### 2. Send and Receive Messages

A simple client program is included to interact with the broker.

**To send (produce) a message:**

Use the `produce` command with a topic, partition number, and your message.

```bash
go run cmd/client/main.go produce <topic> <partition> <message>
```

_Example:_

```bash
go run cmd/client/main.go produce user-signups 0 "new user: alex"
```

**To receive (consume) messages:**

Use the `consume` command with a topic and partition number to retrieve all its messages.

```bash
go run cmd/client/main.go consume <topic> <partition>
```

_Example:_

```bash
go run cmd/client/main.go consume user-signups 0
```

## Check the System's Status

You can view the monitoring metrics in your browser or by using a tool like `curl`.

```bash
curl http://localhost:8081/metrics
```

**Example Response:**

This JSON output shows that the `user-signups` topic (partition 0) has 1 message waiting, and there is 1 active connection to the server.

```json
{
  "queues": {
    "user-signups-0": 1
  },
  "active_connections": 1
}
```

## Project Code Structure

```
.
├── cmd/
│   ├── broker/     # The main server application
│   └── client/     # The example client application
├── data/           # Where message log files are stored
├── internal/       # All the core logic for the queue system
│   ├── broker.go
│   ├── metrics.go
│   ├── queue.go
│   ├── topic.go
│   └── wal/        # The Write-Ahead Log implementation
└── go.mod
```
