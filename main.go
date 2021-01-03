package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-service-bus-go"
	"github.com/Azure/go-amqp"
	"github.com/joho/godotenv"
)

type StepSessionHandler struct {
	sync.RWMutex
	lastProcessedAt time.Time
	messageSession  *servicebus.MessageSession
}

// Read last processed time in thread safe manner
func (sh *StepSessionHandler) GetLastProcessedAt() time.Time {
	sh.RLock()
	sh.RUnlock()
	return sh.lastProcessedAt
}

// Write last processed time in thread safe manner
func (sh *StepSessionHandler) SetLastProcessedAt(timestamp time.Time) {
	sh.Lock()
	sh.lastProcessedAt = timestamp
	sh.Unlock()
}

// End is called when a session is terminated
func (sh *StepSessionHandler) End() {
	fmt.Println("End session")
}

// Start is called when a new session is started
func (sh *StepSessionHandler) Start(ms *servicebus.MessageSession) error {
	sh.messageSession = ms
	fmt.Println("Begin session")
	return nil
}

// Handle is called when a new session message is received
func (sh *StepSessionHandler) Handle(ctx context.Context, msg *servicebus.Message) error {
	sh.SetLastProcessedAt(time.Now())
	fmt.Printf("  Session: %s Data: %s\n", *msg.SessionID, string(msg.Data))

	// Processing of message simulated through delay
	time.Sleep(5 * time.Second)

	return msg.Complete(ctx)
}

func main() {
	// Read env variables from .env file if it exists
	loadEnvFromFileIfExists()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connStr := os.Getenv("SERVICEBUS_CONNECTION_STRING")
	qName := os.Getenv("QUEUE_NAME")
	if connStr == "" || qName == "" {
		fmt.Println("FATAL: expected environment variable SERVICEBUS_CONNECTION_STRING or QUEUE_NAME not set")
		return
	}

	// Create a client to communicate with a Service Bus Namespace.
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connStr))
	if err != nil {
		fmt.Println(err)
		return
	}

	// Create queue receiver
	q, err := ns.NewQueue(qName)
	if err != nil {
		fmt.Println(err)
		return
	}

	timer := time.NewTicker(time.Second * 10)
	defer timer.Stop()

	for {
		qs := q.NewSession(nil)
		sess := &StepSessionHandler{
			lastProcessedAt: time.Now(),
		}

		// Recurring routine to check whether message handler is processing messages in session.
		go func() {
			for {
				now := <-timer.C
				if sess.messageSession == nil {
					fmt.Printf("❗ Waiting to start new session at %v\n", now)
					continue
				}

				fmt.Printf("# Checking timestamp of the last processed message in session at %v\n", now)
				if sess.lastProcessedAt.Add(time.Second * 30).Before(time.Now()) {
					fmt.Println("❌ Session expired. Closing it now.")
					sess.messageSession.Close()
					return
				}

				fmt.Println("✔ Session is active.")
			}
		}()

		if err = qs.ReceiveOne(ctx, sess); err != nil {
			if innerErr, ok := err.(*amqp.Error); ok && innerErr.Condition == "com.microsoft:timeout" {
				fmt.Println("➰ Timeout waiting for messages. Entering next loop.")
				continue
			}

			fmt.Println(err)
			return
		}

		if err = qs.Close(ctx); err != nil {
			fmt.Println(err)
			return
		}
	}
}

func loadEnvFromFileIfExists() {
	envFile := ".env"
	if _, err := os.Stat(envFile); err == nil {
		if err = godotenv.Load(envFile); err != nil {
			log.Fatalf("Error loading .env file")
		}
	}
}