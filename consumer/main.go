package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Config holds all configuration parameters
type Config struct {
	NumberOfConsumers int
	PrefetchCount     int
	Host              string
	Port              int
	Username          string
	Password          string
	RabbitMQURL       string
	QueueName         string
}

// Stats tracks performance metrics
type Stats struct {
	MessagesReceived uint64
	StartTime        time.Time
}

// Consumer represents a message consumer
type Consumer struct {
	ID         int
	Config     Config
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Stats      Stats
	StopChan   chan struct{}
}

// NewConsumer creates a new consumer
func NewConsumer(id int, config Config, conn *amqp.Connection) (*Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Set QoS/prefetch
	err = ch.Qos(
		config.PrefetchCount, // prefetch count
		0,                    // prefetch size
		false,                // global
	)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &Consumer{
		ID:         id,
		Config:     config,
		Connection: conn,
		Channel:    ch,
		Stats: Stats{
			StartTime: time.Now(),
		},
		StopChan: make(chan struct{}),
	}, nil
}

// Start begins consuming messages
func (c *Consumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.Channel.Close()

	log.Printf("Consumer %d starting", c.ID)

	_, err := c.Channel.QueueDeclare(
		c.Config.QueueName, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		log.Printf("Consumer %d: Failed to declare queue: %v", c.ID, err)
		return
	}

	deliveries, err := c.Channel.Consume(
		c.Config.QueueName,        // queue
		fmt.Sprintf("c-%d", c.ID), // consumer tag
		true,                      // auto-ack (true for maximum speed)
		false,                     // exclusive
		false,                     // no-local
		false,                     // no-wait
		nil,                       // args
	)
	if err != nil {
		log.Printf("Consumer %d: Failed to register consumer: %v", c.ID, err)
		return
	}

	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Consumer %d: Shutting down (context cancelled)", c.ID)
			return

		case <-c.StopChan:
			log.Printf("Consumer %d: Shutting down (stop signal received)", c.ID)
			return

		case <-statsTicker.C:
			msgCount := atomic.LoadUint64(&c.Stats.MessagesReceived)
			elapsedSecs := time.Since(c.Stats.StartTime).Seconds()
			rate := float64(msgCount) / elapsedSecs
			log.Printf("Consumer %d: Received %d messages (%.2f msgs/sec)",
				c.ID, msgCount, rate)

		case _, ok := <-deliveries:
			if !ok {
				log.Printf("Consumer %d: Channel closed", c.ID)
				return
			}

			atomic.AddUint64(&c.Stats.MessagesReceived, 1)
		}
	}
}

func (c *Consumer) Stop() {
	close(c.StopChan)
}

func (c *Consumer) GetStats() Stats {
	return Stats{
		MessagesReceived: atomic.LoadUint64(&c.Stats.MessagesReceived),
		StartTime:        c.Stats.StartTime,
	}
}

func main() {
	numConsumers := flag.Int("consumers", 1, "Number of consumer goroutines")
	prefetch := flag.Int("prefetch", 100, "Prefetch count per consumer")

	rabbitURL := flag.String("url", "", "RabbitMQ connection URL (overrides host/port/username/password if provided)")
	host := flag.String("host", "localhost", "RabbitMQ server hostname")
	port := flag.Int("port", 5672, "RabbitMQ server port")
	username := flag.String("username", "guest", "RabbitMQ username")
	password := flag.String("password", "guest", "RabbitMQ password")
	vhost := flag.String("vhost", "/", "RabbitMQ virtual host")

	queueName := flag.String("queue", "messages", "Queue name to consume from")
	durationSec := flag.Int("duration", 0, "Duration in seconds (0 means run until interrupted)")
	flag.Parse()

	connectionURL := *rabbitURL
	if connectionURL == "" {
		connectionURL = fmt.Sprintf("amqp://%s:%s@%s:%d%s",
			*username, *password, *host, *port, *vhost)
	}

	config := Config{
		NumberOfConsumers: *numConsumers,
		PrefetchCount:     *prefetch,
		RabbitMQURL:       connectionURL,
		Host:              *host,
		Port:              *port,
		Username:          *username,
		Password:          *password,
		QueueName:         *queueName,
	}

	log.Printf("Connecting to RabbitMQ at %s:%d (user: %s)", config.Host, config.Port, config.Username)
	conn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	var ctx context.Context
	var cancel context.CancelFunc

	if *durationSec > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(*durationSec)*time.Second)
		log.Printf("Will run for %d seconds", *durationSec)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
		log.Printf("Will run until interrupted (press Ctrl+C to stop)")

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			sig := <-sigChan
			log.Printf("Received signal %v, shutting down gracefully...", sig)
			cancel()
		}()
	}
	defer cancel()

	log.Printf("Starting %d consumers with prefetch count %d from queue '%s'",
		config.NumberOfConsumers, config.PrefetchCount, config.QueueName)

	consumers := make([]*Consumer, config.NumberOfConsumers)
	var wg sync.WaitGroup

	for i := 0; i < config.NumberOfConsumers; i++ {
		consumer, err := NewConsumer(i, config, conn)
		if err != nil {
			log.Fatalf("Failed to create consumer %d: %v", i, err)
		}

		consumers[i] = consumer
		wg.Add(1)
		go consumer.Start(ctx, &wg)
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		startTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var totalMessages uint64
				for _, c := range consumers {
					stats := c.GetStats()
					totalMessages += stats.MessagesReceived
				}

				elapsedSecs := time.Since(startTime).Seconds()
				overallRate := float64(totalMessages) / elapsedSecs
				mbPerSec := (overallRate * 512) / (1024 * 1024) // Assuming 512-byte messages

				log.Printf("OVERALL: Received %d messages (%.2f msgs/sec, %.2f MB/sec)",
					totalMessages, overallRate, mbPerSec)
			}
		}
	}()

	wg.Wait()

	var totalMessages uint64
	for _, c := range consumers {
		stats := c.GetStats()
		totalMessages += stats.MessagesReceived
	}

	elapsedSecs := time.Since(consumers[0].Stats.StartTime).Seconds()
	overallRate := float64(totalMessages) / elapsedSecs
	mbPerSec := (overallRate * 512) / (1024 * 1024)

	log.Printf("All consumers finished")
	log.Printf("Total messages received: %d", totalMessages)
	log.Printf("Average rate: %.2f messages/sec (%.2f MB/sec)", overallRate, mbPerSec)
	log.Printf("Total runtime: %.2f seconds", elapsedSecs)
}
