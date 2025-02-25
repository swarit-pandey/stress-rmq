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
	NumberOfProducers int
	MessagesPerSecond int
	RabbitMQURL       string
	Host              string
	Port              int
	Username          string
	Password          string
	QueueName         string
	Exchange          string
	RoutingKey        string
}

// Stats tracks performance metrics
type Stats struct {
	MessagesSent uint64
	Errors       uint64
	StartTime    time.Time
}

// Producer represents a message producer
type Producer struct {
	ID         int
	Config     Config
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Stats      Stats
	StopChan   chan struct{}
}

// NewProducer creates a new producer
func NewProducer(id int, config Config, conn *amqp.Connection) (*Producer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return &Producer{
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

// Start begins publishing messages at the configured rate
func (p *Producer) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer p.Channel.Close()

	log.Printf("Producer %d starting", p.ID)

	_, err := p.Channel.QueueDeclare(
		p.Config.QueueName, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		log.Printf("Producer %d: Failed to declare queue: %v", p.ID, err)
		return
	}

	if p.Config.Exchange != "" {
		err = p.Channel.ExchangeDeclare(
			p.Config.Exchange, // name
			"direct",          // type
			true,              // durable
			false,             // auto-deleted
			false,             // internal
			false,             // no-wait
			nil,               // arguments
		)
		if err != nil {
			log.Printf("Producer %d: Failed to declare exchange: %v", p.ID, err)
			return
		}

		err = p.Channel.QueueBind(
			p.Config.QueueName,  // queue name
			p.Config.RoutingKey, // routing key
			p.Config.Exchange,   // exchange
			false,
			nil,
		)
		if err != nil {
			log.Printf("Producer %d: Failed to bind queue: %v", p.ID, err)
			return
		}
	}

	// Create a 512-byte message
	message := make([]byte, 512)
	for i := range message {
		message[i] = byte((i + p.ID) % 256) // Filling with a pattern based on producer ID
	}

	interval := time.Second / time.Duration(p.Config.MessagesPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Start publishing
	for {
		select {
		case <-ctx.Done():
			log.Printf("Producer %d: Shutting down (context cancelled)", p.ID)
			return

		case <-p.StopChan:
			log.Printf("Producer %d: Shutting down (stop signal received)", p.ID)
			return

		case <-ticker.C:
			// Update first 16 bytes with message metadata to make each message unique
			// Format: [producer ID (4 bytes)][message count (8 bytes)][timestamp (4 bytes)]
			msgCount := atomic.LoadUint64(&p.Stats.MessagesSent)

			// Producer ID (first 4 bytes)
			for i := 0; i < 4; i++ {
				message[i] = byte(p.ID >> (i * 8))
			}

			// Message count (next 8 bytes)
			for i := 0; i < 8; i++ {
				message[4+i] = byte(msgCount >> (i * 8))
			}

			// Timestamp (next 4 bytes - seconds since epoch)
			ts := uint32(time.Now().Unix())
			for i := 0; i < 4; i++ {
				message[12+i] = byte(ts >> (i * 8))
			}

			err := p.Channel.PublishWithContext(
				ctx,
				p.Config.Exchange,   // exchange
				p.Config.RoutingKey, // routing key
				false,               // mandatory
				false,               // immediate
				amqp.Publishing{
					ContentType:  "application/octet-stream",
					DeliveryMode: amqp.Persistent,
					MessageId:    fmt.Sprintf("%d-%d", p.ID, msgCount),
					Body:         message,
				},
			)

			if err != nil {
				atomic.AddUint64(&p.Stats.Errors, 1)
				log.Printf("Producer %d: Failed to publish message: %v", p.ID, err)
				time.Sleep(time.Second) // Avoid flooding logs on error
				continue
			}

			// Increment message counter
			atomic.AddUint64(&p.Stats.MessagesSent, 1)

			if msgCount%1000 == 0 && msgCount > 0 {
				elapsedSecs := time.Since(p.Stats.StartTime).Seconds()
				rate := float64(msgCount) / elapsedSecs
				log.Printf("Producer %d: Sent %d messages (%.2f msgs/sec)",
					p.ID, msgCount, rate)
			}
		}
	}
}

// Stop signals the producer to stop
func (p *Producer) Stop() {
	close(p.StopChan)
}

// GetStats returns current producer statistics
func (p *Producer) GetStats() Stats {
	return Stats{
		MessagesSent: atomic.LoadUint64(&p.Stats.MessagesSent),
		Errors:       atomic.LoadUint64(&p.Stats.Errors),
		StartTime:    p.Stats.StartTime,
	}
}

func main() {
	numProducers := flag.Int("producers", 1, "Number of producer goroutines")
	mps := flag.Int("mps", 10, "Messages per second per producer")

	rabbitURL := flag.String("url", "", "RabbitMQ connection URL (overrides other connection params if provided)")
	host := flag.String("host", "localhost", "RabbitMQ server hostname or IP address")
	port := flag.Int("port", 5672, "RabbitMQ server port")
	username := flag.String("username", "guest", "RabbitMQ username")
	password := flag.String("password", "guest", "RabbitMQ password")
	vhost := flag.String("vhost", "/", "RabbitMQ virtual host")

	queueName := flag.String("queue", "messages", "Queue name")
	exchange := flag.String("exchange", "", "Exchange name (optional)")
	routingKey := flag.String("routing-key", "", "Routing key (defaults to queue name if not specified)")
	durationSec := flag.Int("duration", 0, "Duration in seconds (0 means run until interrupted)")
	flag.Parse()

	rKey := *routingKey
	if rKey == "" {
		rKey = *queueName
	}

	connectionURL := *rabbitURL
	if connectionURL == "" {
		connectionURL = fmt.Sprintf("amqp://%s:%s@%s:%d%s",
			*username, *password, *host, *port, *vhost)
	}

	config := Config{
		NumberOfProducers: *numProducers,
		MessagesPerSecond: *mps,
		RabbitMQURL:       connectionURL,
		Host:              *host,
		Port:              *port,
		Username:          *username,
		Password:          *password,
		QueueName:         *queueName,
		Exchange:          *exchange,
		RoutingKey:        rKey,
	}

	// Connect to RabbitMQ
	log.Printf("Connecting to RabbitMQ at %s:%d (user: %s)", config.Host, config.Port, config.Username)
	conn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Setup context for graceful shutdown
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

	totalMPS := config.NumberOfProducers * config.MessagesPerSecond
	bytesPerSec := totalMPS * 512 // Each message is exactly 512 bytes
	log.Printf("Starting %d producers, each sending %d messages/sec (512 bytes each)",
		config.NumberOfProducers, config.MessagesPerSecond)
	log.Printf("Total throughput: %d messages/sec (%.2f MB/sec)",
		totalMPS, float64(bytesPerSec)/(1024*1024))

	producers := make([]*Producer, config.NumberOfProducers)
	var wg sync.WaitGroup

	for i := 0; i < config.NumberOfProducers; i++ {
		producer, err := NewProducer(i, config, conn)
		if err != nil {
			log.Fatalf("Failed to create producer %d: %v", i, err)
		}

		producers[i] = producer
		wg.Add(1)
		go producer.Start(ctx, &wg)
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
				var totalMessages, totalErrors uint64
				for _, p := range producers {
					stats := p.GetStats()
					totalMessages += stats.MessagesSent
					totalErrors += stats.Errors
				}

				elapsedSecs := time.Since(startTime).Seconds()
				overallRate := float64(totalMessages) / elapsedSecs
				mbPerSec := (overallRate * 512) / (1024 * 1024)

				log.Printf("OVERALL: Sent %d messages (%.2f msgs/sec, %.2f MB/sec), %d errors",
					totalMessages, overallRate, mbPerSec, totalErrors)
			}
		}
	}()

	wg.Wait()

	var totalMessages, totalErrors uint64
	for _, p := range producers {
		stats := p.GetStats()
		totalMessages += stats.MessagesSent
		totalErrors += stats.Errors
	}

	elapsedSecs := time.Since(producers[0].Stats.StartTime).Seconds()
	overallRate := float64(totalMessages) / elapsedSecs
	mbPerSec := (overallRate * 512) / (1024 * 1024)

	log.Printf("All producers finished")
	log.Printf("Total messages sent: %d", totalMessages)
	log.Printf("Average rate: %.2f messages/sec (%.2f MB/sec)", overallRate, mbPerSec)
	log.Printf("Total errors: %d", totalErrors)
	log.Printf("Total runtime: %.2f seconds", elapsedSecs)
}
