package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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
	ExchangeName      string
	RoutingKey        string
	CertFile          string
	EnableTLS         bool
	DumpJSON          bool
	JSONOutputFile    string
}

// Stats tracks performance metrics
type Stats struct {
	MessagesReceived uint64
	StartTime        time.Time
}

// MessageData represents a message received from RabbitMQ
type MessageData struct {
	Body         string                 `json:"body"`
	DeliveryTag  uint64                 `json:"delivery_tag"`
	ConsumerTag  string                 `json:"consumer_tag"`
	MessageCount uint32                 `json:"message_count,omitempty"`
	Exchange     string                 `json:"exchange"`
	RoutingKey   string                 `json:"routing_key"`
	Timestamp    time.Time              `json:"timestamp,omitempty"`
	Headers      map[string]interface{} `json:"headers,omitempty"`
	ContentType  string                 `json:"content_type,omitempty"`
	ConsumerID   int                    `json:"consumer_id"`
	ReceivedAt   time.Time              `json:"received_at"`
}

// Consumer represents a message consumer
type Consumer struct {
	ID           int
	Config       Config
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	Stats        Stats
	StopChan     chan struct{}
	MessagesChan chan MessageData
}

// NewConsumer creates a new consumer
func NewConsumer(id int, config Config, conn *amqp.Connection, messagesChan chan MessageData) (*Consumer, error) {
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
		StopChan:     make(chan struct{}),
		MessagesChan: messagesChan,
	}, nil
}

// Start begins consuming messages
func (c *Consumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.Channel.Close()

	log.Printf("Consumer %d starting", c.ID)

	// Declare the exchange if specified
	if c.Config.ExchangeName != "" {
		err := c.Channel.ExchangeDeclare(
			c.Config.ExchangeName, // name
			"direct",              // type (default to topic, can be customized if needed)
			true,                  // durable
			true,                  // auto-deleted
			false,                 // internal
			false,                 // no-wait
			nil,                   // arguments
		)
		if err != nil {
			log.Printf("Consumer %d: Failed to declare exchange: %v", c.ID, err)
			return
		}
	}

	// Declare the queue
	q, err := c.Channel.QueueDeclare(
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

	// Bind the queue to the exchange with routing key if both are specified
	if c.Config.ExchangeName != "" && c.Config.RoutingKey != "" {
		err = c.Channel.QueueBind(
			q.Name,                // queue name
			c.Config.RoutingKey,   // routing key
			c.Config.ExchangeName, // exchange
			false,
			nil,
		)
		if err != nil {
			log.Printf("Consumer %d: Failed to bind queue: %v", c.ID, err)
			return
		}
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

		case d, ok := <-deliveries:
			if !ok {
				log.Printf("Consumer %d: Channel closed", c.ID)
				return
			}

			atomic.AddUint64(&c.Stats.MessagesReceived, 1)

			// If JSON dumping is enabled, send message data to the channel
			if c.Config.DumpJSON {
				// Extract headers
				headers := make(map[string]interface{})
				if d.Headers != nil {
					for k, v := range d.Headers {
						headers[k] = v
					}
				}

				msgData := MessageData{
					Body:         string(d.Body),
					DeliveryTag:  d.DeliveryTag,
					ConsumerTag:  d.ConsumerTag,
					MessageCount: d.MessageCount,
					Exchange:     d.Exchange,
					RoutingKey:   d.RoutingKey,
					Timestamp:    d.Timestamp,
					Headers:      headers,
					ContentType:  d.ContentType,
					ConsumerID:   c.ID,
					ReceivedAt:   time.Now(),
				}

				// Non-blocking send to the messages channel
				select {
				case c.MessagesChan <- msgData:
					// Successfully sent
				default:
					// Channel buffer is full, dropping this message
					log.Printf("Consumer %d: Message buffer full, dropping message", c.ID)
				}
			}
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

// JSONDumper listens for messages and writes them to a JSON file
func JSONDumper(ctx context.Context, wg *sync.WaitGroup, messagesChan chan MessageData, outputFile string) {
	defer wg.Done()

	log.Printf("JSON dumper starting, output file: %s", outputFile)

	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create JSON output file: %v", err)
	}
	defer file.Close()

	// Write the opening bracket for the JSON array
	file.WriteString("[\n")

	var messageCount int64

	for {
		select {
		case <-ctx.Done():
			// Write the closing bracket for the JSON array
			file.WriteString("\n]")
			log.Printf("JSON dumper shutting down, wrote %d messages", messageCount)
			return

		case msg, ok := <-messagesChan:
			if !ok {
				// Channel was closed
				file.WriteString("\n]")
				log.Printf("JSON dumper shutting down (channel closed), wrote %d messages", messageCount)
				return
			}

			// Add comma for all messages except the first one
			if messageCount > 0 {
				file.WriteString(",\n")
			}

			// Convert message to JSON and write to file
			jsonData, err := json.MarshalIndent(msg, "  ", "  ")
			if err != nil {
				log.Printf("Failed to marshal message to JSON: %v", err)
				continue
			}

			_, err = file.Write(jsonData)
			if err != nil {
				log.Printf("Failed to write message to file: %v", err)
				continue
			}

			messageCount++
		}
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
	exchangeName := flag.String("exchange", "", "Exchange name (if empty, won't bind to an exchange)")
	routingKey := flag.String("routing-key", "#", "Routing key for binding queue to exchange")

	enableTLS := flag.Bool("tls", false, "Enable TLS connection")
	certFile := flag.String("cert", "", "Path to TLS certificate file (PEM format)")

	dumpJSON := flag.Bool("json", false, "Dump messages to JSON file")
	jsonFile := flag.String("json-file", "messages.json", "Output JSON file path")

	durationSec := flag.Int("duration", 0, "Duration in seconds (0 means run until interrupted)")
	flag.Parse()

	config := Config{
		NumberOfConsumers: *numConsumers,
		PrefetchCount:     *prefetch,
		Host:              *host,
		Port:              *port,
		Username:          *username,
		Password:          *password,
		QueueName:         *queueName,
		ExchangeName:      *exchangeName,
		RoutingKey:        *routingKey,
		EnableTLS:         *enableTLS,
		CertFile:          *certFile,
		DumpJSON:          *dumpJSON,
		JSONOutputFile:    *jsonFile,
	}

	// Build connection URL
	var conn *amqp.Connection
	var err error

	log.Printf("Connecting to RabbitMQ at %s:%d (user: %s)", config.Host, config.Port, config.Username)

	if config.EnableTLS {
		if config.CertFile == "" {
			log.Fatalf("TLS enabled but no certificate file provided")
		}

		log.Printf("TLS enabled, using certificate: %s", config.CertFile)

		// Read the certificate file
		cert, err := os.ReadFile(config.CertFile)
		if err != nil {
			log.Fatalf("Failed to read TLS certificate file: %v", err)
		}

		// Create a certificate pool and add the certificate
		certPool := x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(cert); !ok {
			log.Fatalf("Failed to parse certificate")
		}

		// Create TLS config with the certificate
		tlsConfig := &tls.Config{
			RootCAs: certPool,
		}

		// Use the AMQPS protocol for TLS
		connectionURL := *rabbitURL
		if connectionURL == "" {
			connectionURL = fmt.Sprintf("amqps://%s:%s@%s:%d%s",
				*username, *password, *host, *port, *vhost)
		}

		// Connect with TLS config
		conn, err = amqp.DialTLS(connectionURL, tlsConfig)
		if err != nil {
			log.Fatalf("Failed to connect to RabbitMQ with TLS: %v", err)
		}
	} else {
		// Use standard AMQP protocol without TLS
		connectionURL := *rabbitURL
		if connectionURL == "" {
			connectionURL = fmt.Sprintf("amqp://%s:%s@%s:%d%s",
				*username, *password, *host, *port, *vhost)
		}

		config.RabbitMQURL = connectionURL
		conn, err = amqp.Dial(config.RabbitMQURL)
		if err != nil {
			log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		}
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

	// Create a channel for message data if JSON dumping is enabled
	var messagesChan chan MessageData
	if config.DumpJSON {
		messagesChan = make(chan MessageData, 10000) // Buffer up to 10K messages
	}

	log.Printf("Starting %d consumers with prefetch count %d from queue '%s'",
		config.NumberOfConsumers, config.PrefetchCount, config.QueueName)

	if config.ExchangeName != "" {
		log.Printf("Will bind queue '%s' to exchange '%s' with routing key '%s'",
			config.QueueName, config.ExchangeName, config.RoutingKey)
	}

	consumers := make([]*Consumer, config.NumberOfConsumers)
	var wg sync.WaitGroup

	// Start the JSON dumper if enabled
	if config.DumpJSON {
		log.Printf("JSON dumping enabled, messages will be written to: %s", config.JSONOutputFile)
		wg.Add(1)
		go JSONDumper(ctx, &wg, messagesChan, config.JSONOutputFile)
	}

	for i := 0; i < config.NumberOfConsumers; i++ {
		consumer, err := NewConsumer(i, config, conn, messagesChan)
		if err != nil {
			log.Print("Failed to create consumer: %w", err)
			continue
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

	// Close message channel if it exists
	if messagesChan != nil {
		close(messagesChan)
	}

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
