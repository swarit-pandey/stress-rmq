package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	ClientID           string
	ProduceRate        int
	ConsumeQueue       string
	ProduceQueue       string
	RabbitMQURL        string
	Host               string
	Port               int
	Username           string
	Password           string
	EnableTLS          bool
	CertFile           string
	InsecureSkipVerify bool
	MessageSize        int
	ExchangeName       string
	ProcessingTimeMs   int
	ForwardRatio       float64 // Ratio of consumed messages to forward (0.0-1.0)
	DurationSec        int
}

// Stats tracks performance metrics
type Stats struct {
	MessagesProduced  uint64
	MessagesConsumed  uint64
	MessagesForwarded uint64
	Errors            uint64
	StartTime         time.Time
}

// Client represents a prosumer (producer + consumer) client
type Client struct {
	Config       Config
	Connection   *amqp.Connection
	ProducerChan *amqp.Channel
	ConsumerChan *amqp.Channel
	Stats        Stats
	StopChan     chan struct{}
	mu           sync.Mutex
	reconnectMu  sync.Mutex
}

// NewClient creates a new prosumer client
func NewClient(config Config) (*Client, error) {
	log.Printf("Client %s: Creating connection to RabbitMQ at %s:%d (user: %s)",
		config.ClientID, config.Host, config.Port, config.Username)

	var conn *amqp.Connection
	var err error

	// Set connection properties
	connectionProps := amqp.Table{
		"connection_name": fmt.Sprintf("prosumer-%s", config.ClientID),
	}

	if config.EnableTLS {
		// Create TLS config
		tlsConfig := &tls.Config{
			InsecureSkipVerify: config.InsecureSkipVerify,
		}

		// If certificate file is provided, add it to the cert pool
		if config.CertFile != "" {
			// Read the certificate file
			cert, err := os.ReadFile(config.CertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read TLS certificate file: %w", err)
			}

			// Create a certificate pool and add the certificate
			certPool := x509.NewCertPool()
			if ok := certPool.AppendCertsFromPEM(cert); !ok {
				return nil, fmt.Errorf("failed to parse certificate")
			}

			// Add the cert pool to the TLS config
			tlsConfig.RootCAs = certPool
		}

		// Use the AMQPS protocol for TLS
		connectionURL := config.RabbitMQURL
		if connectionURL == "" {
			connectionURL = fmt.Sprintf("amqps://%s:%s@%s:%d/",
				config.Username, config.Password, config.Host, config.Port)
		}

		// Connect with TLS config
		conn, err = amqp.DialTLS(connectionURL, tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to RabbitMQ with TLS: %w", err)
		}
	} else {
		// Use standard AMQP protocol without TLS
		connectionURL := config.RabbitMQURL
		if connectionURL == "" {
			connectionURL = fmt.Sprintf("amqp://%s:%s@%s:%d/",
				config.Username, config.Password, config.Host, config.Port)
		}

		// Create a connection with connection name
		conn, err = amqp.DialConfig(connectionURL, amqp.Config{
			Properties: connectionProps,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
		}
	}

	// Create separate channels for producing and consuming
	producerChan, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create producer channel: %w", err)
	}

	consumerChan, err := conn.Channel()
	if err != nil {
		producerChan.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to create consumer channel: %w", err)
	}

	// Set QoS on consumer channel
	err = consumerChan.Qos(10, 0, false) // Prefetch count of 10
	if err != nil {
		consumerChan.Close()
		producerChan.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	client := &Client{
		Config:       config,
		Connection:   conn,
		ProducerChan: producerChan,
		ConsumerChan: consumerChan,
		Stats: Stats{
			StartTime: time.Now(),
		},
		StopChan: make(chan struct{}),
	}

	// Setup the topology
	if err := client.setupTopology(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to setup topology: %w", err)
	}

	return client, nil
}

// setupTopology sets up exchanges and queues
func (c *Client) setupTopology() error {
	// Declare exchange if specified
	if c.Config.ExchangeName != "" {
		err := c.ProducerChan.ExchangeDeclare(
			c.Config.ExchangeName, // name
			"direct",              // type
			true,                  // durable
			false,                 // auto-deleted
			false,                 // internal
			false,                 // no-wait
			nil,                   // arguments
		)
		if err != nil {
			return fmt.Errorf("failed to declare exchange: %w", err)
		}
	}

	// Declare consume queue
	_, err := c.ConsumerChan.QueueDeclare(
		c.Config.ConsumeQueue, // name
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare consume queue: %w", err)
	}

	// Declare produce queue
	_, err = c.ProducerChan.QueueDeclare(
		c.Config.ProduceQueue, // name
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare produce queue: %w", err)
	}

	// Bind queues to exchange if specified
	if c.Config.ExchangeName != "" {
		err = c.ConsumerChan.QueueBind(
			c.Config.ConsumeQueue, // queue name
			c.Config.ConsumeQueue, // routing key
			c.Config.ExchangeName, // exchange
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to bind consume queue: %w", err)
		}

		err = c.ProducerChan.QueueBind(
			c.Config.ProduceQueue, // queue name
			c.Config.ProduceQueue, // routing key
			c.Config.ExchangeName, // exchange
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to bind produce queue: %w", err)
		}
	}

	return nil
}

// Start launches the producer and consumer routines
func (c *Client) Start(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(2) // One for producer, one for consumer

	// Start producer goroutine
	go c.runProducer(ctx, wg)

	// Start consumer goroutine
	go c.runConsumer(ctx, wg)

	// Start stats reporter
	go c.reportStats(ctx)
}

// runProducer handles the message production
func (c *Client) runProducer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.ProducerChan.Close()

	log.Printf("Client %s: Producer started, sending to queue '%s'",
		c.Config.ClientID, c.Config.ProduceQueue)

	// Create a base message with the configured size
	baseMessage := make([]byte, c.Config.MessageSize)
	for i := range baseMessage {
		baseMessage[i] = byte(i % 256)
	}

	// Calculate interval between messages based on configured rate
	var interval time.Duration
	if c.Config.ProduceRate > 0 {
		interval = time.Second / time.Duration(c.Config.ProduceRate)
	} else {
		interval = time.Millisecond // Default if rate is 0
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Client %s: Producer shutting down (context cancelled)", c.Config.ClientID)
			return

		case <-c.StopChan:
			log.Printf("Client %s: Producer shutting down (stop signal received)", c.Config.ClientID)
			return

		case <-ticker.C:
			// Clone the message and add metadata
			message := make([]byte, len(baseMessage))
			copy(message, baseMessage)

			// Add metadata in first 16 bytes:
			// - Client ID (4 bytes)
			// - Message number (8 bytes)
			// - Timestamp (4 bytes)
			clientIDNum := uint32(fnv32(c.Config.ClientID))
			msgCount := atomic.LoadUint64(&c.Stats.MessagesProduced)
			timestamp := uint32(time.Now().Unix())

			// Add client ID
			for i := 0; i < 4; i++ {
				message[i] = byte(clientIDNum >> (i * 8))
			}

			// Add message count
			for i := 0; i < 8; i++ {
				message[4+i] = byte(msgCount >> (i * 8))
			}

			// Add timestamp
			for i := 0; i < 4; i++ {
				message[12+i] = byte(timestamp >> (i * 8))
			}

			// Publish the message
			err := c.publishMessage(ctx, message, false) // Regular production
			if err != nil {
				atomic.AddUint64(&c.Stats.Errors, 1)
				log.Printf("Client %s: Failed to publish message: %v", c.Config.ClientID, err)

				if err = c.reconnectProducer(); err != nil {
					log.Printf("Client %s: Failed to reconnect producer: %v", c.Config.ClientID, err)
					time.Sleep(time.Second) // Avoid flooding logs
					continue
				}
				continue
			}

			atomic.AddUint64(&c.Stats.MessagesProduced, 1)
		}
	}
}

// runConsumer handles message consumption
func (c *Client) runConsumer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.ConsumerChan.Close()

	log.Printf("Client %s: Consumer started, consuming from queue '%s'",
		c.Config.ClientID, c.Config.ConsumeQueue)

	// Start consuming
	deliveries, err := c.ConsumerChan.Consume(
		c.Config.ConsumeQueue,                         // queue
		fmt.Sprintf("consumer-%s", c.Config.ClientID), // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Printf("Client %s: Failed to register consumer: %v", c.Config.ClientID, err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Client %s: Consumer shutting down (context cancelled)", c.Config.ClientID)
			return

		case <-c.StopChan:
			log.Printf("Client %s: Consumer shutting down (stop signal received)", c.Config.ClientID)
			return

		case delivery, ok := <-deliveries:
			if !ok {
				log.Printf("Client %s: Delivery channel closed, attempting to reconnect consumer",
					c.Config.ClientID)
				if err := c.reconnectConsumer(); err != nil {
					log.Printf("Client %s: Failed to reconnect consumer: %v", c.Config.ClientID, err)
					time.Sleep(time.Second)
					continue
				}

				// Get a new delivery channel
				deliveries, err = c.ConsumerChan.Consume(
					c.Config.ConsumeQueue,
					fmt.Sprintf("consumer-%s", c.Config.ClientID),
					false,
					false,
					false,
					false,
					nil,
				)
				if err != nil {
					log.Printf("Client %s: Failed to register consumer after reconnect: %v",
						c.Config.ClientID, err)
					time.Sleep(time.Second)
				}
				continue
			}

			// Process the message
			atomic.AddUint64(&c.Stats.MessagesConsumed, 1)

			// Simulate processing time
			if c.Config.ProcessingTimeMs > 0 {
				time.Sleep(time.Duration(c.Config.ProcessingTimeMs) * time.Millisecond)
			}

			// Check if this message should be forwarded
			shouldForward := c.Config.ForwardRatio > 0 &&
				(float64(atomic.LoadUint64(&c.Stats.MessagesForwarded))/
					float64(atomic.LoadUint64(&c.Stats.MessagesConsumed))) < c.Config.ForwardRatio

			if shouldForward {
				err := c.publishMessage(ctx, delivery.Body, true) // Forwarded message
				if err != nil {
					log.Printf("Client %s: Failed to forward message: %v", c.Config.ClientID, err)
					atomic.AddUint64(&c.Stats.Errors, 1)
				} else {
					atomic.AddUint64(&c.Stats.MessagesForwarded, 1)
				}
			}

			// Acknowledge the message
			if err := delivery.Ack(false); err != nil {
				log.Printf("Client %s: Failed to acknowledge message: %v", c.Config.ClientID, err)
				atomic.AddUint64(&c.Stats.Errors, 1)
			}
		}
	}
}

// publishMessage publishes a message to the produce queue
func (c *Client) publishMessage(ctx context.Context, body []byte, isForwarded bool) error {
	headers := amqp.Table{
		"client_id":    c.Config.ClientID,
		"timestamp":    time.Now().UnixNano(),
		"is_forwarded": isForwarded,
	}

	return c.ProducerChan.PublishWithContext(
		ctx,
		c.Config.ExchangeName, // exchange
		c.Config.ProduceQueue, // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			DeliveryMode: amqp.Persistent,
			Headers:      headers,
			Body:         body,
		},
	)
}

// reconnectProducer attempts to reconnect the producer channel
func (c *Client) reconnectProducer() error {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()

	// Close existing channel
	if c.ProducerChan != nil {
		c.ProducerChan.Close()
	}

	// Try to get a new channel
	var err error
	c.ProducerChan, err = c.Connection.Channel()
	if err != nil {
		// Channel failed, try to reconnect the entire connection
		if err := c.reconnectConnection(); err != nil {
			return fmt.Errorf("failed to reconnect: %w", err)
		}

		// Try again to get a channel
		c.ProducerChan, err = c.Connection.Channel()
		if err != nil {
			return fmt.Errorf("failed to create producer channel after reconnect: %w", err)
		}
	}

	// Redeclare the queue and exchange
	err = c.setupTopology()
	if err != nil {
		return fmt.Errorf("failed to setup topology after reconnect: %w", err)
	}

	return nil
}

// reconnectConsumer attempts to reconnect the consumer channel
func (c *Client) reconnectConsumer() error {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()

	// Close existing channel
	if c.ConsumerChan != nil {
		c.ConsumerChan.Close()
	}

	// Try to get a new channel
	var err error
	c.ConsumerChan, err = c.Connection.Channel()
	if err != nil {
		// Channel failed, try to reconnect the entire connection
		if err := c.reconnectConnection(); err != nil {
			return fmt.Errorf("failed to reconnect: %w", err)
		}

		// Try again to get a channel
		c.ConsumerChan, err = c.Connection.Channel()
		if err != nil {
			return fmt.Errorf("failed to create consumer channel after reconnect: %w", err)
		}
	}

	// Set QoS on consumer channel
	err = c.ConsumerChan.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("failed to set QoS after reconnect: %w", err)
	}

	// Redeclare the queue and exchange
	err = c.setupTopology()
	if err != nil {
		return fmt.Errorf("failed to setup topology after reconnect: %w", err)
	}

	return nil
}

// reconnectConnection attempts to reconnect the connection
func (c *Client) reconnectConnection() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Close existing connection
	if c.Connection != nil {
		c.Connection.Close()
	}

	// Recreate connection with same parameters
	var conn *amqp.Connection
	var err error

	connectionProps := amqp.Table{
		"connection_name": fmt.Sprintf("prosumer-%s", c.Config.ClientID),
	}

	if c.Config.EnableTLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: c.Config.InsecureSkipVerify,
		}

		if c.Config.CertFile != "" {
			cert, err := os.ReadFile(c.Config.CertFile)
			if err != nil {
				return fmt.Errorf("failed to read TLS certificate file: %w", err)
			}

			certPool := x509.NewCertPool()
			if ok := certPool.AppendCertsFromPEM(cert); !ok {
				return fmt.Errorf("failed to parse certificate")
			}

			tlsConfig.RootCAs = certPool
		}

		connectionURL := c.Config.RabbitMQURL
		if connectionURL == "" {
			connectionURL = fmt.Sprintf("amqps://%s:%s@%s:%d/",
				c.Config.Username, c.Config.Password, c.Config.Host, c.Config.Port)
		}

		conn, err = amqp.DialTLS(connectionURL, tlsConfig)
		if err != nil {
			return fmt.Errorf("failed to reconnect with TLS: %w", err)
		}
	} else {
		connectionURL := c.Config.RabbitMQURL
		if connectionURL == "" {
			connectionURL = fmt.Sprintf("amqp://%s:%s@%s:%d/",
				c.Config.Username, c.Config.Password, c.Config.Host, c.Config.Port)
		}

		conn, err = amqp.DialConfig(connectionURL, amqp.Config{
			Properties: connectionProps,
		})
		if err != nil {
			return fmt.Errorf("failed to reconnect: %w", err)
		}
	}

	c.Connection = conn
	return nil
}

// reportStats periodically logs statistics
func (c *Client) reportStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.StopChan:
			return
		case <-ticker.C:
			produced := atomic.LoadUint64(&c.Stats.MessagesProduced)
			consumed := atomic.LoadUint64(&c.Stats.MessagesConsumed)
			forwarded := atomic.LoadUint64(&c.Stats.MessagesForwarded)
			errors := atomic.LoadUint64(&c.Stats.Errors)

			elapsedSecs := time.Since(c.Stats.StartTime).Seconds()
			produceRate := float64(produced) / elapsedSecs
			consumeRate := float64(consumed) / elapsedSecs

			log.Printf("Client %s: Produced: %d (%.2f/sec), Consumed: %d (%.2f/sec), Forwarded: %d, Errors: %d",
				c.Config.ClientID, produced, produceRate, consumed, consumeRate, forwarded, errors)
		}
	}
}

// GetStats returns current statistics
func (c *Client) GetStats() Stats {
	return Stats{
		MessagesProduced:  atomic.LoadUint64(&c.Stats.MessagesProduced),
		MessagesConsumed:  atomic.LoadUint64(&c.Stats.MessagesConsumed),
		MessagesForwarded: atomic.LoadUint64(&c.Stats.MessagesForwarded),
		Errors:            atomic.LoadUint64(&c.Stats.Errors),
		StartTime:         c.Stats.StartTime,
	}
}

// Close cleans up resources
func (c *Client) Close() {
	if c.ProducerChan != nil {
		c.ProducerChan.Close()
	}
	if c.ConsumerChan != nil {
		c.ConsumerChan.Close()
	}
	if c.Connection != nil {
		c.Connection.Close()
	}
}

// Stop signals the client to stop
func (c *Client) Stop() {
	close(c.StopChan)
}

// fnv32 computes a 32-bit FNV-1a hash for a string
func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func main() {
	// Client A configuration flags
	clientAID := flag.String("client-a-id", "ClientA", "ID for Client A")
	clientAConsumeQueue := flag.String("client-a-consume", "queue-b", "Queue for Client A to consume from")
	clientAProduceQueue := flag.String("client-a-produce", "queue-a", "Queue for Client A to produce to")
	clientAProduceRate := flag.Int("client-a-rate", 10, "Messages per second for Client A to produce")
	clientAForwardRatio := flag.Float64("client-a-forward", 0.5, "Ratio of messages for Client A to forward (0.0-1.0)")

	// Client B configuration flags
	clientBID := flag.String("client-b-id", "ClientB", "ID for Client B")
	clientBConsumeQueue := flag.String("client-b-consume", "queue-a", "Queue for Client B to consume from")
	clientBProduceQueue := flag.String("client-b-produce", "queue-b", "Queue for Client B to produce to")
	clientBProduceRate := flag.Int("client-b-rate", 10, "Messages per second for Client B to produce")
	clientBForwardRatio := flag.Float64("client-b-forward", 0.5, "Ratio of messages for Client B to forward (0.0-1.0)")

	// Common configuration flags
	rabbitURL := flag.String("url", "", "RabbitMQ connection URL (overrides other connection params if provided)")
	host := flag.String("host", "localhost", "RabbitMQ server hostname or IP address")
	port := flag.Int("port", 5672, "RabbitMQ server port")
	username := flag.String("username", "guest", "RabbitMQ username")
	password := flag.String("password", "guest", "RabbitMQ password")
	vhost := flag.String("vhost", "/", "RabbitMQ virtual host")

	enableTLS := flag.Bool("tls", false, "Enable TLS connection")
	certFile := flag.String("cert", "", "Path to TLS certificate file (PEM format)")
	insecureSkipVerify := flag.Bool("insecure", false, "Skip TLS certificate verification")

	messageSize := flag.Int("msg-size", 512, "Size of messages in bytes")
	processingTime := flag.Int("process-time", 5, "Message processing time in milliseconds")
	exchangeName := flag.String("exchange", "", "Exchange name (if empty, direct queue publishing is used)")

	durationSec := flag.Int("duration", 0, "Duration in seconds (0 means run until interrupted)")

	flag.Parse()

	// Prepare connection URL
	connectionURL := *rabbitURL
	if connectionURL == "" {
		protocol := "amqp"
		if *enableTLS {
			protocol = "amqps"
		}
		connectionURL = fmt.Sprintf("%s://%s:%s@%s:%d%s",
			protocol, *username, *password, *host, *port, *vhost)
	}

	// Common configuration
	commonConfig := Config{
		RabbitMQURL:        connectionURL,
		Host:               *host,
		Port:               *port,
		Username:           *username,
		Password:           *password,
		EnableTLS:          *enableTLS,
		CertFile:           *certFile,
		InsecureSkipVerify: *insecureSkipVerify,
		MessageSize:        *messageSize,
		ExchangeName:       *exchangeName,
		ProcessingTimeMs:   *processingTime,
		DurationSec:        *durationSec,
	}

	// Client A configuration
	clientAConfig := commonConfig
	clientAConfig.ClientID = *clientAID
	clientAConfig.ConsumeQueue = *clientAConsumeQueue
	clientAConfig.ProduceQueue = *clientAProduceQueue
	clientAConfig.ProduceRate = *clientAProduceRate
	clientAConfig.ForwardRatio = *clientAForwardRatio

	// Client B configuration
	clientBConfig := commonConfig
	clientBConfig.ClientID = *clientBID
	clientBConfig.ConsumeQueue = *clientBConsumeQueue
	clientBConfig.ProduceQueue = *clientBProduceQueue
	clientBConfig.ProduceRate = *clientBProduceRate
	clientBConfig.ForwardRatio = *clientBForwardRatio

	log.Printf("Starting prosumer clients...")
	log.Printf("Client A (%s): Consuming from '%s', Producing to '%s', Rate: %d msgs/sec",
		clientAConfig.ClientID, clientAConfig.ConsumeQueue, clientAConfig.ProduceQueue, clientAConfig.ProduceRate)
	log.Printf("Client B (%s): Consuming from '%s', Producing to '%s', Rate: %d msgs/sec",
		clientBConfig.ClientID, clientBConfig.ConsumeQueue, clientBConfig.ProduceQueue, clientBConfig.ProduceRate)

	// Set up context for graceful shutdown
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

	// Create and start Client A
	clientA, err := NewClient(clientAConfig)
	if err != nil {
		log.Fatalf("Failed to create Client A: %v", err)
	}
	defer clientA.Close()

	// Create and start Client B
	clientB, err := NewClient(clientBConfig)
	if err != nil {
		clientA.Close()
		log.Fatalf("Failed to create Client B: %v", err)
	}
	defer clientB.Close()

	// Wait group for graceful shutdown
	var wg sync.WaitGroup

	// Start both clients
	clientA.Start(ctx, &wg)
	clientB.Start(ctx, &wg)

	// Wait for completion
	wg.Wait()

	// Print final statistics
	clientAStats := clientA.GetStats()
	clientBStats := clientB.GetStats()

	log.Printf("\nFinal Statistics:")
	log.Printf("Client %s: Produced: %d, Consumed: %d, Forwarded: %d, Errors: %d, Runtime: %.2f sec",
		clientAConfig.ClientID,
		clientAStats.MessagesProduced,
		clientAStats.MessagesConsumed,
		clientAStats.MessagesForwarded,
		clientAStats.Errors,
		time.Since(clientAStats.StartTime).Seconds())

	log.Printf("Client %s: Produced: %d, Consumed: %d, Forwarded: %d, Errors: %d, Runtime: %.2f sec",
		clientBConfig.ClientID,
		clientBStats.MessagesProduced,
		clientBStats.MessagesConsumed,
		clientBStats.MessagesForwarded,
		clientBStats.Errors,
		time.Since(clientBStats.StartTime).Seconds())
}
