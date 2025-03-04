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
	NumberOfProducers  int
	MessagesPerSecond  int
	RabbitMQURL        string
	Host               string
	Port               int
	Username           string
	Password           string
	QueueName          string
	Exchange           string
	RoutingKey         string
	ConnectionName     string
	EnableTLS          bool
	CertFile           string
	InsecureSkipVerify bool
}

// Stats tracks performance metrics
type Stats struct {
	MessagesSent uint64
	Errors       uint64
	StartTime    time.Time
}

// ConnectionManager handles the shared RabbitMQ connection
type ConnectionManager struct {
	Connection *amqp.Connection
	Config     Config
	mu         sync.Mutex
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(config Config) (*ConnectionManager, error) {
	var conn *amqp.Connection
	var err error

	log.Printf("Creating connection '%s' to RabbitMQ at %s:%d (user: %s)",
		config.ConnectionName, config.Host, config.Port, config.Username)

	// Set connection properties
	connectionProps := amqp.Table{
		"connection_name": config.ConnectionName,
	}

	if config.EnableTLS {
		if config.CertFile == "" && !config.InsecureSkipVerify {
			log.Printf("Warning: TLS enabled without certificate file and InsecureSkipVerify is false")
		}

		if config.CertFile != "" {
			log.Printf("TLS enabled, using certificate: %s", config.CertFile)
		}

		if config.InsecureSkipVerify {
			log.Printf("TLS verification disabled (InsecureSkipVerify=true)")
		}

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

	return &ConnectionManager{
		Connection: conn,
		Config:     config,
	}, nil
}

// GetChannel returns a new channel from the shared connection
func (cm *ConnectionManager) GetChannel() (*amqp.Channel, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.Connection == nil || cm.Connection.IsClosed() {
		// Attempt to reconnect
		var conn *amqp.Connection
		var err error

		if cm.Config.EnableTLS {
			// Create TLS config for reconnection
			tlsConfig := &tls.Config{
				InsecureSkipVerify: cm.Config.InsecureSkipVerify,
			}

			// If certificate file is provided, add it to the cert pool
			if cm.Config.CertFile != "" {
				// Read the certificate file
				cert, err := os.ReadFile(cm.Config.CertFile)
				if err != nil {
					return nil, fmt.Errorf("failed to read TLS certificate file for reconnection: %w", err)
				}

				// Create a certificate pool and add the certificate
				certPool := x509.NewCertPool()
				if ok := certPool.AppendCertsFromPEM(cert); !ok {
					return nil, fmt.Errorf("failed to parse certificate for reconnection")
				}

				// Add the cert pool to the TLS config
				tlsConfig.RootCAs = certPool
			}

			// Use the AMQPS protocol for TLS
			connectionURL := cm.Config.RabbitMQURL
			if connectionURL == "" {
				connectionURL = fmt.Sprintf("amqps://%s:%s@%s:%d/",
					cm.Config.Username, cm.Config.Password, cm.Config.Host, cm.Config.Port)
			}

			// Connect with TLS config
			conn, err = amqp.DialTLS(connectionURL, tlsConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to reconnect to RabbitMQ with TLS: %w", err)
			}
		} else {
			// Standard AMQP reconnection
			connectionURL := cm.Config.RabbitMQURL
			if connectionURL == "" {
				connectionURL = fmt.Sprintf("amqp://%s:%s@%s:%d/",
					cm.Config.Username, cm.Config.Password, cm.Config.Host, cm.Config.Port)
			}

			conn, err = amqp.DialConfig(connectionURL, amqp.Config{
				Properties: amqp.Table{
					"connection_name": cm.Config.ConnectionName,
				},
			})
			if err != nil {
				return nil, fmt.Errorf("failed to reconnect to RabbitMQ: %w", err)
			}
		}

		cm.Connection = conn
	}

	return cm.Connection.Channel()
}

// Close closes the underlying connection
func (cm *ConnectionManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.Connection != nil && !cm.Connection.IsClosed() {
		return cm.Connection.Close()
	}
	return nil
}

// Producer represents a message producer
type Producer struct {
	ID          int
	Config      Config
	Channel     *amqp.Channel
	Stats       Stats
	StopChan    chan struct{}
	ConnManager *ConnectionManager
	ReconnectMu sync.Mutex
}

// NewProducer creates a new producer using a channel from the shared connection
func NewProducer(id int, config Config, connManager *ConnectionManager) (*Producer, error) {
	// Get a channel from the connection manager
	ch, err := connManager.GetChannel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	producer := &Producer{
		ID:      id,
		Config:  config,
		Channel: ch,
		Stats: Stats{
			StartTime: time.Now(),
		},
		StopChan:    make(chan struct{}),
		ConnManager: connManager,
	}

	// Setup topology
	if err := producer.setupTopology(); err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to setup topology: %w", err)
	}

	return producer, nil
}

// Start begins publishing messages at the configured rate
func (p *Producer) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() {
		if p.Channel != nil {
			p.Channel.Close()
		}
	}()

	log.Printf("Producer %d starting", p.ID)

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

			err := p.publish(ctx, message, msgCount)
			if err != nil {
				atomic.AddUint64(&p.Stats.Errors, 1)
				log.Printf("Producer %d: Failed to publish message: %v", p.ID, err)

				// Attempt to reconnect
				if err = p.reconnect(); err != nil {
					log.Printf("Producer %d: Failed to reconnect: %v", p.ID, err)
					time.Sleep(time.Second) // Avoid flooding logs on error
					continue
				}

				// Try again after reconnecting
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

// setupTopology sets up the RabbitMQ queues and exchanges
func (p *Producer) setupTopology() error {
	_, err := p.Channel.QueueDeclare(
		p.Config.QueueName, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
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
			return fmt.Errorf("failed to declare exchange: %w", err)
		}

		err = p.Channel.QueueBind(
			p.Config.QueueName,  // queue name
			p.Config.RoutingKey, // routing key
			p.Config.Exchange,   // exchange
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to bind queue: %w", err)
		}
	}

	return nil
}

// publish sends a message to RabbitMQ
func (p *Producer) publish(ctx context.Context, message []byte, msgCount uint64) error {
	return p.Channel.PublishWithContext(
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
}

// reconnect attempts to restore the RabbitMQ channel
func (p *Producer) reconnect() error {
	p.ReconnectMu.Lock()
	defer p.ReconnectMu.Unlock()

	// Close existing channel
	if p.Channel != nil {
		p.Channel.Close()
	}

	// Retry configuration
	maxRetries := 3
	retryDelay := time.Second

	var err error
	// Try to reconnect with retries
	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("Producer %d: Attempting to reconnect channel (attempt %d/%d)...", p.ID, attempt, maxRetries)

		// Get a new channel from the connection manager
		p.Channel, err = p.ConnManager.GetChannel()
		if err != nil {
			log.Printf("Producer %d: Failed to get new channel (attempt %d/%d): %v",
				p.ID, attempt, maxRetries, err)
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
			continue
		}

		// Re-setup topology
		if err = p.setupTopology(); err != nil {
			log.Printf("Producer %d: Got new channel but failed to setup topology (attempt %d/%d): %v",
				p.ID, attempt, maxRetries, err)
			p.Channel.Close()
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
			continue
		}

		log.Printf("Producer %d: Successfully reconnected channel!", p.ID)
		return nil
	}

	return fmt.Errorf("failed to reconnect channel after %d attempts", maxRetries)
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

// AsyncProducerCreator holds the result of async producer creation
type AsyncProducerCreator struct {
	Producer *Producer
	Error    error
	Index    int
}

// CreateProducerAsync creates a new producer asynchronously
func CreateProducerAsync(id int, config Config, connManager *ConnectionManager, resultChan chan<- AsyncProducerCreator) {
	producer, err := NewProducer(id, config, connManager)
	resultChan <- AsyncProducerCreator{
		Producer: producer,
		Error:    err,
		Index:    id,
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
	connectionName := flag.String("conn-name", "rabbitmq-producer", "RabbitMQ connection name")

	enableTLS := flag.Bool("tls", false, "Enable TLS connection")
	certFile := flag.String("cert", "", "Path to TLS certificate file (PEM format)")
	insecureSkipVerify := flag.Bool("insecure", false, "Skip TLS certificate verification (insecure, but useful for self-signed certs)")

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
		protocol := "amqp"
		if *enableTLS {
			protocol = "amqps"
		}
		connectionURL = fmt.Sprintf("%s://%s:%s@%s:%d%s",
			protocol, *username, *password, *host, *port, *vhost)
	}

	config := Config{
		NumberOfProducers:  *numProducers,
		MessagesPerSecond:  *mps,
		RabbitMQURL:        connectionURL,
		Host:               *host,
		Port:               *port,
		Username:           *username,
		Password:           *password,
		QueueName:          *queueName,
		Exchange:           *exchange,
		RoutingKey:         rKey,
		ConnectionName:     *connectionName,
		EnableTLS:          *enableTLS,
		CertFile:           *certFile,
		InsecureSkipVerify: *insecureSkipVerify,
	}

	// Create a connection manager with a single shared connection
	connManager, err := NewConnectionManager(config)
	if err != nil {
		log.Fatalf("Failed to create connection manager: %v", err)
	}
	defer connManager.Close()

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

	connectionType := "standard"
	if config.EnableTLS {
		connectionType = "TLS"
		if config.InsecureSkipVerify {
			connectionType += " (with certificate verification disabled)"
		}
	}

	log.Printf("Starting %d producers (sharing a single %s connection), each sending %d messages/sec (512 bytes each)",
		config.NumberOfProducers, connectionType, config.MessagesPerSecond)
	log.Printf("Total throughput: %d messages/sec (%.2f MB/sec)",
		totalMPS, float64(bytesPerSec)/(1024*1024))

	// Create producers asynchronously
	resultChan := make(chan AsyncProducerCreator, config.NumberOfProducers)
	for i := 0; i < config.NumberOfProducers; i++ {
		go CreateProducerAsync(i, config, connManager, resultChan)
		time.Sleep(50 * time.Millisecond)
	}

	// Collect results and start producers
	producers := make([]*Producer, config.NumberOfProducers)
	var wg sync.WaitGroup
	var startTime time.Time = time.Now()

	for i := 0; i < config.NumberOfProducers; i++ {
		result := <-resultChan
		if result.Error != nil {
			log.Printf("Failed to create producer %d: %v", result.Index, result.Error)
			continue
		}

		producers[result.Index] = result.Producer
		log.Printf("Producer %d created in %.2f ms", result.Index,
			float64(time.Since(startTime).Microseconds())/1000.0)

		wg.Add(1)
		go result.Producer.Start(ctx, &wg)
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
					if p != nil {
						stats := p.GetStats()
						totalMessages += stats.MessagesSent
						totalErrors += stats.Errors
					}
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
	var firstStartTime time.Time

	for _, p := range producers {
		if p != nil {
			stats := p.GetStats()
			totalMessages += stats.MessagesSent
			totalErrors += stats.Errors

			if firstStartTime.IsZero() || firstStartTime.After(stats.StartTime) {
				firstStartTime = stats.StartTime
			}
		}
	}

	elapsedSecs := time.Since(firstStartTime).Seconds()
	overallRate := float64(totalMessages) / elapsedSecs
	mbPerSec := (overallRate * 512) / (1024 * 1024)

	log.Printf("All producers finished")
	log.Printf("Total messages sent: %d", totalMessages)
	log.Printf("Average rate: %.2f messages/sec (%.2f MB/sec)", overallRate, mbPerSec)
	log.Printf("Total errors: %d", totalErrors)
	log.Printf("Total runtime: %.2f seconds", elapsedSecs)
}
