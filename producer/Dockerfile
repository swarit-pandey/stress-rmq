FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files (if they exist)
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY *.go ./

# Build the application with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o rabbitmq-producer

# Use a minimal alpine image for the final container
FROM alpine:3.18

WORKDIR /app

# Install CA certificates for secure connections
RUN apk --no-cache add ca-certificates

# Copy the binary from the builder stage
COPY --from=builder /app/rabbitmq-producer /app/

# Run as non-root user for better security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Set the entrypoint to the producer binary
ENTRYPOINT ["/app/rabbitmq-producer"]
