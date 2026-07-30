package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr             string
	HTTPReadTimeout      time.Duration
	HTTPWriteTimeout     time.Duration
	HTTPIdleTimeout      time.Duration
	DatabaseURL          string
	KafkaBrokers         []string
	KafkaTopic           string
	KafkaDLQTopic        string
	KafkaGroupID         string
	KafkaBatchSize       int
	KafkaProducerTimeout time.Duration
	CommitInterval       time.Duration
	AnalyticsInterval    time.Duration
	OutboxInterval       time.Duration
	OutboxBatchSize      int
	ShutdownPeriod       time.Duration
}

func Load() (Config, error) {
	batchSize, err := strconv.Atoi(env("KAFKA_BATCH_SIZE", "100"))
	if err != nil || batchSize < 1 {
		return Config{}, fmt.Errorf("KAFKA_BATCH_SIZE must be a positive integer")
	}
	producerTimeout, err := time.ParseDuration(env("KAFKA_PRODUCER_TIMEOUT", "10s"))
	if err != nil || producerTimeout <= 0 {
		return Config{}, fmt.Errorf("KAFKA_PRODUCER_TIMEOUT must be a positive duration")
	}
	commitInterval, err := time.ParseDuration(env("KAFKA_COMMIT_INTERVAL", "5s"))
	if err != nil || commitInterval <= 0 {
		return Config{}, fmt.Errorf("KAFKA_COMMIT_INTERVAL must be a positive duration")
	}
	analyticsInterval, err := time.ParseDuration(env("ANALYTICS_INTERVAL", "5m"))
	if err != nil || analyticsInterval <= 0 {
		return Config{}, fmt.Errorf("ANALYTICS_INTERVAL must be a positive duration")
	}
	outboxInterval, err := time.ParseDuration(env("OUTBOX_INTERVAL", "1s"))
	if err != nil || outboxInterval <= 0 {
		return Config{}, fmt.Errorf("OUTBOX_INTERVAL must be a positive duration")
	}
	outboxBatchSize, err := strconv.Atoi(env("OUTBOX_BATCH_SIZE", "100"))
	if err != nil || outboxBatchSize < 1 {
		return Config{}, fmt.Errorf("OUTBOX_BATCH_SIZE must be a positive integer")
	}
	httpReadTimeout, err := time.ParseDuration(env("HTTP_READ_TIMEOUT", "10s"))
	if err != nil || httpReadTimeout <= 0 {
		return Config{}, fmt.Errorf("HTTP_READ_TIMEOUT must be a positive duration")
	}
	httpWriteTimeout, err := time.ParseDuration(env("HTTP_WRITE_TIMEOUT", "15s"))
	if err != nil || httpWriteTimeout <= 0 {
		return Config{}, fmt.Errorf("HTTP_WRITE_TIMEOUT must be a positive duration")
	}
	httpIdleTimeout, err := time.ParseDuration(env("HTTP_IDLE_TIMEOUT", "60s"))
	if err != nil || httpIdleTimeout <= 0 {
		return Config{}, fmt.Errorf("HTTP_IDLE_TIMEOUT must be a positive duration")
	}
	shutdownPeriod, err := time.ParseDuration(env("SHUTDOWN_PERIOD", "10s"))
	if err != nil || shutdownPeriod <= 0 {
		return Config{}, fmt.Errorf("SHUTDOWN_PERIOD must be a positive duration")
	}

	return Config{
		HTTPAddr:             env("HTTP_ADDR", ":8080"),
		HTTPReadTimeout:      httpReadTimeout,
		HTTPWriteTimeout:     httpWriteTimeout,
		HTTPIdleTimeout:      httpIdleTimeout,
		DatabaseURL:          env("DATABASE_URL", "postgres://audit:audit@127.0.0.1:5433/audit?sslmode=disable"),
		KafkaBrokers:         strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:           env("KAFKA_TOPIC", "user-actions"),
		KafkaDLQTopic:        env("KAFKA_DLQ_TOPIC", "user-actions-dlq"),
		KafkaGroupID:         env("KAFKA_GROUP_ID", "analytics-group"),
		KafkaBatchSize:       batchSize,
		KafkaProducerTimeout: producerTimeout,
		CommitInterval:       commitInterval,
		AnalyticsInterval:    analyticsInterval,
		OutboxInterval:       outboxInterval,
		OutboxBatchSize:      outboxBatchSize,
		ShutdownPeriod:       shutdownPeriod,
	}, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
