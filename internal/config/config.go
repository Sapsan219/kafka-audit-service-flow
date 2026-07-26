package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr          string
	DatabaseURL       string
	KafkaBrokers      []string
	KafkaTopic        string
	KafkaGroupID      string
	KafkaBatchSize    int
	CommitInterval    time.Duration
	AnalyticsInterval time.Duration
	ShutdownPeriod    time.Duration
}

func Load() (Config, error) {
	batchSize, err := strconv.Atoi(env("KAFKA_BATCH_SIZE", "100"))
	if err != nil || batchSize < 1 {
		return Config{}, fmt.Errorf("KAFKA_BATCH_SIZE must be a positive integer")
	}
	commitInterval, err := time.ParseDuration(env("KAFKA_COMMIT_INTERVAL", "5s"))
	if err != nil || commitInterval <= 0 {
		return Config{}, fmt.Errorf("KAFKA_COMMIT_INTERVAL must be a positive duration")
	}
	analyticsInterval, err := time.ParseDuration(env("ANALYTICS_INTERVAL", "5m"))
	if err != nil || analyticsInterval <= 0 {
		return Config{}, fmt.Errorf("ANALYTICS_INTERVAL must be a positive duration")
	}
	shutdownPeriod, err := time.ParseDuration(env("SHUTDOWN_PERIOD", "10s"))
	if err != nil || shutdownPeriod <= 0 {
		return Config{}, fmt.Errorf("SHUTDOWN_PERIOD must be a positive duration")
	}

	return Config{
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		DatabaseURL:       env("DATABASE_URL", "postgres://audit:audit@127.0.0.1:5433/audit?sslmode=disable"),
		KafkaBrokers:      strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:        env("KAFKA_TOPIC", "user-actions"),
		KafkaGroupID:      env("KAFKA_GROUP_ID", "analytics-group"),
		KafkaBatchSize:    batchSize,
		CommitInterval:    commitInterval,
		AnalyticsInterval: analyticsInterval,
		ShutdownPeriod:    shutdownPeriod,
	}, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
