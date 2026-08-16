//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"audit-service/internal/consumer"
	"audit-service/internal/handler"
	"audit-service/internal/outbox"
	"audit-service/internal/producer"
	"audit-service/internal/repository"
	"audit-service/internal/service"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestApplicationFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	brokers := startKafka(t, ctx)
	databaseURL := startPostgres(t, ctx)
	pool := connectPostgres(t, ctx, databaseURL)
	applyMigration(t, ctx, pool)

	const (
		topic    = "user-actions-integration"
		dlqTopic = "user-actions-integration-dlq"
		groupID  = "analytics-integration-group"
	)
	createTopic(t, brokers, topic)
	createTopic(t, brokers, dlqTopic)

	eventProducer, err := producer.NewProducer(
		brokers,
		topic,
		dlqTopic,
		5*time.Second,
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(func() {
		_ = eventProducer.Close()
	})

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	auditRepository := repository.NewAuditRepository(pool)
	analyticsConsumer, err := consumer.New(
		consumer.Config{
			Brokers:           brokers,
			GroupID:           groupID,
			Topic:             topic,
			BatchSize:         1,
			CommitInterval:    time.Second,
			AnalyticsInterval: 100 * time.Millisecond,
		},
		auditRepository,
		eventProducer,
		log,
	)
	if err != nil {
		t.Fatalf("create analytics consumer: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	runResult := make(chan error, 1)
	go func() {
		runResult <- analyticsConsumer.Run(runCtx)
	}()
	t.Cleanup(func() {
		stop()
		_ = analyticsConsumer.Close()
		select {
		case err := <-runResult:
			if err != nil {
				t.Logf("stop analytics consumer: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Log("analytics consumer did not stop within 10 seconds")
		}
	})

	outboxPublisher := outbox.New(
		auditRepository,
		eventProducer,
		50*time.Millisecond,
		10,
		log,
	)
	go outboxPublisher.Run(runCtx)

	auditService := service.NewAuditService(auditRepository)
	httpServer := httptest.NewServer(handler.New(auditService, nil, log).Routes())
	t.Cleanup(httpServer.Close)

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		httpServer.URL+"/api/audit",
		bytes.NewBufferString(
			`{"user_id":"integration-user","action":"purchase","resource_id":"product-42","meta":{"price":1990}}`,
		),
	)
	if err != nil {
		t.Fatalf("create HTTP request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("post audit event: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("POST /api/audit status = %d, want %d", response.StatusCode, http.StatusCreated)
	}

	var created struct {
		EventID string `json:"event_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	waitForDatabaseFlow(t, ctx, pool, created.EventID)
	waitForCommittedOffset(t, ctx, brokers, groupID, topic)
}

func startKafka(t *testing.T, ctx context.Context) []string {
	t.Helper()

	kafkaContainer, err := tckafka.Run(
		ctx,
		"confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("audit-integration-test"),
	)
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("get Kafka brokers: %v", err)
	}
	return brokers
}

func startPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()

	postgresContainer, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "postgres:16",
				ExposedPorts: []string{"5432/tcp"},
				Env: map[string]string{
					"POSTGRES_DB":       "audit",
					"POSTGRES_USER":     "audit",
					"POSTGRES_PASSWORD": "audit",
				},
				WaitingFor: wait.ForListeningPort("5432/tcp").
					WithStartupTimeout(time.Minute),
			},
			Started: true,
		},
	)
	if err != nil {
		t.Fatalf("start PostgreSQL container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Logf("terminate PostgreSQL container: %v", err)
		}
	})

	host, err := postgresContainer.Host(ctx)
	if err != nil {
		t.Fatalf("get PostgreSQL host: %v", err)
	}
	port, err := postgresContainer.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("get PostgreSQL port: %v", err)
	}
	return fmt.Sprintf(
		"postgres://audit:audit@%s:%s/audit?sslmode=disable",
		host,
		port.Port(),
	)
}

func connectPostgres(
	t *testing.T,
	ctx context.Context,
	databaseURL string,
) *pgxpool.Pool {
	t.Helper()

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse PostgreSQL config: %v", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	return pool
}

func applyMigration(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	migration, err := os.ReadFile("../../migrations/001_create_audit_schema.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
}

func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_0_0
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		t.Fatalf("create Kafka admin: %v", err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}

func waitForDatabaseFlow(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	eventID string,
) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var (
			statCount      int64
			analyticsCount int64
			published      bool
		)
		statErr := pool.QueryRow(
			ctx,
			`SELECT count FROM stats_cache WHERE action = 'purchase'`,
		).Scan(&statCount)
		analyticsErr := pool.QueryRow(
			ctx,
			`SELECT count(*) FROM analytics_events WHERE event_id = $1`,
			eventID,
		).Scan(&analyticsCount)
		outboxErr := pool.QueryRow(
			ctx,
			`SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id = $1`,
			eventID,
		).Scan(&published)

		if statErr == nil && analyticsErr == nil && outboxErr == nil &&
			statCount == 1 && analyticsCount == 1 && published {
			return
		}
		if statErr != nil && !errors.Is(statErr, pgx.ErrNoRows) {
			t.Fatalf("query stats cache: %v", statErr)
		}
		if analyticsErr != nil {
			t.Fatalf("query analytics event: %v", analyticsErr)
		}
		if outboxErr != nil {
			t.Fatalf("query outbox event: %v", outboxErr)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("wait for database flow: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("event did not pass audit_log -> outbox -> Kafka -> consumer -> stats_cache")
}

func waitForCommittedOffset(
	t *testing.T,
	ctx context.Context,
	brokers []string,
	groupID string,
	topic string,
) {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_0_0
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		t.Fatalf("create Kafka admin for offsets: %v", err)
	}
	defer admin.Close()

	partitions := []int32{0, 1, 2}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		offsets, err := admin.ListConsumerGroupOffsets(
			groupID,
			map[string][]int32{topic: partitions},
		)
		if err == nil {
			for _, partition := range partitions {
				if block := offsets.GetBlock(topic, partition); block != nil && block.Offset > 0 {
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("wait for committed offset: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("analytics consumer group did not commit an offset")
}
