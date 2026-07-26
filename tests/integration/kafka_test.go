//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"audit-service/internal/domain"
	"audit-service/internal/producer"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func TestProducerToConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

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

	const topic = "user-actions-integration"
	createTopic(t, brokers, topic)

	partitionConsumers, messages := startConsumers(t, brokers, topic)
	for _, partitionConsumer := range partitionConsumers {
		consumer := partitionConsumer
		t.Cleanup(func() {
			_ = consumer.Close()
		})
	}

	eventProducer, err := producer.NewProducer(brokers, topic)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(func() {
		_ = eventProducer.Close()
	})

	expected := domain.Event{
		EventID:    uuid.New(),
		UserID:     "integration-user",
		Action:     "purchase",
		ResourceID: "product-42",
		Meta:       json.RawMessage(`{"price":1990}`),
		Timestamp:  time.Now().UTC(),
	}
	if err := eventProducer.SendEvent(ctx, expected); err != nil {
		t.Fatalf("send event: %v", err)
	}

	select {
	case message := <-messages:
		if string(message.Key) != expected.UserID {
			t.Fatalf("unexpected key: got %q want %q", message.Key, expected.UserID)
		}

		var actual domain.Event
		if err := json.Unmarshal(message.Value, &actual); err != nil {
			t.Fatalf("decode consumed event: %v", err)
		}
		if actual.EventID != expected.EventID {
			t.Fatalf("unexpected event_id: got %s want %s", actual.EventID, expected.EventID)
		}
		if actual.Action != expected.Action || actual.ResourceID != expected.ResourceID {
			t.Fatalf("unexpected event: %#v", actual)
		}

	case <-ctx.Done():
		t.Fatalf("waiting for consumed event: %v", ctx.Err())
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
		t.Fatalf("create topic: %v", err)
	}
}

func startConsumers(
	t *testing.T,
	brokers []string,
	topic string,
) ([]sarama.PartitionConsumer, <-chan *sarama.ConsumerMessage) {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_0_0
	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		t.Fatalf("create Kafka consumer: %v", err)
	}
	t.Cleanup(func() {
		_ = consumer.Close()
	})

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		t.Fatalf("list topic partitions: %v", err)
	}
	if len(partitions) != 3 {
		t.Fatalf("unexpected partition count: got %d want 3", len(partitions))
	}

	result := make(chan *sarama.ConsumerMessage, 1)
	partitionConsumers := make([]sarama.PartitionConsumer, 0, len(partitions))
	for _, partition := range partitions {
		var partitionConsumer sarama.PartitionConsumer
		deadline := time.Now().Add(30 * time.Second)
		for {
			partitionConsumer, err = consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("consume partition %d: %v", partition, err)
			}
			time.Sleep(250 * time.Millisecond)
		}
		partitionConsumers = append(partitionConsumers, partitionConsumer)

		go func(pc sarama.PartitionConsumer) {
			if message, ok := <-pc.Messages(); ok {
				select {
				case result <- message:
				default:
				}
			}
		}(partitionConsumer)
	}

	return partitionConsumers, result
}
