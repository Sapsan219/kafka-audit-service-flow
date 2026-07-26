package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"audit-service/internal/domain"

	"github.com/IBM/sarama"
)

type Producer struct {
	client sarama.SyncProducer
	topic  string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Partitioner = sarama.NewHashPartitioner

	client, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}

	return &Producer{client: client, topic: topic}, nil
}

func (p *Producer) SendEvent(ctx context.Context, event domain.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(event.UserID),
		Value: sarama.ByteEncoder(value),
	}

	_, _, err = p.client.SendMessage(message)
	if err != nil {
		return fmt.Errorf("send Kafka message: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.client.Close()
}
