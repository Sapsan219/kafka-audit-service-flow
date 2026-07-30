package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"audit-service/internal/domain"

	"github.com/IBM/sarama"
)

type Producer struct {
	client   sarama.SyncProducer
	topic    string
	dlqTopic string
}

func NewProducer(
	brokers []string,
	topic string,
	dlqTopic string,
	timeout time.Duration,
) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_0_0
	cfg.Net.DialTimeout = timeout
	cfg.Net.ReadTimeout = timeout
	cfg.Net.WriteTimeout = timeout
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Timeout = timeout
	cfg.Producer.Partitioner = sarama.NewHashPartitioner

	client, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}

	if dlqTopic == "" {
		dlqTopic = topic + "-dlq"
	}

	return &Producer{client: client, topic: topic, dlqTopic: dlqTopic}, nil
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

func (p *Producer) SendDeadLetter(
	ctx context.Context,
	message *sarama.ConsumerMessage,
	cause error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	envelope := struct {
		Topic     string    `json:"topic"`
		Key       []byte    `json:"key"`
		Value     []byte    `json:"value"`
		Partition int32     `json:"partition"`
		Offset    int64     `json:"offset"`
		Error     string    `json:"error"`
		FailedAt  time.Time `json:"failed_at"`
	}{
		Topic:     message.Topic,
		Key:       message.Key,
		Value:     message.Value,
		Partition: message.Partition,
		Offset:    message.Offset,
		Error:     cause.Error(),
		FailedAt:  time.Now().UTC(),
	}

	value, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal dead-letter message: %w", err)
	}

	_, _, err = p.client.SendMessage(&sarama.ProducerMessage{
		Topic: p.dlqTopic,
		Key:   sarama.ByteEncoder(message.Key),
		Value: sarama.ByteEncoder(value),
	})
	if err != nil {
		return fmt.Errorf("send dead-letter message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.client.Close()
}
