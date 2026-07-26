package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"audit-service/internal/domain"

	"github.com/IBM/sarama"
)

type StatsRepository interface {
	RefreshStatsCache(context.Context, time.Time, time.Time) error
}

type Config struct {
	Brokers           []string
	GroupID           string
	Topic             string
	BatchSize         int
	CommitInterval    time.Duration
	AnalyticsInterval time.Duration
}

type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	handler *groupHandler
}

func New(
	settings Config,
	repository StatsRepository,
	log *slog.Logger,
) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = settings.CommitInterval
	cfg.ChannelBufferSize = settings.BatchSize

	group, err := sarama.NewConsumerGroup(settings.Brokers, settings.GroupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer group: %w", err)
	}

	return &Consumer{
		group: group,
		topic: settings.Topic,
		handler: &groupHandler{
			repository:        repository,
			log:               log,
			analyticsInterval: settings.AnalyticsInterval,
			batchSize:         settings.BatchSize,
		},
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := c.group.Consume(ctx, []string{c.topic}, c.handler); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consume Kafka messages: %w", err)
		}
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

type groupHandler struct {
	repository        StatsRepository
	log               *slog.Logger
	analyticsInterval time.Duration
	batchSize         int

	mu      sync.Mutex
	pending []*sarama.ConsumerMessage
}

func (h *groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.log.Info(
		"consumer partitions assigned",
		"member_id", session.MemberID(),
		"generation_id", session.GenerationID(),
		"partitions", session.Claims(),
	)
	return nil
}

func (h *groupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.log.Info(
		"consumer partitions revoked",
		"member_id", session.MemberID(),
		"generation_id", session.GenerationID(),
		"partitions", session.Claims(),
	)
	return h.flush(session)
}

func (h *groupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	ticker := time.NewTicker(h.analyticsInterval)
	defer ticker.Stop()

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return h.flush(session)
			}

			var event domain.Event
			if err := json.Unmarshal(message.Value, &event); err != nil {
				h.log.Error(
					"decode Kafka message",
					"error", err,
					"key", string(message.Key),
					"partition", message.Partition,
					"offset", message.Offset,
				)
				return fmt.Errorf("decode Kafka message at partition %d offset %d: %w", message.Partition, message.Offset, err)
			}

			h.log.Info(
				"Kafka message received",
				"key", string(message.Key),
				"partition", message.Partition,
				"offset", message.Offset,
				"event_id", event.EventID,
			)

			h.mu.Lock()
			h.pending = append(h.pending, message)
			shouldFlush := len(h.pending) >= h.batchSize
			h.mu.Unlock()

			if shouldFlush {
				if err := h.flush(session); err != nil {
					return err
				}
			}

		case <-ticker.C:
			if err := h.flush(session); err != nil {
				h.log.Error("periodic stats refresh failed", "error", err)
			}

		case <-session.Context().Done():
			return h.flush(session)
		}
	}
}

func (h *groupHandler) flush(session sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.pending) == 0 {
		return nil
	}

	windowTo := time.Now().UTC()
	windowFrom := windowTo.Add(-time.Hour)
	if err := h.repository.RefreshStatsCache(session.Context(), windowFrom, windowTo); err != nil {
		return fmt.Errorf("refresh stats before offset commit: %w", err)
	}

	for _, message := range h.pending {
		session.MarkMessage(message, "")
	}
	session.Commit()

	h.log.Info(
		"stats cache refreshed and offsets committed",
		"messages", len(h.pending),
		"window_from", windowFrom,
		"window_to", windowTo,
	)
	h.pending = nil
	return nil
}
