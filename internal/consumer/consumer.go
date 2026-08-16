package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"audit-service/internal/domain"

	"github.com/IBM/sarama"
)

type StatsRepository interface {
	StoreAnalyticsAndRefresh(context.Context, []domain.Event, time.Time, time.Time) error
}

type DeadLetterProducer interface {
	SendDeadLetter(context.Context, *sarama.ConsumerMessage, error) error
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
	log     *slog.Logger
}

func New(
	settings Config,
	repository StatsRepository,
	dlq DeadLetterProducer,
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
			dlq:               dlq,
			log:               log,
			analyticsInterval: settings.AnalyticsInterval,
			batchSize:         settings.BatchSize,
		},
		log: log,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := c.group.Consume(ctx, []string{c.topic}, c.handler); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("consume Kafka messages; retrying", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

type groupHandler struct {
	repository        StatsRepository
	dlq               DeadLetterProducer
	log               *slog.Logger
	analyticsInterval time.Duration
	batchSize         int

	mu          sync.Mutex
	pending     []pendingMessage
	lastRefresh time.Time
}

type pendingMessage struct {
	message *sarama.ConsumerMessage
	event   domain.Event
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
	return h.flush(session, true)
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
				return h.flush(session, true)
			}

			var event domain.Event
			if err := json.Unmarshal(message.Value, &event); err != nil {
				if err := h.sendToDLQ(session, message, fmt.Errorf("decode JSON: %w", err)); err != nil {
					return err
				}
				continue
			}
			if err := validateEvent(event); err != nil {
				if err := h.sendToDLQ(session, message, err); err != nil {
					return err
				}
				continue
			}

			h.log.Info(
				"Kafka message received",
				"key", string(message.Key),
				"partition", message.Partition,
				"offset", message.Offset,
				"event_id", event.EventID,
			)

			h.mu.Lock()
			h.pending = append(h.pending, pendingMessage{message: message, event: event})
			shouldFlush := len(h.pending) >= h.batchSize
			h.mu.Unlock()

			if shouldFlush {
				if err := h.flush(session, false); err != nil {
					return err
				}
			}

		case <-ticker.C:
			if err := h.flush(session, true); err != nil {
				h.log.Error("periodic stats refresh failed", "error", err)
			}

		case <-session.Context().Done():
			return h.flush(session, true)
		}
	}
}

func (h *groupHandler) flush(session sarama.ConsumerGroupSession, periodic bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().UTC()
	refreshDue := h.lastRefresh.IsZero() || now.Sub(h.lastRefresh) >= h.analyticsInterval
	if len(h.pending) == 0 && (!periodic || !refreshDue) {
		return nil
	}

	windowTo := now
	windowFrom := windowTo.Add(-time.Hour)
	events := make([]domain.Event, 0, len(h.pending))
	for _, pending := range h.pending {
		events = append(events, pending.event)
	}

	if err := h.repository.StoreAnalyticsAndRefresh(
		session.Context(),
		events,
		windowFrom,
		windowTo,
	); err != nil {
		return fmt.Errorf("refresh stats before offset commit: %w", err)
	}

	for _, pending := range h.pending {
		session.MarkMessage(pending.message, "")
	}
	if len(h.pending) > 0 {
		session.Commit()
	}

	h.log.Info(
		"stats cache refreshed",
		"messages", len(h.pending),
		"offsets_committed", len(h.pending) > 0,
		"window_from", windowFrom,
		"window_to", windowTo,
	)
	h.pending = nil
	h.lastRefresh = windowTo
	return nil
}

func (h *groupHandler) sendToDLQ(
	session sarama.ConsumerGroupSession,
	message *sarama.ConsumerMessage,
	cause error,
) error {
	// An invalid message can follow valid messages in the same partition.
	// Persist and commit those valid messages before advancing past the poison pill.
	if err := h.flush(session, false); err != nil {
		return fmt.Errorf("flush valid messages before DLQ: %w", err)
	}

	if err := h.dlq.SendDeadLetter(session.Context(), message, cause); err != nil {
		return fmt.Errorf(
			"send message at partition %d offset %d to DLQ: %w",
			message.Partition,
			message.Offset,
			err,
		)
	}

	session.MarkMessage(message, "")
	session.Commit()
	h.log.Warn(
		"invalid Kafka message sent to DLQ",
		"error", cause,
		"key", string(message.Key),
		"partition", message.Partition,
		"offset", message.Offset,
	)
	return nil
}

func validateEvent(event domain.Event) error {
	if event.EventID.String() == "00000000-0000-0000-0000-000000000000" {
		return errors.New("event_id is required")
	}
	if event.UserID == "" {
		return errors.New("user_id is required")
	}
	if _, ok := domain.ValidActions[event.Action]; !ok {
		return fmt.Errorf("unknown action %q", event.Action)
	}
	if event.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	return nil
}
