package outbox

import (
	"context"
	"log/slog"
	"time"

	"audit-service/internal/domain"
)

type Repository interface {
	PendingOutbox(context.Context, int) ([]domain.Event, error)
	MarkOutboxPublished(context.Context, string) error
	MarkOutboxFailed(context.Context, string, string) error
}

type EventProducer interface {
	SendEvent(context.Context, domain.Event) error
}

type Publisher struct {
	repository Repository
	producer   EventProducer
	interval   time.Duration
	batchSize  int
	log        *slog.Logger
}

func New(
	repository Repository,
	producer EventProducer,
	interval time.Duration,
	batchSize int,
	log *slog.Logger,
) *Publisher {
	return &Publisher{
		repository: repository,
		producer:   producer,
		interval:   interval,
		batchSize:  batchSize,
		log:        log,
	}
}

func (p *Publisher) Run(ctx context.Context) {
	p.publishBatch(ctx)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.publishBatch(ctx)
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) {
	events, err := p.repository.PendingOutbox(ctx, p.batchSize)
	if err != nil {
		p.log.Error("load pending outbox events", "error", err)
		return
	}

	for _, event := range events {
		if err := p.producer.SendEvent(ctx, event); err != nil {
			p.log.Error("publish outbox event", "event_id", event.EventID, "error", err)
			if markErr := p.repository.MarkOutboxFailed(
				ctx,
				event.EventID.String(),
				err.Error(),
			); markErr != nil {
				p.log.Error(
					"mark outbox event failed",
					"event_id", event.EventID,
					"error", markErr,
				)
			}
			continue
		}

		if err := p.repository.MarkOutboxPublished(ctx, event.EventID.String()); err != nil {
			// Kafka could already contain this event. Keeping it pending gives us
			// at-least-once delivery; analytics_events deduplicates by event_id.
			p.log.Error(
				"mark outbox event published",
				"event_id", event.EventID,
				"error", err,
			)
			continue
		}

		p.log.Info("outbox event published", "event_id", event.EventID)
	}
}
