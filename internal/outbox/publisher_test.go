package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"audit-service/internal/domain"

	"github.com/google/uuid"
)

type fakeRepository struct {
	events    []domain.Event
	published []string
	failed    []string
}

func (r *fakeRepository) PendingOutbox(context.Context, int) ([]domain.Event, error) {
	return r.events, nil
}

func (r *fakeRepository) MarkOutboxPublished(_ context.Context, eventID string) error {
	r.published = append(r.published, eventID)
	return nil
}

func (r *fakeRepository) MarkOutboxFailed(
	_ context.Context,
	eventID string,
	_ string,
) error {
	r.failed = append(r.failed, eventID)
	return nil
}

type fakeProducer struct {
	failEventID uuid.UUID
}

func (p *fakeProducer) SendEvent(_ context.Context, event domain.Event) error {
	if event.EventID == p.failEventID {
		return errors.New("Kafka timeout")
	}
	return nil
}

func TestPublishBatchMarksSuccessAndFailure(t *testing.T) {
	t.Parallel()

	success := domain.Event{EventID: uuid.New()}
	failure := domain.Event{EventID: uuid.New()}
	repository := &fakeRepository{events: []domain.Event{success, failure}}
	publisher := New(
		repository,
		&fakeProducer{failEventID: failure.EventID},
		time.Second,
		100,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	publisher.publishBatch(context.Background())

	if len(repository.published) != 1 || repository.published[0] != success.EventID.String() {
		t.Fatalf("published = %#v, want %s", repository.published, success.EventID)
	}
	if len(repository.failed) != 1 || repository.failed[0] != failure.EventID.String() {
		t.Fatalf("failed = %#v, want %s", repository.failed, failure.EventID)
	}
}
