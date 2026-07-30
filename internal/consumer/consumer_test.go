package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"audit-service/internal/domain"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type fakeStatsRepository struct {
	calls  int
	events []domain.Event
	err    error
}

func (r *fakeStatsRepository) StoreAnalyticsAndRefresh(
	_ context.Context,
	events []domain.Event,
	_ time.Time,
	_ time.Time,
) error {
	r.calls++
	r.events = append(r.events, events...)
	return r.err
}

type fakeDLQProducer struct {
	messages []*sarama.ConsumerMessage
	err      error
}

func (p *fakeDLQProducer) SendDeadLetter(
	_ context.Context,
	message *sarama.ConsumerMessage,
	_ error,
) error {
	p.messages = append(p.messages, message)
	return p.err
}

type fakeSession struct {
	ctx     context.Context
	marked  []*sarama.ConsumerMessage
	commits int
}

func (s *fakeSession) Claims() map[string][]int32 { return nil }
func (s *fakeSession) MemberID() string           { return "test-member" }
func (s *fakeSession) GenerationID() int32        { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string) {
}
func (s *fakeSession) Commit() { s.commits++ }
func (s *fakeSession) ResetOffset(string, int32, int64, string) {
}
func (s *fakeSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, message)
}
func (s *fakeSession) Context() context.Context { return s.ctx }

func TestPeriodicFlushRefreshesEmptyWindow(t *testing.T) {
	t.Parallel()

	repository := &fakeStatsRepository{}
	handler := newTestHandler(repository, &fakeDLQProducer{})
	session := &fakeSession{ctx: context.Background()}

	if err := handler.flush(session, true); err != nil {
		t.Fatalf("periodic flush: %v", err)
	}
	if repository.calls != 1 {
		t.Fatalf("repository calls = %d, want 1", repository.calls)
	}
	if session.commits != 0 {
		t.Fatalf("commits = %d, want 0", session.commits)
	}
}

func TestFlushStoresEventsBeforeCommittingOffsets(t *testing.T) {
	t.Parallel()

	repository := &fakeStatsRepository{}
	handler := newTestHandler(repository, &fakeDLQProducer{})
	session := &fakeSession{ctx: context.Background()}
	message := &sarama.ConsumerMessage{Topic: "user-actions", Partition: 1, Offset: 7}
	event := validEvent()
	handler.pending = []pendingMessage{{message: message, event: event}}

	if err := handler.flush(session, false); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(repository.events) != 1 || repository.events[0].EventID != event.EventID {
		t.Fatalf("stored events = %#v, want event %s", repository.events, event.EventID)
	}
	if len(session.marked) != 1 || session.marked[0] != message {
		t.Fatalf("marked messages = %#v, want original message", session.marked)
	}
	if session.commits != 1 {
		t.Fatalf("commits = %d, want 1", session.commits)
	}
}

func TestFlushDoesNotCommitWhenDatabaseWriteFails(t *testing.T) {
	t.Parallel()

	repository := &fakeStatsRepository{err: errors.New("database unavailable")}
	handler := newTestHandler(repository, &fakeDLQProducer{})
	session := &fakeSession{ctx: context.Background()}
	handler.pending = []pendingMessage{{
		message: &sarama.ConsumerMessage{Topic: "user-actions", Offset: 3},
		event:   validEvent(),
	}}

	if err := handler.flush(session, false); err == nil {
		t.Fatal("flush error = nil, want database error")
	}
	if len(session.marked) != 0 || session.commits != 0 {
		t.Fatalf("offset advanced after database error: marked=%d commits=%d", len(session.marked), session.commits)
	}
	if len(handler.pending) != 1 {
		t.Fatalf("pending messages = %d, want 1", len(handler.pending))
	}
}

func TestInvalidMessageGoesToDLQAfterPendingMessages(t *testing.T) {
	t.Parallel()

	repository := &fakeStatsRepository{}
	dlq := &fakeDLQProducer{}
	handler := newTestHandler(repository, dlq)
	session := &fakeSession{ctx: context.Background()}
	validMessage := &sarama.ConsumerMessage{Topic: "user-actions", Partition: 0, Offset: 10}
	invalidMessage := &sarama.ConsumerMessage{Topic: "user-actions", Partition: 0, Offset: 11}
	handler.pending = []pendingMessage{{message: validMessage, event: validEvent()}}

	if err := handler.sendToDLQ(session, invalidMessage, errors.New("bad JSON")); err != nil {
		t.Fatalf("send to DLQ: %v", err)
	}
	if len(repository.events) != 1 {
		t.Fatalf("stored valid events = %d, want 1", len(repository.events))
	}
	if len(dlq.messages) != 1 || dlq.messages[0] != invalidMessage {
		t.Fatalf("DLQ messages = %#v, want invalid message", dlq.messages)
	}
	if len(session.marked) != 2 {
		t.Fatalf("marked messages = %d, want 2", len(session.marked))
	}
	if session.commits != 2 {
		t.Fatalf("commits = %d, want 2", session.commits)
	}
}

func newTestHandler(
	repository StatsRepository,
	dlq DeadLetterProducer,
) *groupHandler {
	return &groupHandler{
		repository:        repository,
		dlq:               dlq,
		log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		analyticsInterval: 5 * time.Minute,
		batchSize:         100,
	}
}

func validEvent() domain.Event {
	return domain.Event{
		EventID:   uuid.New(),
		UserID:    "user-1",
		Action:    "login",
		Timestamp: time.Now().UTC(),
	}
}
