package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"audit-service/internal/domain"

	"github.com/google/uuid"
)

var ErrValidation = errors.New("validation error")

type EventRepository interface {
	SaveEvent(context.Context, domain.Event) error
	DeleteEvent(context.Context, string) error
	History(context.Context, domain.HistoryFilter) ([]domain.Event, int64, error)
	Stats(context.Context, string, string) ([]domain.Stat, error)
}

type EventProducer interface {
	SendEvent(context.Context, domain.Event) error
}

type AuditService struct {
	repository EventRepository
	producer   EventProducer
}

func NewAuditService(repository EventRepository, producer EventProducer) *AuditService {
	return &AuditService{repository: repository, producer: producer}
}

func (s *AuditService) CreateEvent(
	ctx context.Context,
	userID string,
	action string,
	resourceID string,
	meta json.RawMessage,
) (domain.Event, error) {
	userID = strings.TrimSpace(userID)
	action = strings.TrimSpace(action)
	resourceID = strings.TrimSpace(resourceID)

	if userID == "" || action == "" || resourceID == "" {
		return domain.Event{}, fmt.Errorf("%w: user_id, action and resource_id are required", ErrValidation)
	}
	if _, ok := domain.ValidActions[action]; !ok {
		return domain.Event{}, fmt.Errorf("%w: action must be login, view or purchase", ErrValidation)
	}
	if len(meta) == 0 || string(meta) == "null" {
		meta = json.RawMessage(`{}`)
	}

	var metaObject map[string]any
	if err := json.Unmarshal(meta, &metaObject); err != nil || metaObject == nil {
		return domain.Event{}, fmt.Errorf("%w: meta must be a JSON object", ErrValidation)
	}

	event := domain.Event{
		EventID:    uuid.New(),
		UserID:     userID,
		Action:     action,
		ResourceID: resourceID,
		Meta:       meta,
		Timestamp:  time.Now().UTC(),
	}

	if err := s.repository.SaveEvent(ctx, event); err != nil {
		return domain.Event{}, err
	}
	if err := s.producer.SendEvent(ctx, event); err != nil {
		if rollbackErr := s.repository.DeleteEvent(ctx, event.EventID.String()); rollbackErr != nil {
			return domain.Event{}, fmt.Errorf("%v; rollback database event: %w", err, rollbackErr)
		}
		return domain.Event{}, err
	}

	return event, nil
}

func (s *AuditService) History(
	ctx context.Context,
	filter domain.HistoryFilter,
) ([]domain.Event, int64, error) {
	filter.UserID = strings.TrimSpace(filter.UserID)
	filter.Action = strings.TrimSpace(filter.Action)

	if filter.UserID == "" {
		return nil, 0, fmt.Errorf("%w: user_id is required", ErrValidation)
	}
	if filter.Action != "" {
		if _, ok := domain.ValidActions[filter.Action]; !ok {
			return nil, 0, fmt.Errorf("%w: action must be login, view or purchase", ErrValidation)
		}
	}
	if filter.Page < 1 {
		return nil, 0, fmt.Errorf("%w: page must be positive", ErrValidation)
	}
	if filter.Limit < 1 || filter.Limit > 100 {
		return nil, 0, fmt.Errorf("%w: limit must be between 1 and 100", ErrValidation)
	}
	if filter.From != nil && filter.ToExclusive != nil && !filter.From.Before(*filter.ToExclusive) {
		return nil, 0, fmt.Errorf("%w: from must not be after to", ErrValidation)
	}

	return s.repository.History(ctx, filter)
}

func (s *AuditService) Stats(
	ctx context.Context,
	userID string,
	groupBy string,
) ([]domain.Stat, error) {
	userID = strings.TrimSpace(userID)
	groupBy = strings.TrimSpace(groupBy)
	if groupBy != "action" && groupBy != "day" {
		return nil, fmt.Errorf("%w: group_by must be action or day", ErrValidation)
	}
	return s.repository.Stats(ctx, userID, groupBy)
}
