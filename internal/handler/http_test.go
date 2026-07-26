package handler

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"audit-service/internal/domain"
	"audit-service/internal/service"
)

type stubEventRepository struct{}

func (stubEventRepository) SaveEvent(context.Context, domain.Event) error {
	return nil
}

func (stubEventRepository) DeleteEvent(context.Context, string) error {
	return nil
}

func (stubEventRepository) History(
	context.Context,
	domain.HistoryFilter,
) ([]domain.Event, int64, error) {
	return nil, 0, nil
}

func (stubEventRepository) Stats(context.Context, string, string) ([]domain.Stat, error) {
	return nil, nil
}

type stubEventProducer struct{}

func (stubEventProducer) SendEvent(context.Context, domain.Event) error {
	return nil
}

func TestCreateAuditJSONBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "valid request",
			body:       `{"user_id":"user-1","action":"login","resource_id":"web","meta":{}}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "unknown field",
			body:       `{"user_id":"user-1","action":"login","resource_id":"web","unexpected":true}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "second JSON object",
			body:       `{"user_id":"user-1","action":"login","resource_id":"web"} {"extra":true}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	auditService := service.NewAuditService(stubEventRepository{}, stubEventProducer{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	routes := New(auditService, nil, logger).Routes()

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := httptest.NewRequest(http.MethodPost, "/api/audit", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()

			routes.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf(
					"status = %d, want %d; response = %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
		})
	}
}
