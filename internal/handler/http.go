package handler

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"audit-service/internal/domain"
	"audit-service/internal/service"
)

//go:embed swagger/*
var swaggerFiles embed.FS

type Handler struct {
	audit    *service.AuditService
	replayer StatsRebuilder
	log      *slog.Logger
}

type StatsRebuilder interface {
	Rebuild(context.Context, time.Time, time.Time) (domain.ReplayResult, error)
}

func New(audit *service.AuditService, replayer StatsRebuilder, log *slog.Logger) *Handler {
	return &Handler{audit: audit, replayer: replayer, log: log}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /api/audit", h.createAudit)
	mux.HandleFunc("GET /api/audit", h.auditHistory)
	mux.HandleFunc("GET /api/stats", h.auditStats)
	mux.HandleFunc("POST /api/admin/rebuild-stats", h.rebuildStats)
	mux.Handle("GET /swagger/", http.FileServer(http.FS(swaggerFiles)))
	return mux
}

type createAuditRequest struct {
	UserID     string          `json:"user_id"`
	Action     string          `json:"action"`
	ResourceID string          `json:"resource_id"`
	Meta       json.RawMessage `json:"meta"`
}

func (h *Handler) createAudit(w http.ResponseWriter, r *http.Request) {
	var request createAuditRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request body must contain exactly one JSON object")
		return
	}

	event, err := h.audit.CreateEvent(
		r.Context(),
		request.UserID,
		request.Action,
		request.ResourceID,
		request.Meta,
	)
	if err != nil {
		if errors.Is(err, service.ErrValidation) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.log.Error("create audit event", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"event_id":  event.EventID,
		"timestamp": event.Timestamp,
	})
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type historyItem struct {
	EventID    string          `json:"event_id"`
	Action     string          `json:"action"`
	ResourceID string          `json:"resource_id"`
	Meta       json.RawMessage `json:"meta"`
	Timestamp  time.Time       `json:"timestamp"`
}

func (h *Handler) auditHistory(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	page, err := positiveInt(query.Get("page"), 1)
	if err != nil {
		writeError(w, http.StatusBadRequest, "page must be a positive integer")
		return
	}
	limit, err := positiveInt(query.Get("limit"), 50)
	if err != nil {
		writeError(w, http.StatusBadRequest, "limit must be a positive integer")
		return
	}

	from, err := optionalDate(query.Get("from"), false)
	if err != nil {
		writeError(w, http.StatusBadRequest, "from must have format YYYY-MM-DD")
		return
	}
	toExclusive, err := optionalDate(query.Get("to"), true)
	if err != nil {
		writeError(w, http.StatusBadRequest, "to must have format YYYY-MM-DD")
		return
	}

	events, total, err := h.audit.History(r.Context(), domain.HistoryFilter{
		UserID:      query.Get("user_id"),
		Action:      query.Get("action"),
		From:        from,
		ToExclusive: toExclusive,
		Page:        page,
		Limit:       limit,
	})
	if err != nil {
		h.handleServiceError(w, err, "get audit history")
		return
	}

	items := make([]historyItem, 0, len(events))
	for _, event := range events {
		items = append(items, historyItem{
			EventID:    event.EventID.String(),
			Action:     event.Action,
			ResourceID: event.ResourceID,
			Meta:       event.Meta,
			Timestamp:  event.Timestamp.UTC(),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"page":  page,
		"limit": limit,
		"total": total,
	})
}

func (h *Handler) auditStats(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	stats, err := h.audit.Stats(
		r.Context(),
		query.Get("user_id"),
		query.Get("group_by"),
	)
	if err != nil {
		h.handleServiceError(w, err, "get audit stats")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"group_by": query.Get("group_by"),
		"items":    stats,
	})
}

func (h *Handler) rebuildStats(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	from, err := optionalDate(query.Get("from"), false)
	if err != nil || from == nil {
		writeError(w, http.StatusBadRequest, "from is required and must have format YYYY-MM-DD")
		return
	}
	toExclusive, err := optionalDate(query.Get("to"), true)
	if err != nil || toExclusive == nil {
		writeError(w, http.StatusBadRequest, "to is required and must have format YYYY-MM-DD")
		return
	}
	if !from.Before(*toExclusive) {
		writeError(w, http.StatusBadRequest, "from must not be after to")
		return
	}

	result, err := h.replayer.Rebuild(r.Context(), *from, *toExclusive)
	if err != nil {
		h.log.Error("rebuild Kafka stats", "error", err)
		writeError(w, http.StatusInternalServerError, "rebuild stats failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"from":             from,
		"to":               toExclusive.Add(-time.Nanosecond),
		"events_processed": result.EventsProcessed,
		"stats":            result.Stats,
	})
}

func (h *Handler) handleServiceError(w http.ResponseWriter, err error, operation string) {
	if errors.Is(err, service.ErrValidation) {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.log.Error(operation, "error", err)
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func positiveInt(value string, fallback int) (int, error) {
	if value == "" {
		return fallback, nil
	}
	number, err := strconv.Atoi(value)
	if err != nil || number < 1 {
		return 0, errors.New("value must be a positive integer")
	}
	return number, nil
}

func optionalDate(value string, endOfDay bool) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	date, err := time.Parse("2006-01-02", value)
	if err != nil {
		return nil, err
	}
	if endOfDay {
		date = date.AddDate(0, 0, 1)
	}
	return &date, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
