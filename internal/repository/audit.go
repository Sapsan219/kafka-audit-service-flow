package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"audit-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AuditRepository struct {
	pool *pgxpool.Pool
}

func NewAuditRepository(pool *pgxpool.Pool) *AuditRepository {
	return &AuditRepository{pool: pool}
}

func (r *AuditRepository) SaveEventWithOutbox(ctx context.Context, event domain.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal outbox event: %w", err)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin audit event transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_log (event_id, user_id, action, resource_id, meta, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, event.EventID, event.UserID, event.Action, event.ResourceID, event.Meta, event.Timestamp); err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (event_id, payload)
		VALUES ($1, $2)
	`, event.EventID, string(payload)); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit audit event transaction: %w", err)
	}
	return nil
}

func (r *AuditRepository) History(
	ctx context.Context,
	filter domain.HistoryFilter,
) ([]domain.Event, int64, error) {
	conditions := []string{"user_id = $1"}
	args := []any{filter.UserID}

	if filter.Action != "" {
		args = append(args, filter.Action)
		conditions = append(conditions, fmt.Sprintf("action = $%d", len(args)))
	}
	if filter.From != nil {
		args = append(args, *filter.From)
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", len(args)))
	}
	if filter.ToExclusive != nil {
		args = append(args, *filter.ToExclusive)
		conditions = append(conditions, fmt.Sprintf("timestamp < $%d", len(args)))
	}

	where := strings.Join(conditions, " AND ")

	var total int64
	if err := r.pool.QueryRow(
		ctx,
		"SELECT count(*) FROM audit_log WHERE "+where,
		args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count audit events: %w", err)
	}

	args = append(args, filter.Limit, (filter.Page-1)*filter.Limit)
	query := fmt.Sprintf(`
		SELECT event_id, user_id, action, resource_id, meta, timestamp
		FROM audit_log
		WHERE %s
		ORDER BY timestamp DESC, event_id DESC
		LIMIT $%d OFFSET $%d
	`, where, len(args)-1, len(args))

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query audit history: %w", err)
	}
	defer rows.Close()

	events := make([]domain.Event, 0)
	for rows.Next() {
		var event domain.Event
		if err := rows.Scan(
			&event.EventID,
			&event.UserID,
			&event.Action,
			&event.ResourceID,
			&event.Meta,
			&event.Timestamp,
		); err != nil {
			return nil, 0, fmt.Errorf("scan audit event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate audit history: %w", err)
	}

	return events, total, nil
}

func (r *AuditRepository) Stats(
	ctx context.Context,
	userID string,
	groupBy string,
) ([]domain.Stat, error) {
	groupExpression := "action"
	if groupBy == "day" {
		groupExpression = "to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD')"
	}

	where := ""
	args := make([]any, 0, 1)
	if userID != "" {
		where = " WHERE user_id = $1"
		args = append(args, userID)
	}

	// По условиям ТЗ агрегаты считаются из PostgreSQL. В потоковой архитектуре
	// эту группировку можно непрерывно считать из user-actions через Kafka Streams
	// или ksqlDB и сохранять результат в materialized view.
	query := fmt.Sprintf(`
		SELECT %s AS group_value, count(*)
		FROM audit_log
		%s
		GROUP BY group_value
		ORDER BY group_value
	`, groupExpression, where)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query audit stats: %w", err)
	}
	defer rows.Close()

	stats := make([]domain.Stat, 0)
	for rows.Next() {
		var stat domain.Stat
		if err := rows.Scan(&stat.Group, &stat.Count); err != nil {
			return nil, fmt.Errorf("scan audit stat: %w", err)
		}
		stats = append(stats, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit stats: %w", err)
	}

	return stats, nil
}

func (r *AuditRepository) StoreAnalyticsAndRefresh(
	ctx context.Context,
	events []domain.Event,
	from time.Time,
	to time.Time,
) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin stats cache transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	for _, event := range events {
		if _, err := tx.Exec(ctx, `
			INSERT INTO analytics_events (event_id, user_id, action, timestamp)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (event_id) DO NOTHING
		`, event.EventID, event.UserID, event.Action, event.Timestamp); err != nil {
			return fmt.Errorf("store analytics event %s: %w", event.EventID, err)
		}
	}

	if _, err := tx.Exec(ctx, `DELETE FROM analytics_events WHERE timestamp < $1`, from); err != nil {
		return fmt.Errorf("delete expired analytics events: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM stats_cache`); err != nil {
		return fmt.Errorf("clear stats cache: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO stats_cache (action, count, window_from, window_to, updated_at)
		SELECT action, count(*), $1, $2, now()
		FROM analytics_events
		WHERE timestamp >= $1 AND timestamp < $2
		GROUP BY action
	`, from, to); err != nil {
		return fmt.Errorf("insert stats cache: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit stats cache transaction: %w", err)
	}
	return nil
}

func (r *AuditRepository) PendingOutbox(
	ctx context.Context,
	limit int,
) ([]domain.Event, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT payload
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at, event_id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]domain.Event, 0, limit)
	for rows.Next() {
		var payload json.RawMessage
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan outbox payload: %w", err)
		}
		var event domain.Event
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, fmt.Errorf("decode outbox payload: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return events, nil
}

func (r *AuditRepository) MarkOutboxPublished(ctx context.Context, eventID string) error {
	if _, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = now(), attempts = attempts + 1, last_error = NULL
		WHERE event_id = $1
	`, eventID); err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	return nil
}

func (r *AuditRepository) MarkOutboxFailed(
	ctx context.Context,
	eventID string,
	message string,
) error {
	if _, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET attempts = attempts + 1, last_error = $2
		WHERE event_id = $1
	`, eventID, message); err != nil {
		return fmt.Errorf("mark outbox event failed: %w", err)
	}
	return nil
}
