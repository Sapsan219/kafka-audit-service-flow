package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

var ValidActions = map[string]struct{}{
	"login":    {},
	"view":     {},
	"purchase": {},
}

type Event struct {
	EventID    uuid.UUID       `json:"event_id"`
	UserID     string          `json:"user_id"`
	Action     string          `json:"action"`
	ResourceID string          `json:"resource_id"`
	Meta       json.RawMessage `json:"meta"`
	Timestamp  time.Time       `json:"timestamp"`
}

type HistoryFilter struct {
	UserID      string
	Action      string
	From        *time.Time
	ToExclusive *time.Time
	Page        int
	Limit       int
}

type Stat struct {
	Group string `json:"group"`
	Count int64  `json:"count"`
}

type ReplayResult struct {
	EventsProcessed int64  `json:"events_processed"`
	Stats           []Stat `json:"stats"`
}
