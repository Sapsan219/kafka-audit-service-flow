-- Creates the audit log, transactional outbox, analytics event store and rolling cache.
BEGIN;

CREATE TABLE IF NOT EXISTS audit_log (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('login', 'view', 'purchase')),
    resource_id TEXT NOT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_log_user_timestamp
    ON audit_log (user_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_audit_log_action_timestamp
    ON audit_log (action, timestamp DESC);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id UUID PRIMARY KEY REFERENCES audit_log(event_id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
    ON outbox_events (created_at, event_id)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS analytics_events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('login', 'view', 'purchase')),
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_analytics_events_timestamp
    ON analytics_events (timestamp);

CREATE TABLE IF NOT EXISTS stats_cache (
    action TEXT PRIMARY KEY CHECK (action IN ('login', 'view', 'purchase')),
    count BIGINT NOT NULL CHECK (count >= 0),
    window_from TIMESTAMPTZ NOT NULL,
    window_to TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
