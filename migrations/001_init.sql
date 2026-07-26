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

CREATE TABLE IF NOT EXISTS stats_cache (
    action TEXT PRIMARY KEY CHECK (action IN ('login', 'view', 'purchase')),
    count BIGINT NOT NULL CHECK (count >= 0),
    window_from TIMESTAMPTZ NOT NULL,
    window_to TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
