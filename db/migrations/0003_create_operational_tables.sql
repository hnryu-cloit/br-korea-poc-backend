CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    domain VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    actor_role VARCHAR(100) NOT NULL,
    route VARCHAR(100) NOT NULL,
    outcome VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_domain_timestamp
    ON audit_logs(domain, timestamp DESC);

CREATE TABLE IF NOT EXISTS ordering_selections (
    id BIGSERIAL PRIMARY KEY,
    option_id VARCHAR(100) NOT NULL,
    reason TEXT,
    actor VARCHAR(100) NOT NULL,
    saved BOOLEAN NOT NULL DEFAULT TRUE,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ordering_selections_selected_at
    ON ordering_selections(selected_at DESC);

CREATE TABLE IF NOT EXISTS production_registrations (
    id BIGSERIAL PRIMARY KEY,
    sku_id VARCHAR(100) NOT NULL,
    qty INTEGER NOT NULL,
    registered_by VARCHAR(100) NOT NULL,
    feedback_type VARCHAR(100),
    feedback_message TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_production_registrations_registered_at
    ON production_registrations(registered_at DESC);
