CREATE INDEX IF NOT EXISTS idx_ordering_selections_store_id
    ON ordering_selections(store_id);

CREATE INDEX IF NOT EXISTS idx_production_registrations_store_id
    ON production_registrations(store_id);

CREATE INDEX IF NOT EXISTS idx_audit_logs_actor_role
    ON audit_logs(actor_role);

CREATE INDEX IF NOT EXISTS idx_audit_logs_outcome
    ON audit_logs(outcome);