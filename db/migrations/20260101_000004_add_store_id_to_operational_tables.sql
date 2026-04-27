ALTER TABLE ordering_selections
    ADD COLUMN IF NOT EXISTS store_id VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_ordering_selections_store_id_selected_at
    ON ordering_selections(store_id, selected_at DESC);

ALTER TABLE production_registrations
    ADD COLUMN IF NOT EXISTS store_id VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_production_registrations_store_id_registered_at
    ON production_registrations(store_id, registered_at DESC);
