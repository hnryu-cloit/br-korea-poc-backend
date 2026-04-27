ALTER TABLE store_clusters
    ADD COLUMN IF NOT EXISTS sido TEXT;

ALTER TABLE store_clusters
    ADD COLUMN IF NOT EXISTS store_type TEXT;
