CREATE TABLE IF NOT EXISTS store_clusters (
    masked_stor_cd TEXT PRIMARY KEY,
    sido TEXT,
    store_type TEXT,
    cluster_id TEXT NOT NULL,
    cluster_label TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_store_clusters_cluster_id
    ON store_clusters(cluster_id);
