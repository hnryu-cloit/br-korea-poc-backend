CREATE TABLE IF NOT EXISTS mart_poc_010_analytics_deadline (
    store_id VARCHAR(64) NOT NULL DEFAULT 'POC_010',
    deadline_at TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (deadline_at)
);

CREATE INDEX IF NOT EXISTS idx_mart_poc_010_analytics_deadline_at
    ON mart_poc_010_analytics_deadline(deadline_at ASC);
