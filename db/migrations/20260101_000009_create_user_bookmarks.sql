CREATE TABLE IF NOT EXISTS user_bookmarks (
    id BIGSERIAL PRIMARY KEY,
    store_id VARCHAR(100),
    type VARCHAR(50) NOT NULL,   -- 'sku' | 'order_option' | 'insight'
    ref_id VARCHAR(200) NOT NULL,
    label TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_bookmarks_unique
    ON user_bookmarks(COALESCE(store_id, ''), type, ref_id);

CREATE INDEX IF NOT EXISTS idx_user_bookmarks_store_type
    ON user_bookmarks(store_id, type, created_at DESC);