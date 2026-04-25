CREATE TABLE IF NOT EXISTS dashboard_recommended_questions (
    year_month VARCHAR(6) NOT NULL,
    domain VARCHAR(32) NOT NULL,
    rank_no SMALLINT NOT NULL,
    question TEXT NOT NULL,
    source_agent VARCHAR(64),
    source_question_no VARCHAR(64),
    source_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (year_month, domain, rank_no)
);

CREATE INDEX IF NOT EXISTS idx_dashboard_recommended_questions_lookup
    ON dashboard_recommended_questions (year_month, domain);
