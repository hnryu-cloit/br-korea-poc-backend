DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'raw_daily_store_online'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'raw_daily_store_channel'
    ) THEN
        EXECUTE 'ALTER TABLE raw_daily_store_online RENAME TO raw_daily_store_channel';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS raw_daily_store_channel (
    masked_stor_cd TEXT,
    masked_stor_nm TEXT,
    sale_dt TEXT,
    tmzon_div TEXT,
    ho_chnl_cd TEXT,
    sales_org_nm TEXT,
    ho_chnl_div TEXT,
    ho_chnl_nm TEXT,
    sale_amt TEXT,
    ord_cnt TEXT,
    source_file TEXT NOT NULL,
    source_sheet VARCHAR(255),
    loaded_at TIMESTAMPTZ NOT NULL
);

DROP VIEW IF EXISTS raw_daily_store_online;

CREATE OR REPLACE VIEW raw_daily_store_online AS
SELECT *
FROM raw_daily_store_channel;
