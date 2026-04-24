-- resource/06. 유통기한 및 납품일/order_arrival_schedule.xlsx 신규 적재 테이블

CREATE TABLE IF NOT EXISTS raw_order_arrival_schedule (
    masked_stor_cd         TEXT,
    masked_stor_nm         TEXT,
    shipment_center        TEXT,
    item_cd                TEXT,
    item_nm                TEXT,
    ord_grp                TEXT,
    ord_grp_nm             TEXT,
    erp_dgre               TEXT,
    erp_dgre_nm            TEXT,
    erp_web_item_grp       TEXT,
    erp_web_item_grp_nm    TEXT,
    arrival_bucket         TEXT,
    order_deadline_at      TEXT,
    arrival_day_offset     TEXT,
    arrival_expected_at    TEXT,
    applied_reference_note TEXT,
    source_file            TEXT NOT NULL,
    source_sheet           VARCHAR(255),
    loaded_at              TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_order_arrival_reference (
    arrival_bucket      TEXT,
    order_deadline_at   TEXT,
    arrival_day_offset  TEXT,
    arrival_expected_at TEXT,
    reference_note_kr   TEXT,
    source_file         TEXT NOT NULL,
    source_sheet        VARCHAR(255),
    loaded_at           TIMESTAMPTZ NOT NULL
);
