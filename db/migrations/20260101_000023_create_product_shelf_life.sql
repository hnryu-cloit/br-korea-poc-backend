-- resource/06. 유통기한 및 납품일/product_shelf_life.xlsx 신규 적재 테이블

CREATE TABLE IF NOT EXISTS raw_product_shelf_life (
    item_cd                TEXT,
    item_nm                TEXT,
    item_group             TEXT,
    shelf_life_days        TEXT,
    source_order_group_cd  TEXT,
    source_order_group_nm  TEXT,
    applied_reference_note TEXT,
    source_file            TEXT NOT NULL,
    source_sheet           VARCHAR(255),
    loaded_at              TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_product_shelf_life_group_reference (
    item_group         TEXT,
    shelf_life_days    TEXT,
    reference_note_kr  TEXT,
    source_file        TEXT NOT NULL,
    source_sheet       VARCHAR(255),
    loaded_at          TIMESTAMPTZ NOT NULL
);
