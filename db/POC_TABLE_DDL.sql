-- br_korea_poc PostgreSQL 실제 테이블 DDL
-- 원본: db/migrations/ 기준 / DB: localhost:5435

-- ──────────────────────────────────────────────
-- Meta / Ingestion
-- ──────────────────────────────────────────────

CREATE TABLE ingestion_runs (
    run_id       BIGSERIAL PRIMARY KEY,
    started_at   TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status       VARCHAR(50) NOT NULL,
    message      TEXT
);

CREATE TABLE ingestion_files (
    file_id      BIGSERIAL PRIMARY KEY,
    run_id       BIGINT NOT NULL REFERENCES ingestion_runs(run_id),
    table_name   VARCHAR(255) NOT NULL,
    source_file  TEXT NOT NULL,
    source_sheet VARCHAR(255),
    row_count    BIGINT NOT NULL DEFAULT 0,
    loaded_at    TIMESTAMPTZ NOT NULL,
    status       VARCHAR(50) NOT NULL,
    message      TEXT
);

-- ──────────────────────────────────────────────
-- Source Raw (Oracle → PostgreSQL 적재)
-- ──────────────────────────────────────────────

CREATE TABLE raw_store_master (
    row_no               TEXT,
    masked_stor_cd       TEXT,
    maked_stor_nm        TEXT,   -- 원본 오타 그대로 유지
    actual_sales_amt     TEXT,
    campaign_sales_ratio TEXT,
    store_type           TEXT,
    business_type        TEXT,
    sido                 TEXT,
    region               TEXT,
    shipment_center      TEXT,
    store_area_pyeong    TEXT,
    embedding            vector(768),
    source_file          TEXT NOT NULL,
    source_sheet         VARCHAR(255),
    loaded_at            TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_pay_cd (
    pay_dc_grp_type TEXT,
    entry_nm_1      TEXT,
    pay_dc_cd       TEXT,
    pay_dc_nm       TEXT,
    pay_dc_type     TEXT,
    entry_nm_2      TEXT,
    source_file     TEXT NOT NULL,
    source_sheet    VARCHAR(255),
    loaded_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_daily_store_item (
    masked_stor_cd   TEXT,
    masked_stor_nm   TEXT,
    item_nm          TEXT,
    sale_dt          TEXT,
    item_cd          TEXT,
    item_tax_div     TEXT,
    sale_qty         TEXT,
    sale_amt         TEXT,
    rtn_qty          TEXT,
    rtn_amt          TEXT,
    dc_amt           TEXT,
    enuri_amt        TEXT,
    vat_amt          TEXT,
    actual_sale_amt  TEXT,
    net_sale_amt     TEXT,
    take_in_amt      TEXT,
    take_in_vat_amt  TEXT,
    take_out_amt     TEXT,
    take_out_vat_amt TEXT,
    svc_fee_amt      TEXT,
    svc_fee_vat_amt  TEXT,
    reg_user_id      TEXT,
    reg_date         TEXT,
    upd_user_id      TEXT,
    upd_date         TEXT,
    source_file      TEXT NOT NULL,
    source_sheet     VARCHAR(255),
    loaded_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_daily_store_item_tmzon (
    masked_stor_cd   TEXT,
    masked_stor_nm   TEXT,
    item_nm          TEXT,
    sale_dt          TEXT,
    tmzon_div        TEXT,
    item_cd          TEXT,
    sale_qty         TEXT,
    sale_amt         TEXT,
    rtn_qty          TEXT,
    rtn_amt          TEXT,
    dc_amt           TEXT,
    enuri_amt        TEXT,
    vat_amt          TEXT,
    actual_sale_amt  TEXT,
    net_sale_amt     TEXT,
    take_in_amt      TEXT,
    take_in_vat_amt  TEXT,
    take_out_amt     TEXT,
    take_out_vat_amt TEXT,
    svc_fee_amt      TEXT,
    svc_fee_vat_amt  TEXT,
    reg_user_id      TEXT,
    reg_date         TEXT,
    upd_user_id      TEXT,
    upd_date         TEXT,
    source_file      TEXT NOT NULL,
    source_sheet     VARCHAR(255),
    loaded_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_daily_store_pay_way (
    masked_stor_cd      TEXT,
    masked_stor_nm      TEXT,
    pay_way_cd_nm       TEXT,
    pay_dtl_cd_nm       TEXT,
    sale_dt             TEXT,
    pay_way_cd          TEXT,
    pay_dtl_cd          TEXT,
    pay_amt             TEXT,
    rec_amt             TEXT,
    change_amt          TEXT,
    rtn_pay_amt         TEXT,
    rtn_rec_amt         TEXT,
    rtn_change          TEXT,
    etc_profit_amt      TEXT,
    rtn_etc_profit_amt  TEXT,
    cash_exchng_cpn     TEXT,
    rtn_cash_exchng_cpn TEXT,
    reg_user_id         TEXT,
    reg_date            TEXT,
    upd_user_id         TEXT,
    upd_date            TEXT,
    source_file         TEXT NOT NULL,
    source_sheet        VARCHAR(255),
    loaded_at           TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_daily_store_cpi_tmzon (
    masked_stor_cd TEXT,
    masked_stor_nm TEXT,
    cpi_cd         TEXT,
    cpi_nm         TEXT,
    bill_cnt       TEXT,
    qty_00 TEXT, qty_01 TEXT, qty_02 TEXT, qty_03 TEXT, qty_04 TEXT,
    qty_05 TEXT, qty_06 TEXT, qty_07 TEXT, qty_08 TEXT, qty_09 TEXT,
    qty_10 TEXT, qty_11 TEXT, qty_12 TEXT, qty_13 TEXT, qty_14 TEXT,
    qty_15 TEXT, qty_16 TEXT, qty_17 TEXT, qty_18 TEXT, qty_19 TEXT,
    qty_20 TEXT, qty_21 TEXT, qty_22 TEXT, qty_23 TEXT,
    dc_amt_00 TEXT, dc_amt_01 TEXT, dc_amt_02 TEXT, dc_amt_03 TEXT, dc_amt_04 TEXT,
    dc_amt_05 TEXT, dc_amt_06 TEXT, dc_amt_07 TEXT, dc_amt_08 TEXT, dc_amt_09 TEXT,
    dc_amt_10 TEXT, dc_amt_11 TEXT, dc_amt_12 TEXT, dc_amt_13 TEXT, dc_amt_14 TEXT,
    dc_amt_15 TEXT, dc_amt_16 TEXT, dc_amt_17 TEXT, dc_amt_18 TEXT, dc_amt_19 TEXT,
    dc_amt_20 TEXT, dc_amt_21 TEXT, dc_amt_22 TEXT, dc_amt_23 TEXT,
    act_amt_00 TEXT, act_amt_01 TEXT, act_amt_02 TEXT, act_amt_03 TEXT, act_amt_04 TEXT,
    act_amt_05 TEXT, act_amt_06 TEXT, act_amt_07 TEXT, act_amt_08 TEXT, act_amt_09 TEXT,
    act_amt_10 TEXT, act_amt_11 TEXT, act_amt_12 TEXT, act_amt_13 TEXT, act_amt_14 TEXT,
    act_amt_15 TEXT, act_amt_16 TEXT, act_amt_17 TEXT, act_amt_18 TEXT, act_amt_19 TEXT,
    act_amt_20 TEXT, act_amt_21 TEXT, act_amt_22 TEXT, act_amt_23 TEXT,
    source_file  TEXT NOT NULL,
    source_sheet VARCHAR(255),
    loaded_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_daily_store_channel (
    masked_stor_cd TEXT,
    masked_stor_nm TEXT,
    sale_dt        TEXT,
    tmzon_div      TEXT,
    ho_chnl_cd     TEXT,
    sales_org_nm   TEXT,
    ho_chnl_div    TEXT,
    ho_chnl_nm     TEXT,
    sale_amt       TEXT,
    ord_cnt        TEXT,
    source_file    TEXT NOT NULL,
    source_sheet   VARCHAR(255),
    loaded_at      TIMESTAMPTZ NOT NULL
);

-- ──────────────────────────────────────────────
-- Workbook Raw
-- ──────────────────────────────────────────────

CREATE TABLE raw_production_extract (
    masked_stor_cd TEXT,
    masked_stor_nm TEXT,
    cmp_cd         TEXT,
    prod_dt        TEXT,
    prod_dgre      TEXT,
    item_cd        TEXT,
    item_nm        TEXT,
    prod_qty       TEXT,
    sale_prc       TEXT,
    item_cost      TEXT,
    prod_qty_2     TEXT,
    prod_qty_3     TEXT,
    reprod_qty     TEXT,
    reg_user_id    TEXT,
    reg_date       TEXT,
    upd_user_id    TEXT,
    upd_date       TEXT,
    source_file    TEXT NOT NULL,
    source_sheet   VARCHAR(255),
    loaded_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_order_extract (
    dlv_dt              TEXT,
    masked_stor_cd      TEXT,
    masked_stor_nm      TEXT,
    ord_grp             TEXT,
    ord_grp_nm          TEXT,
    ord_dgre            TEXT,
    ord_dgre_nm         TEXT,
    ord_type            TEXT,
    ord_type_nm         TEXT,
    item_cd             TEXT,
    item_nm             TEXT,
    erp_send_dt         TEXT,
    erp_web_item_grp    TEXT,
    erp_web_item_grp_nm TEXT,
    ord_unit            TEXT,
    ord_noqqty          TEXT,
    ord_prc             TEXT,
    ord_qty             TEXT,
    ord_amt             TEXT,
    ord_vat             TEXT,
    confrm_prc          TEXT,
    confrm_qty          TEXT,
    confrm_amt          TEXT,
    confrm_vat          TEXT,
    confrm_dc_amt       TEXT,
    auto_ord_yn         TEXT,
    erp_dgre            TEXT,
    erp_dgre_nm         TEXT,
    ord_rec_qty         TEXT,
    source_file         TEXT NOT NULL,
    source_sheet        VARCHAR(255),
    loaded_at           TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_inventory_extract (
    masked_stor_cd TEXT,
    masked_stor_nm TEXT,
    cmp_cd         TEXT,
    stock_dt       TEXT,
    item_cd        TEXT,
    item_nm        TEXT,
    gi_qty         TEXT,
    add_sout_qty   TEXT,
    add_mout_qty   TEXT,
    ins_sout_qty   TEXT,
    ins_mout_qty   TEXT,
    mv_in_qty      TEXT,
    mv_out_qty     TEXT,
    rtn_qty        TEXT,
    disuse_qty     TEXT,
    adj_qty        TEXT,
    sale_qty       TEXT,
    prod_in_qty    TEXT,
    prod_out_qty   TEXT,
    last_sale_dt   TEXT,
    cost           TEXT,
    sale_prc       TEXT,
    sale_gram      TEXT,
    reg_user_id    TEXT,
    reg_date       TEXT,
    upd_user_id    TEXT,
    upd_date       TEXT,
    stock_qty      TEXT,
    source_file    TEXT NOT NULL,
    source_sheet   VARCHAR(255),
    loaded_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_workbook_rows (
    workbook_name   TEXT NOT NULL,
    sheet_name      VARCHAR(255) NOT NULL,
    row_index       BIGINT NOT NULL,
    row_values_json JSONB NOT NULL,
    source_file     TEXT NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL
);

-- ──────────────────────────────────────────────
-- Campaign / Settlement / Telecom Raw
-- ──────────────────────────────────────────────

CREATE TABLE raw_campaign_master (
    cmp_cd       TEXT,
    sales_org_cd TEXT,
    cpi_cd       TEXT,
    cpi_nm       TEXT,
    start_dt     TEXT,
    fnsh_dt      TEXT,
    prgrs_status TEXT,
    cpi_kind     TEXT,
    source_file  TEXT NOT NULL,
    source_sheet VARCHAR(255),
    loaded_at    TIMESTAMPTZ NOT NULL
    -- 전체 컬럼: db/migrations/0006_create_campaign_raw_tables.sql 참조
);

CREATE TABLE raw_campaign_item_group (
    cmp_cd          TEXT,
    sales_org_cd    TEXT,
    cpi_cd          TEXT,
    cpi_item_grp_cd TEXT,
    cpi_item_grp_nm TEXT,
    cpi_cond_type   TEXT,
    cpi_dc_type     TEXT,
    max_dc_amt      TEXT,
    use_yn          TEXT,
    source_file     TEXT NOT NULL,
    source_sheet    VARCHAR(255),
    loaded_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_campaign_item (
    cmp_cd          TEXT,
    sales_org_cd    TEXT,
    cpi_cd          TEXT,
    cpi_item_grp_cd TEXT,
    item_lvl        TEXT,
    item_cd         TEXT,
    dc_rate_amt     TEXT,
    use_yn          TEXT,
    source_file     TEXT NOT NULL,
    source_sheet    VARCHAR(255),
    loaded_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_settlement_master (
    cmp_cd              TEXT,
    sales_org_cd        TEXT,
    pay_dc_ty_cd        TEXT,
    coop_cd             TEXT,
    start_dt            TEXT,
    fnsh_dt             TEXT,
    pay_dc_methd        TEXT,
    hq_allot_rate       TEXT,
    stor_allot_rate     TEXT,
    coop_cmp_allot_rate TEXT,
    source_file         TEXT NOT NULL,
    source_sheet        VARCHAR(255),
    loaded_at           TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_telecom_discount_type (
    cmp_cd             TEXT,
    pay_dc_grp_type    TEXT,
    pay_dc_cd          TEXT,
    sales_org_cd       TEXT,
    pay_dc_grp_type_nm TEXT,
    pay_dc_nm          TEXT,
    grp_prrty          TEXT,
    source_file        TEXT NOT NULL,
    source_sheet       VARCHAR(255),
    loaded_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_telecom_discount_policy (
    cmp_cd            TEXT,
    pay_dc_grp_type   TEXT,
    pay_dc_cd         TEXT,
    coop_cmp_grade_cd TEXT,
    start_dt          TEXT,
    fnsh_dt           TEXT,
    pay_dc_methd      TEXT,
    pay_dc_val        TEXT,
    item_dc_yn        TEXT,
    use_yn            TEXT,
    source_file       TEXT NOT NULL,
    source_sheet      VARCHAR(255),
    loaded_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_telecom_discount_item (
    cmp_cd            TEXT,
    pay_dc_grp_type   TEXT,
    pay_dc_cd         TEXT,
    coop_cmp_grade_cd TEXT,
    item_cd           TEXT,
    item_nm           TEXT,
    use_yn            TEXT,
    source_file       TEXT NOT NULL,
    source_sheet      VARCHAR(255),
    loaded_at         TIMESTAMPTZ NOT NULL
);

-- ──────────────────────────────────────────────
-- External / Enrichment
-- ──────────────────────────────────────────────

CREATE TABLE raw_weather_daily (
    weather_dt       TEXT NOT NULL,
    sido             TEXT NOT NULL,
    avg_temp_c       NUMERIC(8,2) NOT NULL,
    precipitation_mm NUMERIC(10,2) NOT NULL,
    source_provider  TEXT NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (weather_dt, sido)
);

CREATE TABLE raw_stock_rate (
    stor_cd        TEXT,
    masked_stor_cd TEXT,
    masked_stor_nm TEXT,
    prc_dt         TEXT,
    item_cd        TEXT,
    item_nm        TEXT,
    ord_avg        TEXT,
    sal_avg        TEXT,
    stk_avg        TEXT,
    stk_rt         TEXT,
    source_file    TEXT NOT NULL,
    source_sheet   VARCHAR(255),
    loaded_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_stockout_time (
    stor_cd        TEXT,
    masked_stor_cd TEXT,
    masked_stor_nm TEXT,
    prc_dt         TEXT,
    item_cd        TEXT,
    item_nm        TEXT,
    stor_cnt       TEXT,
    ranking_main   TEXT,
    o_ranking1     TEXT,
    o_ranking3     TEXT,
    ord_avg        TEXT,
    sal_avg        TEXT,
    stk_avg        TEXT,
    stk_rt         TEXT,
    sold_out_tm    TEXT,
    source_file    TEXT NOT NULL,
    source_sheet   VARCHAR(255),
    loaded_at      TIMESTAMPTZ NOT NULL
);

-- ──────────────────────────────────────────────
-- Derived
-- ──────────────────────────────────────────────

CREATE TABLE core_inferred_stockout_event (
    id                  BIGSERIAL PRIMARY KEY,
    masked_stor_cd      TEXT NOT NULL,
    sale_dt             TEXT NOT NULL,
    item_cd             TEXT NOT NULL,
    item_nm             TEXT NOT NULL,
    is_stockout         BOOLEAN NOT NULL DEFAULT TRUE,
    stockout_hour       INTEGER NOT NULL,
    rule_type           TEXT NOT NULL,
    source_table        TEXT NOT NULL,
    open_hour           INTEGER,
    close_hour          INTEGER,
    zero_sales_window   INTEGER,
    evidence_start_hour INTEGER,
    evidence_end_hour   INTEGER,
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ──────────────────────────────────────────────
-- Operational
-- ──────────────────────────────────────────────

CREATE TABLE audit_logs (
    id         BIGSERIAL PRIMARY KEY,
    timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    domain     VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    actor_role VARCHAR(100) NOT NULL,
    route      VARCHAR(100) NOT NULL,
    outcome    VARCHAR(50) NOT NULL,
    message    TEXT NOT NULL,
    metadata   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE ordering_selections (
    id          BIGSERIAL PRIMARY KEY,
    option_id   VARCHAR(100) NOT NULL,
    reason      TEXT,
    actor       VARCHAR(100) NOT NULL,
    saved       BOOLEAN NOT NULL DEFAULT TRUE,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    store_id    VARCHAR(100)
);

CREATE TABLE production_registrations (
    id               BIGSERIAL PRIMARY KEY,
    sku_id           VARCHAR(100) NOT NULL,
    qty              INTEGER NOT NULL,
    registered_by    VARCHAR(100) NOT NULL,
    feedback_type    VARCHAR(100),
    feedback_message TEXT,
    registered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    store_id         VARCHAR(100)
);

CREATE TABLE user_bookmarks (
    id         BIGSERIAL PRIMARY KEY,
    store_id   VARCHAR(100),
    type       VARCHAR(50) NOT NULL,
    ref_id     VARCHAR(200) NOT NULL,
    label      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);