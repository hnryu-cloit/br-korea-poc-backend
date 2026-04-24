-- 이월 재고 FIFO Lot 추적 테이블 (DDL only)
--
-- 목적:
--   생산(production) 또는 납품(delivery)으로 입고된 재고를 Lot 단위로 관리한다.
--   판매는 lot_date 오름차순(FIFO)으로 오래된 Lot부터 소진하며,
--   유통기한이 지난 잔여 수량을 wasted_qty로 확정한다.
--
-- 데이터 적재:
--   테이블 생성만 담당한다.
--   실제 Lot 데이터(production/delivery INSERT, FIFO 소진, 만료 확정)는
--   load_resource_to_db.py 의 populate_fifo_lots() 에서 수행한다.
--   이유: migrate 단계는 raw 테이블이 비어 있는 상태에서 실행되기 때문이다.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────
-- 1. 테이블 생성
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory_fifo_lots (
    id              BIGSERIAL    PRIMARY KEY,
    masked_stor_cd  TEXT         NOT NULL,
    item_cd         TEXT,
    item_nm         TEXT         NOT NULL,
    lot_type        TEXT         NOT NULL
                    CHECK (lot_type IN ('production', 'delivery')),
    lot_date        DATE         NOT NULL,
    expiry_date     DATE,
    shelf_life_days INT,
    initial_qty     NUMERIC      NOT NULL DEFAULT 0,
    consumed_qty    NUMERIC      NOT NULL DEFAULT 0,
    wasted_qty      NUMERIC      NOT NULL DEFAULT 0,
    unit_cost       NUMERIC      NOT NULL DEFAULT 0,
    status          TEXT         NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'sold_out', 'expired')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────────────
-- 2. 조회 최적화 인덱스
-- ──────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fifo_lots_store_item_date
    ON inventory_fifo_lots (masked_stor_cd, item_nm, lot_date);

CREATE INDEX IF NOT EXISTS idx_fifo_lots_active
    ON inventory_fifo_lots (masked_stor_cd, item_nm, status, expiry_date)
    WHERE status = 'active';

COMMIT;