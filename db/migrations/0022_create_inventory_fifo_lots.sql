-- 이월 재고 FIFO Lot 추적 테이블
--
-- 목적:
--   생산(production) 또는 납품(delivery)으로 입고된 재고를 Lot 단위로 관리한다.
--   판매는 lot_date 오름차순(FIFO)으로 오래된 Lot부터 소진하며,
--   유통기한이 지난 잔여 수량을 wasted_qty로 확정한다.
--
-- Lot 유형:
--   production : raw_production_extract 기반 완제품(도넛·베이글 등)
--   delivery   : raw_order_extract(confrm_qty) 기반 납품 원재료·패키지

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

-- 조회 최적화 인덱스
CREATE INDEX IF NOT EXISTS idx_fifo_lots_store_item_date
    ON inventory_fifo_lots (masked_stor_cd, item_nm, lot_date);

CREATE INDEX IF NOT EXISTS idx_fifo_lots_active
    ON inventory_fifo_lots (masked_stor_cd, item_nm, status, expiry_date)
    WHERE status = 'active';

-- ──────────────────────────────────────────────────────────────────────
-- 2. 생산 Lot 적재 (raw_production_extract)
--    유통기한 기본값: shelf_life_days 없으면 1일(당일 소진 원칙)
-- ──────────────────────────────────────────────────────────────────────
INSERT INTO inventory_fifo_lots
    (masked_stor_cd, item_cd, item_nm, lot_type,
     lot_date, expiry_date, shelf_life_days,
     initial_qty, unit_cost)
SELECT
    p.masked_stor_cd,
    p.item_cd,
    p.item_nm,
    'production',
    TO_DATE(p.prod_dt, 'YYYYMMDD'),
    TO_DATE(p.prod_dt, 'YYYYMMDD')
        + COALESCE(NULLIF(TRIM(s.shelf_life_days), '')::INT, 1),
    COALESCE(NULLIF(TRIM(s.shelf_life_days), '')::INT, 1),
    COALESCE(NULLIF(TRIM(p.prod_qty),   '')::NUMERIC, 0)
    + COALESCE(NULLIF(TRIM(p.prod_qty_2), '')::NUMERIC, 0)
    + COALESCE(NULLIF(TRIM(p.prod_qty_3), '')::NUMERIC, 0)
    + COALESCE(NULLIF(TRIM(p.reprod_qty), '')::NUMERIC, 0),
    COALESCE(NULLIF(TRIM(p.item_cost), '')::NUMERIC, 0)
FROM raw_production_extract p
LEFT JOIN raw_product_shelf_life s
    ON p.item_nm = s.item_nm
WHERE (
    COALESCE(NULLIF(TRIM(p.prod_qty),   '')::NUMERIC, 0)
    + COALESCE(NULLIF(TRIM(p.prod_qty_2), '')::NUMERIC, 0)
    + COALESCE(NULLIF(TRIM(p.prod_qty_3), '')::NUMERIC, 0)
    + COALESCE(NULLIF(TRIM(p.reprod_qty), '')::NUMERIC, 0)
) > 0;

-- ──────────────────────────────────────────────────────────────────────
-- 3. 납품 Lot 적재 (raw_order_extract, confrm_qty 기준)
--    유통기한 기본값: shelf_life_days 없으면 90일(원재료 기준)
-- ──────────────────────────────────────────────────────────────────────
INSERT INTO inventory_fifo_lots
    (masked_stor_cd, item_cd, item_nm, lot_type,
     lot_date, expiry_date, shelf_life_days,
     initial_qty, unit_cost)
SELECT
    o.masked_stor_cd,
    o.item_cd,
    o.item_nm,
    'delivery',
    TO_DATE(o.dlv_dt, 'YYYYMMDD'),
    TO_DATE(o.dlv_dt, 'YYYYMMDD')
        + COALESCE(NULLIF(TRIM(s.shelf_life_days), '')::INT, 90),
    COALESCE(NULLIF(TRIM(s.shelf_life_days), '')::INT, 90),
    COALESCE(NULLIF(TRIM(o.confrm_qty), '')::NUMERIC, 0),
    COALESCE(NULLIF(TRIM(o.confrm_prc), '')::NUMERIC, 0)
FROM raw_order_extract o
LEFT JOIN raw_product_shelf_life s
    ON o.item_nm = s.item_nm
WHERE COALESCE(NULLIF(TRIM(o.confrm_qty), '')::NUMERIC, 0) > 0;

-- ──────────────────────────────────────────────────────────────────────
-- 4. FIFO 소진 적용
--    판매(core_daily_item_sales)를 날짜 오름차순으로 처리하며
--    lot_date 가장 오래된 production Lot부터 consumed_qty를 차감한다.
-- ──────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    r         RECORD;
    lot       RECORD;
    remaining NUMERIC;
    deduct    NUMERIC;
BEGIN
    FOR r IN
        SELECT masked_stor_cd, sale_dt, item_nm, sale_qty
        FROM   core_daily_item_sales
        WHERE  sale_qty > 0
        ORDER  BY masked_stor_cd, item_nm, sale_dt
    LOOP
        remaining := r.sale_qty;

        FOR lot IN
            SELECT id, initial_qty, consumed_qty
            FROM   inventory_fifo_lots
            WHERE  masked_stor_cd = r.masked_stor_cd
              AND  item_nm        = r.item_nm
              AND  lot_type       = 'production'
              AND  lot_date       <= TO_DATE(r.sale_dt, 'YYYYMMDD')
              AND  status         = 'active'
            ORDER  BY lot_date ASC
        LOOP
            EXIT WHEN remaining <= 0;

            deduct := LEAST(remaining, lot.initial_qty - lot.consumed_qty);
            CONTINUE WHEN deduct <= 0;

            UPDATE inventory_fifo_lots
            SET    consumed_qty = consumed_qty + deduct,
                   status       = CASE
                                    WHEN consumed_qty + deduct >= initial_qty
                                    THEN 'sold_out'
                                    ELSE 'active'
                                  END,
                   updated_at   = NOW()
            WHERE  id = lot.id;

            remaining := remaining - deduct;
        END LOOP;
    END LOOP;
END;
$$;

-- ──────────────────────────────────────────────────────────────────────
-- 5. 유통기한 초과 Lot 확정
--    잔여 수량이 있는 채로 expiry_date가 지난 Lot을 expired로 처리하고
--    남은 수량을 wasted_qty에 기록한다.
-- ──────────────────────────────────────────────────────────────────────
UPDATE inventory_fifo_lots
SET    wasted_qty  = initial_qty - consumed_qty,
       status      = 'expired',
       updated_at  = NOW()
WHERE  status       = 'active'
  AND  expiry_date  IS NOT NULL
  AND  expiry_date  < CURRENT_DATE
  AND  (initial_qty - consumed_qty) > 0;

COMMIT;