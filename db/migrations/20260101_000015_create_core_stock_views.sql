-- core_stock_rate: raw_stock_rate 수치형 정제 뷰
CREATE OR REPLACE VIEW core_stock_rate AS
SELECT
    MASKED_STOR_CD                              AS masked_stor_cd,
    MASKED_STOR_NM                              AS masked_stor_nm,
    PRC_DT                                      AS prc_dt,
    ITEM_CD                                     AS item_cd,
    ITEM_NM                                     AS item_nm,
    NULLIF(TRIM(ORD_AVG), '')::NUMERIC          AS ord_avg,   -- 판매가능수량
    NULLIF(TRIM(SAL_AVG), '')::NUMERIC          AS sal_avg,   -- 판매량
    NULLIF(TRIM(STK_AVG), '')::NUMERIC          AS stk_avg,   -- 재고량 (음수=품절초과)
    NULLIF(TRIM(STK_RT),  '')::NUMERIC          AS stk_rt,    -- 재고율 (음수=품절초과)
    CASE
        WHEN NULLIF(TRIM(STK_AVG), '')::NUMERIC < 0 THEN TRUE
        ELSE FALSE
    END                                         AS is_stockout,
    source_file,
    loaded_at
FROM raw_stock_rate
WHERE MASKED_STOR_CD IS NOT NULL
  AND PRC_DT IS NOT NULL
  AND ITEM_CD IS NOT NULL;


-- core_stockout_time: raw_stockout_time SOLD_OUT_TM 정제 뷰
--
-- SOLD_OUT_TM 정제 규칙:
--   'N시' 형식 → 해당 일 N시에 품절 발생  (stockout_hour = N, remaining_qty = NULL)
--   숫자 N     → 영업 마감 시 잔여 재고량  (stockout_hour = NULL, remaining_qty = N)
--   NULL       → 데이터 없음
CREATE OR REPLACE VIEW core_stockout_time AS
SELECT
    MASKED_STOR_CD                                              AS masked_stor_cd,
    MASKED_STOR_NM                                             AS masked_stor_nm,
    PRC_DT                                                     AS prc_dt,
    ITEM_CD                                                    AS item_cd,
    ITEM_NM                                                    AS item_nm,
    source_file                                                AS category,  -- CK / JBOD / 기타
    NULLIF(TRIM(STOR_CNT),    '')::NUMERIC                    AS stor_cnt,
    NULLIF(TRIM(RANKING_MAIN),'')::NUMERIC                    AS ranking_main,
    NULLIF(TRIM(O_RANKING1),  '')::NUMERIC                    AS o_ranking1,
    NULLIF(TRIM(O_RANKING3),  '')::NUMERIC                    AS o_ranking3,
    NULLIF(TRIM(ORD_AVG),     '')::NUMERIC                    AS ord_avg,
    NULLIF(TRIM(SAL_AVG),     '')::NUMERIC                    AS sal_avg,
    NULLIF(TRIM(STK_AVG),     '')::NUMERIC                    AS stk_avg,
    NULLIF(TRIM(STK_RT),      '')::NUMERIC                    AS stk_rt,

    -- 품절 여부: 'N시' 형식이면 TRUE
    CASE
        WHEN sold_out_tm ~ '^[0-9]+시$' THEN TRUE
        ELSE FALSE
    END                                                        AS is_stockout,

    -- 품절 시각 (정수 시각): 'N시' 또는 'NN시' → N
    CASE
        WHEN sold_out_tm ~ '^[0-9]+시$'
        THEN REGEXP_REPLACE(sold_out_tm, '시$', '')::INT
        ELSE NULL
    END                                                        AS stockout_hour,

    -- 잔여 재고량: 순수 숫자이면 잔여 재고 (품절 없음)
    CASE
        WHEN sold_out_tm ~ '^[0-9]+$'
        THEN sold_out_tm::INT
        ELSE NULL
    END                                                        AS remaining_qty,

    -- 원본값 보존
    sold_out_tm                                               AS sold_out_tm_raw,
    loaded_at
FROM raw_stockout_time
WHERE MASKED_STOR_CD IS NOT NULL
  AND PRC_DT IS NOT NULL
  AND ITEM_CD IS NOT NULL;