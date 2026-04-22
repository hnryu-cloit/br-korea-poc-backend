CREATE OR REPLACE VIEW core_store_master AS
SELECT
    NULLIF(masked_stor_cd, '') AS masked_stor_cd,
    NULLIF(maked_stor_nm, '') AS masked_stor_nm,
    NULLIF(actual_sales_amt, '')::numeric AS actual_sales_amt,
    NULLIF(campaign_sales_ratio, '')::numeric AS campaign_sales_ratio,
    NULLIF(store_type, '') AS store_type,
    NULLIF(business_type, '') AS business_type,
    NULLIF(sido, '') AS sido,
    NULLIF(region, '') AS region,
    NULLIF(shipment_center, '') AS shipment_center,
    NULLIF(store_area_pyeong, '')::numeric AS store_area_pyeong,
    source_file,
    source_sheet,
    loaded_at
FROM raw_store_master;

CREATE OR REPLACE VIEW core_daily_item_sales AS
SELECT
    NULLIF(masked_stor_cd, '') AS masked_stor_cd,
    NULLIF(masked_stor_nm, '') AS masked_stor_nm,
    NULLIF(item_nm, '') AS item_nm,
    NULLIF(sale_dt, '') AS sale_dt,
    NULLIF(item_cd, '') AS item_cd,
    NULLIF(item_tax_div, '') AS item_tax_div,
    COALESCE(NULLIF(sale_qty, '')::numeric, 0) AS sale_qty,
    COALESCE(NULLIF(sale_amt, '')::numeric, 0) AS sale_amt,
    COALESCE(NULLIF(rtn_qty, '')::numeric, 0) AS rtn_qty,
    COALESCE(NULLIF(rtn_amt, '')::numeric, 0) AS rtn_amt,
    COALESCE(NULLIF(dc_amt, '')::numeric, 0) AS dc_amt,
    COALESCE(NULLIF(enuri_amt, '')::numeric, 0) AS enuri_amt,
    COALESCE(NULLIF(vat_amt, '')::numeric, 0) AS vat_amt,
    COALESCE(NULLIF(actual_sale_amt, '')::numeric, 0) AS actual_sale_amt,
    COALESCE(NULLIF(net_sale_amt, '')::numeric, 0) AS net_sale_amt,
    source_file,
    source_sheet,
    loaded_at
FROM raw_daily_store_item;

CREATE OR REPLACE VIEW core_hourly_item_sales AS
SELECT
    NULLIF(masked_stor_cd, '') AS masked_stor_cd,
    NULLIF(masked_stor_nm, '') AS masked_stor_nm,
    NULLIF(item_nm, '') AS item_nm,
    NULLIF(sale_dt, '') AS sale_dt,
    LPAD(NULLIF(tmzon_div, ''), 2, '0') AS tmzon_div,
    NULLIF(item_cd, '') AS item_cd,
    COALESCE(NULLIF(sale_qty, '')::numeric, 0) AS sale_qty,
    COALESCE(NULLIF(sale_amt, '')::numeric, 0) AS sale_amt,
    COALESCE(NULLIF(rtn_qty, '')::numeric, 0) AS rtn_qty,
    COALESCE(NULLIF(rtn_amt, '')::numeric, 0) AS rtn_amt,
    COALESCE(NULLIF(dc_amt, '')::numeric, 0) AS dc_amt,
    COALESCE(NULLIF(vat_amt, '')::numeric, 0) AS vat_amt,
    COALESCE(NULLIF(actual_sale_amt, '')::numeric, 0) AS actual_sale_amt,
    COALESCE(NULLIF(net_sale_amt, '')::numeric, 0) AS net_sale_amt,
    source_file,
    source_sheet,
    loaded_at
FROM raw_daily_store_item_tmzon;

CREATE OR REPLACE VIEW core_channel_sales AS
SELECT
    NULLIF(masked_stor_cd, '') AS masked_stor_cd,
    NULLIF(masked_stor_nm, '') AS masked_stor_nm,
    NULLIF(sale_dt, '') AS sale_dt,
    LPAD(NULLIF(tmzon_div, ''), 2, '0') AS tmzon_div,
    NULLIF(ho_chnl_cd, '') AS ho_chnl_cd,
    NULLIF(sales_org_nm, '') AS sales_org_nm,
    NULLIF(ho_chnl_div, '') AS ho_chnl_div,
    NULLIF(ho_chnl_nm, '') AS ho_chnl_nm,
    COALESCE(NULLIF(sale_amt, '')::numeric, 0) AS sale_amt,
    COALESCE(NULLIF(ord_cnt, '')::numeric, 0) AS ord_cnt,
    source_file,
    source_sheet,
    loaded_at
FROM raw_daily_store_channel;
