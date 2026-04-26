DELETE FROM mart_poc_010_production_waste_monthly
WHERE target_month BETWEEN :start_month AND :end_month;

INSERT INTO mart_poc_010_production_waste_monthly (
    store_id,
    target_month,
    item_cd,
    item_nm,
    total_waste_qty,
    total_waste_amount,
    avg_cost,
    adjusted_loss_qty,
    adjusted_loss_amount,
    estimated_expiry_loss_qty,
    assumed_shelf_life_days,
    expiry_risk_level,
    generated_at,
    updated_at
)
SELECT
    CAST(:store_id AS VARCHAR(64)) AS store_id,
    SUBSTRING(target_date, 1, 4) || '-' || SUBSTRING(target_date, 5, 2) AS target_month,
    item_cd,
    item_nm,
    SUM(total_waste_qty) AS total_waste_qty,
    SUM(total_waste_amount) AS total_waste_amount,
    AVG(avg_cost) AS avg_cost,
    SUM(adjusted_loss_qty) AS adjusted_loss_qty,
    SUM(adjusted_loss_amount) AS adjusted_loss_amount,
    SUM(estimated_expiry_loss_qty) AS estimated_expiry_loss_qty,
    MAX(assumed_shelf_life_days) AS assumed_shelf_life_days,
    MAX(expiry_risk_level) AS expiry_risk_level,
    NOW(),
    NOW()
FROM mart_poc_010_production_waste_daily
WHERE store_id = :store_id
  AND target_date BETWEEN :start_date AND :end_date
GROUP BY
    SUBSTRING(target_date, 1, 4) || '-' || SUBSTRING(target_date, 5, 2),
    item_cd,
    item_nm
ORDER BY target_month, item_nm;
