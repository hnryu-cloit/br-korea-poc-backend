INSERT INTO mart_poc_010_production_waste_daily (
    store_id,
    target_date,
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
VALUES (
    :store_id,
    :target_date,
    :item_cd,
    :item_nm,
    :total_waste_qty,
    :total_waste_amount,
    :avg_cost,
    :adjusted_loss_qty,
    :adjusted_loss_amount,
    :estimated_expiry_loss_qty,
    :assumed_shelf_life_days,
    :expiry_risk_level,
    NOW(),
    NOW()
)
ON CONFLICT (target_date, item_cd, item_nm)
DO UPDATE SET
    total_waste_qty = EXCLUDED.total_waste_qty,
    total_waste_amount = EXCLUDED.total_waste_amount,
    avg_cost = EXCLUDED.avg_cost,
    adjusted_loss_qty = EXCLUDED.adjusted_loss_qty,
    adjusted_loss_amount = EXCLUDED.adjusted_loss_amount,
    estimated_expiry_loss_qty = EXCLUDED.estimated_expiry_loss_qty,
    assumed_shelf_life_days = EXCLUDED.assumed_shelf_life_days,
    expiry_risk_level = EXCLUDED.expiry_risk_level,
    updated_at = NOW();
