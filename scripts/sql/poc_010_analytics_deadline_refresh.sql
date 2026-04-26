DELETE FROM mart_poc_010_analytics_deadline;

INSERT INTO mart_poc_010_analytics_deadline (
    store_id,
    deadline_at,
    generated_at,
    updated_at
)
VALUES
    (:store_id, '14:00', NOW(), NOW());
