INSERT INTO {SCHEMA}.ticket_age_groups (ticket_group, age_range, count)
VALUES (:ticket_group, :age_range, :count)
ON CONFLICT (ticket_group, age_range) 
DO UPDATE SET 
    count = EXCLUDED.count,
    updated_at = CURRENT_TIMESTAMP; 