INSERT INTO {schema}.ticket_capacity_configs (ticket_group, event_day, capacity)
VALUES (:ticket_group, :event_day, :capacity)
ON CONFLICT (ticket_group, event_day) 
DO UPDATE SET 
    capacity = EXCLUDED.capacity,
    updated_at = CURRENT_TIMESTAMP; 