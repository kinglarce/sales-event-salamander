SELECT 
    category,
    value
FROM {SCHEMA}.event_capacity_configs
WHERE category IN ('max_capacity', 'start_wave', 'price_tier'); 