INSERT INTO {schema}.event_capacity_configs (category, value)
VALUES (:category, :value)
ON CONFLICT (category) 
DO UPDATE SET value = EXCLUDED.value; 