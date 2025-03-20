INSERT INTO {schema}.country_configs (
    code,
    country,
    region,
    sub_region
)
VALUES (
    :code,
    :country,
    :region,
    :sub_region
)
ON CONFLICT (code) 
DO UPDATE SET 
    country = EXCLUDED.country,
    region = EXCLUDED.region,
    sub_region = EXCLUDED.sub_region,
    updated_at = CURRENT_TIMESTAMP; 