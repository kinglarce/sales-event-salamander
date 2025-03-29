SELECT 
    CASE 
        WHEN t.region_of_residence IN (
            SELECT code FROM {SCHEMA}.country_configs
        ) THEN (
            SELECT country 
            FROM {SCHEMA}.country_configs 
            WHERE code = t.region_of_residence
        )
        ELSE t.region_of_residence
    END as region,
    COUNT(*) as count
FROM {SCHEMA}.tickets t
WHERE t.region_of_residence IS NOT NULL
GROUP BY t.region_of_residence
ORDER BY count DESC 