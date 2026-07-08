WITH country_mapping AS (
    SELECT 
        code as country_code,
        country as country_name
    FROM {SCHEMA}.country_configs
),

ticket_countries AS (
    SELECT 
        COALESCE(cm.country_name, t.country) as country_name,
        CASE 
            WHEN t.country = :locality THEN 'Local'
            ELSE 'International'
        END as locality_type
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    LEFT JOIN country_mapping cm ON UPPER(t.country) = UPPER(cm.country_code)
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.country IS NOT NULL
),

athlete_nationality AS (
    SELECT 
        country_name,
        locality_type,
        COUNT(*) as count
    FROM ticket_countries
    GROUP BY country_name, locality_type
)

SELECT 
    country_name,
    locality_type,
    count
FROM athlete_nationality
ORDER BY 
    CASE WHEN locality_type = 'Local' THEN 1 ELSE 2 END,
    count DESC;
