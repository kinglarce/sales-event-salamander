WITH country_mapping AS (
    SELECT 
        code as country_code,
        country as country_name
    FROM {SCHEMA}.country_configs
),

ticket_nationality AS (
    SELECT 
        t.country,
        t.nationality,
        ts.ticket_category,
        COALESCE(cm.country_name, t.country) as country_name,
        CASE 
            WHEN t.country = :locality THEN 'Local'
            ELSE 'International'
        END as locality_type
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    LEFT JOIN country_mapping cm ON UPPER(t.country) = UPPER(cm.country_code)
    WHERE ts.ticket_category NOT IN ('extra')
      AND (t.country IS NOT NULL OR t.nationality IS NOT NULL)
),

athlete_nationality AS (
    SELECT 
        country_name,
        locality_type,
        COUNT(*) as count
    FROM ticket_nationality
    WHERE ticket_category != 'spectator'
    GROUP BY country_name, locality_type
),

spectator_nationality AS (
    SELECT 
        country_name,
        locality_type,
        COUNT(*) as count
    FROM ticket_nationality
    WHERE ticket_category = 'spectator'
    GROUP BY country_name, locality_type
),

-- Get top 10 countries for athletes
athlete_top_countries AS (
    SELECT 
        country_name,
        SUM(count) as total_count
    FROM athlete_nationality
    WHERE locality_type = 'International'
    GROUP BY country_name
    ORDER BY total_count DESC
    LIMIT 10
),

-- Get top 10 countries for spectators
spectator_top_countries AS (
    SELECT 
        country_name,
        SUM(count) as total_count
    FROM spectator_nationality
    WHERE locality_type = 'International'
    GROUP BY country_name
    ORDER BY total_count DESC
    LIMIT 10
),

-- Athlete results with top 10 + others
athlete_results AS (
    SELECT 
        'athlete' as category,
        country_name,
        locality_type,
        count
    FROM athlete_nationality
    WHERE locality_type = 'Local'
    
    UNION ALL
    
    SELECT 
        'athlete' as category,
        country_name,
        locality_type,
        count
    FROM athlete_nationality
    WHERE locality_type = 'International'
      AND country_name IN (SELECT country_name FROM athlete_top_countries)
    
    UNION ALL
    
    SELECT 
        'athlete' as category,
        'Other Countries' as country_name,
        'International' as locality_type,
        SUM(count) as count
    FROM athlete_nationality
    WHERE locality_type = 'International'
      AND country_name NOT IN (SELECT country_name FROM athlete_top_countries)
),

-- Spectator results with top 10 + others
spectator_results AS (
    SELECT 
        'spectator' as category,
        country_name,
        locality_type,
        count
    FROM spectator_nationality
    WHERE locality_type = 'Local'
    
    UNION ALL
    
    SELECT 
        'spectator' as category,
        country_name,
        locality_type,
        count
    FROM spectator_nationality
    WHERE locality_type = 'International'
      AND country_name IN (SELECT country_name FROM spectator_top_countries)
    
    UNION ALL
    
    SELECT 
        'spectator' as category,
        'Other Countries' as country_name,
        'International' as locality_type,
        SUM(count) as count
    FROM spectator_nationality
    WHERE locality_type = 'International'
      AND country_name NOT IN (SELECT country_name FROM spectator_top_countries)
)

SELECT 
    category,
    country_name,
    locality_type,
    count
FROM (
    SELECT * FROM athlete_results
    UNION ALL
    SELECT * FROM spectator_results
) combined_results
ORDER BY 
    category,
    CASE WHEN locality_type = 'Local' THEN 1 ELSE 2 END,
    CASE WHEN country_name = 'Other Countries' THEN 1 ELSE 0 END,
    count DESC; 