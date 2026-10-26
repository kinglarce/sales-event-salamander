WITH athlete_tickets AS (
    SELECT 
        t.gender,
        t.ticket_name,
        CASE 
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END as base_name,
        CASE 
            WHEN t.ticket_name LIKE '%ATHLETE 2%' OR t.ticket_name LIKE '%TEAM MEMBER%' 
            THEN 'MEMBER' 
            ELSE 'MAIN' 
        END as ticket_type
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
),

men_counts AS (
    SELECT COUNT(*)::INTEGER as count
    FROM athlete_tickets
    WHERE base_name LIKE '%doubles mixed%'
      AND ticket_type = 'MAIN'

    UNION ALL

    SELECT (COUNT(*) * 2)::INTEGER
    FROM athlete_tickets
    WHERE base_name ~* 'mixed relay'
      AND ticket_type = 'MAIN'

    UNION ALL

    SELECT COUNT(*)::INTEGER
    FROM athlete_tickets
    WHERE NOT (
        base_name LIKE '%doubles mixed%'
        OR base_name ~* 'mixed relay'
    )
      AND gender = 'Male'
),

women_counts AS (
    SELECT COUNT(*)::INTEGER as count
    FROM athlete_tickets
    WHERE base_name LIKE '%doubles mixed%'
      AND ticket_type = 'MAIN'

    UNION ALL

    SELECT (COUNT(*) * 2)::INTEGER
    FROM athlete_tickets
    WHERE base_name ~* 'mixed relay'
      AND ticket_type = 'MAIN'

    UNION ALL

    SELECT COUNT(*)::INTEGER
    FROM athlete_tickets
    WHERE NOT (
        base_name LIKE '%doubles mixed%'
        OR base_name ~* 'mixed relay'
    )
      AND gender = 'Female'
)

SELECT 'Men' as gender, COALESCE(SUM(count), 0) as count FROM men_counts
UNION ALL
SELECT 'Women' as gender, COALESCE(SUM(count), 0) as count FROM women_counts
ORDER BY gender;
