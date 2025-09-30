WITH spectator_data AS (
    SELECT
        CASE
            WHEN UPPER(ticket_name) LIKE '%3 DAYS%' THEN 'SPECTATOR | 3 DAYS'
            WHEN UPPER(ticket_name) LIKE '%2 DAYS%' THEN 'SPECTATOR | 2 DAYS'
            ELSE 'SPECTATOR'
        END AS spectator_type,
        COALESCE(total_count, 0) AS total_count,
        CASE
            WHEN UPPER(ticket_name) LIKE '%3 DAYS%' THEN '3 DAYS'
            WHEN UPPER(ticket_name) LIKE '%2 DAYS%' THEN '2 DAYS'
            ELSE UPPER(ticket_event_day)
        END AS ticket_event_day
    FROM {SCHEMA}.ticket_summary
    WHERE ticket_category = 'spectator'
)

-- Aggregating spectator results by event day
SELECT 
    CASE 
        WHEN summary.category LIKE 'SPECTATOR | 3 DAYS' THEN 'SPECTATOR | MULTI DAYS'
        WHEN summary.category LIKE 'SPECTATOR | 2 DAYS' THEN 'SPECTATOR | MULTI DAYS'
        ELSE CONCAT(summary.category, ' | ', summary.ticket_event_day)
    END AS display_category,
    summary.total
FROM (
    SELECT 
        spectator_type AS category, 
        ticket_event_day, 
        COALESCE(SUM(total_count), 0) AS total 
    FROM spectator_data
    WHERE spectator_type IS NOT NULL
    GROUP BY spectator_type, ticket_event_day
) summary
ORDER BY 
    CASE summary.ticket_event_day
        WHEN 'THURSDAY' THEN 1
        WHEN 'FRIDAY' THEN 2
        WHEN 'SATURDAY' THEN 3
        WHEN 'SUNDAY' THEN 4
        WHEN '2 DAYS' THEN 5
        WHEN '3 DAYS' THEN 6
        ELSE 7
    END; 