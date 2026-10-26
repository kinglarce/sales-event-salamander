-- Average repeat participation % across APAC races, with Japan breakout.
-- Source: tickets.is_returning_athlete (excl. spectator/extra)
-- Comment out any schema that does not exist in the DB yet.

WITH all_athletes AS (
    SELECT 'singapore_25' AS event, 'SG' AS locality, FALSE AS is_japan, t.is_returning_athlete
    FROM singapore_25.tickets t
    JOIN singapore_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'hongkong_25', 'HK', FALSE, t.is_returning_athlete
    FROM hongkong_25.tickets t
    JOIN hongkong_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'sydney_25', 'AU', FALSE, t.is_returning_athlete
    FROM sydney_25.tickets t
    JOIN sydney_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'yokohama_25', 'JP', TRUE, t.is_returning_athlete
    FROM yokohama_25.tickets t
    JOIN yokohama_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'perth_25', 'AU', FALSE, t.is_returning_athlete
    FROM perth_25.tickets t
    JOIN perth_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'seoul_25', 'KR', FALSE, t.is_returning_athlete
    FROM seoul_25.tickets t
    JOIN seoul_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'singapore_expo_25', 'SG', FALSE, t.is_returning_athlete
    FROM singapore_expo_25.tickets t
    JOIN singapore_expo_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'melbourne_25', 'AU', FALSE, t.is_returning_athlete
    FROM melbourne_25.tickets t
    JOIN melbourne_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'osaka_26', 'JP', TRUE, t.is_returning_athlete
    FROM osaka_26.tickets t
    JOIN osaka_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'auckland_26', 'NZ', FALSE, t.is_returning_athlete
    FROM auckland_26.tickets t
    JOIN auckland_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'taipei_26', 'TW', FALSE, t.is_returning_athlete
    FROM taipei_26.tickets t
    JOIN taipei_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'bangkok_26', 'TH', FALSE, t.is_returning_athlete
    FROM bangkok_26.tickets t
    JOIN bangkok_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'singapore_ns_26', 'SG', FALSE, t.is_returning_athlete
    FROM singapore_ns_26.tickets t
    JOIN singapore_ns_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'brisbane_26', 'AU', FALSE, t.is_returning_athlete
    FROM brisbane_26.tickets t
    JOIN brisbane_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'brisbane_championship_26', 'AU', FALSE, t.is_returning_athlete
    FROM brisbane_championship_26.tickets t
    JOIN brisbane_championship_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'hongkong_26', 'HK', FALSE, t.is_returning_athlete
    FROM hongkong_26.tickets t
    JOIN hongkong_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'incheon_26', 'KR', FALSE, t.is_returning_athlete
    FROM incheon_26.tickets t
    JOIN incheon_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'jakarta_26', 'ID', FALSE, t.is_returning_athlete
    FROM jakarta_26.tickets t
    JOIN jakarta_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'sydney_26', 'AU', FALSE, t.is_returning_athlete
    FROM sydney_26.tickets t
    JOIN sydney_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'chiba_26', 'JP', TRUE, t.is_returning_athlete
    FROM chiba_26.tickets t
    JOIN chiba_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'bangkok_2_26', 'TH', FALSE, t.is_returning_athlete
    FROM bangkok_2_26.tickets t
    JOIN bangkok_2_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'perth_26', 'AU', FALSE, t.is_returning_athlete
    FROM perth_26.tickets t
    JOIN perth_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'seoul_26', 'KR', FALSE, t.is_returning_athlete
    FROM seoul_26.tickets t
    JOIN seoul_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'singapore_expo_26', 'SG', FALSE, t.is_returning_athlete
    FROM singapore_expo_26.tickets t
    JOIN singapore_expo_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL

    UNION ALL
    SELECT 'melbourne_26', 'AU', FALSE, t.is_returning_athlete
    FROM melbourne_26.tickets t
    JOIN melbourne_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.is_returning_athlete IS NOT NULL
),

-- Per-event first-time / returning counts and rates
per_event AS (
    SELECT
        event,
        locality,
        is_japan,
        COUNT(*) FILTER (WHERE is_returning_athlete = false) AS first_time_athletes,
        COUNT(*) FILTER (WHERE is_returning_athlete = true)  AS returning_athletes,
        COUNT(*) AS total_athletes,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_returning_athlete = false) / NULLIF(COUNT(*), 0),
            2
        ) AS first_time_pct,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_returning_athlete = true) / NULLIF(COUNT(*), 0),
            2
        ) AS repeat_pct
    FROM all_athletes
    GROUP BY event, locality, is_japan
),

combined AS (
    -- 1) Per-event breakdown
    SELECT
        event,
        locality,
        first_time_athletes,
        returning_athletes,
        total_athletes,
        first_time_pct,
        repeat_pct AS returning_athletes_pct
    FROM per_event

    UNION ALL

    -- 2) APAC: average of per-race repeat participation %
    SELECT
        'APAC (avg of races)' AS event,
        'ALL' AS locality,
        NULL::bigint AS first_time_athletes,
        NULL::bigint AS returning_athletes,
        NULL::bigint AS total_athletes,
        ROUND(AVG(first_time_pct), 2) AS first_time_pct,
        ROUND(AVG(repeat_pct), 2) AS returning_athletes_pct
    FROM per_event

    UNION ALL

    -- 3) Japan: average of per-race repeat participation %
    SELECT
        'Japan (avg of races)' AS event,
        'JP' AS locality,
        NULL::bigint AS first_time_athletes,
        NULL::bigint AS returning_athletes,
        NULL::bigint AS total_athletes,
        ROUND(AVG(first_time_pct), 2) AS first_time_pct,
        ROUND(AVG(repeat_pct), 2) AS returning_athletes_pct
    FROM per_event
    WHERE is_japan
)

SELECT *
FROM combined
ORDER BY
    CASE
        WHEN event = 'APAC (avg of races)' THEN 2
        WHEN event = 'Japan (avg of races)' THEN 3
        ELSE 1
    END,
    -- Season from schema suffix: _25 = 25/26, _26 = 26/27
    CASE
        WHEN event LIKE '%\_25' ESCAPE '\' THEN 1
        WHEN event LIKE '%\_26' ESCAPE '\' THEN 2
        ELSE 3
    END,
    event;
