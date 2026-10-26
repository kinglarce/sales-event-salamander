-- Average participant age by division across APAC races, with Japan breakout.
-- Source: tickets.age (excl. spectator/extra, age > 0)
-- Comment out any schema that does not exist in the DB yet.

WITH all_athletes AS (
    SELECT
        'singapore_25' AS event,
        'SG' AS locality,
        FALSE AS is_japan,
        t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END AS base_name
    FROM singapore_25.tickets t
    JOIN singapore_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'hongkong_25', 'HK', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM hongkong_25.tickets t
    JOIN hongkong_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'sydney_25', 'AU', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM sydney_25.tickets t
    JOIN sydney_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'yokohama_25', 'JP', TRUE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM yokohama_25.tickets t
    JOIN yokohama_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'perth_25', 'AU', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM perth_25.tickets t
    JOIN perth_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'seoul_25', 'KR', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM seoul_25.tickets t
    JOIN seoul_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'singapore_expo_25', 'SG', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM singapore_expo_25.tickets t
    JOIN singapore_expo_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'melbourne_25', 'AU', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM melbourne_25.tickets t
    JOIN melbourne_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'osaka_26', 'JP', TRUE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM osaka_26.tickets t
    JOIN osaka_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'auckland_26', 'NZ', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM auckland_26.tickets t
    JOIN auckland_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'taipei_26', 'TW', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM taipei_26.tickets t
    JOIN taipei_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'bangkok_26', 'TH', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM bangkok_26.tickets t
    JOIN bangkok_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'singapore_ns_26', 'SG', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM singapore_ns_26.tickets t
    JOIN singapore_ns_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'brisbane_26', 'AU', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM brisbane_26.tickets t
    JOIN brisbane_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'brisbane_championship_26', 'AU', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM brisbane_championship_26.tickets t
    JOIN brisbane_championship_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'hongkong_26', 'HK', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM hongkong_26.tickets t
    JOIN hongkong_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'incheon_26', 'KR', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM incheon_26.tickets t
    JOIN incheon_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'jakarta_26', 'ID', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM jakarta_26.tickets t
    JOIN jakarta_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'sydney_26', 'AU', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM sydney_26.tickets t
    JOIN sydney_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'chiba_26', 'JP', TRUE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM chiba_26.tickets t
    JOIN chiba_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'bangkok_2_26', 'TH', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM bangkok_2_26.tickets t
    JOIN bangkok_2_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'perth_26', 'AU', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM perth_26.tickets t
    JOIN perth_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'seoul_26', 'KR', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM seoul_26.tickets t
    JOIN seoul_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'singapore_expo_26', 'SG', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM singapore_expo_26.tickets t
    JOIN singapore_expo_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0

    UNION ALL
    SELECT 'melbourne_26', 'AU', FALSE, t.age,
        CASE
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END
    FROM melbourne_26.tickets t
    JOIN melbourne_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL
      AND t.age > 0
),

classified_athletes AS (
    SELECT
        event,
        locality,
        is_japan,
        age,
        CASE
            WHEN base_name LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            WHEN base_name ~* 'hyrox womens relay' THEN 'HYROX WOMENS RELAY'
            WHEN base_name ~* 'hyrox mens relay' THEN 'HYROX MENS RELAY'
            WHEN base_name ~* 'hyrox mixed relay' THEN 'HYROX MIXED RELAY'
            WHEN base_name ~* 'hyrox pro women|hyrox women pro' THEN 'HYROX PRO WOMEN'
            WHEN base_name ~* 'hyrox pro men|hyrox men pro' THEN 'HYROX PRO MEN'
            WHEN base_name ~* 'hyrox pro doubles women|hyrox doubles women pro' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN base_name ~* 'hyrox pro doubles men|hyrox doubles men pro' THEN 'HYROX PRO DOUBLES MEN'
            WHEN base_name = 'hyrox men' THEN 'HYROX MEN'
            WHEN base_name = 'hyrox women' THEN 'HYROX WOMEN'
            WHEN base_name LIKE '%hyrox adaptive men%' THEN 'HYROX ADAPTIVE MEN'
            WHEN base_name LIKE '%hyrox adaptive women%' THEN 'HYROX ADAPTIVE WOMEN'
            WHEN base_name LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN base_name LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN base_name LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'
        END AS division,
        CASE
            WHEN base_name LIKE '%corporate relay%' OR base_name ~* 'hyrox (womens|mens|mixed) relay' THEN 'relay'
            WHEN base_name LIKE '%doubles%' OR base_name ~* 'hyrox pro doubles' THEN 'double'
            ELSE 'single'
        END AS division_category
    FROM all_athletes
    WHERE base_name IS NOT NULL
),

per_event_division AS (
    SELECT
        event,
        locality,
        is_japan,
        division,
        division_category,
        ROUND(AVG(age)::numeric, 2) AS average_age,
        COUNT(*) AS participant_count
    FROM classified_athletes
    WHERE division IS NOT NULL
    GROUP BY event, locality, is_japan, division, division_category
),

combined AS (
    -- 1) Per-event, per-division breakdown
    SELECT
        event,
        locality,
        division,
        division_category,
        average_age,
        participant_count
    FROM per_event_division

    UNION ALL

    -- 2) APAC: average of per-race division averages
    SELECT
        'APAC (avg of races)' AS event,
        'ALL' AS locality,
        division,
        division_category,
        ROUND(AVG(average_age), 2) AS average_age,
        NULL::bigint AS participant_count
    FROM per_event_division
    GROUP BY division, division_category

    UNION ALL

    -- 3) Japan: average of per-race division averages
    SELECT
        'Japan (avg of races)' AS event,
        'JP' AS locality,
        division,
        division_category,
        ROUND(AVG(average_age), 2) AS average_age,
        NULL::bigint AS participant_count
    FROM per_event_division
    WHERE is_japan
    GROUP BY division, division_category
)

SELECT *
FROM combined
ORDER BY
    CASE
        WHEN event = 'APAC (avg of races)' THEN 2
        WHEN event = 'Japan (avg of races)' THEN 3
        ELSE 1
    END,
    CASE
        WHEN event LIKE '%\_25' ESCAPE '\' THEN 1
        WHEN event LIKE '%\_26' ESCAPE '\' THEN 2
        ELSE 3
    END,
    event,
    CASE
        WHEN division_category = 'single' THEN 1
        WHEN division_category = 'double' THEN 2
        WHEN division_category = 'relay' THEN 3
        ELSE 4
    END,
    CASE
        WHEN division = 'HYROX MEN' THEN 1
        WHEN division = 'HYROX WOMEN' THEN 2
        WHEN division = 'HYROX PRO MEN' THEN 3
        WHEN division = 'HYROX PRO WOMEN' THEN 4
        WHEN division = 'HYROX ADAPTIVE MEN' THEN 5
        WHEN division = 'HYROX ADAPTIVE WOMEN' THEN 6
        WHEN division = 'HYROX DOUBLES MEN' THEN 10
        WHEN division = 'HYROX DOUBLES WOMEN' THEN 11
        WHEN division = 'HYROX DOUBLES MIXED' THEN 12
        WHEN division = 'HYROX PRO DOUBLES MEN' THEN 13
        WHEN division = 'HYROX PRO DOUBLES WOMEN' THEN 14
        WHEN division = 'HYROX MENS RELAY' THEN 20
        WHEN division = 'HYROX WOMENS RELAY' THEN 21
        WHEN division = 'HYROX MIXED RELAY' THEN 22
        WHEN division = 'HYROX MENS CORPORATE RELAY' THEN 23
        WHEN division = 'HYROX WOMENS CORPORATE RELAY' THEN 24
        WHEN division = 'HYROX MIXED CORPORATE RELAY' THEN 25
        ELSE 99
    END;
