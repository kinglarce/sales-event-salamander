WITH ticket_data AS (
    SELECT
        LOWER(ticket_name) AS ticket_name,
        ticket_category,
        total_count
    FROM {SCHEMA}.ticket_summary
    WHERE ticket_category <> 'extra'
),

-- Corporate Relay Data
corporate_data AS (
    SELECT
        CASE
            WHEN ticket_name LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN ticket_name LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN ticket_name LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            ELSE NULL
        END AS corporate_type,
        total_count
    FROM ticket_data
    WHERE ticket_name LIKE '%corporate relay%'
),

-- Relay Data
relay_data AS (
    SELECT
        CASE
            WHEN ticket_name ~* 'hyrox womens relay' THEN 'HYROX WOMENS RELAY'
            WHEN ticket_name ~* 'hyrox mens relay' THEN 'HYROX MENS RELAY'
            WHEN ticket_name ~* 'hyrox mixed relay' THEN 'HYROX MIXED RELAY'
            ELSE NULL
        END AS relay_type,
        total_count
    FROM ticket_data
    WHERE ticket_name LIKE '%relay%'
),

-- Singles and Doubles Data
single_double_data AS (
    SELECT
        CASE
            -- Pro Handling
            WHEN ticket_name ~* 'hyrox pro women|hyrox women pro' THEN 'HYROX PRO WOMEN'
            WHEN ticket_name ~* 'hyrox pro men|hyrox men pro' THEN 'HYROX PRO MEN'
            WHEN ticket_name ~* 'hyrox pro doubles women|hyrox doubles women pro' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN ticket_name ~* 'hyrox pro doubles men|hyrox doubles men pro' THEN 'HYROX PRO DOUBLES MEN'

            -- Standard Categories 
            WHEN ticket_name ~* 'hyrox men(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive men%' THEN 'HYROX MEN with Adaptive'
            WHEN ticket_name ~* 'hyrox women(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive women%' THEN 'HYROX WOMEN with Adaptive'
            WHEN ticket_name LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN ticket_name LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN ticket_name LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'

            -- Spectators. Guardian is reported on its own, not inside this bucket.
            WHEN ticket_name LIKE '%guardian%' THEN NULL
            WHEN ticket_category = 'spectator' THEN 'Spectator'
            ELSE NULL
        END AS ticket_group,
        total_count
    FROM ticket_data
    WHERE ticket_name LIKE 'hyrox%'
       OR ticket_category = 'spectator'
),

-- Youngstars divisions are age ranges (boys/girls, 8-9 through 14-15).
youngstars_data AS (
    SELECT
        'YOUNGSTARS '
            || CASE WHEN ticket_name ~ 'girl' THEN 'GIRLS' ELSE 'BOYS' END
            || ' ('
            || CASE
                WHEN ticket_name ~ '(^|[^0-9])8[^0-9]{{1,3}}9([^0-9]|$)' THEN '8-9'
                WHEN ticket_name ~ '(^|[^0-9])10[^0-9]{{1,3}}11([^0-9]|$)' THEN '10-11'
                WHEN ticket_name ~ '(^|[^0-9])12[^0-9]{{1,3}}13([^0-9]|$)' THEN '12-13'
                ELSE '14-15'
            END
            || ' Years)' AS youngstars_type,
        total_count
    FROM ticket_data
    WHERE ticket_name LIKE '%youngstar%'
      AND ticket_name ~ 'boy|girl'
      AND ticket_name ~ '(^|[^0-9])(8[^0-9]{{1,3}}9|10[^0-9]{{1,3}}11|12[^0-9]{{1,3}}13|14[^0-9]{{1,3}}15)([^0-9]|$)'
),

guardian_data AS (
    SELECT 'YOUNGSTARS Guardian' AS guardian_type, total_count
    FROM ticket_data
    WHERE ticket_name LIKE '%guardian%'
)

-- Final aggregation with capacity information
SELECT 
    summary.category,
    summary.total,
    tc.capacity,
    CASE 
        WHEN tc.capacity IS NOT NULL 
        THEN CONCAT(summary.total, ' / ', tc.capacity)
        ELSE CAST(summary.total AS TEXT)
    END AS formatted_total,
    CASE 
        WHEN tc.capacity IS NOT NULL AND tc.capacity > 0
        THEN ROUND((summary.total::float / tc.capacity::float * 100)::numeric, 2)
        ELSE NULL
    END AS percentage_total
FROM (
    SELECT category, SUM(total) AS total
    FROM (
        SELECT ticket_group AS category, SUM(total_count) AS total
        FROM single_double_data
        WHERE ticket_group IS NOT NULL
        GROUP BY ticket_group

        UNION ALL

        SELECT corporate_type AS category, SUM(total_count) AS total
        FROM corporate_data
        WHERE corporate_type IS NOT NULL
        GROUP BY corporate_type

        UNION ALL

        SELECT relay_type AS category, SUM(total_count) AS total
        FROM relay_data
        WHERE relay_type IS NOT NULL
        GROUP BY relay_type

        UNION ALL

        SELECT youngstars_type AS category, SUM(total_count) AS total
        FROM youngstars_data
        GROUP BY youngstars_type

        UNION ALL

        SELECT guardian_type AS category, SUM(total_count) AS total
        FROM guardian_data
        GROUP BY guardian_type
    ) aggregated
    GROUP BY category
) summary
LEFT JOIN {SCHEMA}.ticket_capacity_configs tc 
    ON tc.ticket_group = summary.category 
    AND tc.event_day = 'ALL'
ORDER BY tc.id;
