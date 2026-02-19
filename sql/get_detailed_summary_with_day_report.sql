WITH ticket_data AS (
    SELECT
        -- Extract optional single-word prefix (e.g. CHARITY, KLOOK, PARTNER) before first ' | '
        (SELECT UPPER((r)[1]) FROM regexp_matches(LOWER(TRIM(ticket_name)), '^([a-z0-9_]+)\s*\|\s*') AS r LIMIT 1) AS prefix,
        -- Normalize: strip any single-word prefix + ' | ', then collapse ATHLETE 2 / TEAM MEMBER for grouping
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(TRIM(ticket_name)), '^[a-z0-9_]+\s*\|\s*', ''),
                '\s+athlete\s+2\s*$', '', 'i'
            ),
            '\s+team\s+member\s*$', '', 'i'
        ) AS ticket_name,
        ticket_category,
        total_count,
        UPPER(ticket_event_day) AS ticket_event_day
    FROM {SCHEMA}.ticket_summary
    WHERE ticket_category <> 'extra'
),

-- Check if Corporate Data Exists
has_corporate_data AS (
    SELECT EXISTS (
        SELECT 1 FROM ticket_data WHERE ticket_name LIKE '%corporate relay%'
    )::BOOLEAN AS exists_flag
),

-- Corporate Relay Data (Only if data exists)
corporate_data AS (
    SELECT
        CASE
            WHEN ticket_name LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN ticket_name LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN ticket_name LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            ELSE NULL
        END AS corporate_type,
        COALESCE(total_count, 0) AS total_count,
        ticket_event_day,
        prefix
    FROM ticket_data
    WHERE ticket_name LIKE '%corporate relay%'
    AND (SELECT exists_flag FROM has_corporate_data) IS TRUE
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
        COALESCE(total_count, 0) AS total_count,
        ticket_event_day,
        prefix
    FROM ticket_data
    WHERE ticket_name LIKE '%relay%'
    AND ticket_name NOT LIKE '%corporate relay%'
),

-- Singles and Doubles Data
single_double_data AS (
    SELECT
        CASE
            WHEN ticket_name ~* 'hyrox pro women|hyrox women pro' THEN 'HYROX PRO WOMEN'
            WHEN ticket_name ~* 'hyrox pro men|hyrox men pro' THEN 'HYROX PRO MEN'
            WHEN ticket_name ~* 'hyrox pro doubles women|hyrox doubles women pro' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN ticket_name ~* 'hyrox pro doubles men|hyrox doubles men pro' THEN 'HYROX PRO DOUBLES MEN'
            WHEN (ticket_name ~* 'hyrox men(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive men%')
                 AND (CASE WHEN {EXCLUDE_ADAPTIVE_SUNDAY}::boolean = true THEN ticket_event_day = 'SUNDAY' ELSE false END) THEN 'HYROX MEN'
            WHEN (ticket_name ~* 'hyrox women(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive women%')
                 AND (CASE WHEN {EXCLUDE_ADAPTIVE_SUNDAY}::boolean = true THEN ticket_event_day = 'SUNDAY' ELSE false END) THEN 'HYROX WOMEN'
            WHEN ticket_name ~* 'hyrox men(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive men%' THEN 'HYROX MEN with Adaptive'
            WHEN ticket_name ~* 'hyrox women(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive women%' THEN 'HYROX WOMEN with Adaptive'
            WHEN ticket_name LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN ticket_name LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN ticket_name LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'
            ELSE NULL
        END AS ticket_group,
        COALESCE(total_count, 0) AS total_count,
        ticket_event_day,
        prefix
    FROM ticket_data
    WHERE ticket_name LIKE 'hyrox%'
),

-- Spectator Data
spectator_data AS (
    SELECT
        CASE
            WHEN UPPER(ticket_name) LIKE '%3 DAYS%' THEN 'SPECTATOR | 3 DAYS'
            WHEN UPPER(ticket_name) LIKE '%2 DAYS%' THEN 'SPECTATOR | 2 DAYS'
            ELSE 'SPECTATOR'
        END AS spectator_type,
        COALESCE(total_count, 0) AS total_count,
        CASE
            WHEN UPPER(ticket_name) LIKE '%3 DAYS%' THEN 'MULTI_DAY'
            WHEN UPPER(ticket_name) LIKE '%2 DAYS%' THEN 'MULTI_DAY'
            ELSE ticket_event_day
        END AS ticket_event_day,
        prefix
    FROM ticket_data
    WHERE ticket_category = 'spectator'
),

-- Aggregated by category, day, and prefix (so we can split main vs CHARITY vs KLOOK in Python)
summary_with_prefix AS (
    SELECT category, ticket_event_day, prefix, COALESCE(SUM(total_count), 0) AS total
    FROM (
        SELECT ticket_group AS category, total_count, ticket_event_day, prefix FROM single_double_data WHERE ticket_group IS NOT NULL
        UNION ALL
        SELECT corporate_type AS category, total_count, ticket_event_day, prefix FROM corporate_data WHERE corporate_type IS NOT NULL
        UNION ALL
        SELECT relay_type AS category, total_count, ticket_event_day, prefix FROM relay_data WHERE relay_type IS NOT NULL
        UNION ALL
        SELECT spectator_type AS category, total_count, ticket_event_day, prefix FROM spectator_data WHERE spectator_type IS NOT NULL
    ) aggregated
    GROUP BY category, ticket_event_day, prefix
)

SELECT
    CONCAT(summary_with_prefix.category, ' | ', summary_with_prefix.ticket_event_day) AS display_category,
    summary_with_prefix.total,
    COALESCE(tc.capacity, 0) AS capacity,
    summary_with_prefix.prefix,
    tc.id AS sort_order
FROM summary_with_prefix
LEFT JOIN {SCHEMA}.ticket_capacity_configs tc
    ON tc.ticket_group = CASE
        WHEN summary_with_prefix.category LIKE 'SPECTATOR%' AND summary_with_prefix.ticket_event_day != 'MULTI_DAY' THEN 'SPECTATOR'
        ELSE summary_with_prefix.category
    END
    AND tc.event_day = summary_with_prefix.ticket_event_day
ORDER BY tc.id, summary_with_prefix.prefix NULLS FIRST; 