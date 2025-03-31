WITH ticket_data AS (
    SELECT
        LOWER(ticket_name) AS ticket_name,
        ticket_category,
        total_count,  
        UPPER(ticket_event_day) AS ticket_event_day 
    FROM {SCHEMA}.ticket_type_summary
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
        ticket_event_day
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
        ticket_event_day
    FROM ticket_data
    WHERE ticket_name LIKE '%relay%'
    AND ticket_name NOT LIKE '%corporate relay%'
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
            
            -- Conditional Sunday handling based on EXCLUDE_ADAPTIVE_SUNDAY
            WHEN (ticket_name ~* 'hyrox men(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive men%') 
                 AND (
                    CASE 
                        WHEN {EXCLUDE_ADAPTIVE_SUNDAY}::boolean = true THEN ticket_event_day = 'SUNDAY'
                        ELSE false 
                    END
                 ) THEN 'HYROX MEN'
            WHEN (ticket_name ~* 'hyrox women(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive women%') 
                 AND (
                    CASE 
                        WHEN {EXCLUDE_ADAPTIVE_SUNDAY}::boolean = true THEN ticket_event_day = 'SUNDAY'
                        ELSE false 
                    END
                 ) THEN 'HYROX WOMEN'
            
            -- Standard Categories without Sunday condition
            WHEN ticket_name ~* 'hyrox men(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive men%' THEN 'HYROX MEN with Adaptive'
            WHEN ticket_name ~* 'hyrox women(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive women%' THEN 'HYROX WOMEN with Adaptive'
            WHEN ticket_name LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN ticket_name LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN ticket_name LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'
            ELSE NULL
        END AS ticket_group,
        COALESCE(total_count, 0) AS total_count,
        ticket_event_day
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
        END AS ticket_event_day
    FROM ticket_data
    WHERE ticket_category = 'spectator'
)

-- Aggregating results and appending event day
SELECT 
    CONCAT(summary.category, ' | ', summary.ticket_event_day) AS display_category,
    summary.total,
    COALESCE(tc.capacity, 0) AS capacity,  
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
    SELECT category, ticket_event_day, COALESCE(SUM(total_count), 0) AS total 
    FROM (
        -- Single and Doubles Data
        SELECT ticket_group AS category, SUM(total_count) AS total_count, ticket_event_day
        FROM single_double_data
        WHERE ticket_group IS NOT NULL
        GROUP BY ticket_group, ticket_event_day

        UNION ALL

        -- Corporate Data
        SELECT corporate_type AS category, SUM(total_count) AS total_count, ticket_event_day
        FROM corporate_data
        WHERE corporate_type IS NOT NULL
        GROUP BY corporate_type, ticket_event_day

        UNION ALL

        -- Relay Data
        SELECT relay_type AS category, SUM(total_count) AS total_count, ticket_event_day
        FROM relay_data
        WHERE relay_type IS NOT NULL
        GROUP BY relay_type, ticket_event_day

        UNION ALL

        -- Spectator Data
        SELECT spectator_type AS category, SUM(total_count) AS total_count, ticket_event_day
        FROM spectator_data
        WHERE spectator_type IS NOT NULL
        GROUP BY spectator_type, ticket_event_day
    ) aggregated
    GROUP BY category, ticket_event_day
) summary
LEFT JOIN {SCHEMA}.ticket_capacity_configs tc 
    ON tc.ticket_group = 
        CASE 
            WHEN summary.category LIKE 'SPECTATOR%' AND summary.ticket_event_day != 'MULTI_DAY' THEN 'SPECTATOR'
            ELSE summary.category
        END
    AND tc.event_day = summary.ticket_event_day
ORDER BY tc.id; 