WITH ticket_data AS (
    SELECT
        LOWER(ticket_name) AS ticket_name,
        ticket_category,
        total_count,
        UPPER(ticket_event_day) AS ticket_event_day 
    FROM {SCHEMA}.ticket_type_summary
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
        total_count,
        ticket_event_day
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
        total_count,
        ticket_event_day
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

            -- Standard Categories (adjusting based on Sunday condition)
            WHEN (ticket_name ~* 'hyrox men(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive men%') 
                 AND ticket_event_day = 'SUNDAY' THEN 'HYROX MEN'
            WHEN (ticket_name ~* 'hyrox women(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive women%') 
                 AND ticket_event_day = 'SUNDAY' THEN 'HYROX WOMEN'
            WHEN ticket_name ~* 'hyrox men(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive men%' THEN 'HYROX MEN with Adaptive'
            WHEN ticket_name ~* 'hyrox women(?!.*relay)' OR ticket_name LIKE '%hyrox adaptive women%' THEN 'HYROX WOMEN with Adaptive'

            WHEN ticket_name LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN ticket_name LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN ticket_name LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'

            -- Spectators and Extras
            WHEN ticket_category = 'spectator' THEN 'SPECTATOR'
            WHEN ticket_category = 'extra' THEN 'EXTRAS'
            ELSE NULL
        END AS ticket_group,
        total_count,
        ticket_event_day
    FROM ticket_data
    WHERE ticket_name LIKE 'hyrox%'
       OR ticket_category IN ('spectator', 'extra')
)

-- Aggregating results and appending event day
SELECT CONCAT(category, ' | ', ticket_event_day) AS category, SUM(total) AS total
FROM (
    SELECT ticket_group AS category, SUM(total_count) AS total, ticket_event_day
    FROM single_double_data
    WHERE ticket_group IS NOT NULL
    GROUP BY ticket_group, ticket_event_day

    UNION ALL

    SELECT corporate_type AS category, SUM(total_count) AS total, ticket_event_day
    FROM corporate_data
    WHERE corporate_type IS NOT NULL
    GROUP BY corporate_type, ticket_event_day

    UNION ALL

    SELECT relay_type AS category, SUM(total_count) AS total, ticket_event_day
    FROM relay_data
    WHERE relay_type IS NOT NULL
    GROUP BY relay_type, ticket_event_day
) aggregated
GROUP BY category, ticket_event_day
ORDER BY category;
