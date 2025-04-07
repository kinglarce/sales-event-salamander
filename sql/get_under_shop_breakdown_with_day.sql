WITH under_shop_data AS (
    SELECT
        tu.shop_category,
        tus.ticket_name,
        tus.ticket_category,
        UPPER(tus.ticket_event_day) AS ticket_event_day,
        SUM(tus.ticket_count) AS total_ticket_count,
        SUM(tus.ticket_volume) AS total_ticket_volume
    FROM {SCHEMA}.ticket_under_shop_summary tus
    JOIN {SCHEMA}.ticket_under_shops tu ON tus.under_shop_id = tu.shop_id
        AND tus.event_id = tu.event_id
    WHERE tu.active = true
    GROUP BY
        tu.shop_category,
        tus.ticket_name,
        tus.ticket_category,
        tus.ticket_event_day
),

-- Corporate Relay Data
corporate_data AS (
    SELECT
        shop_category,
        CASE
            WHEN LOWER(ticket_name) LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN LOWER(ticket_name) LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN LOWER(ticket_name) LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            ELSE NULL
        END AS corporate_type,
        total_ticket_count,
        total_ticket_volume,
        ticket_event_day,
        ticket_category
    FROM under_shop_data
    WHERE LOWER(ticket_name) LIKE '%corporate relay%'
),

-- Relay Data
relay_data AS (
    SELECT
        shop_category,
        CASE
            WHEN LOWER(ticket_name) ~* 'hyrox womens relay' THEN 'HYROX WOMENS RELAY'
            WHEN LOWER(ticket_name) ~* 'hyrox mens relay' THEN 'HYROX MENS RELAY'
            WHEN LOWER(ticket_name) ~* 'hyrox mixed relay' THEN 'HYROX MIXED RELAY'
            ELSE NULL
        END AS relay_type,
        total_ticket_count,
        total_ticket_volume,
        ticket_event_day,
        ticket_category
    FROM under_shop_data
    WHERE LOWER(ticket_name) LIKE '%relay%'
      AND LOWER(ticket_name) NOT LIKE '%corporate relay%'
),

-- Singles and Doubles Data
single_double_data AS (
    SELECT
        shop_category,
        CASE
            -- Pro Handling
            WHEN LOWER(ticket_name) ~* 'hyrox pro women|hyrox women pro' THEN 'HYROX PRO WOMEN'
            WHEN LOWER(ticket_name) ~* 'hyrox pro men|hyrox men pro' THEN 'HYROX PRO MEN'
            WHEN LOWER(ticket_name) ~* 'hyrox pro doubles women|hyrox doubles women pro' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN LOWER(ticket_name) ~* 'hyrox pro doubles men|hyrox doubles men pro' THEN 'HYROX PRO DOUBLES MEN'

            -- Conditional Sunday handling 
            WHEN (LOWER(ticket_name) ~* 'hyrox men(?!.*relay)' OR LOWER(ticket_name) LIKE '%hyrox adaptive men%') 
                 AND (
                    CASE 
                        WHEN {EXCLUDE_ADAPTIVE_SUNDAY}::boolean = true THEN ticket_event_day = 'SUNDAY'
                        ELSE false 
                    END
                 ) THEN 'HYROX MEN'
            WHEN (LOWER(ticket_name) ~* 'hyrox women(?!.*relay)' OR LOWER(ticket_name) LIKE '%hyrox adaptive women%') 
                 AND (
                    CASE 
                        WHEN {EXCLUDE_ADAPTIVE_SUNDAY}::boolean = true THEN ticket_event_day = 'SUNDAY'
                        ELSE false 
                    END
                 ) THEN 'HYROX WOMEN'
            
            -- Standard Categories 
            WHEN LOWER(ticket_name) ~* 'hyrox men(?!.*relay)' OR LOWER(ticket_name) LIKE '%hyrox adaptive men%' THEN 'HYROX MEN with Adaptive'
            WHEN LOWER(ticket_name) ~* 'hyrox women(?!.*relay)' OR LOWER(ticket_name) LIKE '%hyrox adaptive women%' THEN 'HYROX WOMEN with Adaptive'
            WHEN LOWER(ticket_name) LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN LOWER(ticket_name) LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN LOWER(ticket_name) LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'
            ELSE NULL
        END AS ticket_group,
        total_ticket_count,
        total_ticket_volume,
        ticket_event_day,
        ticket_category
    FROM under_shop_data
    WHERE LOWER(ticket_name) LIKE 'hyrox%'
      AND LOWER(ticket_name) NOT LIKE '%relay%'
)

-- Final aggregation grouped by shop_category and day
SELECT 
    shop_category,
    CONCAT(category, ' | ', ticket_event_day) AS display_category,
    SUM(total_ticket_count) AS ticket_count,
    SUM(total_ticket_volume) AS ticket_volume,
    CONCAT(SUM(total_ticket_count), ' / ', SUM(total_ticket_volume)) AS formatted_total,
    MIN(tc.id) AS capacity_config_id
FROM (
    -- Singles and Doubles
    SELECT 
        shop_category, 
        ticket_group AS category, 
        total_ticket_count, 
        total_ticket_volume,
        ticket_event_day,
        ticket_category
    FROM single_double_data
    WHERE ticket_group IS NOT NULL

    UNION ALL

    -- Corporate Relay
    SELECT 
        shop_category, 
        corporate_type AS category, 
        total_ticket_count, 
        total_ticket_volume,
        ticket_event_day,
        ticket_category
    FROM corporate_data
    WHERE corporate_type IS NOT NULL

    UNION ALL

    -- Regular Relay
    SELECT 
        shop_category, 
        relay_type AS category, 
        total_ticket_count, 
        total_ticket_volume,
        ticket_event_day,
        ticket_category
    FROM relay_data
    WHERE relay_type IS NOT NULL
) combined
LEFT JOIN {SCHEMA}.ticket_capacity_configs tc 
    ON tc.ticket_group = combined.category 
    AND tc.event_day = combined.ticket_event_day
GROUP BY shop_category, category, ticket_event_day
ORDER BY shop_category, capacity_config_id NULLS LAST, category, ticket_event_day; 