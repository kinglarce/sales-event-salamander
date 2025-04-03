WITH ticket_data AS (
    SELECT DISTINCT
        t.ticket_name,
        ts.ticket_event_day,
        CASE 
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END as base_name,
        ts.ticket_category
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_type_summary ts 
        ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
),

single_double_data AS (
    SELECT
        CASE
            -- Pro Handling
            WHEN base_name ~* 'hyrox pro women|hyrox women pro' THEN 'HYROX PRO WOMEN'
            WHEN base_name ~* 'hyrox pro men|hyrox men pro' THEN 'HYROX PRO MEN'
            WHEN base_name ~* 'hyrox pro doubles women|hyrox doubles women pro' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN base_name ~* 'hyrox pro doubles men|hyrox doubles men pro' THEN 'HYROX PRO DOUBLES MEN'

            -- Standard Categories 
            WHEN base_name = 'hyrox men' THEN 'HYROX MEN'
            WHEN base_name = 'hyrox women' THEN 'HYROX WOMEN'
            WHEN base_name LIKE '%hyrox adaptive men%' THEN 'HYROX ADAPTIVE MEN'
            WHEN base_name LIKE '%hyrox adaptive women%' THEN 'HYROX ADAPTIVE WOMEN'
            WHEN base_name LIKE '%hyrox men with adaptive%' THEN 'HYROX ADAPTIVE MEN'
            WHEN base_name LIKE '%hyrox women with adaptive%' THEN 'HYROX ADAPTIVE WOMEN'
            
            -- Doubles
            WHEN base_name LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN base_name LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN base_name LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'
            ELSE NULL
        END AS ticket_group,
        ticket_event_day,
        -- Standardized ticket category
        CASE
            WHEN base_name LIKE '%doubles%' THEN 'double'
            ELSE 'single'
        END AS ticket_category
    FROM ticket_data 
    WHERE base_name LIKE 'hyrox%'
),

-- Corporate Relay Data
corporate_data AS (
    SELECT
        CASE
            WHEN base_name LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            ELSE NULL
        END AS ticket_group,
        ticket_event_day,
        'corporate_relay' AS ticket_category
    FROM ticket_data 
    WHERE base_name LIKE '%corporate relay%'
),

-- Relay Data
relay_data AS (
    SELECT
        CASE
            WHEN base_name ~* 'hyrox womens relay' THEN 'HYROX WOMENS RELAY'
            WHEN base_name ~* 'hyrox mens relay' THEN 'HYROX MENS RELAY'
            WHEN base_name ~* 'hyrox mixed relay' THEN 'HYROX MIXED RELAY'
            ELSE NULL
        END AS ticket_group,
        ticket_event_day,
        'relay' AS ticket_category
    FROM ticket_data 
    WHERE base_name LIKE '%relay%'
),

-- Join with ticket_capacity_configs for ordering
ticket_configs AS (
    SELECT 
        ticket_group,
        event_day,
        id AS order_priority
    FROM {SCHEMA}.ticket_capacity_configs
    ORDER BY id
)

SELECT 
    UPPER(CONCAT(ticket_group, ' | ', ticket_event_day)) AS display_ticket_group,
    ticket_category,
    ticket_group,
    ticket_event_day
FROM (
    SELECT 
        tg.ticket_group, 
        tg.ticket_event_day,
        tg.ticket_category,
        -- Use ticket_capacity_configs order if available
        COALESCE(tc.order_priority, 
            CASE 
                -- Singles
                WHEN tg.ticket_group = 'HYROX MEN' THEN 1
                WHEN tg.ticket_group = 'HYROX WOMEN' THEN 2
                WHEN tg.ticket_group = 'HYROX PRO MEN' THEN 3
                WHEN tg.ticket_group = 'HYROX PRO WOMEN' THEN 4
                WHEN tg.ticket_group = 'HYROX ADAPTIVE MEN' THEN 5
                WHEN tg.ticket_group = 'HYROX ADAPTIVE WOMEN' THEN 6
                -- Doubles
                WHEN tg.ticket_group = 'HYROX DOUBLES MEN' THEN 10
                WHEN tg.ticket_group = 'HYROX DOUBLES WOMEN' THEN 11
                WHEN tg.ticket_group = 'HYROX DOUBLES MIXED' THEN 12
                WHEN tg.ticket_group = 'HYROX PRO DOUBLES MEN' THEN 13
                WHEN tg.ticket_group = 'HYROX PRO DOUBLES WOMEN' THEN 14
                -- Relays
                WHEN tg.ticket_group = 'HYROX MENS RELAY' THEN 20
                WHEN tg.ticket_group = 'HYROX WOMENS RELAY' THEN 21
                WHEN tg.ticket_group = 'HYROX MIXED RELAY' THEN 22
                WHEN tg.ticket_group = 'HYROX MENS CORPORATE RELAY' THEN 23
                WHEN tg.ticket_group = 'HYROX WOMENS CORPORATE RELAY' THEN 24
                WHEN tg.ticket_group = 'HYROX MIXED CORPORATE RELAY' THEN 25
                ELSE 99
            END
        ) as sort_order
    FROM (
        SELECT DISTINCT ticket_group, ticket_event_day, ticket_category 
        FROM single_double_data 
        WHERE ticket_group IS NOT NULL
        
        UNION ALL 
        
        SELECT DISTINCT ticket_group, ticket_event_day, ticket_category 
        FROM corporate_data 
        WHERE ticket_group IS NOT NULL
        
        UNION ALL
        
        SELECT DISTINCT ticket_group, ticket_event_day, ticket_category 
        FROM relay_data 
        WHERE ticket_group IS NOT NULL
    ) tg
    LEFT JOIN ticket_configs tc ON 
        tc.ticket_group = tg.ticket_group AND
        tc.event_day = tg.ticket_event_day
) sorted
ORDER BY sort_order;