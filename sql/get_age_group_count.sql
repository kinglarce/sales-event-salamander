WITH ticket_data AS (
    SELECT
        t.ticket_name,
        CASE 
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END as base_name,
        ts.ticket_category,
        t.age
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_type_summary ts 
        ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
    AND (
        (:is_incomplete AND t.age IS NULL) OR
        (NOT :is_incomplete AND t.age IS NOT NULL AND t.age >= :min_age AND t.age <= :max_age)
    )
),

-- Corporate Relay Data
corporate_data AS (
    SELECT
        CASE
            WHEN base_name LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            ELSE NULL
        END AS ticket_group
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
        END AS ticket_group
    FROM ticket_data 
    WHERE base_name LIKE '%relay%'
),

-- Singles and Doubles Data
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
        END AS ticket_group
    FROM ticket_data 
    WHERE base_name LIKE 'hyrox%'
)

SELECT COUNT(*) as count
FROM (
    SELECT ticket_group FROM single_double_data WHERE ticket_group = :ticket_group
    UNION ALL
    SELECT ticket_group FROM corporate_data WHERE ticket_group = :ticket_group
    UNION ALL
    SELECT ticket_group FROM relay_data WHERE ticket_group = :ticket_group
) combined; 