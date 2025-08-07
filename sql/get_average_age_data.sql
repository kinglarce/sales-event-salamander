WITH ticket_groups AS (
    SELECT
        t.transaction_id,
        t.ticket_name,
        t.age,
        ts.ticket_event_day,
        ts.ticket_category,
        CASE 
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END as base_name
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_summary ts 
        ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.age IS NOT NULL 
      AND t.age > 0
),

grouped_tickets AS (
    SELECT
        transaction_id,
        base_name,
        ticket_event_day,
        ticket_category,
        age,
        CASE
            -- Corporate Relay
            WHEN base_name LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            -- Regular Relay
            WHEN base_name ~* 'hyrox womens relay' THEN 'HYROX WOMENS RELAY'
            WHEN base_name ~* 'hyrox mens relay' THEN 'HYROX MENS RELAY'
            WHEN base_name ~* 'hyrox mixed relay' THEN 'HYROX MIXED RELAY'
            -- Pro Categories
            WHEN base_name ~* 'hyrox pro women|hyrox women pro' THEN 'HYROX PRO WOMEN'
            WHEN base_name ~* 'hyrox pro men|hyrox men pro' THEN 'HYROX PRO MEN'
            WHEN base_name ~* 'hyrox pro doubles women|hyrox doubles women pro' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN base_name ~* 'hyrox pro doubles men|hyrox doubles men pro' THEN 'HYROX PRO DOUBLES MEN'
            -- Standard Categories
            WHEN base_name = 'hyrox men' THEN 'HYROX MEN'
            WHEN base_name = 'hyrox women' THEN 'HYROX WOMEN'
            WHEN base_name LIKE '%hyrox adaptive men%' THEN 'HYROX ADAPTIVE MEN'
            WHEN base_name LIKE '%hyrox adaptive women%' THEN 'HYROX ADAPTIVE WOMEN'
            -- Doubles
            WHEN base_name LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN base_name LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN base_name LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'
            ELSE NULL
        END as ticket_group
    FROM ticket_groups
    WHERE base_name IS NOT NULL
),

-- Calculate average age for each group
final_aggregation AS (
    SELECT 
        ticket_group,
        CASE 
            WHEN ticket_group LIKE '%RELAY%' THEN 'relay'
            WHEN ticket_group LIKE '%DOUBLES%' THEN 'double'
            ELSE 'single'
        END as ticket_category,
        AVG(age) as average_age,
        COUNT(*) as total_count
    FROM grouped_tickets
    WHERE ticket_group IS NOT NULL
    GROUP BY ticket_group
)

SELECT 
    ticket_group,
    ticket_category,
    average_age,
    total_count
FROM final_aggregation
WHERE total_count > 0
ORDER BY 
    CASE 
        WHEN ticket_category = 'single' THEN 1
        WHEN ticket_category = 'double' THEN 2
        WHEN ticket_category = 'relay' THEN 3
        ELSE 4
    END,
    CASE 
        WHEN ticket_group = 'HYROX MEN' THEN 1
        WHEN ticket_group = 'HYROX WOMEN' THEN 2
        WHEN ticket_group = 'HYROX PRO MEN' THEN 3
        WHEN ticket_group = 'HYROX PRO WOMEN' THEN 4
        WHEN ticket_group = 'HYROX ADAPTIVE MEN' THEN 5
        WHEN ticket_group = 'HYROX ADAPTIVE WOMEN' THEN 6
        WHEN ticket_group = 'HYROX DOUBLES MEN' THEN 10
        WHEN ticket_group = 'HYROX DOUBLES WOMEN' THEN 11
        WHEN ticket_group = 'HYROX DOUBLES MIXED' THEN 12
        WHEN ticket_group = 'HYROX PRO DOUBLES MEN' THEN 13
        WHEN ticket_group = 'HYROX PRO DOUBLES WOMEN' THEN 14
        WHEN ticket_group = 'HYROX MENS RELAY' THEN 20
        WHEN ticket_group = 'HYROX WOMENS RELAY' THEN 21
        WHEN ticket_group = 'HYROX MIXED RELAY' THEN 22
        WHEN ticket_group = 'HYROX MENS CORPORATE RELAY' THEN 23
        WHEN ticket_group = 'HYROX WOMENS CORPORATE RELAY' THEN 24
        WHEN ticket_group = 'HYROX MIXED CORPORATE RELAY' THEN 25
        ELSE 99
    END; 