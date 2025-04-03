SELECT 
    tag.ticket_group,
    tag.age_range,
    tag.count,
    tag.ticket_event_day,
    UPPER(CONCAT(tag.ticket_group, ' | ', tag.ticket_event_day)) AS display_ticket_group,
    tag.ticket_category
FROM {SCHEMA}.ticket_age_groups tag
LEFT JOIN {SCHEMA}.ticket_capacity_configs tc
    ON tc.ticket_group = tag.ticket_group
    AND tc.event_day = tag.ticket_event_day
ORDER BY 
    -- First order by ticket category
    CASE 
        WHEN tag.ticket_category = 'single' THEN 1
        WHEN tag.ticket_category = 'double' THEN 2
        WHEN tag.ticket_category = 'relay' THEN 3
        ELSE 4
    END,
    -- Then use ticket_capacity_configs ordering if available
    COALESCE(tc.id, 
        CASE 
            WHEN tag.ticket_group = 'HYROX MEN' THEN 1
            WHEN tag.ticket_group = 'HYROX WOMEN' THEN 2
            WHEN tag.ticket_group = 'HYROX PRO MEN' THEN 3
            WHEN tag.ticket_group = 'HYROX PRO WOMEN' THEN 4
            WHEN tag.ticket_group = 'HYROX ADAPTIVE MEN' THEN 5
            WHEN tag.ticket_group = 'HYROX ADAPTIVE WOMEN' THEN 6
            WHEN tag.ticket_group = 'HYROX DOUBLES MEN' THEN 10
            WHEN tag.ticket_group = 'HYROX DOUBLES WOMEN' THEN 11
            WHEN tag.ticket_group = 'HYROX DOUBLES MIXED' THEN 12
            WHEN tag.ticket_group = 'HYROX PRO DOUBLES MEN' THEN 13
            WHEN tag.ticket_group = 'HYROX PRO DOUBLES WOMEN' THEN 14
            WHEN tag.ticket_group = 'HYROX MENS RELAY' THEN 20
            WHEN tag.ticket_group = 'HYROX WOMENS RELAY' THEN 21
            WHEN tag.ticket_group = 'HYROX MIXED RELAY' THEN 22
            WHEN tag.ticket_group = 'HYROX MENS CORPORATE RELAY' THEN 23
            WHEN tag.ticket_group = 'HYROX WOMENS CORPORATE RELAY' THEN 24
            WHEN tag.ticket_group = 'HYROX MIXED CORPORATE RELAY' THEN 25
            ELSE 99
        END),
    CASE 
        WHEN tag.age_range = 'U24' THEN 1
        WHEN tag.age_range = '25-29' THEN 2
        WHEN tag.age_range = '30-34' THEN 3
        WHEN tag.age_range = '35-39' THEN 4
        WHEN tag.age_range = '40-44' THEN 5
        WHEN tag.age_range = '45-49' THEN 6
        WHEN tag.age_range = '50-54' THEN 7
        WHEN tag.age_range = '55-59' THEN 8
        WHEN tag.age_range = '60-64' THEN 9
        WHEN tag.age_range = '65-69' THEN 10
        WHEN tag.age_range = '70+' THEN 11
        WHEN tag.age_range = 'U29' THEN 12
        WHEN tag.age_range = '30-39' THEN 13
        WHEN tag.age_range = '40-49' THEN 14
        WHEN tag.age_range = '50-59' THEN 15
        WHEN tag.age_range = '60-69' THEN 16
        WHEN tag.age_range = 'U40' THEN 17
        WHEN tag.age_range = '40+' THEN 18
        WHEN tag.age_range = 'Incomplete' THEN 97
        WHEN tag.age_range = 'Total' THEN 98
        ELSE 99
    END;