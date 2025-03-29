SELECT 
    ticket_group,
    age_range,
    count
FROM {SCHEMA}.ticket_age_groups
ORDER BY 
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
    END,
    CASE 
        WHEN age_range = 'U24' THEN 1
        WHEN age_range = '25-29' THEN 2
        WHEN age_range = '30-34' THEN 3
        WHEN age_range = '35-39' THEN 4
        WHEN age_range = '40-44' THEN 5
        WHEN age_range = '45-49' THEN 6
        WHEN age_range = '50-54' THEN 7
        WHEN age_range = '55-59' THEN 8
        WHEN age_range = '60-64' THEN 9
        WHEN age_range = '65-69' THEN 10
        WHEN age_range = '70+' THEN 11
        WHEN age_range = 'Total' THEN 12
        ELSE 99
    END