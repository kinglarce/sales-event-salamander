WITH ticket_data AS (
    SELECT
        ticket_name,
        total_count
    FROM {SCHEMA}.ticket_type_summary
),
corporate_data AS (
    SELECT
        CASE
            WHEN LOWER(ticket_name) LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN LOWER(ticket_name) LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN LOWER(ticket_name) LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            ELSE NULL
        END AS corporate_type,
        total_count
    FROM ticket_data
    WHERE LOWER(ticket_name) LIKE '%corporate relay%'
),
relay_data AS (
    SELECT
        CASE
            WHEN LOWER(ticket_name) LIKE '%hyrox womens relay%' THEN 'HYROX WOMENS RELAY'
            WHEN LOWER(ticket_name) LIKE '%hyrox mens relay%' THEN 'HYROX MENS RELAY'
            WHEN LOWER(ticket_name) LIKE '%hyrox mixed relay%' THEN 'HYROX MIXED RELAY'
            ELSE NULL
        END AS relay_type,
        total_count
    FROM ticket_data
    WHERE LOWER(ticket_name) LIKE '%relay%'
),
single_double_data AS (
    SELECT
        CASE
            WHEN LOWER(ticket_name) ~ '^(hyrox men(?!s relay)|hyrox adaptive men).*$' THEN 'HYROX MEN with Adaptive'
            WHEN LOWER(ticket_name) ~ '^(hyrox women(?!s relay)|hyrox adaptive women).*$' THEN 'HYROX WOMEN with Adaptive'
            WHEN LOWER(ticket_name) LIKE '%hyrox pro women%' THEN 'HYROX PRO WOMEN'
            WHEN LOWER(ticket_name) LIKE '%hyrox pro men%' THEN 'HYROX PRO MEN'
            WHEN LOWER(ticket_name) LIKE '%hyrox pro doubles women%' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN LOWER(ticket_name) LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN LOWER(ticket_name) LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN LOWER(ticket_name) LIKE '%hyrox pro doubles men%' THEN 'HYROX PRO DOUBLES MEN'
            WHEN LOWER(ticket_name) LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'
            WHEN LOWER(ticket_name) LIKE '%spectator%' THEN 'Spectator'
            WHEN LOWER(ticket_name) LIKE '%race with a friend%' THEN 'Race with a Friend'
            ELSE NULL
        END AS ticket_group,
        total_count
    FROM ticket_data
    WHERE
        LOWER(ticket_name) LIKE 'hyrox%'
        OR LOWER(ticket_name) LIKE '%spectator%'
        OR LOWER(ticket_name) LIKE '%race with a friend%'
)
SELECT
    ticket_group,
    SUM(total_count)
FROM single_double_data
WHERE ticket_group IS NOT NULL
GROUP BY ticket_group

UNION ALL

SELECT
    corporate_type,
    SUM(total_count)
FROM corporate_data
WHERE corporate_type IS NOT NULL
GROUP BY corporate_type

UNION ALL

SELECT
    relay_type,
    SUM(total_count)
FROM relay_data
WHERE relay_type IS NOT NULL
GROUP BY relay_type

ORDER BY ticket_group;