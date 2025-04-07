WITH gender_mismatch_base AS (
    SELECT 
        t.ticket_name,
        t.gender,
        COUNT(*) as count,
        CASE 
            WHEN UPPER(t.ticket_name) LIKE '%FRIDAY%' THEN 'FRIDAY'
            WHEN UPPER(t.ticket_name) LIKE '%SATURDAY%' THEN 'SATURDAY'
            WHEN UPPER(t.ticket_name) LIKE '%SUNDAY%' THEN 'SUNDAY'
            ELSE 'NONE'
        END as event_day
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_summary tt ON t.ticket_type_id = tt.ticket_type_id
    WHERE (
        (t.ticket_name LIKE '%WOMEN%' AND t.gender = 'Male')
        OR (t.ticket_name LIKE '%MEN%' AND NOT t.ticket_name LIKE '%WOMEN%' AND t.gender = 'Female')
    )
    AND NOT t.ticket_name LIKE '%MIXED%'
    GROUP BY t.ticket_name, t.gender, event_day
),
gender_mismatch_details AS (
    SELECT 
        t.ticket_name,
        t.gender,
        t.barcode,
        t.ticket_type_id,
        t.category_name,
        CASE 
            WHEN UPPER(t.ticket_name) LIKE '%FRIDAY%' THEN 'FRIDAY'
            WHEN UPPER(t.ticket_name) LIKE '%SATURDAY%' THEN 'SATURDAY'
            WHEN UPPER(t.ticket_name) LIKE '%SUNDAY%' THEN 'SUNDAY'
            ELSE 'NONE'
        END as event_day
    FROM {SCHEMA}.tickets t
    JOIN gender_mismatch_base g ON t.ticket_name = g.ticket_name AND t.gender = g.gender AND 
        CASE 
            WHEN UPPER(t.ticket_name) LIKE '%FRIDAY%' THEN 'FRIDAY'
            WHEN UPPER(t.ticket_name) LIKE '%SATURDAY%' THEN 'SATURDAY'
            WHEN UPPER(t.ticket_name) LIKE '%SUNDAY%' THEN 'SUNDAY'
            ELSE 'NONE'
        END = g.event_day
)
SELECT 
    b.ticket_name,
    b.gender,
    b.count,
    b.event_day,
    json_agg(json_build_object(
        'barcode', d.barcode,
        'ticket_type_id', d.ticket_type_id,
        'category_name', d.category_name,
        'event_day', d.event_day
    )) as details
FROM gender_mismatch_base b
JOIN gender_mismatch_details d ON b.ticket_name = d.ticket_name AND b.gender = d.gender AND b.event_day = d.event_day
GROUP BY b.ticket_name, b.gender, b.count, b.event_day
ORDER BY 
    CASE 
        WHEN b.event_day = 'FRIDAY' THEN 1
        WHEN b.event_day = 'SATURDAY' THEN 2
        WHEN b.event_day = 'SUNDAY' THEN 3
        ELSE 4
    END,
    b.ticket_name