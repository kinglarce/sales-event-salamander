WITH gender_mismatch_base AS (
    SELECT 
        t.ticket_name,
        t.gender,
        COUNT(*) as count
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_type_summary tt ON t.ticket_type_id = tt.ticket_type_id
    WHERE (
        (t.ticket_name LIKE '%WOMEN%' AND t.gender = 'Male')
        OR (t.ticket_name LIKE '%MEN%' AND NOT t.ticket_name LIKE '%WOMEN%' AND t.gender = 'Female')
    )
    AND NOT t.ticket_name LIKE '%MIXED%'
    GROUP BY t.ticket_name, t.gender
),
gender_mismatch_details AS (
    SELECT 
        t.ticket_name,
        t.gender,
        t.barcode,
        t.ticket_type_id,
        t.category_name
    FROM {SCHEMA}.tickets t
    JOIN gender_mismatch_base g ON t.ticket_name = g.ticket_name AND t.gender = g.gender
)
SELECT 
    b.ticket_name,
    b.gender,
    b.count,
    json_agg(json_build_object(
        'barcode', d.barcode,
        'ticket_type_id', d.ticket_type_id,
        'category_name', d.category_name
    )) as details
FROM gender_mismatch_base b
JOIN gender_mismatch_details d ON b.ticket_name = d.ticket_name AND b.gender = d.gender
GROUP BY b.ticket_name, b.gender, b.count