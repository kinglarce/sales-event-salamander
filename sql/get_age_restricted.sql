WITH age_restricted_athletes AS (
    SELECT 
        CASE 
            WHEN t.age <= 16 THEN 'under_16'
            WHEN t.age >= 17 AND t.age <= 18 THEN '17_to_18'
        END as age_group,
        json_agg(json_build_object(
            'barcode', t.barcode,
            'ticket_name', t.ticket_name,
            'ticket_type_id', t.ticket_type_id,
            'category_name', t.category_name,
            'age', t.age
        ) ORDER BY t.age) as athletes
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_summary tt ON t.ticket_type_id = tt.ticket_type_id
    WHERE t.age <= 18
      AND tt.ticket_category NOT IN ('spectator', 'extra')
    GROUP BY age_group
)
SELECT age_group, athletes
FROM age_restricted_athletes