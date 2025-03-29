WITH mixed_pairs AS (
    SELECT 
        t.transaction_id,
        t.ticket_name,
        t.barcode,
        t.gender,
        t.category_name,
        CASE 
            WHEN t.ticket_name LIKE '%ATHLETE 2%' OR t.ticket_name LIKE '%TEAM MEMBER%' 
            THEN 'MEMBER' 
            ELSE 'MAIN' 
        END as ticket_type,
        ROW_NUMBER() OVER (
            PARTITION BY t.transaction_id, 
                CASE 
                    WHEN t.ticket_name LIKE '%ATHLETE 2%' OR t.ticket_name LIKE '%TEAM MEMBER%' 
                    THEN 'MEMBER' 
                    ELSE 'MAIN' 
                END
            ORDER BY t.ticket_name
        ) as pair_number
    FROM {SCHEMA}.tickets t
    WHERE t.ticket_name LIKE '%MIXED%'
),
paired_tickets AS (
    SELECT 
        m.transaction_id,
        m.ticket_name,
        m.ticket_type,
        m.barcode as main_barcode,
        m.gender as main_gender,
        m.category_name as main_category,
        p.barcode as partner_barcode,
        p.gender as partner_gender,
        p.category_name as partner_category
    FROM mixed_pairs m
    LEFT JOIN mixed_pairs p ON 
        m.transaction_id = p.transaction_id AND
        m.pair_number = p.pair_number AND
        m.ticket_type = 'MAIN' AND 
        p.ticket_type = 'MEMBER'
    WHERE m.ticket_type = 'MAIN'
),
invalid_pairs AS (
    SELECT 
        pt.*,
        CASE
            WHEN pt.ticket_name LIKE '%MIXED RELAY%' 
            THEN (SELECT COUNT(*) FROM mixed_pairs mp 
                  WHERE mp.transaction_id = pt.transaction_id
                  AND mp.ticket_type = 'MEMBER') != 3
            WHEN pt.ticket_name LIKE '%MIXED DOUBLES%' 
            THEN (SELECT COUNT(*) FROM mixed_pairs mp 
                  WHERE mp.transaction_id = pt.transaction_id
                  AND mp.ticket_type = 'MEMBER') != 1
            ELSE false
        END as has_wrong_member_count,
        CASE
            WHEN pt.ticket_name LIKE '%MIXED RELAY%' 
            THEN (
                SELECT COUNT(*) 
                FROM mixed_pairs mp 
                WHERE mp.transaction_id = pt.transaction_id
                AND mp.gender = 'Male'
            ) != 2 OR
                (
                SELECT COUNT(*) 
                FROM mixed_pairs mp 
                WHERE mp.transaction_id = pt.transaction_id
                AND mp.gender = 'Female'
            ) != 2
            WHEN pt.ticket_name LIKE '%MIXED DOUBLES%' 
            THEN (
                SELECT COUNT(*) 
                FROM mixed_pairs mp 
                WHERE mp.transaction_id = pt.transaction_id
                AND mp.gender = 'Male'
            ) != 1 OR
                (
                SELECT COUNT(*) 
                FROM mixed_pairs mp 
                WHERE mp.transaction_id = pt.transaction_id
                AND mp.gender = 'Female'
            ) != 1
            ELSE false
        END as has_wrong_gender_ratio
    FROM paired_tickets pt
    WHERE pt.ticket_name LIKE '%MIXED%'
)
SELECT 
    ticket_name,
    COUNT(*) as invalid_count,
    json_agg(json_build_object(
        'transaction_id', transaction_id,
        'main_barcode', main_barcode,
        'main_gender', main_gender,
        'main_category', main_category,
        'partner_barcode', partner_barcode,
        'partner_gender', partner_gender,
        'partner_category', partner_category,
        'has_wrong_member_count', has_wrong_member_count,
        'has_wrong_gender_ratio', has_wrong_gender_ratio
    )) as details
FROM invalid_pairs
WHERE has_wrong_member_count OR has_wrong_gender_ratio
GROUP BY ticket_name