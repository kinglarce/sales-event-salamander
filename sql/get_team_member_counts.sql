WITH main_tickets AS (
    SELECT 
        tt.ticket_name,
        tt.total_count as main_count,
        tt.ticket_category,
        CASE 
            WHEN LOWER(tt.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(tt.ticket_name), ' | ', 1)
            ELSE LOWER(tt.ticket_name)
        END as base_name
    FROM {SCHEMA}.ticket_type_summary tt
    WHERE tt.ticket_category IN ('double', 'relay')
    AND NOT (
            tt.ticket_name LIKE '%ATHLETE 2%'
            OR tt.ticket_name LIKE '%ATHLETE2%'
            OR tt.ticket_name LIKE '%TEAM MEMBER%'
            OR tt.ticket_name LIKE '%MEMBER%'
        )
    ),
member_tickets AS (
    SELECT 
        member_ticket_name,
        member_count,
        CASE 
            WHEN LOWER(member_ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(member_ticket_name), ' | ', 1)
            ELSE LOWER(member_ticket_name)
        END as base_name
    FROM (
            SELECT 
                CASE
                    WHEN tt.ticket_name LIKE '%ATHLETE 2%' OR tt.ticket_name LIKE '%ATHLETE2%' THEN 
                        SPLIT_PART(tt.ticket_name, ' ATHLETE', 1)
                    WHEN tt.ticket_name LIKE '%TEAM MEMBER%' THEN 
                        SPLIT_PART(tt.ticket_name, ' TEAM MEMBER', 1)
                    WHEN tt.ticket_name LIKE '%MEMBER%' THEN 
                        SPLIT_PART(tt.ticket_name, ' MEMBER', 1)
                END as member_ticket_name,
                tt.total_count as member_count
            FROM {SCHEMA}.ticket_type_summary tt
            WHERE tt.ticket_name LIKE '%ATHLETE 2%'
                OR tt.ticket_name LIKE '%ATHLETE2%'
                OR tt.ticket_name LIKE '%TEAM MEMBER%'
                OR tt.ticket_name LIKE '%MEMBER%'
        )
)
SELECT 
    m.ticket_name as main_ticket_name,
    m.main_count,
    COALESCE(t.member_count, 0) as member_count,
    m.ticket_category,
    CASE 
        WHEN m.ticket_category = 'relay' AND COALESCE(t.member_count, 0) = m.main_count * 3 THEN 'OK'
        WHEN m.ticket_category = 'double' AND COALESCE(t.member_count, 0) = m.main_count THEN 'OK'
        ELSE 'MISMATCH'
    END as status
FROM main_tickets m
LEFT JOIN member_tickets t ON t.base_name = m.base_name
ORDER BY 
    m.ticket_category,
    m.ticket_name