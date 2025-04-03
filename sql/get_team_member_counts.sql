WITH main_tickets AS (
    SELECT 
        tt.ticket_name,
        tt.total_count as main_count,
        tt.ticket_category,
        -- Simpler base_name extraction, just remove day part
        SPLIT_PART(LOWER(tt.ticket_name), ' | ', 1) as base_name,
        CASE 
            WHEN UPPER(tt.ticket_name) LIKE '%FRIDAY%' THEN 'FRIDAY'
            WHEN UPPER(tt.ticket_name) LIKE '%SATURDAY%' THEN 'SATURDAY'
            WHEN UPPER(tt.ticket_name) LIKE '%SUNDAY%' THEN 'SUNDAY'
            ELSE 'NONE'
        END as event_day
    FROM {SCHEMA}.ticket_type_summary tt
    WHERE tt.ticket_category IN ('double', 'relay', 'corporate_relay')
    AND NOT (
            tt.ticket_name LIKE '%ATHLETE 2%'
            OR tt.ticket_name LIKE '%ATHLETE2%'
            OR tt.ticket_name LIKE '%TEAM MEMBER%'
            OR tt.ticket_name LIKE '%MEMBER%'
        )
    ),
member_tickets AS (
    SELECT 
        original_name,
        member_ticket_name,
        member_count,
        -- For doubles, we need to match exactly with their main counterpart
        CASE 
            WHEN LOWER(original_name) LIKE '%athlete 2%' THEN
                REPLACE(LOWER(SPLIT_PART(original_name, ' | ', 1)), ' athlete 2', '')
            WHEN LOWER(original_name) LIKE '%team member%' THEN
                REPLACE(LOWER(SPLIT_PART(original_name, ' | ', 1)), ' team member', '')
            ELSE
                LOWER(SPLIT_PART(original_name, ' | ', 1))
        END as base_name,
        CASE 
            WHEN UPPER(original_name) LIKE '%FRIDAY%' THEN 'FRIDAY'
            WHEN UPPER(original_name) LIKE '%SATURDAY%' THEN 'SATURDAY'
            WHEN UPPER(original_name) LIKE '%SUNDAY%' THEN 'SUNDAY'
            ELSE 'NONE'
        END as event_day
    FROM (
            SELECT 
                tt.ticket_name as original_name,
                tt.ticket_name as member_ticket_name,
                tt.total_count as member_count
            FROM {SCHEMA}.ticket_type_summary tt
            WHERE (tt.ticket_name LIKE '%ATHLETE 2%'
                OR tt.ticket_name LIKE '%ATHLETE2%'
                OR tt.ticket_name LIKE '%TEAM MEMBER%')
                -- Make sure we're only including double/relay tickets
                AND tt.ticket_category IN ('double', 'relay', 'corporate_relay')
        ) as member_tickets_temp
),
-- Debug view to inspect base name matching
matching_debug AS (
    SELECT 
        m.ticket_name as main_ticket,
        m.base_name as main_base_name,
        m.event_day as main_event_day,
        t.original_name as member_original_name,
        t.base_name as member_base_name,
        t.event_day as member_event_day,
        t.member_count
    FROM main_tickets m
    LEFT JOIN member_tickets t ON 
        LOWER(t.base_name) = LOWER(m.base_name)
        AND t.event_day = m.event_day
)
SELECT 
    m.ticket_name as main_ticket_name,
    m.main_count,
    COALESCE(SUM(t.member_count), 0) as member_count,
    m.ticket_category,
    m.event_day,
    CASE 
        WHEN m.ticket_category = 'relay' AND COALESCE(SUM(t.member_count), 0) = m.main_count * 3 THEN 'OK'
        WHEN m.ticket_category = 'corporate_relay' AND COALESCE(SUM(t.member_count), 0) = m.main_count * 3 THEN 'OK'
        WHEN m.ticket_category = 'double' AND COALESCE(SUM(t.member_count), 0) = m.main_count THEN 'OK'
        ELSE 'MISMATCH'
    END as status
FROM main_tickets m
LEFT JOIN member_tickets t ON 
    LOWER(t.base_name) = LOWER(m.base_name)
    AND t.event_day = m.event_day
GROUP BY 
    m.ticket_name,
    m.main_count,
    m.ticket_category,
    m.event_day
ORDER BY 
    m.ticket_category,
    m.event_day,
    m.ticket_name