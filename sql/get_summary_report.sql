WITH group_summary AS (
    SELECT 
        CASE 
            WHEN ticket_category = 'single' THEN 'All_singles'
            WHEN ticket_category = 'double' THEN 'All_doubles'
            WHEN ticket_category = 'relay' THEN 'All_relays'
            WHEN ticket_category = 'spectator' THEN 'spectators'
            ELSE ticket_category
        END as ticket_group,
        array_agg(ticket_type_id) as ticket_type_ids,
        array_agg(ticket_name) as ticket_names,
        SUM(total_count) as total_count
    FROM {SCHEMA}.ticket_type_summary
    WHERE 
        event_id = :event_id
        AND ticket_category <> 'extra'
    GROUP BY 
        CASE 
            WHEN ticket_category = 'single' THEN 'All_singles'
            WHEN ticket_category = 'double' THEN 'All_doubles'
            WHEN ticket_category = 'relay' THEN 'All_relays'
            WHEN ticket_category = 'spectator' THEN 'spectators'
            ELSE ticket_category
        END
)
SELECT 
    ticket_group,
    ticket_type_ids,
    ticket_names,
    total_count
FROM group_summary
UNION ALL
SELECT 
    'Total_athletes' as ticket_group,
    array_agg(ticket_type_id) as ticket_type_ids,
    array_agg(ticket_name) as ticket_names,
    SUM(total_count) as total_count
FROM {SCHEMA}.ticket_type_summary
WHERE 
    event_id = :event_id 
    AND ticket_category NOT IN ('spectator', 'extra'); 