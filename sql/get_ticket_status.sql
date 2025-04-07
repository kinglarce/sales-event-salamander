SELECT 
    t.status,
    COUNT(*) as count
FROM {SCHEMA}.tickets t
JOIN {SCHEMA}.ticket_summary tt ON t.ticket_type_id = tt.ticket_type_id
WHERE tt.ticket_category NOT IN ('spectator', 'extra')
GROUP BY t.status
ORDER BY t.status