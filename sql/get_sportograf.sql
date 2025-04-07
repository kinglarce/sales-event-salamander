SELECT 
    tt.ticket_name,
    tt.total_count
FROM {SCHEMA}.ticket_summary tt
WHERE tt.ticket_name LIKE '%Sportograf%'
ORDER BY tt.ticket_name