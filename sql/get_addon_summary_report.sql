SELECT 
    addon_name,
    total_count
FROM {SCHEMA}.ticket_addon_summary
WHERE event_id = :event_id
ORDER BY total_count DESC, addon_name ASC 