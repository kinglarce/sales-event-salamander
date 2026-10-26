WITH latest_batch AS (
    SELECT MAX(created_at) AS created_at
    FROM {SCHEMA}.summary_report
    WHERE event_id = :event_id
)
SELECT s.ticket_group, s.total_count
FROM {SCHEMA}.summary_report s
JOIN latest_batch b ON s.created_at = b.created_at
WHERE s.event_id = :event_id
ORDER BY
    CASE
        WHEN s.ticket_group LIKE 'YOUNGSTARS (%' THEN 10
        WHEN s.ticket_group = 'All_singles' THEN 1
        WHEN s.ticket_group = 'All_doubles' THEN 2
        WHEN s.ticket_group = 'All_relays' THEN 3
        WHEN s.ticket_group = 'spectators' THEN 20
        WHEN s.ticket_group = 'YOUNGSTARS Guardian' THEN 21
        WHEN s.ticket_group = 'Total_athletes' THEN 100
        ELSE 50
    END,
    CASE
        WHEN s.ticket_group LIKE '%8-9%' THEN 1
        WHEN s.ticket_group LIKE '%10-11%' THEN 2
        WHEN s.ticket_group LIKE '%12-13%' THEN 3
        WHEN s.ticket_group LIKE '%14-15%' THEN 4
        ELSE 0
    END,
    s.ticket_group 