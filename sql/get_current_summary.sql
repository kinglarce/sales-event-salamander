WITH latest_summary AS (
    SELECT 
        ticket_group,
        total_count,
        ROW_NUMBER() OVER (
            PARTITION BY ticket_group 
            ORDER BY created_at DESC
        ) as rn
    FROM {SCHEMA}.summary_report
    WHERE event_id = :event_id
)
SELECT ticket_group, total_count
FROM latest_summary
WHERE rn = 1 