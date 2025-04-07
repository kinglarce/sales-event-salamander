SELECT 
    CASE 
        WHEN ticket_name LIKE '%ADAPTIVE MEN%' THEN 'Adaptive Men'
        WHEN ticket_name LIKE '%ADAPTIVE WOMEN%' THEN 'Adaptive Women'
    END as group_name,
    SUM(total_count) as total_count
FROM {SCHEMA}.ticket_summary
WHERE ticket_name LIKE '%ADAPTIVE%'
GROUP BY 
    CASE 
        WHEN ticket_name LIKE '%ADAPTIVE MEN%' THEN 'Adaptive Men'
        WHEN ticket_name LIKE '%ADAPTIVE WOMEN%' THEN 'Adaptive Women'
    END
ORDER BY group_name; 