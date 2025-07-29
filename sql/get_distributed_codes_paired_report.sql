-- Distributed Codes Paired Report
-- Shows each code from the distributed list with its usage status
-- This matches exactly with your south-korea-distributed.csv format

SELECT 
    c.code,
    c.name as series_name,
    c.is_used,
    c.used as usage_count,
    CASE 
        WHEN c.is_used THEN 'USED'
        ELSE 'UNUSED'
    END as status,
    CASE 
        WHEN c.is_used THEN '✅'
        ELSE '❌'
    END as status_icon
FROM {SCHEMA}.coupons c
WHERE c.is_tracked = true
ORDER BY 
    c.name,
    c.code; 