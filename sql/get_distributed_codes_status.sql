-- Simplified Distributed Codes Status Report
-- Shows the status of each distributed coupon code

SELECT 
    c.code,
    c.name as series_name,
    c.category,
    c.is_used,
    c.used as usage_count,
    CASE 
        WHEN c.is_used THEN 'USED'
        ELSE 'UNUSED'
    END as status
FROM {SCHEMA}.coupons c
WHERE c.is_tracked = true
ORDER BY 
    c.name,
    c.is_used DESC,
    c.code; 