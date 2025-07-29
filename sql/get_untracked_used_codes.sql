SELECT 
    c.code,
    c.name as series_name,
    c.is_used,
    c.used as usage_count,
    'UNTRACKED_USED' as status
FROM {SCHEMA}.coupons c
WHERE c.is_tracked = false 
    AND c.is_used = true
ORDER BY 
    c.name,
    c.code; 