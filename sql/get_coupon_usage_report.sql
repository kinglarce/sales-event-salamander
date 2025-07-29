-- Simplified Coupon Usage Report
-- Shows which distributed codes have been used

SELECT 
    c.name as series_name,
    COUNT(c.id) as total_codes,
    SUM(CASE WHEN c.is_used THEN 1 ELSE 0 END) as used_codes,
    SUM(CASE WHEN NOT c.is_used THEN 1 ELSE 0 END) as unused_codes,
                    SUM(CASE WHEN c.is_tracked THEN 1 ELSE 0 END) as tracked_codes,
                SUM(CASE WHEN c.is_tracked AND c.is_used THEN 1 ELSE 0 END) as tracked_used_codes,
                SUM(CASE WHEN c.is_tracked AND NOT c.is_used THEN 1 ELSE 0 END) as tracked_unused_codes,
                CASE 
                    WHEN SUM(CASE WHEN c.is_tracked THEN 1 ELSE 0 END) > 0 
                    THEN ROUND(
                        (SUM(CASE WHEN c.is_tracked AND c.is_used THEN 1 ELSE 0 END)::numeric / 
                         SUM(CASE WHEN c.is_tracked THEN 1 ELSE 0 END)::numeric) * 100, 2
                    )
                    ELSE 0 
                END || '%' as tracked_usage_percentage
            FROM {SCHEMA}.coupons c
            WHERE c.is_tracked = true
            GROUP BY c.name
            ORDER BY tracked_usage_percentage DESC, series_name; 