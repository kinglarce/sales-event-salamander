SELECT 
    addon->>'name' as addon_name,
    addon->>'productId' as product_id,
    COUNT(*) as total_count
FROM (
    SELECT jsonb_array_elements(addons) as addon
    FROM {schema}.tickets 
    WHERE event_id = :event_id 
    AND addons IS NOT NULL 
    AND jsonb_array_length(addons) > 0
) addon_data
WHERE addon->>'name' IS NOT NULL 
AND addon->>'productId' IS NOT NULL
GROUP BY addon->>'name', addon->>'productId'
ORDER BY COUNT(*) DESC 