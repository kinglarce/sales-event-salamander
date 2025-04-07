-- Query to get under shop summary data grouped by shop category
SELECT 
    tus.shop_category,
    COUNT(DISTINCT tus.under_shop_id) AS shop_count,
    SUM(tus.ticket_count) AS total_tickets,
    SUM(tus.ticket_volume) AS total_volume,
    STRING_AGG(DISTINCT shops.shop_name, ', ') AS shop_names,
    ARRAY_AGG(DISTINCT shops.shop_name) AS shop_names_array,
    tus.ticket_category,
    tus.ticket_event_day,
    e.start_date AS event_date
FROM 
    {SCHEMA}.ticket_under_shop_summary tus
JOIN 
    {SCHEMA}.ticket_under_shops shops 
    ON tus.under_shop_id = shops.shop_id AND tus.event_id = shops.event_id
JOIN 
    {SCHEMA}.events e 
    ON tus.event_id = e.id
WHERE 
    tus.event_id = :event_id
GROUP BY 
    tus.shop_category,
    tus.ticket_category,
    tus.ticket_event_day,
    e.start_date
ORDER BY 
    tus.shop_category,
    tus.ticket_category,
    tus.ticket_event_day; 