-- Query to get under shop summary data
SELECT 
    tus.id,
    tus.event_id,
    tus.event_name,
    tus.ticket_type_id,
    tus.ticket_name,
    tus.ticket_category,
    tus.ticket_event_day,
    tus.under_shop_id,
    shops.shop_name,
    tus.shop_category,
    tus.ticket_count,
    tus.ticket_volume,
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
ORDER BY 
    tus.shop_category,
    shops.shop_name,
    tus.ticket_category,
    tus.ticket_event_day,
    tus.ticket_name; 