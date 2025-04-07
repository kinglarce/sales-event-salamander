-- Query to get ticket volumes data
SELECT 
    tv.id,
    tv.event_id,
    e.name AS event_name, 
    tv.shop_id,
    tu.shop_name,
    tu.shop_category,
    tv.ticket_type_id,
    t.name AS ticket_name,
    tv.volume,
    tv.ticket_shop_category,
    tv.active,
    tv.created_at,
    tv.updated_at
FROM 
    {SCHEMA}.ticket_volumes tv
JOIN 
    {SCHEMA}.ticket_under_shops tu
    ON tv.shop_id = tu.shop_id AND tv.event_id = tu.event_id
JOIN 
    {SCHEMA}.events e
    ON tv.event_id = e.id
JOIN
    jsonb_to_recordset(e.tickets) AS t(id text, name text)
    ON tv.ticket_type_id = t.id
WHERE 
    tv.event_id = :event_id
    AND tv.active = true
ORDER BY
    tu.shop_category,
    tu.shop_name,
    t.name; 