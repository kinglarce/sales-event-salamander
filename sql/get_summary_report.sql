WITH labeled AS (
    SELECT
        ticket_type_id,
        ticket_name,
        total_count,
        ticket_category,
        -- Youngstars top summary is age range only: boys and girls are combined.
        -- Guardian is a spectator product and is not an athlete division.
        CASE
            WHEN LOWER(ticket_name) LIKE '%guardian%' THEN 'YOUNGSTARS Guardian'
            WHEN LOWER(ticket_name) ~ 'youngstar'
             AND LOWER(ticket_name) ~ '(^|[^0-9])(8[^0-9]{1,3}9|10[^0-9]{1,3}11|12[^0-9]{1,3}13|14[^0-9]{1,3}15)([^0-9]|$)'
            THEN 'YOUNGSTARS ('
                || CASE
                    WHEN LOWER(ticket_name) ~ '(^|[^0-9])8[^0-9]{1,3}9([^0-9]|$)' THEN '8-9'
                    WHEN LOWER(ticket_name) ~ '(^|[^0-9])10[^0-9]{1,3}11([^0-9]|$)' THEN '10-11'
                    WHEN LOWER(ticket_name) ~ '(^|[^0-9])12[^0-9]{1,3}13([^0-9]|$)' THEN '12-13'
                    ELSE '14-15'
                END
                || ' Years)'
            ELSE NULL
        END AS youngstars_division
    FROM {SCHEMA}.ticket_summary
    WHERE event_id = :event_id
      AND ticket_category <> 'extra'
),
group_summary AS (
    SELECT 
        CASE 
            WHEN youngstars_division IS NOT NULL THEN youngstars_division
            WHEN ticket_category = 'single' THEN 'All_singles'
            WHEN ticket_category = 'double' THEN 'All_doubles'
            WHEN ticket_category = 'relay' THEN 'All_relays'
            WHEN ticket_category = 'spectator' THEN 'spectators'
            ELSE ticket_category
        END as ticket_group,
        array_agg(ticket_type_id) as ticket_type_ids,
        array_agg(ticket_name) as ticket_names,
        SUM(total_count) as total_count
    FROM labeled
    GROUP BY 
        CASE 
            WHEN youngstars_division IS NOT NULL THEN youngstars_division
            WHEN ticket_category = 'single' THEN 'All_singles'
            WHEN ticket_category = 'double' THEN 'All_doubles'
            WHEN ticket_category = 'relay' THEN 'All_relays'
            WHEN ticket_category = 'spectator' THEN 'spectators'
            ELSE ticket_category
        END
)
SELECT 
    ticket_group,
    ticket_type_ids,
    ticket_names,
    total_count
FROM group_summary
UNION ALL
SELECT 
    'Total_athletes' as ticket_group,
    array_agg(ticket_type_id) as ticket_type_ids,
    array_agg(ticket_name) as ticket_names,
    SUM(total_count) as total_count
FROM {SCHEMA}.ticket_summary
WHERE 
    event_id = :event_id 
    AND ticket_category NOT IN ('spectator', 'extra')
    AND LOWER(ticket_name) NOT LIKE '%guardian%'; 