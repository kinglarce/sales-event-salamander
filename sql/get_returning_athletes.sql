SELECT 
    SUM(CASE WHEN t.is_returning_athlete = true THEN 1 ELSE 0 END) as returning_athletes,
    SUM(CASE WHEN t.is_returning_athlete_to_city = true THEN 1 ELSE 0 END) as returning_to_city,
    SUM(CASE WHEN t.is_returning_athlete = false THEN 1 ELSE 0 END) as first_time_participants
FROM {SCHEMA}.tickets t
JOIN {SCHEMA}.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
WHERE ts.ticket_category NOT IN ('spectator', 'extra')
