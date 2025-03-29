SELECT 
    SUM(CASE WHEN is_returning_athlete = true THEN 1 ELSE 0 END) as returning_athletes,
    SUM(CASE WHEN is_returning_athlete_to_city = true THEN 1 ELSE 0 END) as returning_to_city
FROM {SCHEMA}.tickets