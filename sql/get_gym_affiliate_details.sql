SELECT 
    t.is_gym_affiliate as membership_type,
    COALESCE(t.gym_affiliate, 'Not Specified') as gym,
    CASE 
        WHEN COALESCE(t.gym_affiliate_location, t.country) IN (
            SELECT code FROM {SCHEMA}.country_configs
        ) THEN (
            SELECT country 
            FROM {SCHEMA}.country_configs 
            WHERE code = COALESCE(t.gym_affiliate_location, t.country)
        )
        ELSE COALESCE(t.gym_affiliate_location, t.country, 'Not Specified')
    END as location,
    COUNT(*) as count
FROM {SCHEMA}.tickets t
WHERE t.is_gym_affiliate IS NOT NULL
GROUP BY 
    t.is_gym_affiliate,
    t.gym_affiliate,
    t.gym_affiliate_location,
    t.country
ORDER BY 
    CASE 
        WHEN t.is_gym_affiliate LIKE 'I''m a member of%' AND 
             t.is_gym_affiliate NOT LIKE 'I''m a member of another%' THEN 1
        WHEN t.is_gym_affiliate LIKE 'I''m a member of another%' THEN 2
        WHEN t.is_gym_affiliate LIKE 'I''m not a member%' THEN 3
        ELSE 4
    END,
    t.is_gym_affiliate,
    count DESC 