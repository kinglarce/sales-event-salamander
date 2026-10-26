-- Women's participation across Singapore races, plus the Nov 2026 Expo breakdown.
-- One result set, so a SQL editor that only shows the first statement still
-- includes the age ranges.
--
-- Athlete tickets only (spectator, extra, and timing-chip fees excluded).
-- A signup counts as a woman when gender is Female, or gender is blank and the
-- ticket name contains "women" (ticket names never say "female"). Mixed tickets
-- with a blank gender stay out.
-- Expo age is age on 26 Nov 2026. Incomplete means no usable birthday.
-- Filter report = 'expo_age_range' for the age table.

WITH raw AS (
    SELECT
        'singapore_25' AS event_schema,
        'National Stadium (28-29 Jun 2025)' AS event_label,
        1 AS event_order,
        FALSE AS is_upcoming,
        t.gender,
        t.birthday,
        t.ticket_name,
        LOWER(t.ticket_name) AS name_l
    FROM singapore_25.tickets t
    JOIN singapore_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.ticket_name NOT ILIKE '%timing chip%'

    UNION ALL
    SELECT
        'singapore_expo_25',
        'Expo (29-30 Nov 2025)',
        2,
        FALSE,
        t.gender,
        t.birthday,
        t.ticket_name,
        LOWER(t.ticket_name)
    FROM singapore_expo_25.tickets t
    JOIN singapore_expo_25.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.ticket_name NOT ILIKE '%timing chip%'

    UNION ALL
    SELECT
        'singapore_ns_26',
        'National Stadium II (3-5 Apr 2026)',
        3,
        FALSE,
        t.gender,
        t.birthday,
        t.ticket_name,
        LOWER(t.ticket_name)
    FROM singapore_ns_26.tickets t
    JOIN singapore_ns_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.ticket_name NOT ILIKE '%timing chip%'

    UNION ALL
    SELECT
        'singapore_expo_26',
        'Expo (26-29 Nov 2026)',
        4,
        TRUE,
        t.gender,
        t.birthday,
        t.ticket_name,
        LOWER(t.ticket_name)
    FROM singapore_expo_26.tickets t
    JOIN singapore_expo_26.ticket_summary ts ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
      AND t.ticket_name NOT ILIKE '%timing chip%'
),
athletes AS (
    SELECT
        event_schema,
        event_label,
        event_order,
        is_upcoming,
        gender,
        ticket_name,
        CASE
            WHEN birthday ~ '^\d{4}-\d{2}-\d{2}' THEN birthday::date
            ELSE NULL
        END AS birthday,
        CASE
            WHEN name_l ~ 'pro doubles women|doubles women pro' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN name_l ~ 'pro doubles men|doubles men pro' THEN 'HYROX PRO DOUBLES MEN'
            WHEN name_l ~ 'doubles women' THEN 'HYROX DOUBLES WOMEN'
            WHEN name_l ~ 'doubles men' THEN 'HYROX DOUBLES MEN'
            WHEN name_l ~ 'doubles mixed' THEN 'HYROX DOUBLES MIXED'
            WHEN name_l ~ 'womens corporate relay' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN name_l ~ 'mens corporate relay' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN name_l ~ 'mixed corporate relay' THEN 'HYROX MIXED CORPORATE RELAY'
            WHEN name_l ~ 'womens relay' THEN 'HYROX WOMENS RELAY'
            WHEN name_l ~ 'mens relay' THEN 'HYROX MENS RELAY'
            WHEN name_l ~ 'mixed relay' THEN 'HYROX MIXED RELAY'
            WHEN name_l ~ 'adaptive women' THEN 'HYROX ADAPTIVE WOMEN'
            WHEN name_l ~ 'adaptive men' THEN 'HYROX ADAPTIVE MEN'
            WHEN name_l ~ 'pro women|women pro' THEN 'HYROX PRO WOMEN'
            WHEN name_l ~ 'pro men|men pro' THEN 'HYROX PRO MEN'
            WHEN name_l ~ 'hyrox women' THEN 'HYROX WOMEN'
            WHEN name_l ~ 'hyrox men' THEN 'HYROX MEN'
            ELSE 'UNMAPPED'
        END AS division,
        (
            gender = 'Female'
            OR (
                (gender IS NULL OR btrim(gender) = '')
                AND name_l LIKE '%women%'
                AND name_l NOT LIKE '%mixed%'
            )
        ) AS is_woman
    FROM raw
),
expo_women AS (
    SELECT
        division,
        birthday,
        ticket_name,
        CASE
            WHEN birthday IS NULL THEN NULL
            ELSE EXTRACT(YEAR FROM AGE(DATE '2026-11-26', birthday))::int
        END AS age_at_race
    FROM athletes
    WHERE event_schema = 'singapore_expo_26'
      AND is_woman
),
expo_age AS (
    SELECT
        CASE
            WHEN age_at_race IS NULL OR age_at_race <= 0 THEN 'Incomplete'
            WHEN age_at_race <= 24 THEN 'U24'
            WHEN age_at_race BETWEEN 25 AND 29 THEN '25-29'
            WHEN age_at_race BETWEEN 30 AND 34 THEN '30-34'
            WHEN age_at_race BETWEEN 35 AND 39 THEN '35-39'
            WHEN age_at_race BETWEEN 40 AND 44 THEN '40-44'
            WHEN age_at_race BETWEEN 45 AND 49 THEN '45-49'
            WHEN age_at_race BETWEEN 50 AND 54 THEN '50-54'
            WHEN age_at_race BETWEEN 55 AND 59 THEN '55-59'
            WHEN age_at_race BETWEEN 60 AND 64 THEN '60-64'
            WHEN age_at_race BETWEEN 65 AND 69 THEN '65-69'
            ELSE '70+'
        END AS age_range
    FROM expo_women
),
age_bands AS (
    SELECT *
    FROM (VALUES
        ('U24', 1),
        ('25-29', 2),
        ('30-34', 3),
        ('35-39', 4),
        ('40-44', 5),
        ('45-49', 6),
        ('50-54', 7),
        ('55-59', 8),
        ('60-64', 9),
        ('65-69', 10),
        ('70+', 11),
        ('Incomplete', 12),
        ('Total', 13)
    ) AS bands(age_range, sort_order)
),
age_counts AS (
    SELECT age_range, COUNT(*)::int AS women
    FROM expo_age
    GROUP BY age_range
    UNION ALL
    SELECT 'Total', COUNT(*)::int
    FROM expo_age
),
age_total AS (
    SELECT women AS total_women
    FROM age_counts
    WHERE age_range = 'Total'
),
combined AS (
    SELECT
        1 AS section_order,
        event_order AS row_order,
        'event_summary'::text AS report,
        event_label,
        is_upcoming,
        NULL::text AS division,
        NULL::text AS age_range,
        COUNT(*)::int AS athlete_signups,
        COUNT(*) FILTER (WHERE is_woman)::int AS women_signups,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_woman) / COUNT(*), 1) AS pct,
        COUNT(*) FILTER (WHERE gender = 'Female')::int AS women_with_profile,
        COUNT(*) FILTER (WHERE gender = 'Male')::int AS men_with_profile,
        NULL::text AS detail
    FROM athletes
    GROUP BY event_label, is_upcoming, event_order

    UNION ALL

    SELECT
        2,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, division)::int,
        'expo_category',
        'Expo (26-29 Nov 2026)',
        TRUE,
        division,
        NULL,
        NULL,
        COUNT(*)::int,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1),
        NULL,
        NULL,
        NULL
    FROM expo_women
    GROUP BY division

    UNION ALL

    SELECT
        3,
        b.sort_order,
        'expo_age_range',
        'Expo (26-29 Nov 2026)',
        TRUE,
        NULL,
        b.age_range,
        NULL,
        COALESCE(c.women, 0),
        CASE
            WHEN b.age_range = 'Total' THEN NULL
            ELSE ROUND(100.0 * COALESCE(c.women, 0) / NULLIF(t.total_women, 0), 1)
        END,
        NULL,
        NULL,
        NULL
    FROM age_bands b
    LEFT JOIN age_counts c ON c.age_range = b.age_range
    CROSS JOIN age_total t

    UNION ALL

    SELECT
        4,
        w.age_at_race,
        CASE WHEN w.age_at_race = bounds.youngest THEN 'expo_youngest' ELSE 'expo_oldest' END,
        'Expo (26-29 Nov 2026)',
        TRUE,
        w.division,
        NULL,
        NULL,
        w.age_at_race,
        NULL,
        NULL,
        NULL,
        w.birthday::text || ' | ' || w.ticket_name
    FROM expo_women w
    CROSS JOIN (
        SELECT MIN(age_at_race) AS youngest, MAX(age_at_race) AS oldest
        FROM expo_women
        WHERE age_at_race IS NOT NULL
    ) bounds
    WHERE w.age_at_race IN (bounds.youngest, bounds.oldest)
)
SELECT
    report,
    event_label,
    is_upcoming,
    division,
    age_range,
    athlete_signups,
    women_signups,
    pct,
    women_with_profile,
    men_with_profile,
    detail
FROM combined
ORDER BY section_order, row_order, detail;
