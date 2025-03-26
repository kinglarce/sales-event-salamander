WITH base_names AS (
    SELECT
        t.transaction_id,
        t.ticket_name,
        t.age,
        ts.ticket_category,
        CASE 
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END as base_name
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_type_summary ts 
        ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
),

ticket_data AS (
    SELECT
        transaction_id,
        ticket_name,
        base_name,
        ticket_category,
        age,
        CASE
            -- Corporate Relay
            WHEN base_name LIKE '%hyrox womens corporate relay%' THEN 'HYROX WOMENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mens corporate relay%' THEN 'HYROX MENS CORPORATE RELAY'
            WHEN base_name LIKE '%hyrox mixed corporate relay%' THEN 'HYROX MIXED CORPORATE RELAY'
            -- Regular Relay
            WHEN base_name ~* 'hyrox womens relay' THEN 'HYROX WOMENS RELAY'
            WHEN base_name ~* 'hyrox mens relay' THEN 'HYROX MENS RELAY'
            WHEN base_name ~* 'hyrox mixed relay' THEN 'HYROX MIXED RELAY'
            -- Pro Categories
            WHEN base_name ~* 'hyrox pro women|hyrox women pro' THEN 'HYROX PRO WOMEN'
            WHEN base_name ~* 'hyrox pro men|hyrox men pro' THEN 'HYROX PRO MEN'
            WHEN base_name ~* 'hyrox pro doubles women|hyrox doubles women pro' THEN 'HYROX PRO DOUBLES WOMEN'
            WHEN base_name ~* 'hyrox pro doubles men|hyrox doubles men pro' THEN 'HYROX PRO DOUBLES MEN'
            -- Standard Categories
            WHEN base_name = 'hyrox men' THEN 'HYROX MEN'
            WHEN base_name = 'hyrox women' THEN 'HYROX WOMEN'
            WHEN base_name LIKE '%hyrox adaptive men%' THEN 'HYROX ADAPTIVE MEN'
            WHEN base_name LIKE '%hyrox adaptive women%' THEN 'HYROX ADAPTIVE WOMEN'
            -- Doubles
            WHEN base_name LIKE '%hyrox doubles women%' THEN 'HYROX DOUBLES WOMEN'
            WHEN base_name LIKE '%hyrox doubles men%' THEN 'HYROX DOUBLES MEN'
            WHEN base_name LIKE '%hyrox doubles mixed%' THEN 'HYROX DOUBLES MIXED'
            ELSE NULL
        END as ticket_group
    FROM base_names
),

-- First separate the processing based on ticket type
categorized_tickets AS (
    SELECT 
        transaction_id,
        ticket_group,
        ticket_name,
        age,
        CASE 
            WHEN ticket_group LIKE '%RELAY%' THEN 'RELAY'
            WHEN ticket_group LIKE '%DOUBLES%' THEN 'DOUBLES'
            ELSE 'SINGLE'
        END as category_type,
        CASE 
            WHEN ticket_name LIKE '%ATHLETE 2%' OR ticket_name LIKE '%TEAM MEMBER%' 
            THEN 'MEMBER' 
            ELSE 'MAIN' 
        END as ticket_type
    FROM ticket_data
    WHERE transaction_id IS NOT NULL
        AND ticket_group = :ticket_group
),

-- Handle doubles pairing
doubles_pairs AS (
    SELECT 
        transaction_id,
        ticket_group,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id, 
            CASE WHEN ticket_type = 'MEMBER' THEN 'MEMBER' ELSE 'MAIN' END
            ORDER BY ticket_name
        ) as pair_number,
        ticket_name,
        age,
        ticket_type
    FROM categorized_tickets
    WHERE category_type = 'DOUBLES'
),

-- Handle relay grouping with proper team separation
relay_teams AS (
    SELECT 
        t1.transaction_id,
        t1.ticket_group,
        t1.ticket_name as main_ticket,
        t1.age as main_age,
        -- Get the next 3 team members for this main ticket
        STRING_AGG(t2.ticket_name, ',' ORDER BY t2.ticket_name) as member_tickets,
        STRING_AGG(CAST(t2.age AS TEXT), ',' ORDER BY t2.ticket_name) as member_ages,
        ARRAY_AGG(t2.age ORDER BY t2.ticket_name) as member_age_array,
        COUNT(t2.*) as member_count,
        -- Calculate average including main ticket and members
        FLOOR((t1.age + SUM(t2.age))::float / 4) as team_avg_age
    FROM categorized_tickets t1
    LEFT JOIN LATERAL (
        SELECT ticket_name, age
        FROM categorized_tickets t2
        WHERE t2.transaction_id = t1.transaction_id
        AND t2.ticket_group = t1.ticket_group
        AND t2.ticket_type = 'MEMBER'
        AND NOT EXISTS (
            SELECT 1 
            FROM categorized_tickets t3
            WHERE t3.transaction_id = t1.transaction_id
            AND t3.ticket_group = t1.ticket_group
            AND t3.ticket_type = 'MEMBER'
            AND t3.ticket_name < t2.ticket_name
            AND t3.age IS NOT NULL
            LIMIT 3
        )
        LIMIT 3
    ) t2 ON true
    WHERE t1.category_type = 'RELAY'
    AND t1.ticket_type = 'MAIN'
    GROUP BY t1.transaction_id, t1.ticket_group, t1.ticket_name, t1.age
),

-- Combine results based on category type
final_groups AS (
    -- Handle doubles
    SELECT 
        d1.transaction_id,
        d1.ticket_group,
        d1.ticket_name as member1_ticket,
        d2.ticket_name as member2_ticket,
        d1.age as member1_age,
        d2.age as member2_age,
        FLOOR((NULLIF(d1.age, 0) + NULLIF(d2.age, 0))::float / 2) as group_avg_age,
        'DOUBLES' as category_type,
        CASE 
            WHEN d1.age IS NULL OR d2.age IS NULL OR  -- Check for missing ages
                 d2.ticket_name IS NULL               -- Check for missing partner
            THEN true 
            ELSE false 
        END as is_incomplete
    FROM doubles_pairs d1
    LEFT JOIN doubles_pairs d2 ON 
        d1.transaction_id = d2.transaction_id AND
        d1.pair_number = d2.pair_number AND
        d1.ticket_type = 'MAIN' AND 
        d2.ticket_type = 'MEMBER'
    WHERE d1.ticket_type = 'MAIN'

    UNION ALL

    -- Handle relays
    SELECT 
        transaction_id,
        ticket_group,
        main_ticket as member1_ticket,
        member_tickets as member2_ticket,
        main_age as member1_age,
        NULL as member2_age,
        team_avg_age as group_avg_age,
        'RELAY' as category_type,
        CASE 
            WHEN member_count < 3 OR                  -- Check for missing members
                 main_age IS NULL OR                  -- Check for missing main age
                 array_length(array_remove(member_age_array, NULL), 1) < 3 OR  -- Check for missing member ages
                 member_tickets IS NULL               -- Check for missing member tickets
            THEN true 
            ELSE false 
        END as is_incomplete
    FROM relay_teams

    UNION ALL

    -- Handle singles
    SELECT 
        transaction_id,
        ticket_group,
        ticket_name as member1_ticket,
        NULL as member2_ticket,
        age as member1_age,
        NULL as member2_age,
        age as group_avg_age,
        'SINGLE' as category_type,
        CASE 
            WHEN age IS NULL OR age = 0  -- Check for missing or invalid age
            THEN true 
            ELSE false 
        END as is_incomplete
    FROM categorized_tickets
    WHERE category_type = 'SINGLE'
),

-- Add validation summary
group_validation AS (
    SELECT 
        transaction_id,
        ticket_group,
        category_type,
        group_avg_age,
        is_incomplete,
        CASE 
            WHEN category_type = 'RELAY' THEN 4
            WHEN category_type = 'DOUBLES' THEN 2
            ELSE 1
        END as expected_count
    FROM final_groups
),

-- Add validation checks CTE before paired_entries
validation_checks AS (
    SELECT 
        transaction_id,
        ticket_group,
        COUNT(*) as total_members,
        COUNT(CASE WHEN ticket_type = 'MAIN' THEN 1 END) as main_count,
        COUNT(CASE WHEN ticket_type = 'MEMBER' THEN 1 END) as member_count,
        CASE 
            WHEN ticket_group LIKE '%RELAY%' AND 
                (COUNT(*) < 4 OR COUNT(*) > 4) THEN 'Invalid relay team size'
            WHEN ticket_group LIKE '%DOUBLES%' AND 
                (COUNT(*) < 2 OR COUNT(*) > 2) THEN 'Invalid doubles team size'
            WHEN COUNT(CASE WHEN ticket_type = 'MAIN' THEN 1 END) = 0 THEN 'Missing main ticket'
            WHEN COUNT(CASE WHEN age IS NULL THEN 1 END) > 0 THEN 'Missing age data'
            ELSE NULL
        END as validation_error
    FROM categorized_tickets
    GROUP BY transaction_id, ticket_group
),

-- Then continue with paired_entries and the rest...
paired_entries AS (
    SELECT 
        g.transaction_id,
        g.ticket_group,
        g.group_avg_age as pair_avg_age,
        CASE 
            WHEN g.is_incomplete THEN true
            WHEN v.validation_error IS NOT NULL THEN true
            ELSE false
        END as is_incomplete,
        v.validation_error,
        g.expected_count
    FROM group_validation g
    LEFT JOIN validation_checks v ON 
        g.transaction_id = v.transaction_id AND 
        g.ticket_group = v.ticket_group
    WHERE g.group_avg_age IS NOT NULL
),

-- Group and validate pairs with improved counting
transaction_status AS (
    SELECT 
        ticket_group,
        transaction_id,
        expected_count as required_members,
        pair_avg_age,
        CASE 
            WHEN pair_avg_age >= :min_age AND pair_avg_age <= :max_age THEN true
            ELSE false
        END as in_age_range,
        is_incomplete,
        validation_error
    FROM paired_entries
),

-- Final counts with detailed breakdown
group_counts AS (
    SELECT
        ticket_group,
        -- Complete entries in age range
        SUM(CASE 
            WHEN NOT is_incomplete AND in_age_range
            THEN required_members
            ELSE 0 
        END) as complete_count,
        -- Incomplete entries
        SUM(CASE 
            WHEN is_incomplete 
            THEN required_members
            ELSE 0
        END) as incomplete_count,
        -- Total count
        SUM(required_members) as total_count,
        -- Additional metrics for verification
        COUNT(DISTINCT CASE WHEN is_incomplete THEN transaction_id END) as incomplete_transactions,
        COUNT(DISTINCT CASE WHEN NOT is_incomplete AND in_age_range THEN transaction_id END) as complete_transactions,
        STRING_AGG(DISTINCT validation_error, '; ' ORDER BY validation_error) FILTER (WHERE validation_error IS NOT NULL) as validation_errors
    FROM transaction_status
    GROUP BY ticket_group
)

-- Final output with detailed information
SELECT 
    CASE 
        WHEN :is_incomplete THEN 
            COALESCE(incomplete_count, 0)
        ELSE 
            COALESCE(complete_count, 0)
    END as count,
    COALESCE(total_count, 0) as total,
    incomplete_transactions,
    complete_transactions,
    validation_errors
FROM group_counts
WHERE ticket_group = :ticket_group;