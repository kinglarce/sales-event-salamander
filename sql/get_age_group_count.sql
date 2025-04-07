WITH base_names AS (
    SELECT
        t.transaction_id,
        t.ticket_name,
        ts.ticket_event_day,
        t.age,
        ts.ticket_category,
        CASE 
            WHEN LOWER(t.ticket_name) LIKE '% | %' THEN SPLIT_PART(LOWER(t.ticket_name), ' | ', 1)
            ELSE LOWER(t.ticket_name)
        END as base_name
    FROM {SCHEMA}.tickets t
    JOIN {SCHEMA}.ticket_summary ts 
        ON t.ticket_type_id = ts.ticket_type_id
    WHERE ts.ticket_category NOT IN ('spectator', 'extra')
),

ticket_data AS (
    SELECT
        transaction_id,
        ticket_name,
        ticket_event_day,
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
        ticket_event_day,
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
        AND ticket_event_day = :event_day
),

-- Handle doubles pairing
doubles_pairs AS (
    SELECT 
        transaction_id,
        ticket_group,
        ticket_event_day,
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
    WITH numbered_tickets AS (
        SELECT 
            transaction_id,
            ticket_group,
            ticket_event_day,
            ticket_name,
            age,
            ticket_type,
            -- Number the MAIN tickets separately
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id, ticket_group, ticket_type 
                ORDER BY ticket_name
            ) as ticket_seq,
            -- Create a continuous sequence for MEMBER tickets
            CASE WHEN ticket_type = 'MEMBER' THEN
                ROW_NUMBER() OVER (
                    PARTITION BY transaction_id, ticket_group, ticket_type 
                    ORDER BY ticket_name
                )
            END as member_seq
        FROM categorized_tickets
        WHERE category_type = 'RELAY'
    )
    SELECT 
        t1.transaction_id,
        t1.ticket_group,
        t1.ticket_event_day,
        t1.ticket_name as main_ticket,
        t1.age as main_age,
        STRING_AGG(t2.ticket_name, ',' ORDER BY t2.member_seq) as member_tickets,
        STRING_AGG(CAST(t2.age AS TEXT), ',' ORDER BY t2.member_seq) as member_ages,
        ARRAY_AGG(t2.age ORDER BY t2.member_seq) as member_age_array,
        COUNT(t2.*) as member_count,
        FLOOR((t1.age + SUM(t2.age))::float / 4) as team_avg_age
    FROM numbered_tickets t1
    LEFT JOIN numbered_tickets t2 ON 
        t1.transaction_id = t2.transaction_id 
        AND t1.ticket_group = t2.ticket_group
        AND t1.ticket_event_day = t2.ticket_event_day
        AND t2.ticket_type = 'MEMBER'
        AND t2.member_seq BETWEEN ((t1.ticket_seq - 1) * 3) + 1 AND (t1.ticket_seq * 3)
    WHERE t1.ticket_type = 'MAIN'
    GROUP BY 
        t1.transaction_id,
        t1.ticket_group,
        t1.ticket_event_day,
        t1.ticket_name,
        t1.age,
        t1.ticket_seq
),

-- Combine results based on category type
final_groups AS (
    -- Handle doubles
    SELECT 
        d1.transaction_id,
        d1.ticket_group,
        d1.ticket_event_day,
        d1.ticket_name as member1_ticket,
        d2.ticket_name as member2_ticket,
        d1.age as member1_age,
        d2.age as member2_age,
        FLOOR((NULLIF(d1.age, 0) + NULLIF(d2.age, 0))::float / 2) as group_avg_age,
        'DOUBLES' as category_type,
        CASE 
            WHEN d1.age IS NULL OR d1.age = 0 OR     -- Check for missing/invalid main age
                 d2.age IS NULL OR d2.age = 0 OR     -- Check for missing/invalid member age
                 d2.ticket_name IS NULL OR            -- Check for missing partner
                 d1.transaction_id IS NULL OR TRIM(d1.transaction_id) = '' OR  -- Check for missing/empty transaction_id
                 d2.transaction_id IS NULL OR TRIM(d2.transaction_id) = ''
            THEN true 
            ELSE false 
        END as is_incomplete
    FROM doubles_pairs d1
    LEFT JOIN doubles_pairs d2 ON 
        d1.transaction_id = d2.transaction_id AND
        d1.pair_number = d2.pair_number AND
        d1.ticket_event_day = d2.ticket_event_day AND
        d1.ticket_type = 'MAIN' AND 
        d2.ticket_type = 'MEMBER'
    WHERE d1.ticket_type = 'MAIN'

    UNION ALL

    -- Handle relays
    SELECT 
        transaction_id,
        ticket_group,
        ticket_event_day,
        main_ticket as member1_ticket,
        member_tickets as member2_ticket,
        main_age as member1_age,
        NULL as member2_age,
        team_avg_age as group_avg_age,
        'RELAY' as category_type,
        CASE 
            WHEN member_count != 3 OR                 -- Must have exactly 3 members
                 main_age IS NULL OR main_age = 0 OR  -- Check for missing/invalid main age
                 array_length(array_remove(array_remove(member_age_array, NULL), 0), 1) != 3 OR  -- Check for missing/invalid member ages
                 member_tickets IS NULL OR            -- Check for missing member tickets
                 transaction_id IS NULL OR TRIM(transaction_id) = ''  -- Check for missing/empty transaction_id
            THEN true 
            ELSE false 
        END as is_incomplete
    FROM relay_teams

    UNION ALL

    -- Handle singles
    SELECT 
        transaction_id,
        ticket_group,
        ticket_event_day,
        ticket_name as member1_ticket,
        NULL as member2_ticket,
        age as member1_age,
        NULL as member2_age,
        age as group_avg_age,
        'SINGLE' as category_type,
        CASE 
            WHEN age IS NULL OR age = 0 OR  -- Check for missing or invalid age
                 transaction_id IS NULL OR TRIM(transaction_id) = ''  -- Check for missing/empty transaction_id
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
        ticket_event_day,
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

-- Then continue with paired_entries directly
paired_entries AS (
    SELECT 
        transaction_id,
        ticket_group,
        ticket_event_day,
        category_type,
        group_avg_age as pair_avg_age,
        is_incomplete,
        expected_count
    FROM group_validation
),

-- Group and validate pairs with improved counting
transaction_status AS (
    SELECT 
        ticket_group,
        ticket_event_day,
        transaction_id,
        category_type,
        expected_count as required_members,
        pair_avg_age,
        CASE 
            WHEN pair_avg_age >= :min_age AND pair_avg_age <= :max_age THEN true
            ELSE false
        END as in_age_range,
        is_incomplete
    FROM paired_entries
),

-- Final counts with detailed breakdown
group_counts AS (
    SELECT
        ticket_group,
        ticket_event_day,
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
        COUNT(DISTINCT CASE WHEN NOT is_incomplete AND in_age_range THEN transaction_id END) as complete_transactions
    FROM transaction_status
    GROUP BY ticket_group, ticket_event_day
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
    -- Add standardized ticket category
    CASE
        WHEN :ticket_group LIKE '%DOUBLES%' THEN 'double'
        WHEN :ticket_group LIKE '%RELAY%' AND :ticket_group LIKE '%CORPORATE%' THEN 'corporate_relay'
        WHEN :ticket_group LIKE '%RELAY%' THEN 'relay'
        ELSE 'single'
    END as ticket_category
FROM group_counts
WHERE ticket_group = :ticket_group
AND ticket_event_day = :event_day;