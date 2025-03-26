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

-- First, separate main tickets and athlete 2 tickets
ticket_pairs AS (
    SELECT 
        transaction_id,
        ticket_group,
        -- Create a row number for each type within the transaction to match pairs
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id, 
            CASE WHEN ticket_name LIKE '%ATHLETE 2%' OR ticket_name LIKE '%TEAM MEMBER%' 
                 THEN 'ATHLETE2' ELSE 'MAIN' END
            ORDER BY ticket_name
        ) as pair_number,
        ticket_name,
        age,
        CASE 
            WHEN ticket_name LIKE '%ATHLETE 2%' OR ticket_name LIKE '%TEAM MEMBER%' 
            THEN 'ATHLETE2' 
            ELSE 'MAIN' 
        END as ticket_type
    FROM ticket_data
    WHERE transaction_id IS NOT NULL
        AND ticket_group = :ticket_group
),

-- Create proper pairs using row numbers
paired_entries AS (
    SELECT 
        m.transaction_id,
        m.ticket_group,
        m.ticket_name as member1_ticket,
        a.ticket_name as member2_ticket,
        m.age as member1_age,
        a.age as member2_age,
        -- Calculate pair average age and round down
        FLOOR((NULLIF(m.age, 0) + NULLIF(a.age, 0))::float / 2) as pair_avg_age
    FROM ticket_pairs m
    LEFT JOIN ticket_pairs a ON 
        m.transaction_id = a.transaction_id AND
        m.pair_number = a.pair_number AND
        m.ticket_type = 'MAIN' AND 
        a.ticket_type = 'ATHLETE2'
    WHERE m.ticket_type = 'MAIN'
),

-- Group and validate pairs
transaction_status AS (
    SELECT 
        ticket_group,
        transaction_id,
        CASE
            WHEN ticket_group LIKE '%RELAY%' THEN 4
            WHEN ticket_group LIKE '%DOUBLES%' THEN 2
            ELSE 1
        END as required_members,
        pair_avg_age,
        -- Check if pair is complete (both ages present)
        CASE 
            WHEN member1_age IS NULL OR member2_age IS NULL THEN true
            ELSE false
        END as is_incomplete,
        -- Check if average age falls within the specified range
        CASE 
            WHEN pair_avg_age >= :min_age AND pair_avg_age <= :max_age THEN true
            ELSE false
        END as in_age_range
    FROM paired_entries
),

-- Final counts
group_counts AS (
    SELECT
        ticket_group,
        -- For complete entries in age range
        SUM(CASE 
            WHEN NOT is_incomplete AND in_age_range
            THEN 
                CASE 
                    WHEN ticket_group LIKE '%RELAY%' THEN 4
                    WHEN ticket_group LIKE '%DOUBLES%' THEN 2
                    ELSE 1
                END
            ELSE 0 
        END) as complete_count,
        -- For incomplete entries
        SUM(CASE 
            WHEN is_incomplete 
            THEN required_members
            ELSE 0
        END) as incomplete_count,
        -- Total count (counting each pair as their required members)
        SUM(required_members) as total_count
    FROM transaction_status
    GROUP BY ticket_group
)

SELECT 
    CASE 
        WHEN :is_incomplete THEN 
            COALESCE(incomplete_count, 0)
        ELSE 
            COALESCE(complete_count, 0)
    END as count,
    COALESCE(total_count, 0) as total
FROM group_counts
WHERE ticket_group = :ticket_group;