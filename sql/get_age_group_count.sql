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

-- First count members by age range
age_range_counts AS (
    SELECT 
        transaction_id,
        ticket_group,
        CASE
            WHEN ticket_group LIKE '%RELAY%' THEN 4
            WHEN ticket_group LIKE '%DOUBLES%' THEN 2
            ELSE 1
        END as required_members,
        -- Count members in the specified age range
        COUNT(CASE 
            WHEN age >= :min_age AND age <= :max_age THEN 1 
        END) as age_range_count,
        -- Count members with any age
        COUNT(CASE WHEN age IS NOT NULL THEN 1 END) as members_with_age,
        COUNT(*) as total_members
    FROM ticket_data
    WHERE transaction_id IS NOT NULL
        AND ticket_group = :ticket_group
    GROUP BY transaction_id, ticket_group
),

-- Determine complete/incomplete status
transaction_status AS (
    SELECT 
        ticket_group,
        transaction_id,
        required_members,
        age_range_count,
        members_with_age,
        total_members,
        CASE 
            WHEN transaction_id IS NULL THEN true
            WHEN members_with_age < total_members THEN true
            ELSE false
        END as is_incomplete
    FROM age_range_counts
),

-- Final counts
group_counts AS (
    SELECT
        ticket_group,
        -- For complete entries in age range
        SUM(CASE 
            WHEN NOT is_incomplete AND age_range_count > 0
            THEN age_range_count
            ELSE 0 
        END) as complete_count,
        -- For incomplete entries
        SUM(CASE 
            WHEN is_incomplete 
            THEN total_members
            ELSE 0
        END) as incomplete_count,
        -- Total count
        SUM(total_members) as total_count
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