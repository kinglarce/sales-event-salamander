INSERT INTO {SCHEMA}.ticket_age_groups
    (ticket_group, ticket_event_day, age_range, count, ticket_category)
VALUES
    (:ticket_group, :ticket_event_day, :age_range, :count, :ticket_category)
ON CONFLICT (ticket_group, ticket_event_day, age_range)
DO UPDATE SET
    count = :count,
    ticket_category = :ticket_category,
    updated_at = CURRENT_TIMESTAMP; 