CREATE TABLE IF NOT EXISTS {SCHEMA}.ticket_age_groups (
    id SERIAL PRIMARY KEY,
    ticket_group VARCHAR(100) NOT NULL,
    ticket_event_day VARCHAR(50) NOT NULL,
    age_range VARCHAR(50) NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    ticket_category VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticket_group, ticket_event_day, age_range)
);

-- Create trigger to update the updated_at column
CREATE OR REPLACE FUNCTION update_ticket_age_groups_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_ticket_age_groups_updated_at ON {SCHEMA}.ticket_age_groups;
CREATE TRIGGER trigger_update_ticket_age_groups_updated_at
BEFORE UPDATE ON {SCHEMA}.ticket_age_groups
FOR EACH ROW
EXECUTE FUNCTION update_ticket_age_groups_updated_at(); 