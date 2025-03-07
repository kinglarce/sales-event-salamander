CREATE TABLE IF NOT EXISTS {schema}.ticket_capacity_configs (
    id SERIAL PRIMARY KEY,
    ticket_group VARCHAR(100) NOT NULL,
    event_day VARCHAR(20) NOT NULL,
    capacity INTEGER NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticket_group, event_day)
);

-- Create update timestamp trigger if it doesn't exist
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if exists and create new one
DROP TRIGGER IF EXISTS update_ticket_capacity_timestamp ON {schema}.ticket_capacity_configs;

CREATE TRIGGER update_ticket_capacity_timestamp
    BEFORE UPDATE ON {schema}.ticket_capacity_configs
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp(); 