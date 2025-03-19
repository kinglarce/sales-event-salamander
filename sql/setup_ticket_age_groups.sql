CREATE TABLE IF NOT EXISTS {SCHEMA}.ticket_age_groups (
    id SERIAL PRIMARY KEY,
    ticket_group VARCHAR(100) NOT NULL,
    age_range VARCHAR(20) NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticket_group, age_range)
);

-- Create update timestamp trigger
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if exists and create new one
DROP TRIGGER IF EXISTS update_age_group_timestamp ON {SCHEMA}.ticket_age_groups;

CREATE TRIGGER update_age_group_timestamp
    BEFORE UPDATE ON {SCHEMA}.ticket_age_groups
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp(); 