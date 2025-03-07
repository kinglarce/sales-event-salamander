-- Create validation function if it doesn't exist
CREATE OR REPLACE FUNCTION validate_price_tier() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.category = 'price_tier' AND NEW.value NOT IN ('EB', 'L1', 'L2', 'L3', 'L4') THEN
        RAISE EXCEPTION 'Invalid price tier. Allowed values are: EB, L1, L2, L3, L4';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create table if it doesn't exist in the specified schema
CREATE TABLE IF NOT EXISTS {schema}.event_capacity_configs (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    value VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(category)
);

-- Create update timestamp trigger if it doesn't exist
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing triggers if they exist
DROP TRIGGER IF EXISTS {trigger_name} ON {schema}.event_capacity_configs;
DROP TRIGGER IF EXISTS update_event_capacity_timestamp ON {schema}.event_capacity_configs;

-- Create price tier validation trigger
CREATE TRIGGER {trigger_name}
    BEFORE INSERT OR UPDATE ON {schema}.event_capacity_configs
    FOR EACH ROW
    WHEN (NEW.category = 'price_tier')
    EXECUTE FUNCTION validate_price_tier();

-- Create timestamp update trigger
CREATE TRIGGER update_event_capacity_timestamp
    BEFORE UPDATE ON {schema}.event_capacity_configs
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp(); 