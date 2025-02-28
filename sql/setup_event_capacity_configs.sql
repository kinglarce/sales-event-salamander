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
    UNIQUE(category)
);

-- Drop existing trigger if exists and create new one
DROP TRIGGER IF EXISTS {trigger_name} ON {schema}.event_capacity_configs;

CREATE TRIGGER {trigger_name}
BEFORE INSERT OR UPDATE ON {schema}.event_capacity_configs
FOR EACH ROW
WHEN (NEW.category = 'price_tier')
EXECUTE FUNCTION validate_price_tier(); 