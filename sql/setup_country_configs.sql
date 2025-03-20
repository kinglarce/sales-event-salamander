CREATE TABLE IF NOT EXISTS {schema}.country_configs (
    code VARCHAR(2) PRIMARY KEY,
    country VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    sub_region VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create trigger to update timestamp
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'update_country_configs_timestamp_{schema}'
    ) THEN
        CREATE TRIGGER update_country_configs_timestamp_{schema}
        BEFORE UPDATE ON {schema}.country_configs
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
    END IF;
END $$; 