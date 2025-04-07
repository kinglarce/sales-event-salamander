-- Migration script to handle table and column renames
-- Run this on databases with existing data

-- Handle constraint name updates for all tables
DO $$
DECLARE
    constraint_rec RECORD;
BEGIN
    -- Find and handle any unique_ticket_volume constraints
    FOR constraint_rec IN
        SELECT tc.table_schema, tc.table_name, tc.constraint_name
        FROM information_schema.table_constraints tc
        WHERE tc.constraint_name = 'unique_ticket_volume'
        AND tc.table_schema = '{SCHEMA}'
    LOOP
        -- Drop the old constraint
        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
            constraint_rec.table_schema, constraint_rec.table_name, constraint_rec.constraint_name);
        RAISE NOTICE 'Dropped constraint % from %.%', 
            constraint_rec.constraint_name, constraint_rec.table_schema, constraint_rec.table_name;
    END LOOP;
END$$;

-- Step 1: Rename ticket_type_summary to ticket_summary
DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{SCHEMA}' AND table_name = 'ticket_type_summary') THEN
    EXECUTE 'ALTER TABLE {SCHEMA}.ticket_type_summary RENAME TO ticket_summary';
    RAISE NOTICE 'Renamed ticket_type_summary to ticket_summary';
  ELSE
    RAISE NOTICE 'Table ticket_type_summary does not exist, skipping rename';
  END IF;
END$$;

-- Step 2: Rename ticket_volume to ticket_volumes
DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{SCHEMA}' AND table_name = 'ticket_volume') THEN
    EXECUTE 'ALTER TABLE {SCHEMA}.ticket_volume RENAME TO ticket_volumes';
    
    -- Create a new constraint with the updated name if needed
    EXECUTE '
    DO $$
    BEGIN
      ALTER TABLE {SCHEMA}.ticket_volumes 
        ADD CONSTRAINT unique_ticket_volumes 
        UNIQUE (event_id, shop_id, ticket_type_id);
      EXCEPTION WHEN duplicate_table THEN
        NULL;
    END$$;';
    
    RAISE NOTICE 'Renamed ticket_volume to ticket_volumes';
  ELSE
    RAISE NOTICE 'Table ticket_volume does not exist, skipping rename';
  END IF;
END$$;

-- Step 3: Rename total_count to ticket_count in ticket_under_shop_summary
DO $$
BEGIN
  IF EXISTS (
    SELECT FROM information_schema.columns 
    WHERE table_schema = '{SCHEMA}' 
    AND table_name = 'ticket_under_shop_summary'
    AND column_name = 'total_count'
  ) THEN
    EXECUTE 'ALTER TABLE {SCHEMA}.ticket_under_shop_summary RENAME COLUMN total_count TO ticket_count';
    RAISE NOTICE 'Renamed total_count to ticket_count in ticket_under_shop_summary';
  ELSE
    RAISE NOTICE 'Column total_count in ticket_under_shop_summary does not exist, skipping rename';
  END IF;
END$$;

-- Step 4: Add ticket_volume column to ticket_under_shop_summary if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT FROM information_schema.columns 
    WHERE table_schema = '{SCHEMA}' 
    AND table_name = 'ticket_under_shop_summary'
    AND column_name = 'ticket_volume'
  ) THEN
    EXECUTE 'ALTER TABLE {SCHEMA}.ticket_under_shop_summary ADD COLUMN ticket_volume INTEGER DEFAULT 0';
    
    -- Update existing records with volume data
    EXECUTE '
      UPDATE {SCHEMA}.ticket_under_shop_summary tus
      SET ticket_volume = tv.volume
      FROM {SCHEMA}.ticket_volumes tv
      WHERE tus.event_id = tv.event_id
      AND tus.ticket_type_id = tv.ticket_type_id
      AND tus.under_shop_id = tv.shop_id';
      
    RAISE NOTICE 'Added ticket_volume column to ticket_under_shop_summary and populated with data';
  ELSE
    RAISE NOTICE 'Column ticket_volume in ticket_under_shop_summary already exists, skipping addition';
  END IF;
END$$;

-- Step 5: Update ticket_shop_category in ticket_volumes to match shop_category
DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{SCHEMA}' AND table_name = 'ticket_volumes') THEN
    EXECUTE '
      UPDATE {SCHEMA}.ticket_volumes tv
      SET ticket_shop_category = tus.shop_category
      FROM {SCHEMA}.ticket_under_shops tus
      WHERE tv.event_id = tus.event_id
      AND tv.shop_id = tus.shop_id
      AND (tv.ticket_shop_category = ''undershop'' OR tv.ticket_shop_category = ''all'')';
    RAISE NOTICE 'Updated ticket_shop_category values in ticket_volumes';
  END IF;
END$$; 