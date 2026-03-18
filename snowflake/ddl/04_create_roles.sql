-- =============================================================
-- Parsely: Create Roles & Permissions
-- =============================================================
-- Sets up a dedicated role for the pipeline with least-privilege
-- access. The pipeline can read/write data but cannot drop tables
-- or modify the database structure.
-- =============================================================

USE DATABASE parsely;

-- Create pipeline role
CREATE ROLE IF NOT EXISTS parsely_pipeline_role;

-- Grant database and schema access
GRANT USAGE ON DATABASE parsely TO ROLE parsely_pipeline_role;
GRANT USAGE ON SCHEMA parsely.bronze TO ROLE parsely_pipeline_role;
GRANT USAGE ON SCHEMA parsely.silver TO ROLE parsely_pipeline_role;
GRANT USAGE ON SCHEMA parsely.gold TO ROLE parsely_pipeline_role;

-- Grant table permissions
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA parsely.bronze TO ROLE parsely_pipeline_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA parsely.silver TO ROLE parsely_pipeline_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA parsely.gold TO ROLE parsely_pipeline_role;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE parsely_pipeline_role;

-- Assign role to your user (replace YOUR_USERNAME)
-- GRANT ROLE parsely_pipeline_role TO USER YOUR_USERNAME;
