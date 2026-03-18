-- =============================================================
-- Parsely: Create Schemas (Medallion Architecture)
-- =============================================================
-- Bronze: Raw extracted data exactly as parsed from documents
-- Silver: Cleaned, normalized, and validated data
-- Gold:   Business-ready dimensional model for analytics and the app
-- =============================================================

USE DATABASE parsely;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA bronze IS 'Raw extracted data from parsed documents';
COMMENT ON SCHEMA silver IS 'Cleaned and normalized document data';
COMMENT ON SCHEMA gold IS 'Business-ready dimensional model';
