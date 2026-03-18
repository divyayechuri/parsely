-- =============================================================
-- Parsely: Create Database
-- =============================================================
-- Run this once in Snowflake to set up the project database.
-- Requires ACCOUNTADMIN or a role with CREATE DATABASE privileges.
-- =============================================================

CREATE DATABASE IF NOT EXISTS parsely;

USE DATABASE parsely;

COMMENT ON DATABASE parsely IS 'Parsely — Document parsing and ETL pipeline for invoice data';
