/*
=============================================================
Create Database and Schemas (PostgreSQL / pgAdmin)
=============================================================
Script Purpose:
    Creates the database 'DataWarehouse' if it does not exist,
    then creates schemas 'bronze', 'silver', and 'gold'.

WARNING:
    \gexec only works in psql, NOT in pgAdmin.
    This version works in pgAdmin using a DO block.
*/

-- =====================================================
-- STEP 1: Run connected to database 'postgres'
-- =====================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_database
        WHERE datname = 'DataWarehouse'
    ) THEN
        PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE "DataWarehouse"');
    END IF;
END
$$;


-- NOTE:
-- If dblink is not enabled, run this once first:
-- CREATE EXTENSION IF NOT EXISTS dblink;


-- =====================================================
-- STEP 2: Connect manually to DataWarehouse
-- =====================================================
-- In pgAdmin: open/query DataWarehouse
-- In psql:
-- \c DataWarehouse


-- =====================================================
-- STEP 3: Create Schemas
-- =====================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
