-- The five privilege classes the M5 preview separates, as roles.
--
-- This half of the policy is everything that can be stated before the schema
-- exists: the roles themselves, what the cluster lets them do at all, and the
-- removal of the privileges PostgreSQL grants to PUBLIC by default. The table,
-- column, and sequence grants are in grants.sql, which the migration role
-- applies after it has created the objects it owns.
--
-- Two rules hold for every class. None of them may hold a cluster-level
-- privilege — no superuser, no database creation, no role creation, no
-- replication — so no class can escape its grants by granting itself more. And
-- no privilege may reach any of them through PUBLIC, which is why the default
-- CONNECT, TEMPORARY, and public-schema privileges are withdrawn here and
-- handed back only to the classes that need them.
--
-- The roles are created without a password. The campaign sets a disposable one
-- per run, so no credential is committed and none is reusable.
--
-- The report applies this file as an administrative identity, connected to the
-- database it has just created.

DROP ROLE IF EXISTS oxide_batch_m5_migration;
DROP ROLE IF EXISTS oxide_batch_m5_runtime;
DROP ROLE IF EXISTS oxide_batch_m5_explorer;
DROP ROLE IF EXISTS oxide_batch_m5_operator;
DROP ROLE IF EXISTS oxide_batch_m5_retention;

CREATE ROLE oxide_batch_m5_migration
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE oxide_batch_m5_runtime
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE oxide_batch_m5_explorer
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE oxide_batch_m5_operator
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE oxide_batch_m5_retention
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

-- The database name is not known to a committed fixture, so the statements
-- that need it are built against the database this script is connected to.
DO $$
BEGIN
    EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO '
        'oxide_batch_m5_migration, oxide_batch_m5_runtime, oxide_batch_m5_explorer, '
        'oxide_batch_m5_operator, oxide_batch_m5_retention',
        current_database());
    -- Only the migration class may add a schema to the database, which is how
    -- it creates the metadata schema it then owns.
    EXECUTE format(
        'GRANT CREATE ON DATABASE %I TO oxide_batch_m5_migration',
        current_database());
END
$$;

-- Nothing in this deployment belongs in the public schema, and no class may
-- create anything there.
REVOKE ALL ON SCHEMA public FROM PUBLIC;
