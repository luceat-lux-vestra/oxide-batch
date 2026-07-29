\set ON_ERROR_STOP on

-- Disposable design-gate credentials only. Production deployments must create
-- and rotate secrets outside OxideBatch migrations.
CREATE ROLE oxide_batch_migrator
    LOGIN PASSWORD 'fixture-migrator-only'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE oxide_batch_runtime
    LOGIN PASSWORD 'fixture-runtime-only'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE oxide_batch_operator_reader
    LOGIN PASSWORD 'fixture-reader-only'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE oxide_batch_operator_writer
    LOGIN PASSWORD 'fixture-writer-only'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA oxide_batch AUTHORIZATION oxide_batch_migrator;
REVOKE ALL ON SCHEMA oxide_batch FROM PUBLIC;
GRANT USAGE ON SCHEMA oxide_batch TO
    oxide_batch_runtime,
    oxide_batch_operator_reader,
    oxide_batch_operator_writer;

ALTER DEFAULT PRIVILEGES FOR ROLE oxide_batch_migrator IN SCHEMA oxide_batch
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO oxide_batch_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE oxide_batch_migrator IN SCHEMA oxide_batch
    GRANT USAGE, SELECT ON SEQUENCES TO oxide_batch_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE oxide_batch_migrator IN SCHEMA oxide_batch
    GRANT SELECT ON TABLES TO
        oxide_batch_operator_reader,
        oxide_batch_operator_writer;
