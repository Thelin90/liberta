#!/bin/bash
set -e # exit immediately if a command exits with a non-zero status.

POSTGRES="psql -U postgres"

# create database for superset
echo "Creating database: metabase"
$POSTGRES <<EOSQL

CREATE DATABASE metabase;

GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;

\connect metabase

CREATE SCHEMA analysis;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analysis TO metabase;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analysis TO postgres;

EOSQL