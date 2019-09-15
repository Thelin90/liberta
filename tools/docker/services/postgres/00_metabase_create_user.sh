#!/bin/bash
set -e # exit if a command exits with a not-zero exit code

POSTGRES="psql -U postgres"

# create a shared role to read & write general datasets into postgres
echo "Creating database role: metabase"
$POSTGRES <<-EOSQL
CREATE USER metabase WITH
    LOGIN
    SUPERUSER
    CREATEDB
    CREATEROLE
    INHERIT
    REPLICATION
    PASSWORD '$METABASE_PASSWORD';
EOSQL