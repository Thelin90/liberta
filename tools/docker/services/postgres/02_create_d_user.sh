#!/bin/bash
set -e # exit immediately if a command exits with a non-zero status.

POSTGRES="psql -U postgres"

# create database for superset
echo "Creating d_user table"
$POSTGRES <<EOSQL

\connect metabase

CREATE TABLE analysis.d_user (
    surr_user_id INTEGER PRIMARY KEY NOT NULL,
    user_id TEXT NOT NULL,
    "user" VARCHAR(256) UNIQUE NOT NULL,
    country_code VARCHAR(32) NULL,
    is_converting VARCHAR(64) NULL,
    is_paying VARCHAR(64) NULL,
    origin VARCHAR(64) NULL,
    pay_ft BIGINT NULL,
    first_build VARCHAR(64) NULL,
    client_ts TIMESTAMP NULL,
    client_ts_from_unix TIMESTAMP NULL,
    cohort_month_from_unix TIMESTAMP NULL,
    cohort_week_from_unix TIMESTAMP NULL,
    install_hour_from_unix TIMESTAMP NULL,
    install_ts_from_unix TIMESTAMP NULL
);

EOSQL