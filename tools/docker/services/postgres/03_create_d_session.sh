#!/bin/bash
set -e # exit immediately if a command exits with a non-zero status.

POSTGRES="psql -U postgres"

# create database for superset
echo "Creating d_session table"
$POSTGRES <<EOSQL

\connect metabase

CREATE TABLE analysis.d_session (
    surr_session_id INTEGER PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    session_num BIGINT NULL,
    arrival_ts_from_unix TIMESTAMP NULL,
    amount BIGINT NULL,
    attempt_num BIGINT NULL,
    category TEXT NULL,
    connection_type VARCHAR(32) NULL,
    jailbroken VARCHAR(128) NULL,
    length BIGINT NULL,
    limited_ad_tracking BOOLEAN NULL,
    manufacturer VARCHAR(64) NULL,
    platform VARCHAR(64) NULL,
    android_id VARCHAR(32) NULL,
    score BIGINT NULL,
    value DOUBLE PRECISION NULL,
    v BIGINT NULL
);
EOSQL