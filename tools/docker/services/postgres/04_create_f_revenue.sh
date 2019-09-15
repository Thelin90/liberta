#!/bin/bash
set -e # exit immediately if a command exits with a non-zero status.

POSTGRES="psql -U postgres"

# create database for superset
echo "Creating f_revenue"
$POSTGRES <<EOSQL

\connect metabase

CREATE TABLE analysis.f_revenue (
    surr_session_id INTEGER REFERENCES analysis.d_session (surr_session_id),
    surr_user_id INTEGER REFERENCES analysis.d_user (surr_user_id),
    AUD BIGINT NULL,
    CZK BIGINT NULL,
    CAD BIGINT NULL,
    HUF BIGINT NULL,
    EUR BIGINT NULL,
    GBP BIGINT NULL,
    NOK BIGINT NULL,
    PHP BIGINT NULL,
    RUB BIGINT NULL,
    SEK BIGINT NULL,
    USD BIGINT NULL,
    ZAR BIGINT NULL,
    created_at TIMESTAMP NULL
);
EOSQL