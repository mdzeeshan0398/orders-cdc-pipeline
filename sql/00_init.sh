#!/bin/bash
# Runs once when the Postgres container is first created.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "CREATE DATABASE shop;" -c "CREATE DATABASE warehouse;"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d shop -f /sql/source_schema.sql
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d warehouse -f /sql/warehouse_schema.sql
