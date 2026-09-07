#!/usr/bin/env bash
set -euo pipefail

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=control_user="$CONTROL_DB_USER" \
  --set=control_password="$CONTROL_DB_PASSWORD" \
  --set=notification_user="$NOTIFICATION_DB_USER" \
  --set=notification_password="$NOTIFICATION_DB_PASSWORD" \
  <<'SQL'

SELECT format(
  'CREATE ROLE %I LOGIN PASSWORD %L',
  :'control_user',
  :'control_password'
)
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_roles
  WHERE rolname = :'control_user'
)
\gexec

SELECT format(
  'CREATE ROLE %I LOGIN PASSWORD %L',
  :'notification_user',
  :'notification_password'
)
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_roles
  WHERE rolname = :'notification_user'
)
\gexec

SELECT format(
  'CREATE SCHEMA IF NOT EXISTS rules AUTHORIZATION %I',
  :'control_user'
)
\gexec

SELECT format(
  'ALTER SCHEMA rules OWNER TO %I',
  :'control_user'
)
\gexec

SELECT format(
  'CREATE SCHEMA IF NOT EXISTS notification AUTHORIZATION %I',
  :'notification_user'
)
\gexec

SELECT format(
  'ALTER SCHEMA notification OWNER TO %I',
  :'notification_user'
)
\gexec

REVOKE ALL ON SCHEMA rules FROM PUBLIC;
REVOKE ALL ON SCHEMA notification FROM PUBLIC;

SQL
