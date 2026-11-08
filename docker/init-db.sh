#!/bin/sh
set -eu
psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv mlflow_password MLFLOW_DB_PASSWORD
\getenv prefect_password PREFECT_DB_PASSWORD
CREATE USER mlflow WITH PASSWORD :'mlflow_password';
CREATE DATABASE mlflow OWNER mlflow;
CREATE USER prefect WITH PASSWORD :'prefect_password';
CREATE DATABASE prefect OWNER prefect;
SQL
