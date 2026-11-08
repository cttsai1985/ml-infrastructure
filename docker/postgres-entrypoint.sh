#!/bin/sh
set -eu
# Read owner-only secrets before the official entrypoint drops to the postgres UID.
export MLFLOW_DB_PASSWORD="$(cat /run/secrets/mlflow_db_password)"
export PREFECT_DB_PASSWORD="$(cat /run/secrets/prefect_db_password)"
exec /usr/local/bin/docker-entrypoint.sh "$@"
