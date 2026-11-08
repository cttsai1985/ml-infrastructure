# Operations

## Dependency baseline

Python packages resolve from `pyproject.toml` into `uv.lock`; containers install that lock. Base images are pinned by digest. The current tested portable baseline is:

| Component | Pin | Upstream reference |
|---|---|---|
| Prefect server/client | 3.9.2 | [Release](https://github.com/PrefectHQ/prefect/releases/tag/3.9.2) |
| MLflow server/client | 3.18.0 | [Release](https://github.com/mlflow/mlflow/releases/tag/v3.18.0) |
| SeaweedFS | 4.52 + image digest | [Release](https://github.com/seaweedfs/seaweedfs/releases/tag/4.52) |
| PostgreSQL | 17.11 + image digest | [Supported versions](https://www.postgresql.org/support/versioning/) |
| Python image | 3.12.16 slim-bookworm + digest | [Official image](https://hub.docker.com/_/python) |
| Container dependency installer | uv 0.10.4 | [uv](https://docs.astral.sh/uv/) |

Prefect runs one server with its background services and an in-process messaging broker. Redis and split server/service processes are unnecessary for the verified single-server scope; revisit that choice before horizontal scaling. The [official Compose guide](https://docs.prefect.io/v3/how-to-guides/self-hosted/docker-compose) distinguishes this from a scaled deployment.

MLflow stores tracking and authentication tables in its own PostgreSQL database; Prefect uses a separate database and role. [MLflow authentication documentation](https://mlflow.org/docs/latest/self-hosting/security/basic-http-auth/) describes the supported auth configuration. Both applications use generated credentials; [Prefect basic authentication](https://docs.prefect.io/v3/advanced/security-settings) is configured through runtime settings.

The MLflow process alone holds bucket-scoped S3 credentials. Its artifact proxy separates clients from physical storage. SeaweedFS runs master, volume, filer and S3 gateway together, with explicit persistent filer metadata. Its single-host data volume is not replicated. The configured maximum of 16 volumes at 256 MiB is intentionally small; monitor capacity and plan growth before storing large experiment histories. See [SeaweedFS S3](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API).

## Secrets and access

`init` generates independent database passwords, an MLflow password, a Prefect authentication string, a Flask signing key, and bucket-scoped S3 keys. Secret files have mode 0600 inside a mode-0700 state directory. Docker mounts them read-only; entrypoints read them into process memory. Secret values are absent from Compose environment definitions and command arguments. The necessary secret-reader capability is limited to `DAC_OVERRIDE`; servers receive no Docker socket.

This still trusts the local OS user and Docker administrators. Basic authentication is not a substitute for TLS or tenant isolation. The sample username is the sole local administrator; per-user roles and centralized secrets distribution require a separate deployment design. Keep passwords out of screenshots and logs. In particular, do not publish `.local`, Docker inspection of process environments, or database dumps.

Changing a secret file does not automatically rotate persisted PostgreSQL or MLflow credentials. Rotate the database role/application credential through its supported interface, update the protected runtime file, restart affected clients, and verify both old-credential rejection and new-credential access. Plan this explicitly; `init` will not overwrite live state.

## Backup and restoration

Backups must cover **both metadata and object bytes**, plus a protected copy of the matching runtime secrets. Copying only PostgreSQL loses models; copying only SeaweedFS loses run/model/workflow associations.

For the portable deployment only, define a Compose helper using its exact state/project identity. Do not substitute an existing production project name into these instructions without its owner's approved change plan.

```bash
platform_project=mlops-platform-development
platform_state=.local/development
dc() { docker compose --project-name "$platform_project" --env-file "$platform_state/compose.env" --file compose.yaml "$@"; }
umask 077
platform_backup="$PWD/.local/backups/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$platform_backup"
# Quiesce project runners first. Stop API writers before taking consistent backups.
dc stop worker mlflow prefect
dc exec -T postgres pg_dump -U platform -Fc mlflow > "$platform_backup/mlflow.dump"
dc exec -T postgres pg_dump -U platform -Fc prefect > "$platform_backup/prefect.dump"
dc stop seaweed
```

While SeaweedFS is stopped, archive the entire `${platform_project}_seaweed_data` volume through a temporary read-only volume mount, including filer metadata and master/volume data. Store that archive with both SQL dumps, a checksum manifest, the Git revision, image digests, and an encrypted copy of the state secrets. Restart the named stack with `dc up -d --wait` after the snapshot. Treat SQL dumps as sensitive because they contain application identity and run metadata.

Restore into a **new isolated project** first. Recreate the declared database roles using matching saved secrets, restore both databases with `pg_restore`, and restore the complete SeaweedFS archive into that new project's empty storage volume. Start services with the pinned pre-backup versions. Retrieve a known run/model artifact and verify its recorded hash; query known Prefect flow/task records. Only after that drill should a production recovery procedure be considered validated. This release verifies container recreation/persistence, not a full backup-archive restore drill.

## Upgrades and diagnostics

Change versions in a PR, regenerate the lock, run unit/config checks and the isolated integration test, then test upgrades against restored copies of real metadata. Database migrations can make a simple image rollback insufficient; rollback means restoring the compatible metadata/object snapshot and previous version pins. Existing deployed Prefect 3.8.1 is not upgraded by this repository's 3.9.2 reference.

Use `uv run python scripts/manage.py status` to inspect the selected portable deployment. Inspect only that project's Compose logs when diagnosing it. Smoke failures retain private diagnostics under their `.local/mlops-platform-smoke-*/failure.log` before cleanup; the command exits nonzero. A deliberate failed fixture flow in a successful smoke is expected.

| Symptom | First check |
|---|---|
| Project cannot reach MLflow | Host versus container endpoint and membership of the approved client network |
| API returns 401/403 | Application credential files and client variables; unset a stale `PREFECT_API_KEY` for self-hosted Prefect |
| Artifact upload fails | MLflow logs, SeaweedFS health/capacity, bucket-scoped permissions; clients should not bypass the proxy |
| Prefect API healthy but UI absent | Writable configured UI cache directory under `/tmp`; the integration test checks HTML delivery |
| Bootstrap reports namespace drift | Existing experiment's artifact location; never silently relocate an experiment |
| Database initialization fails | Secret access and role creation; changing initialization scripts does not update an existing volume |

`examples/project_flow.py` intentionally does not retry training container execution: an automatic retry could duplicate expensive runs or overwrite output. Read-only artifact verification has one bounded retry. Production deployments should explicitly define idempotency, deadlines, retention, capacity alerts, recovery objectives, and who receives failures.
