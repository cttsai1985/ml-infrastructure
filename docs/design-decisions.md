# Infrastructure design decisions

The target is production research/batch operation and online prediction. Design choices must support an agreed workload and service promise. A topology or tool list alone does not demonstrate that promise. The [requirements](requirements.md) record acceptance evidence and unresolved deployment decisions.

## Separate services from workloads

Prefect coordinates work; it does not supply every model's environment. MLflow tracks experiments and serves artifacts; its tracking process does not load all deployed models. PostgreSQL stores application metadata; SeaweedFS stores artifact bytes. Training/batch containers and MLflow serving containers own their dependencies and budgets independently.

This separation prevents a model dependency upgrade from changing the tracking or scheduling environment. A serving rollout must not restart the tracking server. A large training allocation must not starve metadata services. The optional Prefect process worker handles code packaged in its image; another execution environment requires an explicit worker/image configuration, not ad hoc package installation in the shared worker.

## Resource defaults and production sizing

| Current setting | Reason for a bounded default | Evidence required for production selection |
|---|---|---|
| Core services: aggregate limits of 3 CPUs and 4.25 GiB | Bound the service stack's contention on a shared host | Concurrent API load, database/cache behavior, startup and steady-state peaks, disk throughput, headroom |
| Optional worker and transient fixture client: another 1 CPU and 1.5 GiB | Keep background execution and verification allocations explicit | Worker task mix, task concurrency and client overhead; model jobs have separate allocations |
| SeaweedFS: 16 volumes with a 256 MiB target volume-size limit | Small initial storage footprint | Artifact ingest rate × retention, metadata growth, practical allocation behavior, backup space and free-space alert thresholds |
| Per-model serving resources | No universal default inferred from infrastructure limits | Model-load/worker memory, representative p95/p99 latency, sustained throughput, queueing, timeout and recovery tests |

These are limits, not reservations, measured minimum requirements or demonstrated capacity. Their sums omit operating-system overhead, Docker builds, external jobs, online model runtimes, monitoring and backups. The storage setting is a nominal approximately 4 GiB budget before operational overhead; it is not an appropriate unexplained production storage allocation. A deployment configuration must account for those costs and retain headroom.

Do not increase every limit uniformly. Measure the limiting service and the required workload, record the chosen configuration and rerun acceptance checks. Worker count can multiply model memory. No numeric availability, latency, capacity or recovery promise is invented by this repository.

## Network and access boundaries

Database and S3 endpoints are private because normal clients use application APIs and MLflow's artifact proxy. The MLflow process holds bucket-scoped credentials; project clients do not need physical storage credentials. Loopback bindings keep the current defaults from exposing application APIs to the network unintentionally.

A production ingress may expose approved application endpoints through TLS and authentication. The serving runtime needs its own access boundary; tracking-server credentials do not automatically protect `/invocations`. Specify users, principals, permissions and network exposure before deployment. Naming a project does not authorize it. Independently untrusted workloads need an isolation policy beyond Docker resource limits.

## Deployment engine and operator interface

Compose owns container lifecycle, networks and volumes. The small `./infra` interface owns checkout/state selection, protected secret initialization, configuration checks and a complete deployment operation. It forwards lifecycle work to Compose and exposes native diagnostics; it is not another scheduler.

The launcher needs no platform Python SDKs. Development, workflow execution and model packaging can use uv independently. Existing recorded identities survive checkout/repository renaming. New deployments must not silently reuse orphaned database volumes with newly generated passwords. Incomplete state must be restored or investigated, not auto-repaired by rotating secrets. New managed containers and volumes carry a non-secret state fingerprint. Startup checks that fingerprint before reusing resources; an older unlabelled deployment requires an adoption plan. Separate state directories cannot claim the same Compose identity.

A dedicated host with Compose, managed state services, or multiple nodes can each satisfy an appropriate service target. Hosting, supervision, failure domains, capacity and recovery obligations select the topology. The current CLI does not turn a checkout into an approved production target.

## Serving through MLflow

Start with the existing MLflow model format and serving runtime. Keep a generic model endpoint contract instead of adding a serving framework without a requirement. Solutions provide supported prediction flavor, preprocessing, signature and reproducible dependencies; the platform operates the runtime and selected traffic/access boundary.

Resolve aliases to a version at deployment, bind model bytes and image digest, test representative inference, then switch traffic. Keep the prior model/image/configuration available for explicit rollback. A changed registry alias does not reload an already running process. Production completion also requires request/error/latency signals, alert delivery and tested recovery. See [MLflow serving](https://mlflow.org/docs/latest/ml/deployment/) and [registry workflows](https://mlflow.org/docs/latest/ml/model-registry/workflow/).

## Durable state and operational proof

Container restart and retained-volume tests establish persistence under that failure, not recovery from host or disk loss. Production operations require protected off-host backups of both databases, object storage and matching recovery configuration/credentials; scheduled backup outcomes and a measured isolated restore must be observable. The restore must recover known workflow records and artifact/model identities, then pass a prediction check.

Platform updates and model updates have different rollback paths. Model rollback can select a previously verified package/image; a database schema migration may require a compatible backup restore. The deployment owner must define who receives alerts, approves changes and verifies recovery against the agreed objectives.
