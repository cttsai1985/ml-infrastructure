# Infrastructure requirements and acceptance evidence

This repository provides general-purpose infrastructure for production research,
scheduled training and batch delivery, and online model serving. Solutions consume
the infrastructure through documented interfaces and retain ownership of their
scientific code, model packages, identities, and deployment requests. The intended
scope includes both batch operation and online prediction.

This document records accepted requirements from the requirements review on
2026-11-09. It distinguishes the intended capability from evidence of completion.
Existing demonstration workloads establish particular tested behaviors; they do
not limit the intended production scope or establish production readiness.

## Status vocabulary

- **Implemented:** present in the inspected repository, with the stated evidence
  boundary. This does not imply deployment to an existing shared service.
- **Required:** accepted behavior whose complete implementation or operational
  evidence is outstanding. Update its status only when that evidence exists.
- **Deployment decision:** a choice or target that the service owner must supply;
  no value is assumed in this document.

The status below includes the reviewed generic client contracts, operator CLI and
serving fixture. Aggregate execution evidence is retained in `contract-validation.json`;
production deployment acceptance remains separate.

## General-purpose boundary and downstream ownership

| Requirement | Status and acceptance evidence |
|---|---|
| The platform has no source-controlled catalog of solution repositories, competitions, experiments, or deployment names. | **Implemented.** The source-controlled catalog and packaged copy are removed. Explicit client contracts replace catalog lookup/bootstrap; startup and generic fixtures need no solution checkout. |
| Dependency direction is solution to platform. | **Implemented in the interface.** Clients supply experiment/namespace contracts and project/flow/deployment names. New clients require no platform catalog change; authentication and authorization remain separate operating responsibilities. |
| Platform interfaces accept client-supplied identities and execution contracts. | **Implemented contract checks.** Syntax, paths, immutable images and bounded resources are validated without a client whitelist. Access policy and authenticated deployment remain separate requirements. |
| Integration tests use generic fixtures. | **Implemented.** The fixture uses its own synthetic model and `infrastructure/smoke` experiment, independently of solution data, checkpoints and namespaces. |
| Downstream integrations use the same documented contract. | **Updated in companion downstream changes.** Consumer contracts and provisioning errors reflect client ownership, preserve current names and distinguish raw artifact bundles from serving packages. Client tracking/recovery tests are retained. |

Removing the source-controlled client catalog does not remove MLflow Model
Registry. Registry entries are runtime service data that clients create through
the service API. Platform operators may manage authenticated principals and access
policies without maintaining a catalog of scientific projects. A namespace alone
is not an authorization boundary.

Downstream clients need application endpoints and appropriately scoped application
credentials. Database and object-storage administration credentials stay with the
platform. Generic client libraries or schemas may be consumed through an explicit,
versioned dependency; clients must not read another checkout's private state or
import a platform-owned project list.

## Services, workloads, and ownership

| Capability | Status and acceptance evidence |
|---|---|
| Prefect coordinates training and batch workflows. | **Implemented in the existing batch runner.** Stage records, deadlines, failure propagation, restricted retries, quality gates, and verified-output reuse have tests and a recorded fault drill. The runner now accepts client-owned names without a catalog. |
| MLflow supplies experiment tracking, artifact access, and model registry capabilities. | **Tracking and artifact proxy implemented.** Existing reserved model-name conventions do not prove that models have been packaged, registered, promoted, or served. |
| PostgreSQL retains application metadata; SeaweedFS supplies object storage. | **Implemented in Compose.** Separate database roles and private backend networking exist. The current topology does not establish replication or disaster recovery. |
| Online prediction uses MLflow's model-serving runtime as the initial serving engine. | **Generic serving function and client-model canaries verified.** The stdlib controller accepts client-owned packages/runtime environments, serves fresh copies, checks native prediction parity and schema rejection, and records cleanup. The synthetic lifecycle also checks restart-based switching/rollback. Container image identity, protected ingress and production rollout remain required. |
| Workload dependencies and resource budgets are isolated from shared service processes. | **Required.** Each solution/model owns its environment and execution image; adding a model must not install its training dependencies into the tracking or orchestration servers. |

Platform operators own service configuration, infrastructure credentials, access
boundaries, service releases, capacity, backups, monitoring, and recovery. Solution
owners own data contracts, preprocessing, model behavior, evaluation, packaging,
and requested workload resources. The deployment owner for each model endpoint
must be named, including who authorizes promotion and receives failures.

Batch runners and model runtimes must not obtain database administration access or
an unrestricted Docker socket as an incidental consequence of using the platform.
Where a trusted host runner needs Docker access, document its privilege and trust
boundary. Untrusted multi-tenant execution requires a separately selected isolation
design; container resource limits alone do not establish that boundary.

## Model packaging and online deployment

**Required:** an online deployment consumes an MLflow Model package with a supported
prediction flavor, preprocessing/loading code, dependencies, an input/output
signature, and an executable input example. Existing raw checkpoints, arbitrary
artifact bundles, and registered-name conventions do not satisfy this contract.
Use a suitable native flavor or a client-owned `pyfunc` adapter. See the official
[MLflow Model format](https://mlflow.org/docs/latest/ml/model/) and
[model-serving documentation](https://mlflow.org/docs/latest/ml/deployment/).

The minimal deployment lifecycle must:

1. Resolve the requested model to an exact registered version or explicit artifact
   identity, and record the model bytes' digest and runtime image digest. An alias
   may select a version at deployment time; changing an alias does not constitute a
   rollout or a promise of live hot-swapping.
2. Build or select that model's reproducible environment, including preprocessing
   and native dependencies. Preserve a compatible prior image and model package
   for rollback. A registry version alone does not prove artifact immutability.
3. Start a separate serving runtime with explicit CPU/memory, worker/concurrency,
   timeout, and, where needed, GPU settings. Measure model-load and per-worker
   memory rather than assuming workers share a single model allocation.
4. Validate readiness and representative inference through the actual serving
   interface before exposing the candidate. Check schema, preprocessing, output
   compatibility, and prediction equivalence with the packaged model.
5. Route requests through the selected TLS/authentication boundary, record the
   active version, and support an explicit switch and rollback. The rollout method
   and allowed interruption depend on the endpoint's agreed service objective.
6. Emit request/error/latency and resource signals, with logs that identify the
   model version without indiscriminately retaining sensitive input payloads.

MLflow supplies the model execution interface; authentication at the tracking
server must not be assumed to protect a separately running prediction server.
The [registry workflow](https://mlflow.org/docs/latest/ml/model-registry/workflow/)
is distinct from operating a running endpoint. Automated promotion, automatic
retraining, and a more elaborate serving framework are not prerequisites imposed
by this requirements review.

## Deployment and operator experience

**Required:** a lightweight checkout-local operator entry point should provide a
complete deployment operation, useful status/logs, configuration checks, and a
data-preserving stop operation. Starting infrastructure must not require installing
Prefect, MLflow, and every model dependency in the operator's host environment.
Docker Compose remains a suitable implementation engine when it meets the chosen
hosting and service objectives; Kubernetes is not an implicit requirement.

The operator interface must:

- Resolve checkout/configuration paths explicitly and report the selected target.
  Invoking it from another working directory must not change deployment identity.
- Preflight the selected target, validate configuration, reuse complete existing
  state, and create credentials only for a genuinely new deployment. Repetition
  must not rotate secrets, create a second stack, or rename persistent resources.
- Reject incomplete or conflicting state with an actionable error. Ambient
  environment variables must not silently redirect secrets, networks, or identity.
- Keep independently named deployments isolated, preserve stored network names,
  and keep credentials out of command arguments and routine output.
- Preserve data during normal lifecycle commands. Destructive test cleanup must
  be limited to resources owned by that particular disposable test.
- Keep native service diagnostics accessible without building a second
  orchestration framework. Installation, development checks, service deployment,
  and model deployment must be distinguishable in documentation and help.

**Implemented baseline:** stdlib management code, protected secret-file creation,
explicit Compose project selection, private backend networking, health checks,
and an isolated persistence smoke test. The revised `./infra` CLI has focused safe-reuse tests and an isolated Compose
integration path. Newly managed containers and volumes carry a state fingerprint;
older unlabelled resources require reviewed adoption before mutating lifecycle commands.

## Production operation and evidence

| Requirement | Status and acceptance evidence |
|---|---|
| Authenticated access and TLS at every required external application boundary. | **Partially implemented.** The fresh Compose stack has application basic authentication and loopback bindings. TLS, the production identity policy, and prediction-endpoint protection require implementation and verification. Do not assume the existing shared deployment has the same authentication configuration. |
| Service and workload observability with an accountable recipient. | **Partially implemented.** Container health checks and batch diagnostics exist. Add selected service metrics, centralized/retained diagnostics as needed, actionable alerts, and evidence that an operator receives them. |
| Durable off-host backups covering metadata, object bytes, and protected recovery credentials/configuration. | **Required.** A backup procedure is documented; scheduled successful backups, retention, failure alerts, and off-host recovery evidence remain outstanding. |
| Tested restoration and recovery objectives. | **Required.** Restore into an isolated target and verify known artifacts, model versions, workflow records, and a representative prediction. Record measured recovery time and recoverable data point. Container recreation with retained volumes is not backup restoration. |
| Controlled platform and model releases with rollback. | **Partially implemented.** Dependency/image pins exist. Validate candidate releases, preserve deployment receipts, test model rollback, and account for database migrations that may require restoring state rather than just switching images. |
| Capacity and failure-domain choices justified by workload evidence. | **Required.** Measure representative load, resource peaks, artifact growth, retention, and recovery time before selecting production limits and topology. |

Current core-container limits total 3 CPU cores and 4.25 GiB RAM. The optional
worker and transient client bring declared limits to 4 cores and 5.75 GiB. These
are configuration values, not capacity results, and exclude model-serving
runtimes, external training containers, builds, the operating system, monitoring,
and backup overhead. SeaweedFS is configured for 16 volumes with a 256 MiB target
volume-size limit, a nominal approximately 4 GiB budget before practical storage
overheads and behavior. Replace demonstration sizing with ingest/retention and
measured workload calculations; retain disk headroom and capacity alerts.

## Deployment decisions still open

The owner must select hosting location and budget, the user/trust model, network
exposure, administrative ownership, service and endpoint availability objectives,
batch deadlines, latency/throughput targets, concurrency, maintenance allowance,
recovery time objective, recovery point objective, retention, and growth horizon.
No numeric SLO, cloud provider, public exposure, GPU requirement, or HA topology has
been approved by this review.

A managed single-host deployment, applications backed by managed state services,
or a multi-node topology may each be appropriate. Select among them using the
requirements above. Production does not inherently mean multiple hosts; an
acceptable single-host failure and recovery policy must nevertheless be explicit.
Service supervision and IaC should follow the selected hosting design rather than
be added solely to expand the tool list.

## Existing deployments and migration

Existing shared MLflow, PostgreSQL, SeaweedFS, and separately operated Prefect are
not implicitly transferred to this repository's control. Preserve live ports,
Compose identities, networks, volumes, buckets, credentials, experiments, model
names, and artifact locations. Repository branding and generic client support
must not silently rename those resources or rotate their credentials.

Operational adoption requires an inventory, named owners, protected backups and
tested recovery, a reviewed cutover/rollback plan, and explicit authorization for
the actual deployment change. Code and documentation work alone does not perform
that adoption. The alternative repository name is not yet an accepted migration.

## Completion checklist

- Generic platform startup and smoke tests have no solution catalog dependency.
- Downstream clients retain their identities and pass their own integration tests.
- A lightweight deployment path is verified against fresh and existing isolated
  state, including invocation outside the checkout and preservation of secrets.
- A generic model is packaged, served, prediction-tested, switched, and rolled back
  with immutable model/runtime identity recorded.
- Production access, monitoring, backups, recovery, capacity, and service ownership
  have implementation and evidence matched to agreed deployment decisions.
- README presents the intended capabilities and practical workflows; this document
  and verification receipts identify remaining work without redefining the product
  as demonstration-only or claiming that unperformed production operations passed.
