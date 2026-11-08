# Shared deployment adoption

The intent is one shared service deployment for quant research and the three solution repositories, with this repository owning the reviewed infrastructure definition and operating procedures. Repository ownership and running-deployment ownership are different until an explicit handover is completed.

## Audited running state — 2026-11-08

Read-only Docker labels, image versions, network membership and volume names established:

| Service | Current owner | Version | Client boundary |
|---|---|---|---|
| MLflow | `kaggle-quant-collections` Compose project | 3.18.0 | Host `127.0.0.1:5500`; Docker `mlflow:5000` on `kaggle-quant-collections_ui` |
| PostgreSQL for MLflow | Same quant deployment | 17.11 | Private network only; volume `kaggle-quant-collections_postgres_data` |
| SeaweedFS | Same quant deployment | 4.52 | Private network only; volume `kaggle-quant-collections_seaweed_data`; existing bucket `mlflow` |
| Prefect | Separately managed production deployment | 3.8.1, PostgreSQL 16.13 | Port 4200; outside this repository's operational ownership |

The existing quant MLflow starts without the portable stack's basic-auth option. Do not claim that adopting the client names has retrofitted authentication. No services, images, secrets, old experiments or volumes were changed during this inventory. The portable stack's integration test ran only in disposable, independently named projects.

New experiments were provisioned on the existing MLflow by the solution integration work: `kaggle/iceberg` (ID 5), `kaggle/home-credit` (ID 6), and `kaggle/quest` (ID 7), with artifact locations `mlflow-artifacts:/projects/<project>`. The server still proxies to its original bucket. IDs are observations, not client configuration. The quant-research base experiment was not created, and historical collection experiments were not renamed.

## Connect a project now

Host clients use `MLFLOW_TRACKING_URI=http://127.0.0.1:5500`. Project containers join the external `kaggle-quant-collections_ui` network and use `MLFLOW_TRACKING_URI=http://mlflow:5000`.

```yaml
services:
  lab:
    environment:
      MLFLOW_TRACKING_URI: http://mlflow:5000
    networks: [mlops]
networks:
  mlops:
    external: true
    name: ${MLOPS_DOCKER_NETWORK:-kaggle-quant-collections_ui}
```

Project overrides may default to the canonical future network `mlops-platform_clients`; set `MLOPS_DOCKER_NETWORK=kaggle-quant-collections_ui` for the existing deployment. `MLOPS_NETWORK_NAME` configures the platform's actual network, whereas `MLOPS_DOCKER_NETWORK` selects an existing network in project clients. Neither clients nor notebooks join the database/storage network.

`host.docker.internal` does not solve access to a service bound exclusively to the host's loopback address. Use the approved Docker client network. On future authenticated services, inject only application credentials at runtime; never hand project clients the database or SeaweedFS credentials.

There is no approved shared Prefect endpoint for these clients yet. Do not silently reuse port 4200 or point the portable Compose file at production volumes. Real model runs can be imported into shared MLflow now; the isolated Prefect fixture proves the platform workflow, not a completed migration of all training schedules.

## Complete the operational handover

1. Agree with the existing service owners which deployment becomes authoritative, including Prefect ownership, endpoint/auth policy, and change windows.
2. Inventory existing projects, experiments, registry models, schedules, backup policy, ports, networks, storage layout and credential distribution without exporting secret values.
3. Make the reviewed configuration in this repository reproduce the existing deployment's names, volume identities, bucket and endpoints before switching operational ownership. Keep adopted overrides separate from portable fresh-install defaults.
4. Back up metadata, credentials and object storage; prove restoration in an isolated project. Test versions and migrations against copies.
5. Plan a reversible cutover, validate real project artifact retrieval and workflow visibility, then retire the old configuration entry point. Existing files and dirty work are not deleted as part of a repository refactor.

Changing Compose project names normally creates new volumes; assigning the old names without a reviewed adoption can target live state. Neither action is performed by this release. Authentication changes and the Prefect version upgrade are separate changes, not implicit side effects of sharing the new repository.
