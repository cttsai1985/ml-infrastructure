"""Synthetic end-to-end evidence; these runs are never competition benchmarks."""

import asyncio
import hashlib
import json
import os
import tempfile
import uuid
from pathlib import Path

import mlflow
import numpy as np
import requests
from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.runtime import flow_run

from mlops_platform.registry import ensure_experiment

FIXTURE_CONTRACT = {
    "experiment_name": "infrastructure/smoke",
    "artifact_namespace": "fixtures/smoke",
}


@task(name="fit-fixture", persist_result=False)
def fit_fixture() -> dict:
    features = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0], [2.0, -1.0]])
    targets = np.array([1.5, -0.4, 1.1, 3.4])
    coefficients = np.linalg.lstsq(features, targets, rcond=None)[0]
    np.testing.assert_allclose(features @ coefficients, targets, atol=1e-10)
    return {"model_type": "synthetic-linear-fixture", "coefficients": coefficients.tolist()}


@task(name="record-model", persist_result=False)
def record_model(model: dict, project_id: str, logical_id: str) -> dict:
    experiment_id = ensure_experiment(FIXTURE_CONTRACT)
    with mlflow.start_run(
        experiment_id=experiment_id,
        run_name="platform-artifact-smoke",
        tags={
            "project_id": project_id,
            "logical_run_id": logical_id,
            "prefect.flow_run_id": flow_run.id,
            "run_kind": "infrastructure_fixture",
        },
    ) as active:
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "model.json"
            artifact.write_text(json.dumps(model, sort_keys=True) + "\n")
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            mlflow.log_param("fixture_model", model["model_type"])
            mlflow.log_artifact(str(artifact), artifact_path="model")
        return {
            "project_id": project_id,
            "mlflow_run_id": active.info.run_id,
            "logical_run_id": logical_id,
            "prefect_flow_run_id": flow_run.id,
            "artifact_sha256": digest,
            "artifact_uri": active.info.artifact_uri + "/model/model.json",
            "object_key": FIXTURE_CONTRACT["artifact_namespace"]
            + "/"
            + active.info.run_id
            + "/artifacts/model/model.json",
        }


def check_artifact(receipt: dict) -> None:
    with tempfile.TemporaryDirectory() as directory:
        artifact = Path(
            mlflow.artifacts.download_artifacts(
                run_id=receipt["mlflow_run_id"],
                artifact_path="model/model.json",
                dst_path=directory,
            )
        )
        if hashlib.sha256(artifact.read_bytes()).hexdigest() != receipt["artifact_sha256"]:
            raise ValueError("Downloaded artifact checksum differs")
        model = json.loads(artifact.read_text())
        np.testing.assert_allclose(model["coefficients"], [1.5, -0.4], atol=1e-10)


@task(name="verify-artifact", retries=1, retry_delay_seconds=1, persist_result=False)
def verify_artifact(receipt: dict) -> dict:
    check_artifact(receipt)
    return receipt


@flow(name="infrastructure-fixture", persist_result=False)
def fixture_flow(project_id: str = "infrastructure-fixture") -> dict:
    return verify_artifact(record_model(fit_fixture(), project_id, str(uuid.uuid4())))


@task(name="intentional-failure", persist_result=False)
def fail_fixture():
    raise ValueError("Intentional infrastructure failure propagation check")


@flow(name="platform-failure-smoke", persist_result=False)
def failure_flow():
    fail_fixture()


async def check_prefect(receipt: dict) -> dict:
    async with get_client() as client:
        run = await client.read_flow_run(uuid.UUID(receipt["prefect_flow_run_id"]))
        if not run.state.is_completed():
            raise ValueError("Fixture flow did not complete")
        tasks = await client.read_task_runs(
            flow_run_filter=FlowRunFilter(id={"any_": [uuid.UUID(receipt["prefect_flow_run_id"])]})
        )
        if len(tasks) != 3 or any(not item.state.is_completed() for item in tasks):
            raise ValueError("Expected three completed Prefect tasks")
        return {"flow_state": run.state.name, "completed_tasks": len(tasks)}


def check_auth() -> None:
    endpoints = [
        (os.environ["MLFLOW_TRACKING_URI"] + "/api/2.0/mlflow/experiments/search", {}),
        (os.environ["PREFECT_API_URL"] + "/flows/filter", {}),
    ]
    for endpoint, body in endpoints:
        response = requests.post(endpoint, json=body, timeout=5)
        if response.status_code not in (401, 403):
            raise ValueError("Anonymous access was not rejected")
    ui = requests.get(os.environ["PREFECT_API_URL"].removesuffix("/api") + "/", timeout=5)
    if ui.status_code != 200 or "text/html" not in ui.headers.get("Content-Type", ""):
        raise ValueError("Prefect UI was not served")


def main(command: str = "smoke") -> None:
    path = Path(os.environ.get("PLATFORM_RECEIPT", "/receipts/smoke.json"))
    if command == "smoke":
        check_auth()
        receipt = fixture_flow()
        receipt.update(asyncio.run(check_prefect(receipt)))
        failed_state = failure_flow(return_state=True)
        if not failed_state.is_failed():
            raise ValueError("Intentional failure was not surfaced")
        receipt.update(
            failure_propagation_verified=True,
            anonymous_access_rejected=True,
            prefect_ui_verified=True,
            run_kind="infrastructure_fixture",
            status="verified",
        )
        path.write_text(json.dumps(receipt, indent=2) + "\n")
        path.chmod(0o644)
    else:
        receipt = json.loads(path.read_text())
        check_auth()
        check_artifact(receipt)
        receipt.update(asyncio.run(check_prefect(receipt)), persistence_verified=True)
        path.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
