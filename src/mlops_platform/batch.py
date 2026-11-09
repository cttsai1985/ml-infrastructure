"""Observed batch execution with immutable resume checks and explicit quality gates.

This trusted-host runner controls Docker. It does not deploy a serving endpoint,
retry model fitting, or interpret a passed research gate as production approval.
"""

import fcntl
import hashlib
import json
import math
import os
import re
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from prefect import flow, task
from prefect.runtime import flow_run
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_URL,
    PREFECT_SERVER_ALLOW_EPHEMERAL_MODE,
    temporary_settings,
)

from mlops_platform.registry import load_registry


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def canonical(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def confined(root, relative):
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root.resolve()) or candidate == root.resolve():
        raise ValueError("Artifact path escapes its root")
    return candidate


def fingerprints(root, names):
    result = {}
    for name in names:
        path = confined(root, name)
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"Required regular artifact missing: {name}")
        result[name] = digest(path)
    return result


def validate_config(config):
    if config.get("schema_version") != 1:
        raise ValueError("Unsupported batch configuration")
    if config["project_id"] not in load_registry()["projects"]:
        raise ValueError("Unknown project")
    if not 0 < config.get("cpus", 4) <= 8:
        raise ValueError("CPU allocation must be at most eight cores")
    if not re.fullmatch(r"(?:[1-9]|1[0-6])g", config.get("memory", "4g")):
        raise ValueError("Memory allocation must be between 1g and 16g")
    if not config.get("owner") or not config.get("input_files"):
        raise ValueError("Specify the operator and input files")
    if not re.fullmatch(r"(?:[a-z0-9][a-z0-9._/:-]*@sha256:|sha256:)[0-9a-f]{64}", config["image"]):
        raise ValueError("Pin the Docker image by immutable digest")
    names = set()
    gated = False
    for stage in config["stages"]:
        name = stage["name"]
        if not re.fullmatch(r"[a-z][a-z0-9-]*", name) or name in names:
            raise ValueError("Stage names must be unique stable IDs")
        names.add(name)
        kind = stage.get("kind", "container")
        if kind == "gate":
            if stage.get("retries", 0):
                raise ValueError("Quality rejection is not retried")
            if not stage.get("checks"):
                raise ValueError("Quality gate needs checks")
            gated = True
        elif kind == "container":
            if not stage.get("command") or any(
                not isinstance(x, str) or not x for x in stage["command"]
            ):
                raise ValueError("Use a command argument vector")
            if not 1 <= stage.get("timeout_seconds", 600) <= 86400:
                raise ValueError("Stage deadline must be between 1 and 86400 seconds")
            retries = stage.get("retries", 0)
            if retries not in (0, 1, 2) or (retries and not stage.get("read_only")):
                raise ValueError("Only explicitly read-only stages allow at most two retries")
            if stage.get("publishes_candidate") and not gated:
                raise ValueError("Candidate publication requires an earlier quality gate")
        else:
            raise ValueError("Unsupported stage kind")
    if not names or not gated:
        raise ValueError("A batch workflow must include an explicit quality gate")


def event(state, name, **values):
    record = {"at": datetime.now(UTC).isoformat(), "event": name, **values}
    with (state / "events.jsonl").open("a") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")


class GateRejected(ValueError):
    def __init__(self, results):
        super().__init__("Quality gate rejected this candidate")
        self.results = results


def stage_artifacts(stage):
    return sorted(
        set(stage.get("outputs", [])) | {check["path"] for check in stage.get("checks", [])}
    )


def gate(output, checks):
    results = []
    for check in checks:
        value = json.loads(confined(output, check["path"]).read_text())
        for part in check["metric"].split("."):
            value = value[part]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("Quality gate expects a finite numeric metric")
        passed = math.isfinite(value)
        passed &= "min" not in check or value >= check["min"]
        passed &= "max" not in check or value <= check["max"]
        if "min" not in check and "max" not in check:
            raise ValueError("Quality gate requires a threshold")
        results.append({**check, "observed": value, "passed": bool(passed)})
    if not all(item["passed"] for item in results):
        raise GateRejected(results)
    return results


def container(stage, config, data, output, state, attempt, workflow_id):
    name = "mlops-batch-" + uuid.uuid4().hex[:20]
    log = state / f"{stage['name']}-attempt-{attempt}.log"
    arguments = [
        "docker",
        "run",
        "--name",
        name,
        "--rm",
        "--cpus",
        str(config.get("cpus", 4)),
        "--memory",
        config.get("memory", "4g"),
        "--pids-limit",
        "256",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--network",
        config.get("network", "none"),
        "--label",
        f"mlops.project_id={config['project_id']}",
        "--env",
        f"PREFECT_FLOW_RUN_ID={workflow_id}",
        "--mount",
        f"type=bind,src={data},dst=/data,readonly",
        "--mount",
        f"type=bind,src={output},dst=/outputs" + (",readonly" if stage.get("read_only") else ""),
    ]
    for key in ["MLFLOW_TRACKING_URI", "MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"]:
        if key in os.environ and config.get("network", "none") != "none":
            arguments.extend(["--env", key])
    if config.get("entrypoint"):
        arguments.extend(["--entrypoint", config["entrypoint"]])
    arguments.extend([config["image"], *stage["command"]])
    try:
        with log.open("w") as stream:
            log.chmod(0o600)
            result = subprocess.run(
                arguments,
                stdout=stream,
                stderr=subprocess.STDOUT,
                timeout=stage.get("timeout_seconds", 600),
                check=False,
            )
        if result.returncode:
            raise RuntimeError(f"Container exited {result.returncode}; inspect private stage log")
    finally:
        # Exact uniquely named container only; also cleans up on deadline or cancellation.
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False, timeout=30)
    return {"private_log": log.name}


@task(persist_result=False)
def run_stage(stage, config, data, output, state, workflow_id):
    data, output, state = Path(data), Path(output), Path(state)
    path = state / f"{stage['name']}.json"
    run_identity = (
        json.loads((state / "manifest.json").read_text())
        if (state / "manifest.json").exists()
        else None
    )
    if run_identity and fingerprints(data, config["input_files"]) != run_identity["input_hashes"]:
        raise ValueError("Inputs changed during the workflow")
    identity = canonical({"config": config, "stage": stage})
    if path.exists():
        previous = json.loads(path.read_text())
        if previous.get("status") == "completed" and stage_artifacts(stage):
            if previous["identity"] != identity:
                raise ValueError("Stage identity changed; use a new output directory")
            actual = fingerprints(output, stage_artifacts(stage))
            if actual != previous["outputs"]:
                raise ValueError("Completed stage outputs changed; refusing unsafe resume")
            event(state, "stage_reused", stage=stage["name"], prefect_flow_run_id=workflow_id)
            return previous
    attempt = 1
    if path.exists():
        attempt = json.loads(path.read_text()).get("attempt", 0) + 1
    receipt = {
        "stage": stage["name"],
        "identity": identity,
        "status": "running",
        "attempt": attempt,
        "prefect_flow_run_id": workflow_id,
        "started_at": datetime.now(UTC).isoformat(),
    }
    write_json(path, receipt)
    event(
        state,
        "stage_started",
        stage=stage["name"],
        attempt=attempt,
        prefect_flow_run_id=workflow_id,
    )
    started = time.monotonic()
    try:
        if stage.get("kind") == "gate":
            details = {"checks": gate(output, stage["checks"])}
        else:
            details = container(stage, config, data, output, state, attempt, workflow_id)
        receipt.update(
            status="completed", outputs=fingerprints(output, stage_artifacts(stage)), **details
        )
    except BaseException as error:
        receipt.update(status="failed", error_type=type(error).__name__)
        if isinstance(error, GateRejected):
            receipt["checks"] = error.results
        event(
            state,
            "stage_failed",
            stage=stage["name"],
            error_type=type(error).__name__,
            attempt=attempt,
        )
        raise
    finally:
        receipt["elapsed_seconds"] = time.monotonic() - started
        receipt["finished_at"] = datetime.now(UTC).isoformat()
        write_json(path, receipt)
    event(state, "stage_completed", stage=stage["name"], attempt=attempt)
    return receipt


@flow(name="research-batch", persist_result=False)
def batch_flow(config, data, output):
    """Each stage is a separately observed task; rejected gates block later tasks."""
    state = Path(output) / ".batch-ops"
    workflow_id = str(flow_run.id)
    results = []
    try:
        for stage in config["stages"]:
            result = run_stage.with_options(
                name=stage["name"],
                retries=stage.get("retries", 0),
                retry_delay_seconds=1,
            )(stage, config, str(data), str(output), str(state), workflow_id)
            results.append(result)
        status = {
            "status": "completed",
            "prefect_flow_run_id": workflow_id,
            "stage_count": len(results),
        }
    except BaseException as error:
        status = {
            "status": "failed",
            "prefect_flow_run_id": workflow_id,
            "completed_stages": [result["stage"] for result in results],
            "failed_stage": stage["name"],
            "error_type": type(error).__name__,
            "owner": config["owner"],
            "recovery": (
                "Inspect the private stage log and incident. Fix the external fault and rerun "
                "the identical config. Changed inputs or completed outputs need a new directory; "
                "partial failed training outputs need explicit operator recovery."
            ),
        }
        write_json(state / "incident.json", status)
        event(state, "workflow_failed", **status)
        raise
    finally:
        write_json(state / "status.json", status)
    if (state / "incident.json").exists():
        incident = json.loads((state / "incident.json").read_text())
        incident.update(resolved_at=datetime.now(UTC).isoformat(), resolved_by=workflow_id)
        write_json(state / "incident.json", incident)
    event(state, "workflow_completed", **status)
    return status


def execute(config, data, output):
    if not os.environ.get("PREFECT_API_URL"):
        raise ValueError("Set an explicitly approved Prefect API; ephemeral servers are disabled")
    validate_config(config)
    data, output = Path(data).resolve(), Path(output).resolve()
    if (
        not data.is_dir()
        or data.is_relative_to(output)
        or output.is_relative_to(data)
        or any("," in str(p) for p in [data, output])
    ):
        raise ValueError("Use separate data/output directories with Docker-safe paths")
    output.mkdir(parents=True, exist_ok=True)
    state = output / ".batch-ops"
    state.mkdir(mode=0o700, exist_ok=True)
    identity = {
        "config": config,
        "data": str(data),
        "input_hashes": fingerprints(data, config["input_files"]),
    }
    with (state / "lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest = state / "manifest.json"
        if manifest.exists():
            if json.loads(manifest.read_text()) != identity:
                raise ValueError("Input/config identity changed; use a new output directory")
        else:
            write_json(manifest, identity)
        settings = {
            PREFECT_API_URL: os.environ["PREFECT_API_URL"],
            PREFECT_SERVER_ALLOW_EPHEMERAL_MODE: False,
        }
        if "PREFECT_API_AUTH_STRING" in os.environ:
            settings[PREFECT_API_AUTH_STRING] = os.environ["PREFECT_API_AUTH_STRING"]
        with temporary_settings(settings):
            return batch_flow.with_options(name="kaggle-" + config["project_id"])(
                config, data, output
            )
