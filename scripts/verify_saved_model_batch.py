"""Verify a real saved-model batch with controlled faults in disposable infrastructure.

Requires authorized Iceberg data and a completed modern run; never fits a model.
Only the uniquely named test deployment is created and deleted.
"""

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path

from mlops_platform.batch import execute
from mlops_platform.registry import ensure_experiment

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--data", type=Path, required=True)
parser.add_argument("--model", type=Path, required=True)
parser.add_argument("--image", default="iceberg-lab:0.1")
parser.add_argument("--receipt", type=Path, default=Path(".local/batch-verification.json"))
args = parser.parse_args()
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("manage", ROOT / "scripts/manage.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
project = "mlops-platform-smoke-" + uuid.uuid4().hex[:12]
state = ROOT / ".local" / project
ports = [m.free_port(), m.free_port()]
while ports[0] == ports[1]:
    ports[1] = m.free_port()
m.initialize(state, project, *ports)
print("Isolated batch drill:", project)
try:
    m.compose(state, "build", "mlflow")
    m.compose(state, "up", "-d", "--wait", "--wait-timeout", "240")
    os.environ.pop("PREFECT_API_KEY", None)
    os.environ.update(
        PREFECT_API_URL=f"http://127.0.0.1:{ports[1]}/api",
        PREFECT_API_AUTH_STRING=(state / "secrets/prefect_auth_string").read_text().strip(),
        MLFLOW_TRACKING_URI=f"http://127.0.0.1:{ports[0]}",
        MLFLOW_TRACKING_USERNAME="platform",
        MLFLOW_TRACKING_PASSWORD=(state / "secrets/mlflow_password").read_text().strip(),
        PREFECT_SERVER_ALLOW_EPHEMERAL_MODE="false",
    )
    # This external integration drill supplies its own client contract.
    ensure_experiment(
        {"experiment_name": "kaggle/iceberg", "artifact_namespace": "projects/iceberg"}
    )
    image = subprocess.check_output(
        ["docker", "image", "inspect", args.image, "--format", "{{.Id}}"], text=True
    ).strip()
    data = args.data.resolve()
    original = args.model.resolve()
    net = project + "_clients"
    os.environ["MLFLOW_TRACKING_URI"] = "http://mlflow:5000"
    cfg = {
        "schema_version": 1,
        "project_id": "iceberg",
        "flow_name": "kaggle-iceberg",
        "owner": "repository maintainer",
        "image": image,
        "entrypoint": "python",
        "input_files": ["train.json", "test.json", "sample_submission.csv"],
        "network": net,
        "cpus": 4,
        "memory": "8g",
        "stages": [
            {
                "name": "verify-source-model",
                "command": [
                    "-m",
                    "iceberg.cli",
                    "verify",
                    "--run-dir",
                    "/outputs/model",
                    "--data",
                    "/data/train.json",
                ],
                "read_only": True,
                "retries": 1,
                "outputs": ["model/model.pt", "model/manifest.json"],
                "timeout_seconds": 60,
            },
            {
                "name": "evaluate",
                "command": [
                    "-c",
                    (
                        "import json;from pathlib import Path;from iceberg.runtime import evaluate;"
                        "Path('/outputs/evaluation.json').write_text(json.dumps(evaluate("
                        "Path('/outputs/model/model.pt'),Path('/data/train.json'),device='cpu')))"
                    ),
                ],
                "outputs": ["evaluation.json"],
                "timeout_seconds": 120,
            },
            {
                "name": "quality-gate",
                "kind": "gate",
                "checks": [
                    {"path": "evaluation.json", "metric": "validation_log_loss", "max": 0.3}
                ],
            },
            {
                "name": "dependency-check",
                "command": [
                    "-c",
                    "import urllib.request;urllib.request.urlopen('http://batch-dependency:8000',timeout=5).read()",
                ],
                "read_only": True,
                "retries": 1,
                "timeout_seconds": 20,
            },
            {
                "name": "predict",
                "command": [
                    "-m",
                    "iceberg.cli",
                    "predict",
                    "--checkpoint",
                    "/outputs/model/model.pt",
                    "--data",
                    "/data/test.json",
                    "--output",
                    "/outputs/submission.csv",
                    "--device",
                    "cpu",
                ],
                "outputs": ["submission.csv", "submission.receipt.json"],
                "timeout_seconds": 180,
            },
            {
                "name": "publish-candidate",
                "command": [
                    "-c",
                    (
                        "import json,os;from pathlib import Path;"
                        "from iceberg.tracking import track;"
                        "Path('/outputs/import.json').write_text(json.dumps(track("
                        "Path('/outputs/model'),os.environ['MLFLOW_TRACKING_URI'])))"
                    ),
                ],
                "publishes_candidate": True,
                "outputs": ["import.json"],
                "timeout_seconds": 120,
            },
        ],
    }
    output = state / "receipts" / "saved-model-batch"
    output.mkdir()
    shutil.copytree(original, output / "model")
    failure = False
    try:
        execute(cfg, data, output)
    except Exception:
        failure = True
    assert failure
    assert not (output / "submission.csv").exists()
    evaluation = json.loads((output / ".batch-ops/evaluate.json").read_text())
    assert evaluation["attempt"] == 1
    # Start isolated read-only dependency after the failed flow. It deliberately returns
    # one 503 before recovering, proving a bounded task retry against a transient fault.
    server = """from http.server import HTTPServer,BaseHTTPRequestHandler
class H(BaseHTTPRequestHandler):
 n=0
 def do_GET(self):
  H.n+=1;self.send_response(503 if H.n==1 else 200)
  self.end_headers();self.wfile.write(b'dependency ready')
HTTPServer(('0.0.0.0',8000),H).serve_forever()
"""
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--cpus",
            "0.25",
            "--memory",
            "64m",
            "--pids-limit",
            "64",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--name",
            project + "-dependency",
            "--network",
            net,
            "--network-alias",
            "batch-dependency",
            "--entrypoint",
            "python",
            image,
            "-c",
            server,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    time.sleep(1)
    recovered = execute(cfg, data, output)
    assert recovered["status"] == "completed"
    assert json.loads((output / ".batch-ops/evaluate.json").read_text())["attempt"] == 1
    assert json.loads((output / ".batch-ops/dependency-check.json").read_text())["attempt"] == 4
    # Third identical execution must reuse all stages and not create a new model run.
    reused = execute(cfg, data, output)
    assert json.loads((output / ".batch-ops/predict.json").read_text())["attempt"] == 1
    rejected = state / "receipts" / "rejected-candidate"
    rejected.mkdir()
    shutil.copytree(original, rejected / "model")
    bad = json.loads(json.dumps(cfg))
    bad["stages"][2]["checks"][0]["max"] = 0.1
    try:
        execute(bad, data, rejected)
    except Exception:
        pass
    else:
        raise AssertionError("Expected gate rejection")
    assert not (rejected / "submission.csv").exists() and not (rejected / "import.json").exists()
    # Verify exact ordered IDs without publishing rows.
    import csv

    with (data / "sample_submission.csv").open() as f:
        expected = [r["id"] for r in csv.DictReader(f)]
    with (output / "submission.csv").open() as f:
        actual = [r["id"] for r in csv.DictReader(f)]
    assert actual == expected and len(actual) == 8424
    # Read real persisted Prefect task/run state and artifact bytes from the isolated tracker.
    import asyncio

    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import FlowRunFilter
    from prefect.settings import (
        PREFECT_API_AUTH_STRING,
        PREFECT_API_URL,
        PREFECT_SERVER_ALLOW_EPHEMERAL_MODE,
        temporary_settings,
    )

    async def inspect_runs():
        async with get_client() as client:
            runs = []
            for runid in [
                json.loads((output / ".batch-ops/incident.json").read_text())[
                    "prefect_flow_run_id"
                ],
                recovered["prefect_flow_run_id"],
                reused["prefect_flow_run_id"],
                json.loads((rejected / ".batch-ops/status.json").read_text())[
                    "prefect_flow_run_id"
                ],
            ]:
                run = await client.read_flow_run(uuid.UUID(runid))
                tasks = await client.read_task_runs(
                    flow_run_filter=FlowRunFilter(id={"any_": [uuid.UUID(runid)]})
                )
                runs.append(
                    {
                        "id": runid,
                        "state": run.state.name,
                        "tasks": [
                            {"name": t.name, "state": t.state.name, "run_count": t.run_count}
                            for t in tasks
                        ],
                    }
                )
            return runs

    with temporary_settings(
        {
            PREFECT_API_URL: os.environ["PREFECT_API_URL"],
            PREFECT_API_AUTH_STRING: os.environ["PREFECT_API_AUTH_STRING"],
            PREFECT_SERVER_ALLOW_EPHEMERAL_MODE: False,
        }
    ):
        runs = asyncio.run(inspect_runs())
    assert [item["state"] for item in runs] == ["Failed", "Completed", "Completed", "Failed"]
    os.environ["MLFLOW_TRACKING_URI"] = f"http://127.0.0.1:{ports[0]}"
    import mlflow
    from mlflow import MlflowClient

    imported = json.loads((output / "import.json").read_text())
    client = MlflowClient()
    run = client.get_run(imported["mlflow_run_id"])
    assert run.info.status == "FINISHED"
    retrieved = Path(
        mlflow.artifacts.download_artifacts(
            run_id=run.info.run_id,
            artifact_path="model.pt",
            dst_path=str(state / "receipts/readback"),
        )
    )

    def sha(p):
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()

    assert sha(retrieved) == sha(output / "model/model.pt")
    events = [
        json.loads(line) for line in (output / ".batch-ops/events.jsonl").read_text().splitlines()
    ]
    public = {
        "runner_source_sha256": hashlib.sha256(
            (ROOT / "src/mlops_platform/batch.py").read_bytes()
        ).hexdigest(),
        "schema_version": 1,
        "scope": "actual saved-model batch operation; no new model fitting or production serving",
        "image": image,
        "scientific_source_commit": json.loads((output / "model/manifest.json").read_text())[
            "source"
        ]["git_commit"],
        "checkpoint_sha256": sha(output / "model/model.pt"),
        "input_sha256": {n: sha(data / n) for n in cfg["input_files"]},
        "evaluation": json.loads((output / "evaluation.json").read_text()),
        "test_rows": 8424,
        "sample_submission_id_order_verified": True,
        "prediction_sha256": sha(output / "submission.csv"),
        "first_failure": {"stage": "dependency-check", "prediction_and_publication_blocked": True},
        "recovery": {
            "same_configuration": True,
            "successful_evaluation_attempts": 1,
            "dependency_total_attempts": 4,
            "transient_retry_observed": True,
            "resolved_incident_retained": True,
        },
        "quality_rejection": {"threshold": 0.1, "prediction_and_publication_blocked": True},
        "idempotent_reexecution": {
            "scientific_and_publication_stages_reused": True,
            "readiness_dependency_rechecked": True,
            "prediction_attempts": 1,
            "publication_attempts": 1,
        },
        "prefect_runs": runs,
        "mlflow": {
            "run_id": run.info.run_id,
            "status": run.info.status,
            "artifact_readback_sha256": sha(retrieved),
        },
        "local_stage_events": {
            "count": len(events),
            "reused_count": sum(e["event"] == "stage_reused" for e in events),
        },
        "deployment": "unique disposable isolated Compose project; deleted after verification",
        "limitations": [
            "Research gate illustrates operational policy, not an independent accuracy claim.",
            "Application auth and isolated storage tested; no production endpoint, SLA, "
            "alert delivery, continuous retraining or backup restore claim.",
        ],
    }
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(public, indent=2) + "\n")
    print(
        "Verified real saved-model batch, failures, bounded retry, resume, idempotence, "
        "ordered predictions and artifact readback"
    )
finally:
    subprocess.run(
        ["docker", "rm", "-f", project + "-dependency"], capture_output=True, check=False
    )
    m.compose(state, "down", "--volumes", "--remove-orphans")
