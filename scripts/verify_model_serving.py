"""Exercise MLflow's actual prediction server with isolated synthetic model packages.

This verifies schema, predictions and explicit restart-based version/rollback
selection. It does not deploy production ingress or claim zero-downtime routing.
"""

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
import requests
from mlflow.models import infer_signature

ROOT = Path(__file__).resolve().parents[1]


class LinearFixture(mlflow.pyfunc.PythonModel):
    def __init__(self, coefficient, intercept):
        self.coefficient = coefficient
        self.intercept = intercept

    def predict(self, context, model_input, params=None):
        return pd.DataFrame({"prediction": model_input["x"] * self.coefficient + self.intercept})


def package_digest(path):
    digest = hashlib.sha256()
    for item in sorted(path.rglob("*")):
        if item.is_file():
            digest.update(str(item.relative_to(path)).encode() + b"\0")
            digest.update(item.read_bytes())
    return digest.hexdigest()


def verify(output: Path, timeout=90):
    output = output.resolve()
    if not output.is_relative_to(ROOT / ".local"):
        raise ValueError(
            "Serving verification output must be under this checkout's .local directory"
        )
    if output.exists():
        raise ValueError("Use a fresh output directory to retain previous evidence")
    output.mkdir(parents=True, mode=0o700)
    environment = os.environ.copy()
    environment["PATH"] = (
        str(Path(sys.executable).parent) + os.pathsep + environment.get("PATH", "")
    )
    environment.update(
        MLFLOW_TRACKING_URI=(output / "tracking").as_uri(),
        MLFLOW_ENABLE_ASYNC_LOGGING="false",
        MLFLOW_DISABLE_TELEMETRY="true",
        MLFLOW_DISABLE_AGENT_HINT="1",
    )
    os.environ.update(
        {key: environment[key] for key in ["MLFLOW_TRACKING_URI", "MLFLOW_DISABLE_AGENT_HINT"]}
    )
    inputs = pd.DataFrame({"x": [0.0, 1.0, 3.0]})
    packages = {}
    for version, coefficient, intercept in [("v1", 1.5, -0.4), ("v2", 2.0, 0.2)]:
        model = LinearFixture(coefficient, intercept)
        expected = model.predict(None, inputs)
        path = output / version
        mlflow.pyfunc.save_model(
            path=str(path),
            python_model=model,
            signature=infer_signature(inputs, expected),
            input_example=inputs,
            pip_requirements=[
                f"mlflow=={mlflow.__version__}",
                f"numpy=={np.__version__}",
                f"pandas=={pd.__version__}",
            ],
        )
        packages[version] = {
            "path": path,
            "sha256": package_digest(path),
            "expected": expected["prediction"].tolist(),
        }
    events = []
    # Same endpoint is explicitly restarted with v1 -> v2 -> v1. This is not hot reload.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    for index, version in enumerate(["v1", "v2", "v1"]):
        package = packages[version]
        command = [
            sys.executable,
            "-m",
            "mlflow",
            "models",
            "serve",
            "--model-uri",
            str(package["path"]),
            "--env-manager",
            "local",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "1",
            "--timeout",
            "30",
        ]
        log_path = output / f"serve-{index}.log"
        descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as log:
            process = subprocess.Popen(
                command,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            try:
                deadline = time.monotonic() + timeout
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(f"MLflow serving exited; inspect private {log_path}")
                    try:
                        if requests.get(endpoint + "/ping", timeout=1).status_code == 200:
                            break
                    except requests.RequestException:
                        pass
                    if time.monotonic() > deadline:
                        raise TimeoutError("Serving readiness deadline exceeded")
                    time.sleep(0.25)
                response = requests.post(
                    endpoint + "/invocations",
                    timeout=10,
                    json={"dataframe_split": inputs.to_dict(orient="split")},
                )
                response.raise_for_status()
                actual = [row["prediction"] for row in response.json()["predictions"]]
                np.testing.assert_allclose(actual, package["expected"], atol=1e-12, rtol=0)
                invalid = requests.post(
                    endpoint + "/invocations",
                    timeout=10,
                    json={"dataframe_split": {"columns": ["wrong"], "data": [[1.0]]}},
                )
                if invalid.status_code != 400:
                    raise ValueError("Missing required signature column was not rejected")
                if package_digest(package["path"]) != package["sha256"]:
                    raise ValueError("Serving mutated the model package")
                events.append(
                    {
                        "selected_fixture_version": version,
                        "model_package_sha256": package["sha256"],
                        "health_status": 200,
                        "prediction_rows": len(actual),
                        "schema_rejection_status": invalid.status_code,
                        "prediction_parity": True,
                    }
                )
            finally:
                # MLflow starts a child server; terminate this isolated process group only.
                import signal

                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
                # The MLflow parent can exit before its uvicorn child finishes.
                # Never let the next version's readiness probe hit the prior server.
                deadline = time.monotonic() + 15
                while True:
                    with socket.socket() as probe:
                        probe.settimeout(0.25)
                        listening = probe.connect_ex(("127.0.0.1", port)) == 0
                    if not listening:
                        break
                    if time.monotonic() > deadline:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        raise TimeoutError("Prior serving process did not release its endpoint")
                    time.sleep(0.1)
    receipt = {
        "schema_version": 1,
        "status": "verified",
        "run_kind": "infrastructure_fixture",
        "engine": "MLflow Model Serving",
        "mlflow_version": mlflow.__version__,
        "environment": {
            "python": sys.version.split()[0],
            "numpy": np.__version__,
            "pandas": pd.__version__,
        },
        "events": events,
        "restart_based_version_switch_and_rollback": True,
        "zero_downtime_claimed": False,
        "container_image_verified": False,
        "production_ingress_verified": False,
        "shared_services_modified": False,
    }
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    verify(args.output)


if __name__ == "__main__":
    main()
