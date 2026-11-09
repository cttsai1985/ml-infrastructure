"""Verify client-owned MLflow packages through an isolated HTTP canary.

The controller uses only the standard library. MLflow and model dependencies belong
to the explicitly selected client Python environment, not this process.
"""

import hashlib
import json
import math
import os
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path


def package_digest(path: Path) -> str:
    """Hash relative paths and bytes in sorted order; reject external file links."""
    path = Path(path)
    if not path.is_dir() or path.is_symlink() or not (path / "MLmodel").is_file():
        raise ValueError("Supply an MLflow Model directory containing MLmodel")
    digest = hashlib.sha256()
    for item in sorted(path.rglob("*")):
        if item.is_symlink() or not (item.is_file() or item.is_dir()):
            raise ValueError("Model packages must contain regular files and directories only")
        if item.is_file():
            digest.update(str(item.relative_to(path)).encode() + b"\0")
            with item.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
    return digest.hexdigest()


def private_json(path: Path, value) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def compare_predictions(actual, expected, *, atol: float, rtol: float) -> dict:
    """Compare shape, keys, finite numeric values and nonnumeric labels exactly."""
    count, maximum = 0, 0.0

    def compare(left, right):
        nonlocal count, maximum
        if isinstance(right, dict):
            if not isinstance(left, dict) or left.keys() != right.keys():
                raise ValueError("Prediction fields differ from expected output")
            for key in right:
                compare(left[key], right[key])
        elif isinstance(right, list):
            if not isinstance(left, list) or len(left) != len(right):
                raise ValueError("Prediction shape differs from expected output")
            for left_item, right_item in zip(left, right, strict=True):
                compare(left_item, right_item)
        elif isinstance(right, (int, float)) and not isinstance(right, bool):
            if not isinstance(left, (int, float)) or isinstance(left, bool):
                raise ValueError("Prediction type differs from expected output")
            if not math.isfinite(left) or not math.isfinite(right):
                raise ValueError("Predictions must be finite")
            difference = abs(left - right)
            count += 1
            maximum = max(maximum, difference)
            if difference > atol + rtol * abs(right):
                raise ValueError("Prediction parity tolerance exceeded")
        elif type(left) is not type(right) or left != right:
            raise ValueError("Prediction labels differ from expected output")

    compare(actual, expected)
    if count == 0:
        raise ValueError("Expected predictions must contain numeric model outputs")
    return {"numeric_values": count, "maximum_absolute_error": maximum, "atol": atol, "rtol": rtol}


def request_json(endpoint: str, payload=None, *, timeout=10):
    data = None if payload is None else json.dumps(payload, allow_nan=False).encode()
    request = urllib.request.Request(
        endpoint, data=data, headers={"Content-Type": "application/json"}
    )
    # Canary requests stay on loopback regardless of the operator's HTTP proxy settings.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def port_listening(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.25)
        return probe.connect_ex(("127.0.0.1", port)) == 0


def stop_server(process, port: int, *, timeout=15) -> None:
    """Terminate only our new process group, including MLflow's child server."""

    def send(sig):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass

    send(signal.SIGTERM)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        parent_done = process.poll() is not None
        if parent_done and not port_listening(port):
            # A child may remain after closing its socket; finish the owned group.
            send(signal.SIGKILL)
            process.wait(timeout=5)
            return
        time.sleep(0.1)
    send(signal.SIGKILL)
    process.wait(timeout=5)
    deadline = time.monotonic() + 5
    while port_listening(port):
        if time.monotonic() >= deadline:
            raise TimeoutError("Canary process group did not release its endpoint")
        time.sleep(0.1)


def runtime_environment(python: Path, workspace: Path) -> dict:
    environment = os.environ.copy()
    for key in list(environment):
        if key.startswith(("MLFLOW_", "PREFECT_", "AWS_")) or key in {
            "PYTHONPATH",
            "PYTHONHOME",
            "VIRTUAL_ENV",
        }:
            environment.pop(key)
    environment.update(
        PATH=str(python.parent) + os.pathsep + environment.get("PATH", ""),
        MLFLOW_TRACKING_URI=(workspace / "tracking").as_uri(),
        MLFLOW_ENABLE_ASYNC_LOGGING="false",
        MLFLOW_DISABLE_TELEMETRY="true",
        MLFLOW_DISABLE_AGENT_HINT="1",
        PYTHONDONTWRITEBYTECODE="1",
        HF_HUB_OFFLINE="1",
        TRANSFORMERS_OFFLINE="1",
        OMP_NUM_THREADS="2",
        OPENBLAS_NUM_THREADS="2",
        MKL_NUM_THREADS="2",
        NUMEXPR_NUM_THREADS="2",
    )
    return environment


def runtime_json(python: Path, code: str, arguments, environment, cwd: Path):
    result = subprocess.run(
        [str(python), "-c", code, *map(str, arguments)],
        env=environment,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode:
        descriptor = os.open(
            cwd.parent / "runtime-error.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        with os.fdopen(descriptor, "w") as log:
            log.write(result.stderr)
        raise RuntimeError("Selected model runtime failed package or expected-output validation")
    return json.loads(result.stdout)


def verify_model(
    model: Path,
    runtime_python: Path,
    request: Path,
    expected: Path,
    output: Path,
    *,
    expected_digest: str | None = None,
    invalid_request: Path | None = None,
    atol=1e-6,
    rtol=1e-5,
    timeout=120,
    inference_timeout=60,
    port: int | None = None,
) -> dict:
    """Copy, validate, serve, compare and stop one trusted client model package.

    Expected JSON is the response's ``predictions`` value or an object wrapping
    that value. NPY files contain the equivalent numeric array. Request files are
    private MLflow invocation envelopes. No rows are copied into the receipt.
    """
    if os.name != "posix":
        raise ValueError("Canary process-group management requires Linux or another POSIX host")
    if any(not math.isfinite(value) or value < 0 for value in [atol, rtol]):
        raise ValueError("Prediction tolerances must be finite and nonnegative")
    if any(not math.isfinite(value) or value <= 0 for value in [timeout, inference_timeout]):
        raise ValueError("Canary deadlines must be finite and positive")
    model, request, expected, output = map(
        lambda p: Path(p).absolute(), [model, request, expected, output]
    )
    # Do not resolve the interpreter symlink: its venv path determines Python's environment.
    runtime_python = Path(runtime_python).absolute()
    if not runtime_python.is_file() or not os.access(runtime_python, os.X_OK):
        raise ValueError("Supply an executable Python from the model's environment")
    digest = package_digest(model)
    if expected_digest is not None and expected_digest != digest:
        raise ValueError("Source model package differs from the declared digest")
    if output.resolve().is_relative_to(model.resolve()) or model.resolve().is_relative_to(
        output.resolve()
    ):
        raise ValueError("Canary output and model package directories must be separate")
    if output.exists() or output.is_symlink():
        raise ValueError("Use a fresh canary output directory")
    request_bytes, expected_bytes = request.read_bytes(), expected.read_bytes()
    payload = json.loads(request_bytes)
    invalid = (
        json.loads(Path(invalid_request).read_text())
        if invalid_request is not None
        else {"invalid_canary_envelope": []}
    )
    output.mkdir(parents=True, mode=0o700)
    workspace = output / "workspace"
    workspace.mkdir(mode=0o700)
    copied = workspace / "model"
    environment = runtime_environment(runtime_python, workspace)
    receipt = {
        "schema_version": 1,
        "deployment_id": "canary-" + uuid.uuid4().hex,
        "scope": "canary",
        "status": "failed",
        "engine": "MLflow Model Serving",
        "started_at": datetime.now(UTC).isoformat(),
        "model_package_sha256": digest,
        "declared_digest_checked": expected_digest is not None,
        "runtime_kind": "client_python_environment",
        "runtime_python": str(runtime_python),
        "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
        "expected_sha256": hashlib.sha256(expected_bytes).hexdigest(),
        "controller_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "production_ingress_verified": False,
        "container_image_verified": False,
        "zero_downtime_claimed": False,
        "shared_services_modified": False,
    }
    process, failure, phase = None, None, "package_validation"
    try:
        shutil.copytree(model, copied)
        if package_digest(copied) != digest:
            raise ValueError("Copied model package differs from its source digest")
        metadata = runtime_json(
            runtime_python,
            """
import importlib.metadata, json, sys
from mlflow.models import Model
model = Model.load(sys.argv[1])
if ('python_function' not in model.flavors or not model.signature
        or not model.signature.inputs or not model.signature.outputs):
    raise ValueError('A pyfunc flavor and input/output signature are required')
packages = sorted({(d.metadata['Name'] or '').lower(): d.version
    for d in importlib.metadata.distributions()}.items())
print(json.dumps({'python': sys.version.split()[0],
    'mlflow': importlib.metadata.version('mlflow'), 'packages': packages,
    'signature': model.signature.to_dict()}))
""",
            [copied],
            environment,
            workspace,
        )
        receipt["runtime"] = {key: metadata[key] for key in ["python", "mlflow"]}
        receipt["runtime_packages_sha256"] = hashlib.sha256(
            json.dumps(metadata["packages"], sort_keys=True).encode()
        ).hexdigest()
        private_json(output / "runtime.json", metadata)
        if expected.suffix == ".npy":
            copied_expected = workspace / "expected.npy"
            descriptor = os.open(copied_expected, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(expected_bytes)
        values = (
            runtime_json(
                runtime_python,
                "import json,numpy as np,sys; "
                "print(json.dumps(np.load(sys.argv[1], allow_pickle=False).tolist(), "
                "allow_nan=False))",
                [copied_expected],
                environment,
                workspace,
            )
            if expected.suffix == ".npy"
            else json.loads(expected_bytes)
        )
        if isinstance(values, dict) and "predictions" in values:
            values = values["predictions"]
        compare_predictions(values, values, atol=atol, rtol=rtol)
        with socket.socket() as reservation:
            reservation.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            reservation.bind(("127.0.0.1", port or 0))
            port = reservation.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}"
        receipt["endpoint"] = endpoint
        command = [
            str(runtime_python),
            "-m",
            "mlflow",
            "models",
            "serve",
            "--model-uri",
            str(copied),
            "--env-manager",
            "local",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "1",
            "--timeout",
            str(inference_timeout),
        ]
        receipt["command"] = command
        phase = "startup"
        descriptor = os.open(output / "server.log", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as log:
            process = subprocess.Popen(
                command,
                cwd=workspace,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        deadline = time.monotonic() + timeout
        while True:
            if process.poll() is not None:
                raise RuntimeError("MLflow serving exited; inspect the private server.log")
            try:
                if request_json(endpoint + "/ping", timeout=1)[0] == 200:
                    break
            except (OSError, urllib.error.URLError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError("Model readiness deadline exceeded")
            time.sleep(0.25)
        receipt["health_status"] = 200
        phase = "prediction"
        status, body = request_json(endpoint + "/invocations", payload, timeout=inference_timeout)
        if status != 200:
            raise ValueError("Model inference returned a non-success HTTP status")
        actual = json.loads(body)["predictions"]
        receipt["parity"] = compare_predictions(actual, values, atol=atol, rtol=rtol)
        receipt["prediction_rows"] = len(actual) if isinstance(actual, list) else None
        phase = "schema_rejection"
        status, _ = request_json(endpoint + "/invocations", invalid, timeout=inference_timeout)
        if status != 400:
            raise ValueError("Invalid inference request was not rejected with HTTP 400")
        receipt["schema_rejection"] = {
            "status": status,
            "scope": "client_supplied_invalid_request" if invalid_request else "serving_envelope",
        }
        phase = "package_integrity"
        if package_digest(copied) != digest or package_digest(model) != digest:
            raise ValueError("Model package changed during canary verification")
        receipt["prediction_parity"] = True
        receipt["package_integrity_verified"] = True
        receipt["status"] = "verified"
    except BaseException as error:
        failure = error
        receipt.update(status="failed", failed_phase=phase, error_type=type(error).__name__)
    finally:
        try:
            if process is not None:
                stop_server(process, port)
            receipt["cleanup_complete"] = True
        except BaseException as error:
            receipt.update(
                status="failed", cleanup_complete=False, cleanup_error=type(error).__name__
            )
            if failure is None:
                failure = error
        receipt["finished_at"] = datetime.now(UTC).isoformat()
        private_json(output / "receipt.json", receipt)
    if failure is not None:
        raise failure
    return receipt


def cli(argv=None):
    import argparse

    parser = argparse.ArgumentParser(prog="./infra model verify", description=__doc__)
    for name in ["model", "python", "request", "expected", "output"]:
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--digest", help="Expected source package SHA256")
    parser.add_argument("--invalid-request", type=Path)
    parser.add_argument("--atol", type=float, default=1e-6)
    parser.add_argument("--rtol", type=float, default=1e-5)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--inference-timeout", type=int, default=60)
    args = parser.parse_args(argv)
    receipt = verify_model(
        args.model,
        args.python,
        args.request,
        args.expected,
        args.output,
        expected_digest=args.digest,
        invalid_request=args.invalid_request,
        atol=args.atol,
        rtol=args.rtol,
        timeout=args.timeout,
        inference_timeout=args.inference_timeout,
    )
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    cli()
