"""Canary integrity, numerical acceptance, and owned-process cleanup guarantees."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from mlops_platform import serving


def model_files(tmp_path):
    model = tmp_path / "source" / "model"
    model.mkdir(parents=True)
    (model / "MLmodel").write_text("fixture metadata; real validation occurs in client runtime")
    (model / "weights.bin").write_bytes(b"preserved trained weights")
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"dataframe_split": {"columns": ["x"], "data": [[2.0]]}}))
    expected = tmp_path / "expected.json"
    expected.write_text(json.dumps({"predictions": [{"score": 4.0}]}))
    return model, Path(sys.executable), request, expected, tmp_path / "canary"


def test_bundle_digest_rejects_mutation_and_external_files(tmp_path):
    args = model_files(tmp_path)
    model = args[0]
    digest = serving.package_digest(model)
    (model / "weights.bin").write_bytes(b"different weights")
    with pytest.raises(ValueError, match="declared digest"):
        serving.verify_model(*args, expected_digest=digest)
    assert not args[-1].exists()
    (model / "external").symlink_to(args[2])
    with pytest.raises(ValueError, match="regular files"):
        serving.package_digest(model)
    (model / "external").unlink()
    (model / "MLmodel").unlink()
    with pytest.raises(ValueError, match="MLflow Model"):
        serving.package_digest(model)


def test_parity_rejects_shape_drift_nonfinite_and_out_of_tolerance():
    result = serving.compare_predictions([[1.0, 2.000001]], [[1.0, 2.0]], atol=1e-5, rtol=0)
    assert result["numeric_values"] == 2
    assert result["maximum_absolute_error"] < 1e-5
    for actual in [[[1.0]], [[1.0, 2.1]], [[1.0, float("nan")]], [[True, 2.0]]]:
        with pytest.raises(ValueError):
            serving.compare_predictions(actual, [[1.0, 2.0]], atol=1e-5, rtol=0)
    with pytest.raises(ValueError, match="fields"):
        serving.compare_predictions({"wrong": 1.0}, {"score": 1.0}, atol=0, rtol=0)


@pytest.mark.parametrize("inference_status", [200, 500])
def test_inference_uses_fresh_copy_and_always_cleans_up(tmp_path, monkeypatch, inference_status):
    args = model_files(tmp_path)
    metadata = {
        "python": "3.12",
        "mlflow": "3.18.0",
        "packages": [["mlflow", "3.18.0"]],
        "signature": {"inputs": "fixture", "outputs": "fixture"},
    }
    monkeypatch.setattr(serving, "runtime_json", lambda *a: metadata)
    process = SimpleNamespace(poll=lambda: None)
    launches, cleaned = [], []

    def launch(command, **kwargs):
        launches.append((command, kwargs))
        return process

    def request(endpoint, payload=None, **kwargs):
        if endpoint.endswith("/ping"):
            return 200, b""
        if "invalid_canary_envelope" in payload:
            return 400, b"private error details"
        return inference_status, b'{"predictions": [{"score": 4.0}]}'

    monkeypatch.setenv("MLFLOW_TRACKING_PASSWORD", "must-not-reach-canary")
    monkeypatch.setenv("PYTHONPATH", "/producer-only-code")
    monkeypatch.setattr(serving.subprocess, "Popen", launch)
    monkeypatch.setattr(serving, "request_json", request)
    monkeypatch.setattr(serving, "stop_server", lambda proc, port: cleaned.append(proc))
    if inference_status == 200:
        result = serving.verify_model(*args, expected_digest=serving.package_digest(args[0]))
        assert result["status"] == "verified"
        assert result["prediction_rows"] == 1
        assert result["parity"]["maximum_absolute_error"] == 0
    else:
        with pytest.raises(ValueError, match="non-success"):
            serving.verify_model(*args)
    assert cleaned == [process]
    command, options = launches[0]
    assert str(args[0]) not in command
    assert command[command.index("--model-uri") + 1] == str(args[-1] / "workspace/model")
    assert options["cwd"] == args[-1] / "workspace"
    assert "MLFLOW_TRACKING_PASSWORD" not in options["env"]
    assert "PYTHONPATH" not in options["env"]
    assert options["start_new_session"] is True
    receipt = json.loads((args[-1] / "receipt.json").read_text())
    assert receipt["cleanup_complete"] is True
    assert "predictions" not in receipt and "dataframe_split" not in receipt
    assert (args[-1] / "receipt.json").stat().st_mode & 0o077 == 0
    if inference_status == 500:
        assert receipt["status"] == "failed"
        assert receipt["failed_phase"] == "prediction"


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups")
def test_cleanup_kills_child_that_outlives_mlflow_parent():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    child = (
        "import http.server,signal;signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        f"http.server.HTTPServer(('127.0.0.1',{port}),http.server.BaseHTTPRequestHandler).serve_forever()"
    )
    parent = (
        "import subprocess,sys,time;"
        f"subprocess.Popen([sys.executable,'-c',{child!r}]);time.sleep(60)"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", parent],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while not serving.port_listening(port):
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.05)
        serving.stop_server(process, port, timeout=0.2)
        assert process.poll() is not None
        assert not serving.port_listening(port)
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
