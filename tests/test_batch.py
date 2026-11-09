import copy
import json
import subprocess

import pytest

from mlops_platform import batch


def config():
    return {
        "schema_version": 1,
        "project_id": "iceberg",
        "owner": "repository maintainer",
        "image": "sha256:" + "a" * 64,
        "input_files": ["train.json"],
        "stages": [
            {"name": "fit", "command": ["train"], "outputs": ["model.json"]},
            {
                "name": "gate",
                "kind": "gate",
                "checks": [{"path": "model.json", "metric": "loss", "max": 0.3}],
            },
            {"name": "publish", "command": ["track"], "publishes_candidate": True},
        ],
    }


def test_training_retry_and_ungated_publication_are_rejected():
    cfg = config()
    batch.validate_config(cfg)
    cfg["stages"][0]["retries"] = 1
    with pytest.raises(ValueError, match="read-only"):
        batch.validate_config(cfg)
    cfg = config()
    cfg["stages"] = [cfg["stages"][-1], *cfg["stages"][:-1]]
    with pytest.raises(ValueError, match="earlier quality gate"):
        batch.validate_config(cfg)


def test_resume_reuses_fit_but_rejects_tampered_model(tmp_path, monkeypatch):
    data = tmp_path / "data"
    output = tmp_path / "out"
    state = output / ".batch-ops"
    data.mkdir()
    state.mkdir(parents=True)
    cfg = config()
    calls = []

    def fake_container(*arguments):
        calls.append(arguments[0]["name"])
        (output / "model.json").write_text('{"loss": 0.2}')
        return {}

    monkeypatch.setattr(batch, "container", fake_container)
    first = batch.run_stage.fn(cfg["stages"][0], cfg, data, output, state, "flow1")
    second = batch.run_stage.fn(cfg["stages"][0], cfg, data, output, state, "flow2")
    assert first == second
    assert calls == ["fit"]
    (output / "model.json").write_text('{"loss": 0.1}')
    with pytest.raises(ValueError, match="changed"):
        batch.run_stage.fn(cfg["stages"][0], cfg, data, output, state, "flow3")


def test_failed_gate_records_observed_value_and_blocks_publication(tmp_path, monkeypatch):
    cfg = config()
    output = tmp_path / "out"
    state = output / ".batch-ops"
    state.mkdir(parents=True)
    (output / "model.json").write_text('{"loss": 0.5}')
    cfg["stages"] = cfg["stages"][1:]
    calls = []
    real = batch.run_stage.fn

    class LocalTask:
        def with_options(self, **kwargs):
            def invoke(*arguments):
                calls.append(arguments[0]["name"])
                return real(*arguments)

            return invoke

    monkeypatch.setattr(batch, "run_stage", LocalTask())
    with pytest.raises(batch.GateRejected):
        batch.batch_flow.fn(cfg, tmp_path, output)
    assert calls == ["gate"]
    receipt = json.loads((state / "gate.json").read_text())
    assert receipt["checks"][0]["observed"] == 0.5
    incident = json.loads((state / "incident.json").read_text())
    assert incident["failed_stage"] == "gate"
    assert incident["owner"] == cfg["owner"]


def test_quality_gate_resume_rechecks_metric_fingerprint(tmp_path):
    state = tmp_path / ".batch-ops"
    state.mkdir()
    (tmp_path / "model.json").write_text('{"loss": 0.2}')
    cfg = config()
    stage = cfg["stages"][1]
    batch.run_stage.fn(stage, cfg, tmp_path, tmp_path, state, "first")
    (tmp_path / "model.json").write_text('{"loss": 0.9}')
    with pytest.raises(ValueError, match="changed"):
        batch.run_stage.fn(stage, cfg, tmp_path, tmp_path, state, "second")


def test_resume_refuses_changed_data_or_image(tmp_path, monkeypatch):
    monkeypatch.setenv("PREFECT_API_URL", "http://approved.invalid/api")
    data = tmp_path / "data"
    output = tmp_path / "output"
    data.mkdir()
    (data / "train.json").write_text("original")

    class LocalFlow:
        def with_options(self, **kwargs):
            def invoke(*args):
                from prefect.settings import get_current_settings

                settings = get_current_settings()
                assert settings.api.url == "http://approved.invalid/api"
                assert batch.PREFECT_SERVER_ALLOW_EPHEMERAL_MODE.value() is False
                return {"status": "completed"}

            return invoke

    monkeypatch.setattr(batch, "batch_flow", LocalFlow())
    batch.execute(config(), data, output)
    changed = copy.deepcopy(config())
    changed["image"] = "sha256:" + "b" * 64
    with pytest.raises(ValueError, match="identity changed"):
        batch.execute(changed, data, output)
    (data / "train.json").write_text("different")
    with pytest.raises(ValueError, match="identity changed"):
        batch.execute(config(), data, output)


def test_deadline_always_removes_exact_container_and_keeps_private_log(tmp_path, monkeypatch):
    calls = []

    def timed_out(arguments, **kwargs):
        calls.append(arguments)
        if arguments[1] == "run":
            raise subprocess.TimeoutExpired(arguments, 1)
        return subprocess.CompletedProcess(arguments, 0)

    monkeypatch.setattr(batch.subprocess, "run", timed_out)
    stage = {"name": "timeout", "command": ["train"], "timeout_seconds": 1}
    with pytest.raises(subprocess.TimeoutExpired):
        batch.container(stage, config(), tmp_path, tmp_path, tmp_path, 1, "flow")
    name = calls[0][calls[0].index("--name") + 1]
    assert calls[1] == ["docker", "rm", "-f", name]
    assert (tmp_path / "timeout-attempt-1.log").stat().st_mode & 0o777 == 0o600


def test_gate_rejects_nonfinite_and_path_escape(tmp_path):
    (tmp_path / "metric.json").write_text('{"loss": NaN}')
    with pytest.raises(batch.GateRejected):
        batch.gate(tmp_path, [{"path": "metric.json", "metric": "loss", "max": 0.3}])
    with pytest.raises(ValueError, match="escapes"):
        batch.fingerprints(tmp_path, ["../outside"])


def test_live_readiness_without_artifacts_is_not_permanently_cached(tmp_path, monkeypatch):
    state = tmp_path / ".batch-ops"
    state.mkdir()
    calls = []
    monkeypatch.setattr(batch, "container", lambda *args: calls.append(1) or {})
    cfg = config()
    stage = {"name": "readiness", "command": ["check"], "read_only": True}
    batch.run_stage.fn(stage, cfg, tmp_path, tmp_path, state, "first")
    batch.run_stage.fn(stage, cfg, tmp_path, tmp_path, state, "second")
    assert len(calls) == 2
