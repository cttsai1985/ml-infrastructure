"""Operator guarantees: deployment identity is independent of the calling shell."""

import importlib.util
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def manager(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("operator_manage", ROOT / "scripts/manage.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    return module


def snapshot(state):
    return {
        str(p.relative_to(state)): (p.read_bytes(), p.stat().st_mode)
        for p in state.rglob("*")
        if p.is_file()
    }


def test_repeated_deploy_preserves_credentials_and_selected_identity(manager, monkeypatch, capsys):
    state = manager.ROOT / ".local" / "development"
    manager.initialize(state, "mlops-platform-development", 5501, 4201)
    before = snapshot(state)
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(manager.subprocess, "run", run)
    monkeypatch.setenv("PLATFORM_SECRET_DIR", "/unrelated/secrets")
    monkeypatch.setenv("MLOPS_NETWORK_NAME", "unrelated-network")
    monkeypatch.setenv("COMPOSE_PROFILES", "worker")
    for _ in range(2):
        manager.main(["deploy"])
    assert snapshot(state) == before
    compose_calls = [(argv, kw) for argv, kw in calls if "--project-name" in argv]
    assert len(compose_calls) == 4
    for argv, kw in compose_calls:
        assert argv[argv.index("--project-name") + 1] == "mlops-platform-development"
        assert argv[argv.index("--env-file") + 1] == str(state / "compose.env")
        assert all(
            key not in kw["env"]
            for key in ["PLATFORM_SECRET_DIR", "MLOPS_NETWORK_NAME", "COMPOSE_PROFILES"]
        )
    assert compose_calls[1][0][-6:] == ["up", "-d", "--build", "--wait", "--wait-timeout", "240"]
    output = capsys.readouterr().out
    assert "http://127.0.0.1:5501" in output
    assert (state / "secrets/mlflow_password").read_text() not in output


def test_partial_state_or_conflicting_identity_never_regenerates(manager, monkeypatch):
    state = manager.ROOT / ".local" / "development"
    manager.initialize(state, "mlops-platform-development", 5501, 4201)
    monkeypatch.setattr(
        manager.subprocess, "run", lambda *a, **k: pytest.fail("Docker must not run")
    )
    with pytest.raises(ValueError, match="differs"):
        manager.main(["deploy", "--mlflow-port", "9999"])
    secret = state / "secrets/mlflow_password"
    secret.unlink()
    before = snapshot(state)
    with pytest.raises(ValueError, match="state file"):
        manager.main(["deploy"])
    assert snapshot(state) == before
    assert not secret.exists()


def test_new_deployment_refuses_orphaned_volumes_before_generating_credentials(
    manager, monkeypatch
):
    def run(argv, **kwargs):
        return SimpleNamespace(
            stdout="orphaned_data" if argv[:3] == ["docker", "volume", "ls"] else ""
        )

    monkeypatch.setattr(manager.subprocess, "run", run)
    with pytest.raises(ValueError, match="restore its original state"):
        manager.main(["deploy"])
    assert not (manager.ROOT / ".local/development").exists()


def test_new_named_networks_isolated_and_legacy_state_reused(manager):
    for name in ["alpha", "beta"]:
        state = manager.ROOT / ".local" / name
        project = "mlops-platform-" + name
        manager.initialize(state, project, 5501, 4201)
        assert f"MLOPS_NETWORK_NAME={project}_clients" in (state / "compose.env").read_text()
        manager.validate_state(state)
    legacy = manager.ROOT / ".local/alpha/compose.env"
    legacy.write_text(
        legacy.read_text().replace("mlops-platform-alpha_clients", "mlops-platform_clients")
    )
    manager.validate_state(legacy.parent)


def test_stop_retains_volumes_and_logs_use_argument_vector(manager, monkeypatch):
    state = manager.ROOT / ".local/development"
    manager.initialize(state, "mlops-platform-development", 5501, 4201)
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(manager.subprocess, "run", run)
    manager.main(["stop"])
    assert calls[-1][-1] == "down"
    assert "--volumes" not in calls[-1]
    manager.main(["logs", "mlflow", "--tail", "25", "--follow"])
    assert calls[-1][-5:] == ["logs", "--tail", "25", "--follow", "mlflow"]


def test_launcher_help_runs_outside_checkout_without_python_packages(tmp_path):
    result = subprocess.run(
        [str(ROOT / "infra"), "--help"], cwd=tmp_path, capture_output=True, text=True, check=True
    )
    assert "deploy" in result.stdout
    assert "uv" not in result.stdout


def test_separately_prepared_state_cannot_rebind_same_project(manager):
    manager.initialize(manager.ROOT / ".local/a", "mlops-platform-example", 5501, 4201)
    with pytest.raises(ValueError, match="already owns"):
        manager.initialize(manager.ROOT / ".local/b", "mlops-platform-example", 5501, 4201)
    assert not (manager.ROOT / ".local/b").exists()


def test_existing_resources_require_matching_state_fingerprint(manager, monkeypatch):
    import json

    state = manager.ROOT / ".local/development"
    manager.initialize(state, "mlops-platform-development", 5501, 4201)
    labels = {}

    def run(argv, **kwargs):
        if argv[:3] == ["docker", "volume", "ls"]:
            return SimpleNamespace(stdout="persistent_data\n")
        if argv[:3] == ["docker", "volume", "inspect"]:
            return SimpleNamespace(stdout=json.dumps(labels))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(manager.subprocess, "run", run)
    with pytest.raises(ValueError, match="unlabelled state"):
        manager.verify_resource_ownership(state)
    labels["io.applied-ml.state"] = manager.state_fingerprint(state)
    manager.verify_resource_ownership(state)
    labels["io.applied-ml.state"] = "another-state"
    with pytest.raises(ValueError, match="different or unlabelled"):
        manager.verify_resource_ownership(state)


def test_foreign_state_cannot_stop_another_deployment(manager, monkeypatch):
    import json

    state = manager.ROOT / ".local/development"
    manager.initialize(state, "mlops-platform-development", 5501, 4201)
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        if argv[:3] == ["docker", "ps", "--all"]:
            return SimpleNamespace(stdout="another-container\n")
        if argv[:3] == ["docker", "container", "inspect"]:
            return SimpleNamespace(stdout=json.dumps({"io.applied-ml.state": "foreign-state"}))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(manager.subprocess, "run", run)
    with pytest.raises(ValueError, match="different or unlabelled"):
        manager.main(["stop"])
    assert not any("down" in argv for argv in calls)


def test_exact_unlabelled_volume_name_cannot_be_reused(manager, monkeypatch):
    state = manager.ROOT / ".local/development"
    manager.initialize(state, "mlops-platform-development", 5501, 4201)

    def run(argv, **kwargs):
        if argv == ["docker", "volume", "ls", "--format", "{{.Name}}"]:
            return SimpleNamespace(stdout="mlops-platform-development_postgres_data\n")
        if argv[:3] == ["docker", "volume", "inspect"]:
            return SimpleNamespace(stdout="{}")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(manager.subprocess, "run", run)
    with pytest.raises(ValueError, match="unlabelled state"):
        manager.verify_resource_ownership(state)
    with pytest.raises(ValueError, match="restore its original state"):
        manager.reject_existing_resources("mlops-platform-development")
