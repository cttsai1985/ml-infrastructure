import copy
import importlib.util
import json
import stat
from pathlib import Path

import pytest
import yaml

from mlops_platform.registry import load_registry

ROOT = Path(__file__).resolve().parents[1]


def load_script(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_registry_identities_and_namespace_drift(tmp_path):
    registry = load_registry()
    assert set(registry["projects"]) == {
        "iceberg",
        "home-credit",
        "quest",
        "mercedes-benz",
        "quant-research",
    }
    assert registry["projects"]["quant-research"]["scope"] == "collection"
    altered = copy.deepcopy(registry)
    altered["projects"]["quest"]["artifact_namespace"] = "projects/iceberg"
    path = tmp_path / "projects.json"
    path.write_text(json.dumps(altered))
    with pytest.raises(ValueError, match="drift"):
        load_registry(path)
    altered = copy.deepcopy(registry)
    altered["projects"]["quest"]["repository"] = altered["projects"]["iceberg"]["repository"]
    path.write_text(json.dumps(altered))
    with pytest.raises(ValueError, match="Duplicate"):
        load_registry(path)


def test_runtime_secrets_are_owner_only_and_never_overwritten(tmp_path, monkeypatch):
    manage = load_script("platform_manage", ROOT / "scripts/manage.py")
    monkeypatch.setattr(manage, "ROOT", tmp_path)
    state = tmp_path / ".local" / "test"
    manage.initialize(state, "mlops-platform-smoke-test", 5551, 4251)
    contents = {}
    for secret in (state / "secrets").iterdir():
        assert stat.S_IMODE(secret.stat().st_mode) == 0o600
        contents[secret.name] = secret.read_text()
    assert (
        "MLOPS_NETWORK_NAME=mlops-platform-smoke-test_clients"
        in (state / "compose.env").read_text()
    )
    with pytest.raises(ValueError, match="already exists"):
        manage.initialize(state, "mlops-platform-smoke-test", 5551, 4251)
    assert {p.name: p.read_text() for p in (state / "secrets").iterdir()} == contents
    with pytest.raises(ValueError, match="namespace"):
        manage.initialize(tmp_path / ".local" / "bad", "predictive-research", 5551, 4251)
    with pytest.raises(ValueError, match="under"):
        manage.initialize(tmp_path / "outside", "mlops-platform-test", 5551, 4251)
    link = state / "link"
    link.symlink_to(state / "identity.json")
    with pytest.raises(ValueError, match="replace"):
        manage.secure_write(link, "bad")


def test_compose_network_storage_and_resource_boundaries():
    config = yaml.safe_load((ROOT / "compose.yaml").read_text())
    services = config["services"]
    assert not services["postgres"].get("ports")
    assert not services["seaweed"].get("ports")
    assert config["networks"]["backend"]["internal"] is True
    assert "MLOPS_NETWORK_NAME" in config["networks"]["clients"]["name"]
    for name in ["client", "worker"]:
        assert services[name]["networks"] == ["clients"]
        assert "seaweed_s3_config" not in services[name]["secrets"]
        assert "postgres_password" not in services[name]["secrets"]
    for service in services.values():
        assert "container_name" not in service
        assert all(port.startswith("127.0.0.1:") for port in service.get("ports", []))
        assert all("docker.sock" not in volume for volume in service.get("volumes", []))
        assert "latest" not in service["image"]
    assert sum(float(service["cpus"]) for service in services.values()) <= 4
    assert sum(int(service["mem_limit"].removesuffix("m")) for service in services.values()) <= 8192
    assert all(not volume.get("external") for volume in config["volumes"].values())


def test_project_runner_uses_argument_vector_and_no_secret_values(tmp_path, monkeypatch):
    example = load_script("project_flow_example", ROOT / "examples/project_flow.py")
    data, output = tmp_path / "data", tmp_path / "output"
    data.mkdir()
    output.mkdir()
    monkeypatch.setenv("MLFLOW_TRACKING_PASSWORD", "test-secret-not-for-argv")
    command = ["python", "-m", "iceberg.cli", "train", "--run-dir", "/outputs/run with spaces"]
    image = "example/project@sha256:" + "a" * 64
    args = example.docker_command("iceberg", image, command, data, output, "approved-network", "id")
    assert args[-len(command) :] == command
    assert "MLFLOW_TRACKING_PASSWORD" in args
    assert "test-secret-not-for-argv" not in " ".join(args)
    assert any(part.endswith("dst=/data,readonly") for part in args)
    with pytest.raises(ValueError, match="digest"):
        example.docker_command("iceberg", "example:latest", command, data, output, "none", "id")
    local_image = "sha256:" + "b" * 64
    local = example.docker_command("iceberg", local_image, ["train"], data, output, "none", "id")
    assert local[-2:] == [local_image, "train"]
    with pytest.raises(ValueError, match="digest"):
        example.docker_command(
            "iceberg", "--privileged@sha256:" + "a" * 64, command, data, output, "none", "id"
        )
