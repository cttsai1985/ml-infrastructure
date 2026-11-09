#!/usr/bin/env python3
"""Operate only explicitly named platform stacks; smoke stacks are disposable."""

import argparse
import hashlib
import json
import os
import re
import secrets
import socket
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def secure_write(path: Path, content: str) -> None:
    if path.exists() or path.is_symlink():
        raise ValueError(f"Refusing to replace existing state: {path.name}")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(content)


def initialize(state: Path, project: str, mlflow_port: int, prefect_port: int) -> None:
    if not re.fullmatch(r"mlops-platform(?:-[a-z0-9-]+)?", project):
        raise ValueError("Compose project must be in the mlops-platform namespace")
    state = state.resolve()
    if not state.is_relative_to(ROOT / ".local"):
        raise ValueError("Platform state must live under this repository's .local directory")
    if state.exists():
        raise ValueError("State already exists; reuse it rather than rotating live credentials")
    if (
        not all(1024 <= port <= 65535 for port in [mlflow_port, prefect_port])
        or mlflow_port == prefect_port
    ):
        raise ValueError("Ports must be distinct unprivileged TCP ports")
    reject_duplicate_identity(state, project)
    state.mkdir(parents=True, mode=0o700)
    secret_dir = state / "secrets"
    secret_dir.mkdir(mode=0o700)
    (state / "receipts").mkdir(mode=0o700)
    for name in [
        "postgres_password",
        "mlflow_db_password",
        "prefect_db_password",
        "mlflow_password",
        "flask_secret",
    ]:
        secure_write(secret_dir / name, secrets.token_hex(32))
    secure_write(secret_dir / "prefect_auth_string", "platform:" + secrets.token_hex(32))
    storage = {
        "identities": [
            {
                "name": "mlflow",
                "credentials": [
                    {"accessKey": secrets.token_hex(16), "secretKey": secrets.token_hex(32)}
                ],
                "actions": [
                    "Read:mlops-artifacts",
                    "Write:mlops-artifacts",
                    "List:mlops-artifacts",
                    "Admin:mlops-artifacts",
                ],
            }
        ]
    }
    secure_write(secret_dir / "seaweed_s3_config.json", json.dumps(storage))
    network = (
        project + "_clients"
        if project != "mlops-platform-development"
        else "mlops-platform_clients"
    )
    secure_write(
        state / "compose.env",
        f"PLATFORM_SECRET_DIR={secret_dir}\n"
        f"PLATFORM_RECEIPT_DIR={state / 'receipts'}\n"
        f"MLFLOW_PORT={mlflow_port}\nPREFECT_PORT={prefect_port}\n"
        f"MLOPS_NETWORK_NAME={network}\n",
    )
    secure_write(
        state / "identity.json",
        json.dumps({"project": project, "mlflow_port": mlflow_port, "prefect_port": prefect_port}),
    )


def reject_duplicate_identity(state: Path, project: str) -> None:
    for path in (ROOT / ".local").glob("**/identity.json"):
        if path.parent.resolve() == state.resolve():
            continue
        try:
            identity = json.loads(path.read_text())
        except (ValueError, OSError):
            continue
        if identity.get("project") == project:
            raise ValueError("Another state directory already owns this Compose project")


def state_fingerprint(state: Path) -> str:
    return hashlib.sha256((state / "secrets/flask_secret").read_bytes()).hexdigest()


def discovered_resources(project: str) -> dict[str, set[str]]:
    resources = {}
    for kind, listing in [("container", ["ps", "--all"]), ("volume", ["volume", "ls"])]:
        result = subprocess.run(
            [
                "docker",
                *listing,
                "--filter",
                f"label=com.docker.compose.project={project}",
                "--format",
                "{{.ID}}" if kind == "container" else "{{.Name}}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        resources[kind] = set(result.stdout.splitlines())
    # Compose can reuse exact volume names even when project labels are absent/wrong.
    result = subprocess.run(
        ["docker", "volume", "ls", "--format", "{{.Name}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    resources["volume"].update(
        set(result.stdout.splitlines()) & {project + "_postgres_data", project + "_seaweed_data"}
    )
    return resources


def verify_resource_ownership(state: Path) -> None:
    identity = validate_state(state)
    reject_duplicate_identity(state, identity["project"])
    expected = state_fingerprint(state)
    for kind, resources in discovered_resources(identity["project"]).items():
        for resource in resources:
            inspected = subprocess.run(
                [
                    "docker",
                    kind,
                    "inspect",
                    resource,
                    "--format",
                    "{{json .Config.Labels}}" if kind == "container" else "{{json .Labels}}",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            labels = json.loads(inspected.stdout) or {}
            if labels.get("io.applied-ml.state") != expected:
                raise ValueError(
                    "Deployment resources belong to different or unlabelled state; "
                    "review adoption and restore the matching state before starting"
                )


def validate_state(state: Path) -> dict:
    """Read an existing deployment without rotating or repairing its credentials."""
    state = state.resolve()
    if not state.is_relative_to(ROOT / ".local"):
        raise ValueError("Invalid state location")
    if state.stat().st_mode & 0o077:
        raise ValueError("State directory must be private")
    identity = json.loads((state / "identity.json").read_text())
    if not re.fullmatch(r"mlops-platform(?:-[a-z0-9-]+)?", identity["project"]):
        raise ValueError("Refusing to operate another Compose project")
    ports = [identity["mlflow_port"], identity["prefect_port"]]
    if (
        any(type(port) is not int or not 1024 <= port <= 65535 for port in ports)
        or ports[0] == ports[1]
    ):
        raise ValueError("Invalid stored service ports")
    secret_dir = state / "secrets"
    if secret_dir.is_symlink() or secret_dir.stat().st_mode & 0o077:
        raise ValueError("Secret directory must be private and not a symlink")
    names = [
        "postgres_password",
        "mlflow_db_password",
        "prefect_db_password",
        "mlflow_password",
        "flask_secret",
        "prefect_auth_string",
        "seaweed_s3_config.json",
    ]
    for name in ["identity.json", "compose.env", *["secrets/" + name for name in names]]:
        path = state / name
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
            raise ValueError(f"Missing or insecure state file: {name}")
        if not path.read_text().strip():
            raise ValueError(f"Empty state file: {name}")
    # Retain the network recorded by old deployments; new named stacks are scoped.
    lines = (state / "compose.env").read_text().splitlines()
    stored = dict(line.split("=", 1) for line in lines)
    network = stored.get("MLOPS_NETWORK_NAME")
    allowed_networks = {identity["project"] + "_clients"}
    if not identity["project"].startswith("mlops-platform-smoke-"):
        allowed_networks.add("mlops-platform_clients")
    if network not in allowed_networks:
        raise ValueError("Stored client network differs from deployment identity")
    expected = {
        "PLATFORM_SECRET_DIR": str(secret_dir),
        "PLATFORM_RECEIPT_DIR": str(state / "receipts"),
        "MLFLOW_PORT": str(ports[0]),
        "PREFECT_PORT": str(ports[1]),
        "MLOPS_NETWORK_NAME": network,
    }
    if len(lines) != len(expected) or dict(line.split("=", 1) for line in lines) != expected:
        raise ValueError("Stored Compose configuration differs from deployment identity")
    if not (state / "receipts").is_dir() or (state / "receipts").is_symlink():
        raise ValueError("Missing or invalid receipt directory")
    return identity


def compose(state: Path, *arguments, capture=False):
    state = state.resolve()
    if not state.is_relative_to(ROOT / ".local"):
        raise ValueError("Invalid state location")
    identity = validate_state(state)
    project = identity["project"]
    command = [
        "docker",
        "compose",
        "--project-name",
        project,
        "--env-file",
        str(state / "compose.env"),
        "--file",
        str(ROOT / "compose.yaml"),
        *arguments,
    ]
    # Shell variables must not override the selected state's paths, ports or network.
    environment = os.environ.copy()
    for key in [
        "PLATFORM_SECRET_DIR",
        "PLATFORM_RECEIPT_DIR",
        "MLFLOW_PORT",
        "PREFECT_PORT",
        "MLOPS_NETWORK_NAME",
        "COMPOSE_PROFILES",
    ]:
        environment.pop(key, None)
    environment["PLATFORM_STATE_FINGERPRINT"] = state_fingerprint(state)
    return subprocess.run(
        command, cwd=ROOT, env=environment, check=True, capture_output=capture, text=True
    )


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def smoke() -> Path:
    project = "mlops-platform-smoke-" + uuid.uuid4().hex[:12]
    state = ROOT / ".local" / project
    ports = [free_port(), free_port()]
    while ports[0] == ports[1]:
        ports[1] = free_port()
    initialize(state, project, *ports)
    try:
        main(["deploy", "--state", str(state)])
        compose(state, "run", "--rm", "client", "smoke")
        receipt = json.loads((state / "receipts" / "smoke.json").read_text())
        compose(
            state,
            "exec",
            "-T",
            "mlflow",
            "python",
            "docker/entrypoint.py",
            "storage-check",
            receipt["object_key"],
            receipt["artifact_sha256"],
        )
        # Recreate containers while retaining only this project's persistent volumes.
        compose(state, "down")
        main(["deploy", "--state", str(state)])
        compose(state, "run", "--rm", "client", "verify")
        compose(
            state,
            "exec",
            "-T",
            "mlflow",
            "python",
            "docker/entrypoint.py",
            "storage-check",
            receipt["object_key"],
            receipt["artifact_sha256"],
        )
    except Exception:
        result = compose(state, "logs", "--no-color", "--tail", "60", capture=True)
        secure_write(state / "failure.log", result.stdout)
        raise
    finally:
        # No shared project name, external volume, or general prune command is accepted.
        if not project.startswith("mlops-platform-smoke-"):
            raise RuntimeError("Refusing non-smoke volume removal")
        compose(state, "down", "--volumes", "--remove-orphans")
    receipt_path = state / "receipts" / "smoke.json"
    print(f"Verified isolated integration; receipt: {receipt_path}")
    return receipt_path


def reject_existing_resources(project: str) -> None:
    """Never generate new passwords against orphaned stateful Docker resources."""
    if not re.fullmatch(r"mlops-platform(?:-[a-z0-9-]+)?", project):
        raise ValueError("Compose project must be in the mlops-platform namespace")
    if any(discovered_resources(project).values()):
        raise ValueError(
            f"Existing resources for {project}; restore its original state "
            "instead of generating credentials"
        )
    result = subprocess.run(
        [
            "docker",
            "network",
            "ls",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{.Name}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout.strip():
        raise ValueError("Existing deployment networks; restore the original state")


def deployment_summary(state: Path) -> None:
    identity = validate_state(state)
    print(f"Deployment: {identity['project']}")
    print(f"Checkout: {ROOT}")
    print(f"State: {state.resolve()}")
    print(f"MLflow: http://127.0.0.1:{identity['mlflow_port']}")
    print(f"Prefect: http://127.0.0.1:{identity['prefect_port']}")
    print(
        f"MLflow username: platform; password file: {state.resolve() / 'secrets/mlflow_password'}"
    )
    print(f"Prefect authentication file: {state.resolve() / 'secrets/prefect_auth_string'}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="./infra", description="Operate this checkout's local Docker Compose deployment."
    )
    commands = parser.add_subparsers(dest="action", required=True)
    descriptions = {
        "deploy": "Create or reuse protected state, validate and start services",
        "status": "Show services in the selected deployment",
        "logs": "Show service logs (may contain sensitive diagnostics)",
        "stop": "Stop the selected deployment, preserving its data volumes",
        "check": "Validate protected state and Compose configuration without starting services",
        "init": "Create new protected state without starting services (optional)",
        "verify": "Run and remove an isolated integration deployment",
    }
    aliases = {"stop": ["down"], "check": ["config"], "verify": ["smoke"]}
    for action, description in descriptions.items():
        command = commands.add_parser(
            action, help=description, description=description, aliases=aliases.get(action, [])
        )
        if action != "verify":
            command.add_argument("--state", type=Path, default=ROOT / ".local" / "development")
        if action in {"init", "deploy"}:
            command.add_argument(
                "--project", help="Compose identity for NEW state; existing identity must match"
            )
            command.add_argument(
                "--mlflow-port", type=int, help="MLflow port for NEW state (default: 5501)"
            )
            command.add_argument(
                "--prefect-port", type=int, help="Prefect port for NEW state (default: 4201)"
            )
        if action == "logs":
            command.add_argument(
                "service", nargs="?", choices=["postgres", "seaweed", "mlflow", "prefect", "worker"]
            )
            command.add_argument("--tail", type=int, default=100)
            command.add_argument("--follow", action="store_true")
    # Keep the old up action as a start-only compatibility path; it never initializes state.
    command = commands.add_parser(
        "up", help="Start existing state without initialization (compatibility)"
    )
    command.add_argument("--state", type=Path, default=ROOT / ".local" / "development")
    args = parser.parse_args(argv)
    if args.action in {"verify", "smoke"}:
        smoke()
        return
    if not args.state.is_absolute():
        args.state = ROOT / args.state
    if args.action == "init":
        reject_existing_resources(args.project or "mlops-platform-development")
        initialize(
            args.state,
            args.project or "mlops-platform-development",
            args.mlflow_port if args.mlflow_port is not None else 5501,
            args.prefect_port if args.prefect_port is not None else 4201,
        )
        deployment_summary(args.state)
        return
    if args.action == "deploy":
        if args.state.exists() or args.state.is_symlink():
            identity = validate_state(args.state)
            for key in ["project", "mlflow_port", "prefect_port"]:
                if getattr(args, key) is not None and getattr(args, key) != identity[key]:
                    raise ValueError(
                        f"Requested {key} differs from existing state; use its recorded identity"
                    )
        subprocess.run(["docker", "compose", "version"], check=True, capture_output=True, text=True)
        subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            check=True,
            capture_output=True,
            text=True,
        )
        if not args.state.exists():
            reject_existing_resources(args.project or "mlops-platform-development")
            initialize(
                args.state,
                args.project or "mlops-platform-development",
                args.mlflow_port if args.mlflow_port is not None else 5501,
                args.prefect_port if args.prefect_port is not None else 4201,
            )
        verify_resource_ownership(args.state)
        compose(args.state, "config", "--quiet")
        compose(args.state, "up", "-d", "--build", "--wait", "--wait-timeout", "240")
        deployment_summary(args.state)
    elif args.action == "logs":
        if args.tail < 1:
            raise ValueError("Log tail must be positive")
        arguments = ["logs", "--tail", str(args.tail)]
        if args.follow:
            arguments.append("--follow")
        if args.service:
            arguments.append(args.service)
        compose(args.state, *arguments)
    else:
        actions = {
            "config": ["config", "--quiet"],
            "check": ["config", "--quiet"],
            "up": ["up", "-d", "--build", "--wait"],
            "down": ["down"],
            "stop": ["down"],
            "status": ["ps"],
        }
        if args.action in {"up", "stop", "down"}:
            verify_resource_ownership(args.state)
        compose(args.state, *actions[args.action])
        if args.action == "status":
            deployment_summary(args.state)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        print(f"Infrastructure operation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
