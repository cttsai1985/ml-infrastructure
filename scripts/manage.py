#!/usr/bin/env python3
"""Operate only explicitly named platform stacks; smoke stacks are disposable."""

import argparse
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
        if project.startswith("mlops-platform-smoke-")
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


def compose(state: Path, *arguments, capture=False):
    state = state.resolve()
    if not state.is_relative_to(ROOT / ".local"):
        raise ValueError("Invalid state location")
    identity = json.loads((state / "identity.json").read_text())
    project = identity["project"]
    if not re.fullmatch(r"mlops-platform(?:-[a-z0-9-]+)?", project):
        raise ValueError("Refusing to operate another Compose project")
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
    return subprocess.run(command, cwd=ROOT, check=True, capture_output=capture, text=True)


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
        compose(state, "build", "mlflow")
        compose(state, "up", "-d", "--wait", "--wait-timeout", "240")
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
        compose(state, "up", "-d", "--wait", "--wait-timeout", "240")
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["init", "config", "up", "down", "status", "smoke"])
    parser.add_argument("--state", type=Path, default=ROOT / ".local" / "development")
    parser.add_argument("--project", default="mlops-platform-development")
    parser.add_argument("--mlflow-port", type=int, default=5501)
    parser.add_argument("--prefect-port", type=int, default=4201)
    args = parser.parse_args()
    if args.action == "init":
        initialize(args.state, args.project, args.mlflow_port, args.prefect_port)
        print(f"Initialized owner-only secrets at {args.state / 'secrets'}; no services started")
    elif args.action == "smoke":
        smoke()
    else:
        actions = {
            "config": ["config", "--quiet"],
            "up": ["up", "-d", "--build", "--wait"],
            "down": ["down"],
            "status": ["ps"],
        }
        compose(args.state, *actions[args.action])


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as error:
        print(f"Platform operation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
