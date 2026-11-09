"""Opt-in host runner: a Prefect flow executes a digest-pinned project image.

The operator must explicitly run this on a host with Docker access. No server
container or default worker receives the Docker socket. This example is not a
claim that the competition benchmarks were executed through Prefect.
"""

import argparse
import json
import os
import re
import subprocess
from pathlib import Path

from prefect import flow, task
from prefect.runtime import flow_run

from mlops_platform.registry import validate_project_id


def docker_command(project_id, image, command, data, output, network, flow_id):
    validate_project_id(project_id)
    if not re.fullmatch(r"(?:[a-z0-9][a-z0-9._/:-]*@sha256:|sha256:)[0-9a-f]{64}", image):
        raise ValueError("Use an immutable Docker image digest")
    if not command or any(not isinstance(part, str) or not part for part in command):
        raise ValueError("Provide a nonempty command argument vector")
    data, output = Path(data).resolve(), Path(output).resolve()
    if not data.is_dir() or not output.is_dir() or data == output:
        raise ValueError("Use separate existing data and output directories")
    if any("," in str(path) for path in [data, output]):
        raise ValueError("Docker mount paths must not contain commas")
    args = [
        "docker",
        "run",
        "--rm",
        "--cpus",
        "2",
        "--memory",
        "4g",
        "--pids-limit",
        "256",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--network",
        network,
        "--label",
        f"mlops.project_id={project_id}",
        "--env",
        f"MLOPS_PROJECT_ID={project_id}",
        "--env",
        f"PREFECT_FLOW_RUN_ID={flow_id}",
        "--mount",
        f"type=bind,src={data},dst=/data,readonly",
        "--mount",
        f"type=bind,src={output},dst=/outputs",
    ]
    if network != "none":
        # Docker reads values from the runner environment; secrets do not enter argv.
        for name in ["MLFLOW_TRACKING_URI", "MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"]:
            if name in os.environ:
                args.extend(["--env", name])
    return [*args, image, *command]


@task(name="run-project-container", persist_result=False)
def run_container(project_id, image, command, data, output, network):
    args = docker_command(project_id, image, command, data, output, network, flow_run.id)
    # A failed training CLI raises CalledProcessError and fails the Prefect task/flow.
    subprocess.run(args, check=True)
    receipt = {
        "project_id": project_id,
        "image": image,
        "command": command,
        "prefect_flow_run_id": flow_run.id,
        "status": "container_completed",
    }
    (Path(output) / "orchestration.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


@flow(name="project-container", persist_result=False)
def project_flow(project_id, image, command, data, output, network="none"):
    return run_container(project_id, image, command, data, output, network)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--network", default="none")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--flow-name", required=True, help="Client-owned Prefect flow name")
    parser.add_argument("--deployment", help="Client-owned deployment name; required with --serve")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not os.environ.get("PREFECT_API_URL"):
        raise ValueError("Set the approved shared Prefect endpoint; no ephemeral server is started")
    values = {
        "project_id": args.project_id,
        "image": args.image,
        "command": args.command,
        "data": str(args.data.resolve()),
        "output": str(args.output.resolve()),
        "network": args.network,
    }
    if values["command"] and values["command"][0] == "--":
        values["command"] = values["command"][1:]
    docker_command(**values, flow_id="preflight")
    if args.serve and not args.deployment:
        parser.error("--serve requires --deployment")
    configured = project_flow.with_options(name=args.flow_name)
    if args.serve:
        configured.serve(name=args.deployment, parameters=values, limit=1)
    else:
        configured(**values)


if __name__ == "__main__":
    main()
