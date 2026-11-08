"""Stable identities shared by repository clients and infrastructure bootstrap."""

import json
import re
from pathlib import Path

REGISTRY = Path(__file__).resolve().parents[2] / "configs" / "projects.json"


def load_registry(path: Path = REGISTRY) -> dict:
    registry = json.loads(path.read_text())
    if registry.get("schema_version") != 1 or registry.get("artifact_bucket") != "mlops-artifacts":
        raise ValueError("Unsupported registry schema or bucket")
    projects = registry.get("projects", {})
    if not projects:
        raise ValueError("Registry must contain projects")
    required = {
        "scope",
        "repository",
        "mlflow_experiment",
        "registered_model",
        "prefect_flow",
        "prefect_deployment",
        "artifact_namespace",
    }
    for project_id, project in projects.items():
        if not re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", project_id):
            raise ValueError("Invalid project ID")
        if set(project) != required or project["scope"] not in {"competition", "collection"}:
            raise ValueError(f"Invalid fields for {project_id}")
        if not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", project["repository"]):
            raise ValueError("Invalid GitHub repository")
        expected = {
            "mlflow_experiment": f"kaggle/{project_id}",
            "registered_model": f"kaggle-{project_id}",
            "prefect_flow": f"kaggle-{project_id}",
            "prefect_deployment": "local",
            "artifact_namespace": f"projects/{project_id}",
        }
        if any(project[key] != value for key, value in expected.items()):
            raise ValueError(f"Identity drift for {project_id}")
    for field in required - {"scope", "prefect_deployment"}:
        values = [project[field] for project in projects.values()]
        if len(set(values)) != len(values):
            raise ValueError(f"Duplicate registry field: {field}")
    return registry


def bootstrap_experiments() -> dict[str, str]:
    from mlflow import MlflowClient

    client = MlflowClient()
    result = {}
    for project_id, project in load_registry()["projects"].items():
        location = "mlflow-artifacts:/" + project["artifact_namespace"]
        experiment = client.get_experiment_by_name(project["mlflow_experiment"])
        if experiment is None:
            experiment_id = client.create_experiment(
                project["mlflow_experiment"],
                artifact_location=location,
                tags={"project_id": project_id, "repository": project["repository"]},
            )
        else:
            if experiment.artifact_location != location:
                raise ValueError(f"Existing experiment artifact namespace differs: {project_id}")
            experiment_id = experiment.experiment_id
        result[project_id] = experiment_id
    return result
