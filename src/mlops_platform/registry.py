"""Generic experiment provisioning for a client-supplied MLflow contract.

Names identify artifacts; they do not authorize access. MLflow authentication and
permissions are configured by the service operator independently of this helper.
"""

import re


def validate_project_id(project_id: str) -> None:
    if not isinstance(project_id, str) or not re.fullmatch(
        r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", project_id
    ):
        raise ValueError("Invalid project ID")


def validate_contract(contract: dict) -> None:
    if not isinstance(contract, dict) or set(contract) != {"experiment_name", "artifact_namespace"}:
        raise ValueError("Supply experiment_name and artifact_namespace explicitly")
    for key, value in contract.items():
        if (
            not isinstance(value, str)
            or not value
            or any(
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", part) or part in {".", ".."}
                for part in value.split("/")
            )
        ):
            raise ValueError(f"Invalid client contract field: {key}")


def ensure_experiment(contract: dict, *, client=None) -> str:
    """Explicitly create or reuse one namespace; never relocate an existing one."""
    validate_contract(contract)
    if client is None:
        from mlflow import MlflowClient

        client = MlflowClient()
    name = contract["experiment_name"]
    location = "mlflow-artifacts:/" + contract["artifact_namespace"]
    experiment = client.get_experiment_by_name(name)
    if experiment is None:
        try:
            return client.create_experiment(name, artifact_location=location)
        except Exception as error:
            # Only a concurrent create of this same name permits a re-read.
            if getattr(error, "error_code", None) != "RESOURCE_ALREADY_EXISTS":
                raise
            experiment = client.get_experiment_by_name(name)
            if experiment is None:
                raise
    if experiment.lifecycle_stage != "active" or experiment.artifact_location != location:
        raise ValueError(
            "Existing experiment lifecycle or artifact namespace differs from client contract"
        )
    return experiment.experiment_id
