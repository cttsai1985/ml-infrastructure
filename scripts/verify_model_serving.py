"""Verify generic serving with synthetic packages and explicit v1 -> v2 -> v1 restarts."""

import argparse
import json
import os
import socket
import sys
from pathlib import Path

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
from mlflow.models import infer_signature

from mlops_platform.serving import package_digest, private_json, verify_model

ROOT = Path(__file__).resolve().parents[1]


class LinearFixture(mlflow.pyfunc.PythonModel):
    def __init__(self, coefficient, intercept):
        self.coefficient = coefficient
        self.intercept = intercept

    def predict(self, context, model_input, params=None):
        return pd.DataFrame({"prediction": model_input["x"] * self.coefficient + self.intercept})


def verify(output: Path, timeout=90):
    output = output.resolve()
    if not output.is_relative_to(ROOT / ".local"):
        raise ValueError(
            "Serving verification output must be under this checkout's .local directory"
        )
    if output.exists():
        raise ValueError("Use a fresh output directory to retain previous evidence")
    output.mkdir(parents=True, mode=0o700)
    os.environ["MLFLOW_TRACKING_URI"] = (output / "tracking").as_uri()
    os.environ["MLFLOW_DISABLE_AGENT_HINT"] = "1"
    inputs = pd.DataFrame({"x": [0.0, 1.0, 3.0]})
    private_json(output / "request.json", {"dataframe_split": inputs.to_dict(orient="split")})
    private_json(
        output / "invalid.json", {"dataframe_split": {"columns": ["wrong"], "data": [[1.0]]}}
    )
    packages = {}
    for version, coefficient, intercept in [("v1", 1.5, -0.4), ("v2", 2.0, 0.2)]:
        model = LinearFixture(coefficient, intercept)
        expected = model.predict(None, inputs)
        path = output / version
        mlflow.pyfunc.save_model(
            path=str(path),
            python_model=model,
            signature=infer_signature(inputs, expected),
            input_example=inputs,
            pip_requirements=[
                f"mlflow=={mlflow.__version__}",
                f"numpy=={np.__version__}",
                f"pandas=={pd.__version__}",
            ],
        )
        private_json(output / f"{version}-expected.json", expected.to_dict(orient="records"))
        packages[version] = path
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    events = []
    for index, version in enumerate(["v1", "v2", "v1"]):
        result = verify_model(
            packages[version],
            Path(sys.executable),
            output / "request.json",
            output / f"{version}-expected.json",
            output / f"canary-{index}",
            expected_digest=package_digest(packages[version]),
            invalid_request=output / "invalid.json",
            atol=1e-12,
            rtol=0,
            timeout=timeout,
            port=port,
        )
        events.append({"selected_fixture_version": version, **result})
    receipt = {
        "schema_version": 1,
        "status": "verified",
        "run_kind": "infrastructure_fixture",
        "engine": "MLflow Model Serving",
        "mlflow_version": mlflow.__version__,
        "events": events,
        "restart_based_version_switch_and_rollback": True,
        "zero_downtime_claimed": False,
        "container_image_verified": False,
        "production_ingress_verified": False,
        "shared_services_modified": False,
    }
    private_json(output / "receipt.json", receipt)
    print(json.dumps(receipt, indent=2))
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    verify(args.output)


if __name__ == "__main__":
    main()
