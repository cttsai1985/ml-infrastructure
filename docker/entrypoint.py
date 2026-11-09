"""Read Docker secrets at runtime, never interpolate them into Compose or argv."""

import base64
import json
import os
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import quote


def secret(name):
    return (Path("/run/secrets") / name).read_text().strip()


def client_environment():
    os.environ.pop("PREFECT_API_KEY", None)
    os.environ.update(
        PREFECT_API_AUTH_STRING=secret("prefect_auth_string"),
        MLFLOW_TRACKING_USERNAME="platform",
        MLFLOW_TRACKING_PASSWORD=secret("mlflow_password"),
    )


def storage_client():
    import boto3
    from botocore.config import Config

    credential = json.loads(secret("seaweed_s3_config"))["identities"][0]["credentials"][0]
    os.environ.update(
        AWS_ACCESS_KEY_ID=credential["accessKey"],
        AWS_SECRET_ACCESS_KEY=credential["secretKey"],
        AWS_DEFAULT_REGION="us-east-1",
        MLFLOW_S3_ENDPOINT_URL="http://seaweed:8333",
        MLFLOW_BOTO_CLIENT_ADDRESSING_STYLE="path",
    )
    return boto3.client(
        "s3",
        endpoint_url="http://seaweed:8333",
        config=Config(
            connect_timeout=3,
            read_timeout=5,
            retries={"max_attempts": 2},
            s3={"addressing_style": "path"},
        ),
    )


def main():
    command = sys.argv[1]
    if command == "mlflow":
        client = storage_client()
        from botocore.exceptions import ClientError, EndpointConnectionError

        for attempt in range(30):
            try:
                try:
                    client.head_bucket(Bucket="mlops-artifacts")
                except ClientError as error:
                    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                        raise
                    client.create_bucket(Bucket="mlops-artifacts")
                break
            except EndpointConnectionError:
                if attempt == 29:
                    raise RuntimeError("SeaweedFS is unavailable") from None
                time.sleep(2)
        database = "postgresql+psycopg2://mlflow:" + quote(secret("mlflow_db_password"), safe="")
        database += "@postgres:5432/mlflow"
        os.environ.update(
            MLFLOW_BACKEND_STORE_URI=database,
            MLFLOW_AUTH_ADMIN_USERNAME="platform",
            MLFLOW_AUTH_ADMIN_PASSWORD=secret("mlflow_password"),
            MLFLOW_FLASK_SERVER_SECRET_KEY=secret("flask_secret"),
            MLFLOW_AUTH_CONFIG_PATH="/tmp/mlflow-auth.ini",
        )
        auth = Path("/tmp/mlflow-auth.ini")
        auth.write_text(
            "[mlflow]\ndatabase_uri = " + database + "\ndefault_permission = NO_PERMISSIONS\n"
        )
        auth.chmod(0o600)
        os.execvp(
            "mlflow",
            [
                "mlflow",
                "server",
                "--host",
                "0.0.0.0",
                "--port",
                "5000",
                "--workers",
                "1",
                "--app-name",
                "basic-auth",
                "--serve-artifacts",
                "--artifacts-destination",
                "s3://mlops-artifacts",
                "--allowed-hosts",
                "localhost:*,127.0.0.1:*,mlflow:5000",
            ],
        )
    elif command == "prefect":
        database = "postgresql+asyncpg://prefect:" + quote(secret("prefect_db_password"), safe="")
        os.environ.update(
            PREFECT_SERVER_DATABASE_CONNECTION_URL=database + "@postgres:5432/prefect",
            PREFECT_SERVER_API_AUTH_STRING=secret("prefect_auth_string"),
            PREFECT_API_AUTH_STRING=secret("prefect_auth_string"),
            PREFECT_SERVER_API_CSRF_PROTECTION_ENABLED="true",
        )
        os.execvp("prefect", ["prefect", "server", "start", "--host", "0.0.0.0"])
    elif command == "worker":
        client_environment()
        os.execvp(
            "prefect",
            [
                "prefect",
                "worker",
                "start",
                "--pool",
                "platform-local",
                "--type",
                "process",
                "--limit",
                "1",
            ],
        )
    elif command == "health":
        service = sys.argv[2]
        endpoint = (
            "http://127.0.0.1:5000/health"
            if service == "mlflow"
            else "http://127.0.0.1:4200/api/health"
        )
        request = urllib.request.Request(endpoint)
        value = (
            "platform:" + secret("mlflow_password")
            if service == "mlflow"
            else secret("prefect_auth_string")
        )
        request.add_header("Authorization", "Basic " + base64.b64encode(value.encode()).decode())
        with urllib.request.urlopen(request, timeout=3) as response:
            if response.status != 200:
                raise RuntimeError("Unhealthy service")
    elif command == "storage-check":
        import hashlib

        client = storage_client()
        key, expected = sys.argv[2:]
        content = client.get_object(Bucket="mlops-artifacts", Key=key)["Body"].read()
        if hashlib.sha256(content).hexdigest() != expected:
            raise RuntimeError("Stored object checksum mismatch")
        print(
            json.dumps({"seaweed_object_verified": True, "bucket": "mlops-artifacts", "key": key})
        )
    elif command in {"smoke", "verify", "bootstrap"}:
        client_environment()
        if command == "bootstrap":
            from mlops_platform.registry import ensure_experiment

            if len(sys.argv) != 3:
                raise SystemExit("bootstrap requires an explicit client contract JSON file")
            contract = json.loads(Path(sys.argv[2]).read_text())
            print(json.dumps({"experiment_id": ensure_experiment(contract)}))
        else:
            from mlops_platform.smoke import main as smoke_main

            smoke_main(command)
    else:
        raise SystemExit("Unknown platform command")


if __name__ == "__main__":
    main()
