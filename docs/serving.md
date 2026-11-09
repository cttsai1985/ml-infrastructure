# A shared serving function for evaluated models

A solution supplies an MLflow Model package and a prediction contract. The shared infrastructure checks that package through MLflow's actual inference server. No application repository, project name, model architecture or feature list is built into the controller.

## Client handoff

| Input | Purpose |
|---|---|
| Model directory containing `MLmodel` | Supported prediction flavor, fitted preprocessing, code, dependencies and model artifacts |
| Model-environment Python executable | The client-owned runtime; serving does not install model dependencies into the tracking server |
| Strict JSON request | The real serving request, such as `dataframe_split`; represent missing values as `null`, not a literal `NaN` |
| Independent native expected output | `{"predictions": [...]}` or a numeric `.npy` result, with the same input population and output order |
| Invalid request | A client-specific schema/input rejection check, such as a missing required field |
| Optional expected package digest | Binds the selected package to a previously reviewed identity |

The model package is executable code. Accept it only from the selected trusted release process. A checksum detects differences; it does not establish trust or model quality.

## Verify the real endpoint

```bash
./infra model verify \
  --model /absolute/path/to/model \
  --python /absolute/path/to/model-environment/bin/python \
  --request /absolute/path/to/request.json \
  --expected /absolute/path/to/expected.json \
  --invalid-request /absolute/path/to/invalid-request.json \
  --output .local/model-canary \
  --atol 1e-7 --rtol 0
```

Choose tolerance from the client model's numerical contract. Startup and inference have separate deadlines (`--timeout` and `--inference-timeout`). The host controller uses the standard library; MLflow and model libraries belong to the selected environment. The runtime fingerprint records its installed packages, while the client's lockfile remains the reproducible installation authority.

The controller hashes the source package, copies it into a fresh private workspace and starts `mlflow models serve` from that workspace with one worker. It binds only to loopback, disables bytecode writes and avoids network weight downloads. It checks `/ping`, calls `/invocations`, compares output fields/shapes/values and requires the invalid request to return HTTP 400. The source and copied package hashes must still match afterward.

The temporary server process group is terminated and its endpoint must be released before completion. This matters for version changes: a readiness request sent before an old server finishes stopping can observe the wrong model. The synthetic lifecycle check explicitly exercises v1 → v2 → v1; application checks compare the selected package to that application's native predictions.

## Use the verified model

After the canary passes, the same selected model environment can run MLflow's foreground serving command for an application or interactive test:

```bash
export PATH=/absolute/path/to/model-environment/bin:"$PATH"
export PYTHONDONTWRITEBYTECODE=1 HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
python -m mlflow models serve \
  --model-uri /absolute/path/to/verified-model \
  --env-manager local --host 127.0.0.1 --port 8080 --workers 1
curl --fail http://127.0.0.1:8080/invocations \
  -H 'Content-Type: application/json' \
  --data-binary @/absolute/path/to/request.json
```

Keep the selected package unchanged and retain its verified digest. Run under the client's pinned environment with the model's measured resources; stop the foreground process after use. For a managed endpoint, supervise this same runtime through the selected deployment configuration and its access/health/traffic controls. The prediction process is separate from the MLflow tracking server.

## Evidence and release scope

The private output directory retains the deployment receipt, copied workspace and server logs. Aggregate evidence includes controller/package/runtime fingerprints, request and expected-output hashes, parity counts/differences, schema-rejection status and cleanup. Publish aggregate evidence only after reviewing it; request rows and model weights remain private unless their release is separately authorized.

A canary prerelease communicates the tested software and model-interface scope. This verifier does not split customer traffic, deploy an authenticated ingress, select a model's business policy or claim an availability/latency objective. Model packaging, evaluation and acceptance belong to the client; production access, traffic routing, monitoring and recovery follow the [infrastructure requirements](requirements.md).

## Build a complete solution around the function

A successful request proves that the packaged prediction function is callable and agrees with its reference. The solution still needs its upstream data preparation, representative evaluation and a decision that can use the output. Feature-vector endpoints must state which feature construction happens before scoring. Quality-label endpoints must state what the labels mean. This keeps model serving connected to a usable ML solution rather than treating HTTP success as the entire outcome.
