# Deployment configuration

The platform contains no client catalog. Solutions own their experiment, model, flow and artifact names. Service operators supply deployment configuration separately from those client identities.

The optional experiment bootstrap accepts a single explicit JSON contract, for example:

```json
{"experiment_name": "team/task", "artifact_namespace": "team/task"}
```

`ensure_experiment(contract)` creates or reuses that namespace through an authorized MLflow client and rejects an existing namespace mismatch. It does not grant permissions, register models, deploy flows or enumerate other clients. Existing experiments and artifact locations remain unchanged.
