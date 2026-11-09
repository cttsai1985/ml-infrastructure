# Operating a research batch

The operational unit is a **versioned batch job**, not a notebook session. A successful job must verify its inputs and saved model, pass a declared quality policy, produce valid predictions, and leave a traceable delivery receipt. A failed job must identify the failed stage and permit safe recovery without silently repeating completed computation.

The [real saved-model drill](batch-verification.json) uses the authorized Iceberg checkpoint and all 8,424 official test observations. It demonstrates orchestration of existing scientific artifacts; it does not claim the original training experiment was scheduled through Prefect.

## Why these stages have separate success states

The controlled drill first completes a valid model evaluation, then deliberately makes a downstream dependency unavailable. The scientific result survives while delivery remains blocked. Repairing that dependency should not create a new fitted model or a new evaluation population.

The recovered flow therefore reuses the verified evaluation and performs prediction and delivery once, preserving all 8,424 expected IDs and checking model bytes after retrieval. A second run deliberately rejects the evaluation at a quality threshold. Its operational meaning is different: the candidate fails policy, while the evaluation remains useful evidence. The runner records both outcomes without converting them into an undifferentiated “job failed.”

This is the design rationale for stage receipts, explicit retry scope and separate quality/readiness checks. The [receipt](batch-verification.json) binds that reasoning to observed states in an isolated saved-model fault drill. It does not establish operation of a live prediction service.

## Run and observe

[`src/mlops_platform/batch.py`](../src/mlops_platform/batch.py) executes a client-supplied JSON stage declaration through Prefect. `project_id` is a validated identifier, not a catalog lookup or authorization grant. `flow_name` belongs to the client; if omitted it defaults to the project ID. Existing clients can supply their existing flow name explicitly. Each container stage has an immutable image, argument vector, read-only data mount, bounded CPU/memory, deadline and uniquely named container. A timeout or interrupt removes only that stage's container. The host runner needs Docker privileges; server containers do not receive its socket.

```bash
# Supply the approved Prefect URL/auth and, if needed, MLflow client environment.
uv run python scripts/run_batch.py \
  --config /absolute/path/to/batch.json \
  --data /absolute/path/to/input-data \
  --output /absolute/path/to/batch-output
```

[The Iceberg declaration](../examples/iceberg-saved-model-batch.json) is a complete example. Copy a verified completed run into `<output>/model` first, and replace the image digest and client network with the approved local values. The fault-drill command below prepares its own copy and resolves the image digest automatically.

The runner requires an explicitly configured Prefect API and disables ephemeral-server fallback. Runtime credentials stay in the operator environment; only their variable names reach Docker arguments. Data and output directories must be separate and must not contain each other.

Each output directory contains a private `.batch-ops/` directory:

| Artifact | Operator use |
|---|---|
| `manifest.json` | Frozen configuration, exact image and input-file hashes |
| `<stage>.json` | Stage attempt, state, duration, output hashes and flow-run ID |
| `<stage>-attempt-<n>.log` | Private stdout/stderr for the exact failed container attempt |
| `events.jsonl` | Ordered stage-start, failure, completion and reuse events |
| `status.json` | Latest workflow state and Prefect run ID |
| `incident.json` | Failed stage, responsible operator and recovery instruction; resolution preserves the original incident |

These files are operator diagnostics, not public data artifacts. Logs can contain model/library messages; inspect them before sharing. They are not shipped with portfolio releases.

## Gates, retry and recovery

A quality gate reads declared numeric metrics and records observed values and policy thresholds. Nonfinite values and rejected thresholds fail the task and block downstream candidate-publication stages. This is an explicit research acceptance policy; its threshold is not evidence of generalization or production suitability. Rejected experiments can still be retained for analysis; candidate delivery is the gated action.

Model fitting is never automatically retried. Only stages explicitly declared read-only may request one or two retries. A local read-only mount is a guardrail, not proof that an arbitrary command has no remote side effects; the declaration requires operator review. Current project tracking commands are not declared read-only. Their explicit import recovery is documented in the solution repositories.

To recover from an external dependency failure, stop the previous producer, repair the dependency and rerun the identical configuration against the same output directory. Successful stages are reused only if all declared artifact hashes still match. The input, image, configuration and command identities must also match. A gate fingerprints its metric files, so editing a completed evaluation to pass a gate is rejected. Stages without declared artifact outputs run again; live dependency readiness is not treated as a permanent cached fact.

A changed input/configuration/image or altered completed artifact requires a new output directory. An interrupted training stage may leave partial scientific output; operator recovery must deal with that explicitly. This is stage-level continuation, not automatic model-training checkpoint resumption. A local lock prevents concurrent runners using the same output directory; it is not a distributed lease across hosts.

## Reproduce the fault drill

The following command creates a uniquely named temporary reference stack, runs the real saved-model batch and removes only that test deployment and its volumes. It requires a completed modern Iceberg run, authorized raw inputs and the locally built Iceberg image. It does not touch the adopted shared tracker or separately managed production Prefect.

```bash
uv run python scripts/verify_saved_model_batch.py \
  --data /absolute/path/to/iceberg-data \
  --model /absolute/path/to/completed-iceberg-run \
  --image iceberg-lab:0.1 \
  --receipt .local/batch-verification.json
```

The drill checks four paths:

1. **Dependency failure:** model verification and evaluation complete; an unavailable read-only dependency exhausts its bounded retries. Prediction and publication do not execute.
2. **Recovery:** the dependency is repaired but returns one controlled HTTP 503. A bounded retry succeeds; the original model evaluation is reused without recomputation. Full test prediction and MLflow delivery complete, and downloaded model bytes match the checkpoint hash.
3. **Idempotent reexecution:** scientific/delivery stages with unchanged artifacts are reused; the dependency readiness check runs again.
4. **Quality rejection:** a separate run uses an intentionally rejecting threshold. Its observed metric is recorded, and neither predictions nor candidate publication is produced.

The resulting aggregate receipt includes actual Prefect flow/task states, image/model/input hashes, evaluation metrics and artifact-readback evidence. It excludes rows, secrets and stdout logs. CI additionally exercises mutation rejection, quality-gate blocking, deadline cleanup, owner-visible incidents and forbidden training retries using controlled tests.

## What this demonstrates

This is evidence of observable, gated and recoverable batch operation around a research model. It does not demonstrate a production prediction service, live model-quality monitoring, automated drift-triggered retraining, external alert delivery, availability guarantees or tested backup restoration. Those require an actual deployment/data lifecycle and explicit service ownership; adding tool names would not establish them.
