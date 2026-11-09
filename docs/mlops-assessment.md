# From applied science to operated batch research

The three Kaggle solutions are **consumers of MLOps capabilities**, not infrastructure products. Their primary evidence should answer scientific questions: what is the hypothesis, what changes under controlled ablation, how much does the result vary, and where does the model fail? Engineering then preserves the connection between those conclusions and the executable experiment.

## What the operating model needs to protect

A reproducible model is necessary but insufficient. An operator must also know whether the right data arrived, which stage failed, whether a retry is safe, which outputs are complete, and whether a candidate is suitable for delivery. The shared platform supplies reusable execution controls; each solution supplies its data contracts, model evaluation and batch prediction implementation.

| Consumer responsibility | Platform responsibility | Evidence to inspect |
|---|---|---|
| Define dataset schema, IDs and evaluation boundary | Execute declared validation before dependent work | Solution validation/analyzer commands and input fingerprints |
| State scientific hypothesis and fixed comparison budget | Preserve exact recipe and execution identity | Modeling studies, saved folds and immutable image/config identity |
| Produce a verifiable model and batch predictions | Retain task state, logs, deadlines and incident information | [Batch operations case study](batch-operations.md) |
| Declare an explicit delivery acceptance rule | Stop downstream delivery when that gate rejects a candidate | Gate result and absence of downstream outputs in the drill |
| Export completed evidence even without connectivity | Recover artifact transfer without retraining or changing logical identity | Interrupted-import recovery tests in each solution |
| Separate experimental results from approved deliverables | Keep candidate outputs and execution history inspectable | Local run receipts and shared MLflow artifact readback |

A failed quality gate is useful scientific evidence. It should not erase an experiment or prevent it being logged for analysis. A delivery gate controls a downstream candidate/prediction handoff; it is not a statement that only favorable experiments belong in the tracker.

## Comparison with Google's framework

Google's [MLOps architecture guide](https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning) includes data and model validation, pipeline automation, metadata management, delivery and monitoring. Batch prediction is a valid delivery mode. Its maturity levels describe complete operating processes, not a score obtained by installing named tools.

| Capability | Scope in this portfolio | Boundary |
|---|---|---|
| Scientific iteration | Hypotheses, controlled comparisons, grouped/stratified CV, fold and seed diagnostics | Static competition data; observed dispersion does not prove general stability |
| Reproducible components | Shared package per solution, locks, versioned recipes, saved preprocessing and input/model hashes | Hosted dependency versions can differ; receipts record them |
| Data/model checks | Identity, shape and split checks; complete OOF reconstruction; model reloads | These are not continuous monitoring of a changing production population |
| Workflow operation | Explicit multi-stage batch consumer, durable logs/status, deadlines, safe retry/resume and delivery gates | Shared case study exercises the Iceberg consumer; other solutions must not inherit unexecuted claims |
| Artifact lifecycle | Completed-run tracking, checksum readback and recoverable transfer | Naming a model does not implement approval or production promotion |
| Code CI | Package tests and lint on supported Python versions; isolated platform integration | Not automatic deployment of a production training pipeline |
| Production operation | Not implemented: live service SLOs, on-call routing, production drift/label feedback, automatic retraining, HA and disaster recovery | Do not claim Google's full level 1 or level 2 maturity |

The appropriate claim is **applied-science solutions with explicit MLOps consumer contracts and demonstrated batch controls**. The isolated reference stack supports that demonstration. It is not evidence that these competition models continuously serve users or that a production platform has been operated at scale.

## How to read the portfolio

Start with each solution's findings and percentile context, then its modeling study and executed notebook. Inspect the consumer contract to see how those results are handed to a runner and tracker. Use the shared operations case study for the failure/recovery evidence. Infrastructure versions and commands are implementation references after that story, not its main claim.
