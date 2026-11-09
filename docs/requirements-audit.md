# Portfolio acceptance audit

## Presentation update — standard 1.1

The current primary display pairs **Public LB** and **Private LB** scores with
**Public Top %**. Rank/team counts and ties remain in detailed source evidence;
they are no longer repeated in the README table or GitHub description. Private
scores do not inherit public percentiles. This supersedes the canary-2 presentation
format reviewed below; its historical regression and source-preservation review
remain dated evidence. Local reproducibility may use scientific functions, a
notebook, script or existing thin command entry point; no bespoke CLI is required.
The existing canary-2 releases and completed merge/branch cleanup remain intact.
The original profile/standard are retained at `v0.2.0-canary.2`; the current
profile receipt records the latest approved compact description.


Reviewed: **2026-11-09** · Standard: [version 1.1](portfolio-standard.md).
The shared repository remains **ml-infrastructure**; renaming is paused. This
review changes the evidence contract and its check, not deployed services.

**Verified** means the linked implementation or receipt establishes its stated
scope. **Open** means accepted work remains. **Not applicable** explains a
solution-specific requirement rather than dropping it from the review.

| Requirement | Status | Evidence and boundary |
|---|---|---|
| R01 — accumulated requirements | Verified for this correction | This complete matrix and the versioned standard; `scripts/check_portfolio.py --kind infrastructure` checks the frozen standard and required records. |
| R02 — complete solution | Applicable as generic infrastructure | [README](../README.md), [client contracts](../configs/README.md) and [serving](serving.md) connect client-owned workflows and packages to service functions. Scientific claims belong to clients. |
| R03 — primary score/rank display | Not applicable | The platform has no competition score or leaderboard. Service acceptance replaces model-quality ranking; no client score catalog is introduced. |
| R04 — score protocol | Not applicable to leaderboard | [Verification](verification.json), [contract evidence](contract-validation.json) and [canary](canary-validation.json) record actual service-test scopes. |
| R05 — separate result scopes | Verified within service scope | [Requirements](requirements.md) distinguish test evidence, existing deployments and outstanding production acceptance. Client model rankings are not inferred. |
| R06 — scientific insight | Client-owned; operating rationale verified | [Batch operations](batch-operations.md) explains why evaluation and delivery need separate states and why recovery reuses verified outputs. No model improvement is claimed. |
| R07 — error/path analysis | Applicable to operational failures | [Batch fault drill](batch-verification.json), [batch rationale](batch-operations.md) and [serving](serving.md) trace failures, recovery and actual inference boundaries. Model hard cases belong to clients. |
| R08 — trustworthy evaluation | Client-owned; interfaces verified | [Batch quality gates](batch-operations.md) preserve the declared evaluation population and immutable artifact identity. The platform does not validate a client's statistical design. |
| R09 — no post-hoc score manipulation | Verified as contract | Quality rejection records the observed result; it does not rewrite it or discard difficult observations. See [batch operations](batch-operations.md). |
| R10 — reloadable actual model | Verified bounded capability | [Serving](serving.md), `src/mlops_platform/serving.py`, [canary](canary-validation.json). Model packages, preprocessing and environments remain client-owned. |
| R11 — actual HTTP parity | Verified recorded scope | [Canary](canary-validation.json) records real client-model checks; generic lifecycle and rollback evidence is in [contract validation](contract-validation.json). Historical receipts are not relabeled as a fresh run. |
| R12 — input boundary | Verified | [Serving contract](serving.md) requires a client package, schema, compatible runtime and native reference outputs; it does not claim raw-data preprocessing for every model. |
| R13 — maintained CLI/environments | Verified; notebook requirement client-owned | `./infra`, [README](../README.md), `Dockerfile`, `uv.lock` and CI. Infrastructure startup requires only stdlib Python plus Docker, not every client SDK. |
| R14 — one-way dependency | Verified | [Requirements](requirements.md), `src/mlops_platform/registry.py`, [configs](../configs/README.md). No platform-owned client catalog or fixed project naming authority. |
| R15 — lightweight safe deployment | Verified isolated tests | `./infra`, `scripts/manage.py`, [adoption](adoption.md), [contract validation](contract-validation.json). State reuse, ownership checks and logs/status are explicit. |
| R16 — preserve existing services | Verified change boundary | No deployment or state mutation in this correction. [Adoption](adoption.md) preserves live identities; separately managed Prefect and existing MLflow are not silently adopted. |
| R17 — private evidence boundary | Verified publication scope | Public receipts contain aggregates; private requests, logs, secrets and model bytes stay outside release assets. [Operations](operations.md), [serving](serving.md). |
| R18 — exact provenance | Verified with historical scope | [Canary](canary-validation.json) and [contract validation](contract-validation.json) retain their original hashes/source. This correction's release source and fresh code checks are reported separately. |
| R19 — explicit canary release | Existing canary verified; correction release pending until published | `v0.2.0-canary.1` retains its source/assets. A new correction prerelease must record its own source/checks and reuse of historical runtime evidence. No traffic splitting is implied. |
| R20 — credible identity/attribution | Verified; rename paused | README presents shared functions, not an assignment. Existing license and attribution retained. No new platform brand or repository rename. |
| R21 — problem/value grounding | Applicable to service purpose | [Design choices](design-decisions.md) and [requirements](requirements.md) connect services to operator/client needs; no invented enterprise ROI or customer deployment. |
| R22 — production batch and online | Open, partially verified | [Requirements](requirements.md): both remain the target. Batch fault/recovery and fresh-workspace serving are measured capabilities, not full production acceptance. |
| R23 — production ownership/controls | Open | Hosting, accountable owners, SLOs, capacity, TLS/endpoint authentication, alert delivery, off-host backups and tested restoration require decisions and evidence. [Requirements](requirements.md), [operations](operations.md). |
| R24 — batch recovery | Verified bounded behavior | [Real saved-model fault drill](batch-verification.json), [batch operations](batch-operations.md), `src/mlops_platform/batch.py`: immutable inputs, output-hash reuse, bounded read-only retries, quality rejection and incidents. Not distributed checkpoint recovery. |
| R25 — deeper scientific diagnosis | Client-owned | Solutions own OOF/holdout hard-case analyses and next modeling hypotheses. Platform failure diagnostics do not substitute for statistical insight. |
| R26 — coherent URLs/releases/branches | Rename intentionally paused; branch/release checks pending completion | Current GitHub name stays `ml-infrastructure`. Merge/release verification and deletion of only verified merged, unoccupied branches complete this correction. |

## Preservation review

The comparison base is the existing **v0.2.0-canary.1** tag (merge
`d5408b2b7d59e8eb3de93cdc67cdaed3ce850681`). At this correction's initial
review, `git diff v0.2.0-canary.1 -- src uv.lock infra scripts/manage.py
scripts/verify_model_serving.py docs/*validation.json docs/verification.json
 docs/batch-verification.json` is empty. New standard/audit/check files and the CI
check are additions; no existing runtime receipt is edited. A package-version
update, if made for the correction release, is metadata and must be recorded in
that release. This is a scoped preservation check, not a claim that every file in
the repository is unchanged.

## Release review rule

Run the frozen-standard check, lint, format and unit checks; retain CI integration
results and exact merge/tag identities in the release. Existing runtime receipts
remain dated evidence for their original source. Do not call the open production
requirements complete on the strength of a documentation correction.
