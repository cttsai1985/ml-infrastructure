# Reusable competition refactor scaffold and infra release

Standard practice: a small shared scaffold plus deterministic evidence checks. We differ because adoption must preserve existing solution implementations and cannot prescribe identical scientific code, force a new CLI or introduce a platform client catalog.

## Scope
- Shared tooling: scripts/check_portfolio.py; scripts/scaffold_solution.py; scripts/leaderboard_reference.py.
- Cookiecutter-compatible skeleton: templates/competition/cookiecutter.json; templates/competition/{{cookiecutter.repository_name}}/{README.md,docs/,scripts/check_portfolio.py,.github/workflows/portfolio.yaml}; docs cover results, problem/value, modeling study, error analysis, inference and the complete R01–R26 audit. Copy the canonical standard/checker with byte-equality tests, never weaken the frozen standard.
- Docs/CI/tests: docs/competition-template.md, docs/requirements-audit.md, README.md, .github/workflows/quality.yaml; tests/test_{portfolio,scaffold_solution,leaderboard_reference}.py.
- Release metadata: pyproject.toml, uv.lock, src/mlops_platform/__init__.py if it carries a version. New prerelease 0.2.0.dev3 / v0.2.0-canary.3 from merged source with lock, source, docs, aggregate receipts and checksums.

## Behavior and verification
- No-overwrite/idempotent scaffolding for new or existing repositories; useful draft output, explicit missing evidence, no fabricated model results.
- Reuse existing public/private/Top % table format. Add weighted AUC, explicit unsubmitted/unavailable outcomes, configurable root/analyzer path, exact ranking/tie math and strict source binding. Preserve compatibility with the current four competition refactors.
- Keep the platform generic. Templates accept client-supplied identity and supported prediction entry points; no hardcoded client catalog or runtime dependency on the scaffold.
- Write behavior tests before new tooling, run existing platform checks and isolated CI integration, independent review, merge and publish only after current-commit checks pass. Read back uploaded assets/tag. Remove merged/unoccupied worktree/branches.

## Risk boundary
No deployment, service migration, host bootstrap, credential rotation, traffic switch or model retraining. Existing runtime receipts retain their original source/scope. Open batch/online production requirements remain open. No raw private rows/models/requests/logs in release assets.
