# Repository Guidelines

## Project Structure & Module Organization
- Core Rust service code lives in `src/`.
- Python operational helpers live at the repo root and test coverage for them lives in `tests/`.
- Supporting docs live in `README.md` and `docs/`.
- Keep service runtime logic, resumable upload handling, and media follow-up hooks separated instead of growing broad shared files.

## Build, Test, and Development Commands
- `cargo test`: run the Rust test suite.
- `cargo check`: run a fast compile-only validation pass.
- `python3 -m unittest discover -s tests -p 'test_*.py'`: run the Python helper tests.
- `cargo run`: start the service locally.
- If runtime configuration or upload semantics change, update the relevant docs and examples in the same change.

## Coding Style & Naming Conventions
- Use idiomatic Rust with explicit error handling and focused modules.
- Keep Python operational helpers small and task-specific.
- Keep PRs tightly scoped. Do not mix unrelated cleanup, formatting churn, or speculative refactors into the same change.
- Temporary or transitional code must include `TODO(#issue):` with the tracking issue for removal.

## Pull Request Guardrails
- PR titles must use Conventional Commit format: `type(scope): summary` or `type: summary`.
- Set the correct PR title when opening the PR. Do not rely on fixing it afterward.
- If a PR title changes after opening, verify that the semantic PR title check reruns successfully.
- PR descriptions must include a short summary, motivation, linked issue, and manual test plan.
- Changes to upload flow, resumable sessions, GCS writes, or media follow-up hooks should include representative requests or rollout notes when helpful.

## Sensitive Information
- Do not commit secrets, private keys, private media, or sensitive operational details.
- Public issues, PRs, branch names, screenshots, and descriptions must not mention corporate partners, customers, brands, campaign names, or other sensitive external identities unless a maintainer explicitly approves it. Use generic descriptors instead.
