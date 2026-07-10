# Repository Guidelines

## Divine Context And Brain

Before broad product, architecture, protocol, cross-repo, or service-boundary work, read the shared Divine context primer.

Use `DIVINE_CONTEXT_ROOT` if set; otherwise look for `../divine-context`. If it is missing, try:

`gh repo clone divinevideo/divine-context ../divine-context`

The `divine-context` repo is private, so cloning requires GitHub access. If clone, network, or auth fails, continue from the local repo docs and avoid cross-repo assumptions.

Before updating an existing context checkout, verify it is clean and on its default branch. If it is clean and on the default branch, update it with `git -C <context-dir> pull --ff-only`. If it is dirty, on another branch, cannot fast-forward, or network/auth fails, leave it untouched and say the context may be stale.

Read `<context-dir>/AGENT_CONTEXT.md` and follow its instructions. If unavailable, continue from the local repo docs and avoid cross-repo assumptions.

If a Divine Brain search or ask tool is available, you may use it for company memory. Treat it as optional and credentialed: tool names vary by client, and work must continue when Brain is unavailable. When Brain results influence work, cite the returned document ids. Never commit Brain credentials or expose Brain-derived sensitive content in public PRs, issues, branch names, commit messages, code comments, logs, screenshots, release notes, or externally shared agent transcripts.

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
