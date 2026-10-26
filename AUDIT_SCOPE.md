# Audit scope

Written for an outside, independent security audit. Revision described:
`a39788b0eaaabc694dfa17652420e35b7dc3eda5` on `main`, 2026-10-26. Everything below is taken from
the repository's own files at that revision; items marked NOT_RUN were not executed while writing
this document.

## What this repository is

aegis-runtime is an agent runtime in Rust that builds one program, `aegis`. It runs a local
gateway (WebSocket, server-sent events and REST, default port 18096), a heartbeat loop that claims
and runs queued tasks, a dependency-aware task workflow, and SQLite storage in WAL mode. With
`--inference http` it sends model calls to a local OpenAI-compatible server; the default
`--inference embedded` is a mock that loads no model. Status (README): pre-alpha, version 0.2.0,
not part of the WHOLE-SYSTEM-E2E v1 release; the production runtime is aien-sovereign-core.

## What to review, in priority order

1. `src/auth.rs` and `src/gateway.rs`. Operator token (`AEGIS_OPERATOR_TOKEN` or
   `--operator-token-file`, at least 32 printable characters) required on every route except a
   short list of read-only `GET` routes; with no token configured, protected routes refuse every
   request. Loopback bind by default (`auth::resolve_bind_addr`). Check header parsing, comparison,
   the WebSocket path (which can run shell and skills) and the open route list.
2. `src/skills.rs`, `src/execution/`, `src/policy_guard.rs`, `src/enforcement.rs`. Shell allowlist
   (exactly `git status`, `git diff`, `git log -1 --oneline`, `ls`), registered skill names,
   probe gating on every call (`AIEN_PROBE_ENFORCE` can only raise the 0.5 threshold), and
   `write_file` containment under a held directory descriptor.
3. `src/vault.rs`. `VaultResolver` asks `atlas-vault`, keeps values in memory, redacts output with
   `[REDACTED_BY_ATLAS_VAULT]`, reads the environment only with `AIEN_DEV_SECRET_FALLBACK=1`.
4. `src/agent.rs`, `src/heartbeat.rs`, `src/orchestration/`. Multi-turn tool calling on model
   output; task claiming; failure and retry rules (`docs/FAILURE-PROVENANCE.md`,
   `docs/WORKFLOW-DAG.md`).
5. `src/persistence/`, `src/sessions/`, `src/events/`, `src/defense/`, `src/security.rs`.
6. `install.sh`.
7. `.github/workflows/aien-verification.yml` (advisory checks, Alpha bundle verification,
   `contents: read`) and `release.yml` (tag `v*` or manual; publish job has `contents: write`).
8. `tests/gates/run.sh` and the gate manifests.

## Trust boundaries

- Untrusted: any network client of the gateway; model output driving `aegis agent` tool calls;
  task payloads; inference server responses.
- Trusted: the holder of the operator token; `atlas-vault`; the shared protocol crates in the
  sibling `aien-protocols` checkout.
- The gateway listens on loopback unless `--bind <ip> --allow-non-loopback-bind` is given.

## Threat model summary

- Assets: the host shell and filesystem reachable through skills and `write_file`; secrets from
  `atlas-vault`; task records (which carry command text and output).
- Attackers considered: a local or network client without the token; a prompt-injected model;
  a crafted path or symlink in a write request.
- Defended: token on protected routes, loopback default, shell and skill allowlists, probe
  gating, descriptor-relative writes, no plaintext secret files, redaction.
- Not defended / explicitly out of scope: containment applies to `write_file`, not to arbitrary
  shell commands (README); the mandatory capability checks, approval desk and effect
  authorization live in aien-sovereign-core, not here (README).

## Build from source at a pin

No `rust-toolchain.toml`; the README asks for a stable Rust toolchain, edition 2021, so the exact
version is unknown from the repository. `Cargo.toml` has six path dependencies on a sibling
`../aien-protocols` checkout, which is not pinned to a commit by this repository.

```sh
git clone https://github.com/aien-dev/aegis-runtime-mv aegis-runtime
git clone https://github.com/aien-dev/aien-protocols-mv aien-protocols
cd aegis-runtime
cargo build --release
```

## Run every test

```sh
cargo test -- --test-threads=1
./scripts/agent-preflight.sh
tests/gates/run.sh crumb-verify
tests/gates/run.sh fmt
tests/gates/run.sh check
tests/gates/run.sh clippy
tests/gates/run.sh test
tests/gates/run.sh zero-disk-secrets
tests/gates/run.sh anti-slop
```

- README: on 2026-10-19 the test suite passed 158 tests on the Spark at commit `08ea496` (55
  library tests plus 103 in `tests/*.rs`).
- `zero-disk-secrets` prints `Zero plaintext .env files verified.`
- The DGX Spark is the test judge (`docs/PIPELINE.md`); GitHub checks are advisory. The signed
  Spark run: NOT_RUN here. All commands above: NOT_RUN while writing this document.

## Known limitations and NOT_RUN

- The default inference backend is a mock; `aegis status` output naming "NativeTransformer" refers
  to the mock.
- The delegated task workflow work G1 to G6 is open in pull requests, not on `main`.
- The sibling `aien-protocols` revision used by a build is whatever is checked out beside it.

## Dependencies and provenance

- `Cargo.lock`; crates.io dependencies including axum, tokio, SQLite bindings and rustls (updated
  to 0.23.46 for four advisories, #66).
- Path dependencies on `aien-protocols` (unpinned).
- External services at runtime: `atlas-vault`; optional local model server.
- Licence: see `LICENSE`.

## Evidence

- No `evidence/` directory in this repository.
- Signed Spark bundles: `aien-dev/aien-receipts` at `aegis-runtime/<sha>/bundle.json` (workflow
  path). Receipts are append-only and never edited.
