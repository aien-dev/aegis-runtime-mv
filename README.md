# aegis-runtime

aegis-runtime is an agent runtime written in native Rust: a local gateway (WebSocket, server-sent events and REST on port 18096), a heartbeat loop that claims and runs queued tasks, a dependency-aware task workflow, and SQLite storage. It sends model calls to a local OpenAI-compatible model server when started with `--inference http`; the default `--inference embedded` backend is a mock that loads no model and ignores `--model-path` and `--tokenizer-path`.

An attachment of [Aienix](https://github.com/aien-dev/aienix), the core tree of AIEN. It lives in its own repository and plugs into the core at a boundary.

**Status (2026-10-18): pre-alpha, version 0.2.0, not part of the WHOLE-SYSTEM-E2E v1 release** (Drake, 2026-10-10). The production runtime in [aien-sovereign-core](https://github.com/aien-dev/aien-sovereign-core) carries the mandatory capability checks, the approval desk, effect authorization and fail-closed behaviour, and WHOLE-SYSTEM-E2E measures them there. The last merged changes (2026-10-15 and 2026-10-16) removed Modular MAX and Mojo and moved testing to AIEN's Spark pipeline ([docs/PIPELINE.md](docs/PIPELINE.md)). Open pull requests propose more workflow work for delegated tasks (G1 to G6); none of it is on `main`.

## Copyright

Copyright (c) 2026 Drake Stapleton <aien@aienos.com> and AIEN Contributors. Authored by Drake Stapleton in collaboration with AIEN.

## Architecture

```
                                  +-----------------------------+
                                  |   atlas-vault client        |
                                  |   (No Secrets on Disk)      |
                                  +--------------+--------------+
                                                 |
+---------------------------+     +--------------v--------------+     +-----------------------------+
|   Axum Control Plane      |     |     Heartbeat Engine        |     |  Local model seat (optional)|
|   - Port 18096            | <-> |     - Autonomous Tick [2]   | <-> |      - Port 18006           |
|   - WebSockets & SSE      |     |     - State Machine         |     |      - GB10 Local Weights   |
|   - REST API              |     |     - Task Evaluation       |     |      - TTFT per model [1]   |
+---------------------------+     +--------------+--------------+     +-----------------------------+
                                                 |
                                  +--------------v--------------+
                                  |   Rust Vector Math          |
                                  |   - Vector Cosine & Entropy |
                                  |   - SQLite WAL Persistence  |
                                  +-----------------------------+
```

[1] Measured TTFT depends on the model. For example, Nemotron 3.5 Lightning 30B measured 426.91 ms (`data/benchmarks_20260919.json` in [aien-dev/benchmarks](https://github.com/aien-dev/benchmarks)). That repository has withdrawn the figure: it predates its evidence standard and has not been regenerated.

[2] The tick interval is 30 seconds for `aegis heartbeat` and 60 seconds for `aegis serve` unless `--heartbeat-secs` is set.

## Core Subsystems

1. **Axum Control Plane**:
   Full duplex WebSockets (`/ws`, `/api/v1/ws`), Server-Sent Events (`/api/v1/events`, `/events`), and REST APIs on port 18096: `/api/v1/chat`, `/api/v1/agent/run`, `/api/v1/heartbeat/tick`, `/api/v1/tasks`, `/api/v1/skills`, `/api/v1/shell`, `/api/v1/discord/relay`, plus OpenAI-style `/v1/chat/completions` and `/v1/models`, and `/v1/probe`.

2. **Heartbeat Engine**:
   Autonomous polling loop (30 seconds under `aegis heartbeat`, 60 seconds by default under `aegis serve`) that claims pending tasks, runs them, and emits a pulse receipt.

3. **Local Model Seat Inference** (optional):
   With `--inference http`, requests go to the OpenAI-style endpoint given by `--model-url` (default `http://127.0.0.1:18006/v1/chat/completions`) using the model name `atlas-lightning-omni`. Without it, the mock backend answers. The mock (`MockInferenceClient` from aien-protocols) returns a mock answer and loads no model.

   Port 18006 was the old Modular MAX serving endpoint. A queued `model_health` heartbeat task (or one still queued under its old name `max_health`) checks the model seat through the configured backend, or polls `http://127.0.0.1:18006/v1/models` directly when no backend is set, and reports whether anything answers on port 18006. The project does not use MAX or CUDA on its production path (decision of 2026-10-02); the production runtime in aien-sovereign-core runs its GPU path through AIEN's own Omega kernels.

4. **Vector Math**:
   Vector cosine similarity, Shannon entropy, linear projection, and temperature scaling in plain Rust (`src/vector_math.rs`).

5. **Secret resolution**:
   No plaintext secret files. `VaultResolver` asks `atlas-vault`, then keeps the value in memory. It is a client of that provider, not a TPM. The process environment is read only when `AIEN_DEV_SECRET_FALLBACK=1`. Output is redacted with `[REDACTED_BY_ATLAS_VAULT]`.

6. **SQLite Persistence**:
   Embedded SQLite store running in Write-Ahead-Logging (WAL) mode for transactional task and message durability.

7. **Task workflow and failure handling**:
   A dependency-aware task workflow ([docs/WORKFLOW-DAG.md](docs/WORKFLOW-DAG.md)) and structured agent failure records with effect-safe retry rules ([docs/FAILURE-PROVENANCE.md](docs/FAILURE-PROVENANCE.md)).

## Build and Execution

### Prerequisites
- A stable Rust toolchain (`cargo`); the crate uses edition 2021
- A checkout of [aien-dev/aien-protocols](https://github.com/aien-dev/aien-protocols) next to this one at `../aien-protocols` (six path dependencies in `Cargo.toml`)
- Optional: an OpenAI-compatible model server on port 18006 (the local model seat), only for `--inference http`
- Linux aarch64 (Grace Blackwell GB10) or x86_64

### Compilation
```bash
cargo build --release
```

### Running the Gateway
```bash
# Operator token: at least 32 printable characters, kept out of the repo.
export AEGIS_OPERATOR_TOKEN="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
./target/release/aegis serve --port 18096
```

Security defaults:
- The gateway listens on 127.0.0.1 only. Any other address needs `--bind <ip> --allow-non-loopback-bind`.
- Every route except a few read-only `GET` routes (`/health`, `/api/v1/health`, `/v1/models`, `/api/v1/skills`, `/api/v1/events`, `/events`) needs the operator token, sent as `Authorization: Bearer <token>` or `X-Aegis-Operator-Token: <token>`. This covers `/api/v1/shell`, `/api/v1/skills/execute`, the task list (`GET /api/v1/tasks`, whose records carry command text and output), every other `POST`, and the WebSocket. With no token configured those routes refuse every request. The token can also come from `--operator-token-file <path>`.
- Skills and shell commands run only from an allowlist: the registered skill names, and for shell exactly `git status`, `git diff`, `git log -1 --oneline`, `ls`.
- Probe gating runs on every skill and shell call. `AIEN_PROBE_ENFORCE` takes a number above 0.5 and up to 1.0 and can only raise the threshold; any other value keeps the default of 0.5, so it cannot turn gating off.

### CLI Commands
```bash
# Execute a single autonomous tick
aegis tick

# Run the heartbeat loop on its own (30 second default)
aegis heartbeat

# Send one prompt to the inference backend (add --inference http to reach the local model seat)
aegis ask "Analyze system health and pending tasks"

# Run an agent task with multi-turn tool calling (default 8 turns)
aegis agent "Summarize the repository status" --max-turns 8

# Check engine status
aegis status

# Print a small vector math evaluation
aegis simd --steps 10
```

## Mathematical verification

The vector math functions are checked against an analytical reference and the committed vectors in `tests/fixtures/simd_math_vectors.json`:

```bash
cargo test --lib vector_math::tests::test_analytical_parity
```

## License

Licensed under the GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later). See [LICENSE](LICENSE). Project values live in the nonbinding [COVENANT.md](COVENANT.md), which grants and restricts no legal rights.
