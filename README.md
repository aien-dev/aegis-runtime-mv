# aegis-runtime

aegis-runtime is an agent runtime written in Rust. It builds one program, `aegis`. That program runs a local gateway (WebSocket, server-sent events and REST on port 18096), a heartbeat loop that claims and runs queued tasks, a dependency-aware task workflow, and SQLite storage. With `--inference http` it sends model calls to a local OpenAI-compatible model server. The default, `--inference embedded`, is a mock: it loads no model and ignores `--model-path` and `--tokenizer-path`.

## Where it fits in AIEN

aegis-runtime is an attachment of [AienOS](https://github.com/aien-dev/aienix), the core tree of AIEN. It lives in its own repository and plugs into the core at a boundary.

- [aien-dev/aien-protocols-mv](https://github.com/aien-dev/aien-protocols-mv): the shared protocol crates. This repository needs a checkout of it to build (see Build).
- [aien-dev/aien-sovereign-core-mv](https://github.com/aien-dev/aien-sovereign-core-mv): the production runtime. It also holds the Spark test tools (`tools/aien-test`, `tools/aien-local-ci`).
- [aien-dev/aien-receipts](https://github.com/aien-dev/aien-receipts): where the Spark publishes signed test results for this repository.

## Status (2026-10-19)

- **Pre-alpha, version 0.2.0** (`Cargo.toml`).
- **Not part of the WHOLE-SYSTEM-E2E v1 release** (Drake, 2026-10-10). The production runtime in aien-sovereign-core carries the mandatory capability checks, the approval desk, effect authorization and fail-closed behaviour, and WHOLE-SYSTEM-E2E measures them there.
- **No published release of 0.2.0.** The GitHub release `v0.1.0` (2026-09-20) is from before the rename from OpenClaw. `v0.2.0` is an unpublished draft from 2026-09-19.
- **Recent changes on `main`:** Modular MAX and Mojo removed (#49, 2026-10-15). The Spark became the test judge (#51, 2026-10-16). rustls updated to 0.23.46 for four security advisories (#66, 2026-10-18). The hosted test workflows were removed, so the Spark is now the only place tests run (#69, 2026-10-18).
- **Merged on 2026-10-19:** docs/PIPELINE.md describes phase 3 as finished (#71), and the `.crumb` files stopped storing generated data; the crumb tool computes it on demand (#70).
- **Open, not on `main`:** the delegated task workflow work G1 to G6 (pull requests #60 to #65, tracked in issue #53).

## Architecture

```
                                  +-----------------------------+
                                  |   atlas-vault client        |
                                  |   (no secrets on disk)      |
                                  +--------------+--------------+
                                                 |
+---------------------------+     +--------------v--------------+     +-----------------------------+
|   Axum control plane      |     |     Heartbeat engine        |     |  Local model seat (optional)|
|   - port 18096            | <-> |     - tick loop [1]         | <-> |  - port 18006               |
|   - WebSocket and SSE     |     |     - claims queued tasks   |     |  - any OpenAI-compatible    |
|   - REST API              |     |     - runs and records them |     |    server                   |
+---------------------------+     +--------------+--------------+     +-----------------------------+
                                                 |
                                  +--------------v--------------+
                                  |   Rust vector math          |
                                  |   SQLite (WAL mode)         |
                                  +-----------------------------+
```

[1] The tick interval is 30 seconds for `aegis heartbeat` and 60 seconds for `aegis serve` unless `--heartbeat-secs` is set (`aegis <command> --help`).

## Core subsystems

1. **Axum control plane** (`src/gateway.rs`).
   WebSockets (`/ws`, `/api/v1/ws`), server-sent events (`/api/v1/events`, `/events`), and REST on port 18096: `/health`, `/api/v1/health`, `/api/v1/chat`, `/api/v1/agent/run`, `/api/v1/heartbeat/tick`, `/api/v1/tasks`, `/api/v1/skills`, `/api/v1/skills/execute`, `/api/v1/shell`, `/api/v1/discord/relay`, plus OpenAI-style `/v1/chat/completions` and `/v1/models`, and `/v1/probe`.
   Agent runs return `status: "completed"` only for nonempty terminal text with finish reason `stop` and no terminal tool failure. Failed attempts recovered by the configured retry policy do not count as terminal failures. Truncated (`length`), empty-text and turn-budget-limited results are `incomplete`; an empty inference choices array is an execution error, not a synthesized answer. The producer and workflow adapter share the terminal-tool-failure classifier. HTTP 200 means the run returned a result, not that its objective completed. WebSocket `agent_result` messages carry the same top-level status and retain `result.completed`; execution errors remain `agent_error`.

2. **Heartbeat engine** (`src/heartbeat.rs`).
   A polling loop that claims each pending task before running it and emits a pulse receipt. Only successful actions become `completed` tasks and `task_completed` transcript events. Unhealthy model checks, rejected or failed shell execution, failed memory writes, and unknown task types become `failed` tasks with the error in `result` and a `task_failed` event. Failed tasks are not automatically retried. A pulse is `degraded` if any action, task persistence operation, or queue read/claim fails, otherwise `nominal`; `actions_dispatched` counts claimed actions, not successful completions.
   Claims carry fresh owner and claim IDs. Completion/failure updates require the matching running claim and refuse second terminal writes. Task state and one uniquely identified event commit in a single SQLite transaction (`task_terminal_events`). JSONL is a retryable projection of those stored events; consumers should deduplicate by `event_id`. An interrupted trailing append is discarded before retry. A transcript export or heartbeat receipt write failure degrades the returned/broadcast pulse; it does not undo a committed task outcome. A heartbeat receipt's JSONL projection failure changes its receipt and SQLite row to `degraded` before the receipt transaction commits, matching the returned receipt, broadcast and crumb command vector.

   Failed persistence retains the original outcome on the database owner for later pulses, including after replacing the heartbeat engine. Only persistence is retried, never task execution. Flushing a retained failure or cancellation degrades that pulse even when it dispatches no new work. Cancellation retains `abandoned: outcome unknown`; action panics become failures and do not kill the heartbeat loop. At open and before each pending-work scan, a per-owner lifetime `flock` distinguishes live owners from abandoned running claims, including a peer that dies after this database handle was opened. Dead-owner claims become failed with `abandoned: outcome unknown`, once, without replay. Lock files live beside the canonical database under `<database>.task-owners` and are not unlinked while owners can reference them.
   
   Legacy running rows without owner/claim metadata fail closed during normal open. An operator must first stop **all** processes using this database, including old binaries that do not hold owner locks, and keep them stopped until migration exits. Then run `aegis migrate-legacy-tasks --db-path /absolute/path/to/aegis.sqlite --quiescent` before restarting the runtime. This explicit acknowledgement does not stop services for you. Migration additionally refuses detected live owners and never creates a missing database. It preserves legacy task identity, payload and creation time, assigns migration owner/claim IDs, and atomically marks every unknown running task `failed` with `abandoned: outcome unknown` plus one unique event. It never executes or requeues those tasks; rerunning it does not duplicate terminal events. A transcript projection failure can be retried by rerunning the same command after fixing the transcript path.
   
   Each model health action has a 15 second deadline, including a backend's `check_health` future. The direct HTTP fallback also has connection and total-request deadlines. Expiry records a failed unknown outcome without replay; later pulses can process other tasks.


3. **Local model seat inference** (optional, `src/inference.rs`).
   With `--inference http`, requests go to the OpenAI-style endpoint given by `--model-url` (default `http://127.0.0.1:18006/v1/chat/completions`) with the model name `atlas-lightning-omni`. Without it, the mock backend (`MockInferenceClient` from aien-protocols) answers. `aegis status` then reports the backend as `mock-protocol-model`; the words "NativeTransformer" in that output name the mock, not a real model.

   Port 18006 was the old Modular MAX serving endpoint. A queued `model_health` heartbeat task (or one still queued under its old name `max_health`) checks the model seat through the configured backend, or polls `http://127.0.0.1:18006/v1/models` directly when no backend is set, and reports whether anything answers on port 18006. The project does not use MAX or CUDA on its production path (decision of 2026-10-02); the production runtime in aien-sovereign-core runs its GPU path through AIEN's own Omega kernels.

4. **Vector math** (`src/vector_math.rs`).
   Cosine similarity, Shannon entropy, linear projection and temperature scaling in plain Rust.

5. **Secret resolution** (`src/vault.rs`).
   No plaintext secret files. `VaultResolver` asks `atlas-vault`, then keeps the value in memory. It is a client of that provider, not a TPM (a hardware security chip). The process environment is read only when `AIEN_DEV_SECRET_FALLBACK=1`. Output is redacted with `[REDACTED_BY_ATLAS_VAULT]`.

6. **SQLite persistence** (`src/persistence/`).
   An embedded SQLite store in write-ahead-logging (WAL) mode, so task and message writes survive a crash.

7. **Task workflow and failure handling.**
   A dependency-aware task workflow ([docs/WORKFLOW-DAG.md](docs/WORKFLOW-DAG.md)) and structured agent failure records with effect-safe retry rules ([docs/FAILURE-PROVENANCE.md](docs/FAILURE-PROVENANCE.md)).

## Build

### Prerequisites

- A stable Rust toolchain (`cargo`). The crate uses edition 2021.
- A checkout of [aien-dev/aien-protocols-mv](https://github.com/aien-dev/aien-protocols-mv) next to this one, in a folder named `aien-protocols`. `Cargo.toml` has six path dependencies on `../aien-protocols/crates/`.
- Optional: an OpenAI-compatible model server on port 18006 (the local model seat), only for `--inference http`.
- Linux. The Spark test runs are on aarch64 (Grace Blackwell GB10). The release workflow (`.github/workflows/release.yml`) builds Linux x86_64.

### Get the code and compile

```bash
git clone https://github.com/aien-dev/aegis-runtime-mv aegis-runtime
git clone https://github.com/aien-dev/aien-protocols-mv aien-protocols
cd aegis-runtime
cargo build --release
```

### Run the gateway

```bash
# Operator token: at least 32 printable characters, kept out of the repo.
export AEGIS_OPERATOR_TOKEN="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
./target/release/aegis serve --port 18096
```

Security defaults:
- The gateway listens on 127.0.0.1 only. Any other address needs `--bind <ip> --allow-non-loopback-bind`.
- Every route except a few read-only `GET` routes (`/health`, `/api/v1/health`, `/v1/models`, `/api/v1/skills`, `/api/v1/events`, `/events`) needs the operator token, sent as `Authorization: Bearer <token>` or `X-Aegis-Operator-Token: <token>`. This covers `/api/v1/shell`, `/api/v1/skills/execute`, the task list (`GET /api/v1/tasks`, whose records carry command text and output), every other `POST`, and the WebSocket. With no token configured those routes refuse every request. The token can also come from `--operator-token-file <path>`.
- Skills and shell commands run only from an allowlist: the registered skill names, and for shell exactly `git status`, `git diff`, `git log -1 --oneline`, `ls`.
- `write_file` opens each directory and the leaf relative to a held workspace directory descriptor. Symlinks and multiply linked or non-regular target files are refused; nested directories are created inside that descriptor tree. This containment applies to `write_file`, not to arbitrary shell commands.
- Probe gating runs on every skill and shell call. `AIEN_PROBE_ENFORCE` takes a number above 0.5 and up to 1.0 and can only raise the threshold; any other value keeps the default of 0.5, so it cannot turn gating off.

### CLI commands

`aegis --help` lists `serve`, `tick`, `heartbeat`, `ask`, `agent`, `simd` and `status`. Examples, run from the build folder:

```bash
# Run a single heartbeat tick
./target/release/aegis tick

# Run the heartbeat loop on its own (30 second default)
./target/release/aegis heartbeat

# Send one prompt to the inference backend (add --inference http to reach the local model seat)
./target/release/aegis ask "Analyze system health and pending tasks"

# Run an agent task with multi-turn tool calling (default 8 turns)
./target/release/aegis agent "Summarize the repository status" --max-turns 8

# Show runtime status
./target/release/aegis status

# Print a small vector math evaluation
./target/release/aegis simd --steps 10
```

Commands that use storage write `aegis.sqlite` in the current folder unless `--db-path` is set.

## Test

The full test suite, as the Spark runs it:

```bash
cargo test -- --test-threads=1
```

On 2026-10-19 this passed 158 tests on the Spark at commit `08ea496` (55 library tests plus 103 in `tests/*.rs`).

The vector math functions are checked against an analytical reference and the committed vectors in `tests/fixtures/simd_math_vectors.json`:

```bash
cargo test --lib vector_math::tests::test_analytical_parity
```

### Gates and where the evidence lives

Every test that decides a merge runs on the DGX Spark, not on GitHub ([docs/PIPELINE.md](docs/PIPELINE.md)). The gates are the files in `tests/gates/`. Each one calls `tests/gates/run.sh` with one step word, so you can run any of them by hand:

```bash
tests/gates/run.sh fmt
tests/gates/run.sh check
tests/gates/run.sh clippy
tests/gates/run.sh test
tests/gates/run.sh zero-disk-secrets
tests/gates/run.sh anti-slop
tests/gates/run.sh crumb-verify
```

The last one needs the `crumb` tool on your `PATH` (or set `CRUMB`). `.aien/always` lists the gates that run on every commit: `CRUMB-VERIFY`, `AEGIS-ZERO-DISK-SECRETS` and `AEGIS-ANTI-SLOP`.

The Spark signs each result and publishes it to [aien-dev/aien-receipts](https://github.com/aien-dev/aien-receipts/tree/main/aegis-runtime), one folder per tested commit. The only required GitHub check, `AIEN Verification`, verifies that signed result.

## License

Copyright (c) 2026 Drake Stapleton <aien@aienos.com> and AIEN Contributors. Authored by Drake Stapleton in collaboration with AIEN.

Licensed under the GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later). See [LICENSE](LICENSE). Project values live in the nonbinding [COVENANT.md](COVENANT.md), which grants and restricts no legal rights.

## Human-Directed Agentic Engineering

Development follows Human-Directed Agentic Engineering (HDAE): humans define goals,
architecture, constraints, acceptance criteria, and stopping points; agents implement,
execute checks, diagnose failures, and revise within those boundaries. Human review
retains responsibility for claims, acceptance, and release decisions. This development
method is distinct from the runtime's autonomous task execution.

For the documented method and a systems-software case study, see Gregory M. Kurtzer,
*Ginux: Building an Independent Kernel That Runs Linux Userspace through Human-Directed
Agentic Engineering* (2026), https://doi.org/10.5281/zenodo.23682548.
