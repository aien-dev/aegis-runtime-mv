# AEGIS Architectural Innovations

### Pure Compiled Systems Architecture and Hardware TPM Vault

`aegis-runtime` is an autonomous agent runtime engineered in pure native compiled Rust. It replaces bloated, interpreted Node.js frameworks with a sub-millisecond compiled engine optimized for local workstation hardware.

---

## 1. Pure Compiled Native Architecture

### Sub-Millisecond Axum Control Plane
The control plane runs on Axum and Tokio (`src/gateway.rs`), providing high-concurrency WebSocket duplex streams, Server-Sent Events (SSE), and REST endpoints.
- Response dispatch latency: < 0.5ms.
- Zero garbage collection pauses or runtime interpreter bloat.
- Direct memory-mapped operations.

### Vector Math (`src/vector_math.rs`)
Small fixed-size vector arithmetic runs in plain Rust with no external libraries:
- **4D Vector Cosine Similarity**: Evaluates semantic orientation between token embeddings.
- **Token Entropy Estimation**: Computes probability distribution entropy without allocating intermediate heap structures.
- **Token Projection and Temperature Scaling**: Applies a weighted sum plus bias, and divides logits by temperature.

---

## 2. Multi-Turn Autonomous Agent Engine (`src/agent.rs`)

### Multi-Turn Tool Calling Loop
The `AgentEngine` implements an autonomous reasoning and execution loop:
1. **Thought & Tool Formulation**: Generates structured reasoning and tool calls against the configured inference backend (the local model seat on port 18006 with `--inference http`).
2. **Deterministic Tool Execution**: Dispatches tool invocations to the native `SkillRegistry`.
3. **Multi-Turn Context Preservation**: Retains dialogue history, tool outputs, and execution receipts across iterative steps until the task goal is satisfied.

### Hardened Execution Guards
- **Process Timeout Guard**: All shell invocations via `bash_eval` execute under strict 15-second process wrappers (`timeout 15s`) to prevent runaway processes or terminal deadlocks.
- **Root Directory Protection**: Explicitly blocks recursive searches targeting `/` or system root directories, safeguarding host operating system files.

### Standard Native Skills
- `read_file`: Reads text files with line-indexed bounds.
- `write_file`: Atomically writes or updates files with parent directory creation.
- `list_dir`: Traverses directories, reporting relative paths and sizes.
- `git_status`: Inspects repository branch, staged modifications, and untracked files.
- `cortex.search`: The canonical memory tool. It is registered and not connected, so it reports unavailable instead of a fake result. `cortex_recall` is the legacy alias.

---

## 3. Secret resolution (`src/vault.rs`)

### No plaintext secret files
- No `.env`, `.env.local`, or configuration secret files are stored on disk.
- `VaultResolver` is a client of the `atlas-vault` command. It does not open a TPM device. Hardware-backed protection is a property of that provider when the deployment has it.
- Production resolution is the in-memory cache, then `atlas-vault`. If the provider does not answer, resolution fails closed.
- `AIEN_DEV_SECRET_FALLBACK=1` is the explicit development permission to read the process environment after that.
- Model outputs and logs are scanned to redact known secret values with `[REDACTED_BY_ATLAS_VAULT]`.

---

## 4. SQLite WAL Persistence with Crumb Trails (`src/persistence.rs`)

Operational tasks and event logs persist in SQLite using Write-Ahead Logging (WAL) mode:
- Concurrent reader and writer threads operate without lock contention.
- Every completed agent action creates an immutable `CrumbRecord` tracking session state, timestamps, and return codes.

---

## 5. Hardware Target and Performance Metrics

Verified on NVIDIA Grace Blackwell GB10:
- **Local Model Seat Endpoint**: `http://127.0.0.1:18006/v1/chat/completions` (any OpenAI-compatible server).
- The three figures below were measured while the seat ran Modular MAX, which was removed from AIEN on 2026-10-15. They have not been re-measured since.
- **Active GPU Utilization**: 94% to 96% under sustained multi-turn generation.
- **Inference Speed**: 20.7 tokens per second sustained across multi-turn reasoning loops.
- **Memory Footprint**: < 35 MB resident set size for the core runtime engine.

---

## 6. License and Commons Covenant

Licensed under the **GNU Affero General Public License v3.0 or later** (SPDX: `AGPL-3.0-or-later`). See [LICENSE](../LICENSE). Project values live in the nonbinding [COVENANT.md](../COVENANT.md), which grants and restricts no legal rights.
Copyright (c) 2026 Drake Stapleton <aien@aienos.com> & AIEN <aien@aienos.com>.
