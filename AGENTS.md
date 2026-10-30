# Agent Integration Guide: aegis-runtime

This document defines the interface and protocols for autonomous agents interacting with or contributing to `aegis-runtime`.

Development follows [Human-Directed Agentic Engineering (HDAE)](README.md#human-directed-agentic-engineering).
Humans define goals, architecture, constraints, acceptance criteria and stopping points;
agents implement, execute checks, diagnose failures and revise within those boundaries.
Human review retains responsibility for claims, acceptance and release decisions.
The README cites Gregory M. Kurtzer's documented method and systems-software case study (2026).
This development method does not rename or limit the runtime behavior described below.

## Autonomous Operating Rules
1. **Branch Isolation**: Never commit directly to `main`. Create feature branches (`feat/`, `fix/`, `perf/`), execute preflight checks, and open public Pull Requests immediately.
2. **Zero Disk Secrets**: Never write plaintext `.env` files. Secrets resolve in memory from `atlas-vault get <KEY>`. `VaultResolver` is a client of that command, not a TPM. Read the process environment only when `AIEN_DEV_SECRET_FALLBACK=1`. Redact secrets with `[REDACTED_BY_ATLAS_VAULT]`.
3. **Unslop Standard**: Zero em dashes (`-`) and zero en dashes (`-`). Use commas, colons, or parentheses. Do not use AI clichés or conversational filler.
4. **Pure Native Execution**: Core runtime services must compile to native Rust. Do not introduce Node.js or Python daemons.

## Architecture and Endpoints
- **Control Plane**: Port 18096 (Axum REST, SSE, WebSocket).
- **Local Model Seat**: Port 18006 (any OpenAI-compatible model server on GB10, used with `--inference http`).
- **SQLite Database**: `aegis.sqlite` in WAL mode.

## Preflight Verification
Before pushing changes or opening a PR:
```bash
./scripts/agent-preflight.sh
```
All checks must pass with exit code 0.
