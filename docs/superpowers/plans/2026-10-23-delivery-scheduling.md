# Durable delivery, scheduling and admission implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this cohesive task with TDD. Steps use checkbox syntax. Drake's standing instruction opts out of approval questions: decide, act, report. Do not ask for permission between tasks.

**Goal:** Implement and qualify every acceptance condition of aegis-runtime-mv#68 in the actual gateway and heartbeat paths, not just a detached state model.

**Architecture:** Keep deterministic content identities and transition validation separable from the SQLite and transport adapters. Share one durable authority between gateway and heartbeat; persist intent before an external effect, fence stale owners and generations, and explicitly preserve unknown outcomes across restart. Reuse current native Rust/SQLite/sha2 and canonical protocol verification where appropriate; no copied Hermes implementation.

**Tech stack:** Existing Rust2021 crate, Axum/Tokio, SQLite, SHA-256, standard signature verification only if no existing verified receipt boundary fits.

**Spec:** https://github.com/aien-dev/aegis-runtime-mv/issues/68 (body retrieved before this plan). Prior art: Hermes Agent, Nous Research, MIT, commit5480918; original reimplementation only.

## Global constraints

- Base f85dbe3910dd2c29faff74a3a96d657531ca1320; isolated branch feat/68-delivery-scheduling-guarantees.
- Replies: ledger commit before send, stable identity hashes session key + message reference + exact reply text with unambiguous field encoding. Re-recording identical content collapses to one row. Crash-recovered delivery visibly says it may be a duplicate.
- Owner is actual pid + actual process start time, not pid alone or a caller-provided `verified=true` flag. Ownership changes use compare-and-set; preserve per-session FIFO, runtime epoch and monotonically fenced generations. Old-epoch started admission rows become unknown and need an explicit operator decision, never automatic replay.
- Scheduled occurrences are identified by exact scheduled instant. Persist the schedule advance and occurrence before invoking the job. Persistence failure means intentionally no fire. Unknown outcomes pause the job and carry a concrete expected receipt path; only a cryptographically verified, trusted, exactly matching receipt resolves that uncertainty.
- Reclamation measures silence since genuine last progress, not total duration or an artificial timer that hides a wedge. Reclamation never automatically reruns an uncertain side effect.
- Put each guarantee beside its actual code choice and attribute the mechanism in module headers. No new service, model, engine, Python runtime, or GPU/hardware experiment.
- Never weaken operator authentication, workspace capabilities, zero-disk-secret rules or existing tests. Do not edit historical receipts or G1-G6 PRs. L342 owns legacy SQLite journal/sync tuning; do not change that logic.
- Test data, credentials and replies are synthetic and must be labelled; transport, file persistence and process-kill controls must be real. No production DB, secrets, live endpoints or default service ports. Use127.0.0.1:0 and own temporary dirs.
- Whole-response identity necessarily requires the complete text before committing an obligation. If a streaming path must buffer, bound it, document that timing tradeoff and test it; do not pretend tokens were durably identified before they existed. All user reply paths need explicit guarantees, not silent legacy best-effort exceptions. Stable client identity requirements must fail closed and be documented, not silently invented on recovery.
- Push source/evidence branch only, no PR creation/update/merge or issue closure; sole Renda publishes. No auto-closing keyword until integrated acceptance.

## Review focus

1. Kill after transport accepted bytes but before completion acknowledgement: recover one pending obligation with an unmistakable duplicate marker, no hidden loss.
2. Two runtimes or pid reuse, stale leases and generation races: no stale owner can finish or unpause a newer occurrence/request.
3. SQLite commit/read-only failure: no network send or scheduled effect escapes the persistence barrier; retain the original error rather than fallback success.
4. Forged, wrong-key, wrong-instant, wrong-epoch, wrong-generation or replayed recovery receipts: no pause lift, even when a generic signature is valid.
5. Actual long work with continuing progress versus a silent wedge, clock anomalies and restart: do not kill the busy work or silently redispatch the wedge.

### Task1: Implement the cohesive durable runtime and wire the real callers

**Files:**
- Create focused files under src/durability/ (pure identities/transitions, persistence adapter and OS identity adapter) or an equivalently focused module following the discovered database API.
- Modify src/gateway.rs, src/heartbeat.rs and only necessary exports/startup wiring. Minimal legacy persistence module declaration/adapter access is acceptable; do not retune its existing connections or task operations.
- Create tests/delivery_scheduling_tests.rs plus native real-process kill/network fixtures; add public evidence under evidence/DELIVERY-SCHEDULING-L650/.

**Interfaces:**
- Shared runtime entry point `DurableRuntime`; exact subinterfaces are settled and documented in a short spec before code after inspecting the actual persistence API. No later independent implementer consumes a speculative signature.
- `record_reply(session: &str, message_ref: &str, reply: &str)` returns the same durable obligation for identical inputs and refuses conflicting identity reuse.
- Claims/acks/progress/receipt resolution consume fenced typed tokens, not unfenced string IDs. Gateway and heartbeat share the same epoch authority, not independently reset epochs per request.
- Receipt verification is a real trusted-key verification boundary, not successful JSON parsing, a SHA hash alone, or a Boolean supplied by the caller.

- [ ] Read existing gateway, heartbeat, persistence, auth and tests, then write a concise design/interface record which covers every spec bullet and distinguishes application acknowledgement from socket send success. Resolve compatibility explicitly. Search existing peer branches/components before adding a new part.
- [ ] Pin an owned fresh aien-protocols sibling checkout, record its exact SHA and run the unchanged full native baseline with raw output. Never use or mutate another task's shared sibling checkout. Preserve any baseline failure.
- [ ] Write real failing tests for deduplication/boundaries, commit-before-send, recovered duplicate, occurrence advance-before-run, missing/forged receipt, stale epoch/generation and progress-based liveness. Capture RED before implementation. An import failure alone is not the final discriminating regression.
- [ ] Implement the focused pure transitions plus real SQLite/OS adapters, wire production gateway/heartbeat callers and operator-only reconciliation. Reject runtime ownership/persistence errors before effects. Preserve unknown outcomes and concrete receipt paths through restart.
- [ ] Execute native subprocess SIGKILL cases at controlled send/fire barriers, use actual loopback traffic and effect files, reopen the same real temporary SQLite files, and assert no silent reply loss or second scheduled fire. Do not substitute fabricated fixture outputs or only in-memory state serialization tests.
- [ ] Execute actual busy-progress and wedged-silence controls and the five review-focus families; all controls must discriminate against missing barriers/fences/verification. Run full native suite, formatting, Clippy, build and repository preflight. Expected: every required check exits0, no skipped required gate. Report unrelated or environment blockers honestly rather than removing checks.
- [ ] Commit/push working milestones and raw proof. Record exact source/protocol/tool hashes, commands, exit codes and original failed probes. Expected: final head is clean and matches remote. Do not call implementation complete if any issue acceptance row is not exercised in real callers.
- [ ] Leave exact source/evidence and remaining integrated publication gates in the handoff. Independent whole-candidate review is the coordinator's next gate, not something to invent or claim merely because tests pass.
