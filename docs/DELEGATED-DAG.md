# Delegated DAG: one authority per fact (issue #54, lane G1)

**Status:** DRAFT. Sections 3 and 4 hold decisions that are **proposed, awaiting owner acknowledgement** from the aien-sovereign-core#379 owner and the AIENOS owner. Until both acknowledge on the PR, nothing here is accepted and no recovery code may treat it as settled.

Parent: aien-dev/aegis-runtime-mv#53 (lane G of aien-dev/aien-architecture-mv#231). Wire format: PST1 Draft v0, aien-protocols `specs/pi-spark-task/SPEC.md` (aien-dev/aien-protocols-mv#34, branch `integ/allen-pispark-lane-c`).

## In plain words

When the Pi hands the Spark a job made of several steps, many facts describe it: what the user asked for, which steps exist and in what order, whether each step is done, whether each real-world change happened, who approved it and where the proof is. Each fact gets exactly one owner. AEGIS keeps only pointers to those owners, so after a crash there is no second copy that can disagree with the real one.

## 1. Ground truth read for this document

Read on 2026-10-16. Each line names what the code says, not what a plan says.

| Source | What it says |
|---|---|
| aegis `README.md` line 3 | AEGIS is not part of the WHOLE-SYSTEM-E2E v1 release. The production runtime in aien-sovereign-core carries the capability checks, the approval desk, effect authorization and fail-closed behaviour. |
| aegis `docs/WORKFLOW-DAG.md`, `src/orchestration/workflow.rs` (`971d450`) | The DAG is memory only. `WorkflowSnapshot` serializes; nothing stores or reloads it. |
| aegis `src/persistence/sqlite/schema.rs` | Tables for sessions, messages, runs, actions, approvals, audit events, turns, heartbeats, single queued `tasks` (id, type, payload, status, result), memory crumbs, skills. None holds a workflow id, a dependency edge or an effect pointer. |
| aien-sovereign-core `crates/aien-runtime/src/objective.rs` | Objective id = SHA-256 of `AIEN_E2E_OBJECTIVE_V1`, LF, the objective text (64 lowercase hex). The record `objectives/<id>.json` lives under the provenance root on the Spark. The operator tool (`aien-cli objective record`) writes it; the daemon only reads it. |
| aien-sovereign-core `crates/aien-runtime/src/effects.rs` | Durable effect intents on the compose home's Cortex journal, states OPEN, DONE, NOT_DONE, UNRESOLVED. The world decides the state. An effect is never re-run by that module; a retry after NOT_DONE needs a new authorization. Grants are journal records (`approved_grant`, `minted_grant`) and, since sovereign-core #408, may be bound to an objective record (`GrantObjective`), re-checked before every intent. Refusals by name: `NotAuthorized`, `AlreadySpent`, `ReconciliationRequired`, `Stopped`, `ObjectiveMissing`, `ObjectiveMismatch`. |
| aien-sovereign-core `crates/aien-mcp/src/approval.rs` | A second desk, `ApprovalDesk`, for the MCP effect lane. Grants are bound to an intent digest, idempotency key, world and J-node, and live in memory only: a restart forgets every grant. |
| aien-dev/aien-sovereign-core-mv#379 (open) | No durable objective record with its planned effects, no `resume-objective` command. Its text says "grants live in memory only"; that is true of the MCP desk, not of the compose ledger grants above (finding F1 below). |
| AIENOS `docs/adr/0018-allen-subject-state-object.md` | The subject's standing intents are 96-byte rows with a `kind` and a payload of two u64 values. v0 defines one kind, `GOAL_LATENCY`. At most one ACTIVE intent per (kind, payload[0]) slot. |
| aien-architecture `docs/hardening/causal-id-join-v0.md` sections 2.1 and 2.3 (DRAFT) | One 32-byte cause id, minted where the outside request enters AIEN, copied across every handoff and never minted again. |
| PST1 Draft v0 sections 4, 5, 8 | Header carries `cause_id`, `origin_run_id`, `objective_id`, `subject_id`, `parent_task_id`, `task_id`, `idempotence_key`, `effect_class`. Status comes from authoritative state on the Spark. Uncertain effects are reconciled against the sovereign-core effect ledger before any retry. |

**Finding F1.** There are two grant authorities in aien-sovereign-core today. The compose effect ledger's grants are durable (Cortex journal) and can be objective-bound (#408). The MCP `ApprovalDesk` grants are memory only. aegis-runtime-mv#59 (G6) and aien-sovereign-core-mv#379 both name the MCP desk. Section 3, decision D3, proposes which one a delegated task uses.

## 2. One authority per fact

AEGIS stores **pointers only**: ids and digests that name a record held elsewhere. `WorkflowSnapshot` and workflow events are hints for display, never a source for recovery.

| Fact | The one authority | Id that names it | What AEGIS stores |
|---|---|---|---|
| Cause of the work | The Pi, at the user's turn (causal id join 2.1, 2.3) | `cause_id`, 32 bytes (PST1 header) | The cause id, copied. AEGIS never mints one; work without it is refused (PST1 `PST_NO_CAUSE`). |
| User objective | aien-sovereign-core objective record on the Spark (`objective.rs`), with the Pi as requester (D1) | `objective_id`, 64 hex = 32 bytes (PST1 header) | The objective id. |
| Task list and dependency edges | The aien-sovereign-core#379 durable objective record ("the planned effects, their grant ids and states"), extended with one row per AEGIS task and its predecessors (D2) | `objective_id` plus AEGIS `TaskId` per row; PST1 `task_id` and `parent_task_id` | A SHA-256 digest of the submitted spec (task ids and edges, sorted), so a rebuilt spec that differs is refused. |
| Each task's state | Nobody stores it. It is derived on demand from the effect state and receipt of that task (section 3, rebuild rule) | AEGIS `RunId` plus `TaskId` | Nothing durable. The in-memory `TaskState` is a cache. |
| Each effect's state | aien-sovereign-core effect ledger (`effects.rs`): OPEN, DONE, NOT_DONE, UNRESOLVED | Effect intent record id (Cortex host record id) in one compose home | Task id to effect intent id. |
| Each approval | aien-sovereign-core compose ledger grant, objective-bound (#408) (D3) | Grant record id; it names the objective id and the proposal digest | Task id to grant id. Never the grant itself, never an approval decision. |
| Each receipt | aien-sovereign-core: the terminal effect record (ack or reconcile) for an effectful task; the `WorkflowSnapshot.results` digest for a no-effect result | Receipt digest, 32 bytes (PST1 `receipt_reference` payload) | Task id to receipt digest. |
| Idempotence of a request | The Pi, which picks the key once per request (PST1 section 8) | `idempotence_key`, 16 bytes, with `task_id` | Request key to `RunId`, so a repeated request maps to the same run. |

Every pointer AEGIS keeps can be thrown away and rebuilt from the authorities. If AEGIS's pointers and an authority disagree, the authority wins and the disagreement is reported.

## 3. Rebuild rule after a restart, fact by fact

The recovery code (G3, aegis-runtime-mv#56) follows these rules. A missing pointer or an unreachable authority is a named refusal, never a fresh start.

| Fact | Rule |
|---|---|
| Cause id | Taken from the Pi's `resume_request`. If it differs from the stored cause id: refuse. |
| Objective | Re-read the objective record. Missing: refuse (`ObjectiveMissing`). Text changed: refuse (`ObjectiveMismatch`). |
| Task list and edges | Rebuilt from the durable objective record. Its digest must equal the stored spec digest; otherwise refuse. Until aien-sovereign-core#379 lands, no such record exists, so the host stand-in in G3 is the only source and the physical rebuild is NOT_RUN. |
| Effectful task | Ask the effect ledger by intent id. DONE with a receipt: Completed, never run again. DONE without a receipt: refuse (negative control). OPEN or UNRESOLVED: the uncertain state from G2, dependents blocked, nothing retried until reconciled. NOT_DONE: may run again only under a grant that is still valid or newly issued. No intent recorded and no grant spent: not started. |
| No-effect task | Never finished: Ready again (re-running it changes nothing outside). Finished with a result digest: Completed. |
| Approval | Re-checked against the desk before any effect. Stale, foreign or forged: refuse with a named reason (G6). |
| Receipt | Must exist at the authority for every Completed effectful task. |

## 4. Proposed decisions (awaiting owner acknowledgement)

Each decision is **proposed, awaiting owner acknowledgement**. Acknowledgement requests: aien-dev/aien-sovereign-core-mv#379 (sovereign-core owner, D1 to D4) and aien-dev/aienos-mv#399 (AIENOS owner, D1).

### D1. Where the Pi-side objective lives (proposed)

**Proposal:** the sovereign-core objective record on the Spark is the one authority, with the Pi as the requester. The Pi keeps only the cause id and the objective id as pointers. No new ADR 0018 standing-intent kind.

Reasons:
1. Grants and effect intents already bind to this record (#408) and are re-checked against it before every intent. A second objective store on the Pi would be a second source of truth for the same fact.
2. The objective id is a pure function of the text (`objective.rs`). The Pi can compute and check it without trusting the Spark, which gives the Pi ownership of *what was asked* without holding the record.
3. ADR 0018 standing intents describe what the subject continuously intends, one ACTIVE row per (kind, payload[0]) slot, with a 16-byte payload. A per-request user task is short-lived, many may be open at once, and its 32-byte objective id does not fit the payload. Fitting it would change the frozen-pending format of kind 24.

Gap stated plainly: today only the operator tool writes objective records. A path where a Pi request causes the record to be written belongs to aien-sovereign-core#379, not to AEGIS.

### D2. Where the task list and edges live (proposed)

**Proposal:** inside the aien-sovereign-core#379 durable objective record, one row per planned task (AEGIS `TaskId`, predecessors, effect class, grant id, effect intent id). AEGIS submits the spec; it does not store it durably.

Reason: #379 already plans "the planned effects, their grant ids and states" in that record. Adding the edges there keeps one record per objective instead of a parallel workflow table in AEGIS's SQLite.

### D3. Which grant a delegated effect uses (proposed)

**Proposal:** the compose effect ledger grant (durable, objective-bound, #408), not the in-memory MCP `ApprovalDesk` grant.

Reason: a delegated task must survive a Spark restart between approval and effect (parent item 5). An in-memory grant is forgotten on restart, so the task could never finish without a new human approval, and AEGIS could not tell a forgotten grant from a forged one. G6's fake desk uses an opaque grant id so either choice fits the same wrapper.

### D4. Whether the delegated DAG stays in AEGIS (proposed)

**Proposal:** the host first cuts G2 to G6 stay in AEGIS, written as pure functions behind traits (`EffectLookup`, `GrantDesk`) with no store, no network and no authority. Before the physical run of lane H (aien-dev/aien-architecture-mv#232), the DAG moves next to the sovereign-core runtime that serves the Pi on the Spark.

Reasons:
1. README line 3: the desk, effect authorization and fail-closed behaviour live in aien-sovereign-core, and AEGIS is outside release v1. A delegated DAG that runs real effects must sit beside them.
2. The existing DAG (`workflow.rs`, 1022 lines at `971d450`, and 25 tests in `tests/workflow_dag_tests.rs`) lives here. Moving it first would add a port to every child issue without changing what they prove. The trait seams make the later move a change of host, not of rules.

## 5. Fire drills (G4, aegis-runtime-mv#57), frozen before code

Frozen 2026-10-16 in its own commit, before any drill test was written. A row changes only by a later commit that says why; the tests are fitted to the rows, never the reverse.

Host stand-ins only. The stand-in world plays the authorities of section 2: a desk that issues single-use grants and opens one effect intent per grant, an effect ledger (OPEN, DONE, NOT_DONE, UNRESOLVED) whose reconcile checks the world, receipts, and an **effect counter** that counts every time the effect touches the world. A "kill" drops everything AEGIS holds in memory; the stand-in world survives, as durable records would. "Restart" is `rebuild` (G3) over the pointers AEGIS had stored before the kill. A human re-approval is the test issuing a new grant only when the rebuilt status asks for one.

Every row checks two things at the end: the effect counter shows no duplicate final effect, and every status reported on the way was true when it was reported. With one task at a time, every row gives the identical sequence of steps and states on two runs.

| Row | Drill | Stand-in setup | Truthful status after the restart | Final effect count | Deliberate break that must turn the row red | First cut |
|---|---|---|---|---|---|---|
| 1 | Kill between approval and effect | Grant issued and its id stored; the desk opened the intent; the kill comes before AEGIS stored the intent id and before the effect | `write` Uncertain (the desk names the OPEN intent), `report` Pending. After reconcile (NOT_DONE): `write` WaitingForApproval, since the grant is spent. After a new grant and the resumed run: Completed | 1 | A task with no stored intent id is treated as not started (the desk's intent is ignored) | Test |
| 2 | Kill after the effect, before acknowledgement | Intent opened, effect applied, kill before the ack and before the receipt is stored | `write` Uncertain, `report` Pending, no new approval asked. After reconcile (DONE, receipt): Completed, and `report` Ready | 1 | Retry when uncertain: an OPEN intent is treated as needing a new approval, so a human approves it again | Test |
| 3 | The same request sent twice with one idempotence key | The first request ran to Completed and its pointers are stored under its key | The second request is answered with the first run id and Completed; nothing starts | 1 | Admission ignores the key and starts a fresh run for the repeat | Test |
| 4 | Link cut while text is streaming | `text_chunk` ordinals 0 to n sent, link drops mid stream | Task state comes from the records, unchanged by the cut; chunks resume after the last ordinal the Pi holds, none repeated as new | 0 (compute only) | Resend from ordinal 0 as new chunks | Later |
| 5 | Pi restart, then a resume request carrying the old cause id | Workflow mid run when the Pi restarts | Resume summary built from `rebuild`; a resume request with a different cause id is refused | 1 | Accept a resume request whose cause id differs from the stored one | Later |
| 6 | Cancel racing a completion | Cancel arrives while the effect is being acknowledged | Completed if the ledger says DONE with a receipt; Cancelled only if no intent was opened; Uncertain otherwise. Never Cancelled while the ledger says DONE | At most 1 | Report Cancelled whenever a cancel was received | Later |
| 7 | A prerequisite failing | `write` fails with no effect | `write` Failed, `report` Skipped and never run | 0 | Run the dependent anyway | Later |

## 6. What is not here

No schema and no wire bytes. The physical Pi and Spark run belongs to lane H.
