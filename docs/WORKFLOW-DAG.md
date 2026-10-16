# Workflow DAG (issue #39)

A small dependency-aware task layer for multi-step agent work. It decides
when a caller-supplied task may start. Nothing else.

## Existing machinery

Inspected before writing any code.

| Area | What exists |
|---|---|
| `src/orchestration/run.rs` | One `Run` with `RunState {Running, WaitingForApproval, WaitingForTool, Completed, Failed, Cancelled}`, step and action counters, a `Trigger`. A run is a single linear lifecycle. |
| `src/orchestration/budget.rs` | `RunBudget` counters (steps, inference calls, actions, wall seconds) for one run. |
| `src/orchestration/termination.rs`, `trigger.rs` | Why a run ended; what started it. |
| `src/agent.rs` | `AgentEngine::execute_task`: a sequential model/tool loop. Since #38 it classifies tool failures (`FailureRecord`), retries only idempotent no-effect cases, and stops on `ApprovalPending`. |
| `src/failure.rs` (#38) | `FailureClass`, `EffectCertainty`, `FailureRecord`, `CancelToken`. Reused here. |
| `src/events/{event,envelope,bus}.rs` | `AegisEvent`, `EventEnvelope` (has a `sequence` field), broadcast `EventBus`. |
| `src/execution/{broker,action,receipt}.rs` | `ExecutionAuthority` and action receipts: the only authority for effects. |
| `src/sessions`, `tests/aegis_tests.rs` | Ids (`RunId`, `SessionId`), stores, and the integration test conventions (tokio tests, scripted fakes). |

Already present: the single-run lifecycle, approval-gate states, a per-run budget, cancellation tokens, failure vocabulary, events.

Absent before this change: prerequisite edges between tasks, readiness tracking, bounded concurrent execution of independent tasks, propagation of a prerequisite's failure to dependents, a workflow-level event order. No equivalent DAG exists, so `src/orchestration/workflow.rs` is new. It is not an inference scheduler: AIEN's `aien-scheduler` and the swarm manager are untouched and never called.

## Model

- `TaskId` newtype. `WorkflowSpec::builder().add_task(id, &[deps], runner)...build()` rejects empty ids, duplicate tasks, duplicate edges, self dependency, unknown predecessors and cycles with typed `WorkflowError`.
- `TaskState {Pending, Ready, Running, WaitingForApproval, Completed, Failed(FailureRecord), Cancelled, Skipped{prerequisite}}`.
- `TaskRunner` is the task body and returns `TaskOutcome {Completed(value), Failed(ToolFault), ApprovalPending, Cancelled}`. `certainty_on_interrupt()` says what is known about effects if the DAG interrupts the body (timeout, cancel); the default is `Uncertain`.
- `WorkflowConfig {run_id, session_id, max_parallel, per_task_timeout, total_budget}`.

## Rules the executor enforces

- A dependent becomes Ready only when every prerequisite is `Completed`.
- A prerequisite that is `Failed`, `Cancelled` or `Skipped` makes the dependent `Skipped` with a `FailureRecord` of class `Rejected`, certainty `NoEffect`, cause `prerequisite failed: <id>`. This is transitive.
- `WaitingForApproval` is not success and not terminal. Dependents stay `Pending`; the report lists them in `unfinished` and the status is `WaitingForApproval`.
- The DAG never retries or reschedules anything. An `Uncertain` outcome runs once and is final.
- Timeout gives `Failed` with class `Timeout` and the runner's certainty. Cancellation (token or total budget) cancels running tasks and every Pending/Ready task and starts nothing new.
- Parallelism is bounded by a `tokio::sync::Semaphore`. The coordinator only awaits; task bodies run on spawned tasks. No `block_on`.
- The DAG authorizes nothing and caches no decision. Real effects go through the agent/skill gate path or `ExecutionAuthority` inside the task body.

## Events and replay

Every transition takes a workflow sequence number under one lock, together with the state change, and is published in that same critical section. New `AegisEvent` variants: `WorkflowTaskReady/Started/Completed/Waiting/Failed/Cancelled/Skipped`, `WorkflowFinished`. They carry `workflow_seq` and go out with `EventEnvelope.sequence` set to it, so sorting by sequence replays the exact order. With `max_parallel = 1` the whole order is fixed for a given spec; with more parallelism the order of simultaneous completions follows real completion order, but the sequence is still gapless and dependency-consistent. Existing variants are unchanged; the canonical mapping returns `None` for the new ones.

Results: `WorkflowSnapshot.results` maps task id to a sha256 digest of the result value. Failure records for Failed, Skipped and Cancelled tasks are in `WorkflowReport.failures`.

## Agent adapter

`AgentTaskRunner` runs one `AgentEngine::execute_task` as a task; `agent_outcome` maps its result (failure class and certainty from the last tool failure, `ApprovalPending` stays pending, an incomplete run is `Failed/Uncertain`). It is a thin seam and is not exercised against a live model in tests.

## Restart boundary

Workflow state is in memory only. After a process restart nothing is recovered and no task is resumed or re-run. `WorkflowSnapshot` is serializable (tested) so a later change can persist it, but no schema, table or recovery path exists. A resumed workflow must not re-run a task whose effect is `Uncertain`; that recovery policy is not designed here. Which record owns each fact of a delegated task, and the rebuild rule after a restart, are in [DELEGATED-DAG.md](DELEGATED-DAG.md).

## Not claimed

AEGIS is outside release v1; nothing here is wired into a release path or the frozen E2E contract. Timings printed by the demo test compare `max_parallel = 1` against 2 with sleep-based fake tasks on CPU. They are informational, and no GPU or throughput claim is made.
