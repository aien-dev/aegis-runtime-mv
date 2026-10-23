# L650 design/interface record

Issue contract read from GitHub issue 68; raw issue body is archived under `evidence/DELIVERY-SCHEDULING-L650/issue-68.json`.

## Existing boundaries and compatibility

`Database` in `src/persistence/legacy.rs` owns the SQLite connection behind `Arc<Mutex<Connection>>`; `to_async()` shares that same connection. Gateway currently calls inference then best-effort `record_turn` in chat/OpenAI/Discord/WebSocket, while OpenAI streaming directly returns provider chunks and has no complete-response barrier. Heartbeat claims legacy `tasks` rows and executes several task types, including shell side effects, without a persisted occurrence barrier. `GatewayState` and `HeartbeatEngine` are created separately in `main.rs`, so the new `DurableRuntime` must be constructed once and cloned into both. Existing legacy task operations and SQLite journal/sync tuning are out of scope.

## Interface decisions

`DurableRuntime` is a cloneable façade over one shared connection and a process-owned epoch. Database operations run in SQLite immediate transactions and propagate errors; no caller sends or invokes a scheduled effect unless the obligation/occurrence transaction commits. Process ownership uses OS pid and OS process start identity (never a supplied boolean or pid-only identity). Reply identity hashes length-prefixed UTF-8 session key, message reference, and exact complete reply text; conflicting reuse of the same message reference is rejected. An unknown send is retained and recovery retries with an explicit duplicate marker. Socket acceptance is not application acknowledgement, so HTTP/SSE/WebSocket can only report transport acceptance and remain at-least-once until an explicit ack is recorded.

Streaming has no stable complete reply before generation completes. To guarantee durable complete-content identity, collect the full result with a strict cap, persist it, then emit chunks; document the latency and memory tradeoff and refuse overflow before emitting reply bytes. Stable client/message reference is required. Existing routes that cannot supply one fail closed rather than minting an unrepeatable recovery identity.

Heartbeat occurrence identity is `(job_id, exact scheduled_at instant)`. Persist advance plus occurrence before effect. A started occurrence recovered after process epoch change is `unknown`, never auto-fired again. Unknown outcomes pause the job with an expected receipt path; only trusted public-key signature verification over a canonical receipt bound to job, instant, epoch, and generation can resolve it. Liveness is monotonic elapsed silence since last durable progress; elapsed run duration alone does not reclaim. Admission uses persisted epoch/generation fencing, FIFO per session, and old-epoch started rows require an operator decision. Operator reconciliation remains behind existing operator authentication.

## Scope caveat recorded before implementation

Current public interfaces lack message references, cron schedule definitions, and a cryptographic receipt authority. Any compatibility adapter must not silently fabricate stable identities or trusted receipt keys. Existing constructors may remain for tests only if the production startup path installs one shared durable runtime. No Hermes implementation code is reused; attribution belongs in the new module header.
