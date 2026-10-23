# L650 handoff (implementation in progress)

Branch: `feat/68-delivery-scheduling-guarantees`, initial plan-only HEAD `d734a99`, base `f85dbe3910dd2c29faff74a3a96d657531ca1320`.

- Issue 68 was fetched from GitHub and its exact response is archived as `issue-68.json` (SHA256 in `input-hashes.txt`).
- Owned sibling `../aien-protocols` cloned from its origin; exact HEAD `4529f393546c87d5ae673e5d66d700c8e8c5db03`.
- Native baseline `cargo test --all-targets`: exit 101, 54/55 unit tests passed. Existing failure: `skills::tests::test_filesystem_skills_with_workspace_containment`, assertion at `src/skills.rs:491` expects "Security rejection". Raw full output: `baseline-cargo-test.log`. Preserve as baseline failure; do not silently fix unrelated behavior.
- Existing product has no delivery ledger, scheduled occurrence mechanism or admission epoch. Real gateway callers currently ignore `record_turn` failures; streaming emits before complete reply exists. Heartbeat task claims are not occurrence receipts. `Database` shares the SQLite connection internally but does not expose it except via `to_async`.
- Design/interface record is in `docs/superpowers/plans/2026-10-23-delivery-scheduling-design.md`.

## Current acceptance status

Not implemented or qualified yet. Specifically missing: shared DurableRuntime wired into production callers; content-addressed reply ledger with commit-before-effect; duplicate-marker recovery; exact-instant schedule persistence, verified receipt/key trust boundary; fenced per-session admission and OS process start identity; native kill/network acceptance, progress liveness controls, final full suite/Clippy/preflight; milestone pushes and remote verification. The baseline only establishes a known unrelated failure; it is not acceptance evidence.
