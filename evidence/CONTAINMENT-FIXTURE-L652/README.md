# L652: portable workspace-containment fixture

Parent feature: [aegis-runtime-mv#68](https://github.com/aien-dev/aegis-runtime-mv/issues/68), implementation L650. This is a separately scoped **test-fixture repair**, not an implementation of that feature and not a runtime security fix.

## Reproduced cause

On unchanged product base `f85dbe3910dd2c29faff74a3a96d657531ca1320`, the library baseline had54 passing tests and one failure, `skills::tests::test_filesystem_skills_with_workspace_containment`. The old fixture used `../../etc/passwd` from a random temporary workspace. That only addresses an existing outside file for some temporary-directory layouts. With the Mac's deeply nested TMPDIR it points at a nonexistent path, so the existing validator correctly returns `Path not found: File does not exist: ../../etc/passwd`, before reaching the containment check. The test expected `Security rejection` instead. `red.log` retains the actual exit101 and diagnostic; only an assertion diagnostic was added to expose the existing error.

## Correction and discrimination

Functional fixture commit: `468b680`.

Only `src/skills.rs`'s test is changed. Create a temporary parent containing a real `outside.txt`, put the authorized workspace in a child directory, then attempt `../outside.txt`. Keep both original rejection assertions: the read must fail and report `Security rejection`. The test also proves the outside file exists and its contents remain unchanged. The positive in-workspace write/read/list checks are retained.

No production guard, permissions, capability, runtime behavior or test expectation is relaxed. `src/security.rs` is byte-identical to the base in the delivered candidate.

- `green.log`: corrected fixture passes, exit0.
- `missing-guard.log`: temporarily disabled **only the read-containment guard** in this owned worktree. The read then succeeded and the preserved `!escape_res.success` assertion failed, exit101. This proves the fixture exercises the actual boundary, not a missing-file error.
- The mutation was restored before qualification; no mutated source is delivered.
- `full-suite.log`: `cargo test --locked`, **153 passed, 0 failed, 0 ignored**, across the complete native suite.
- `fmt.log`: formatting check exit0.
- `clippy.log`: all-target Clippy with warnings denied, exit0.

Protocol sibling pinned to `4529f393546c87d5ae673e5d66d700c8e8c5db03`; tool/platform and raw-log hashes are in `qualification.json` and final input hashes in `inputs.json`.

## Replay

Use Rust1.97.1/Cargo1.97.1 and two owned sibling checkouts. Check out this fixture branch's exact final revision, then the protocol revision above at `../aien-protocols`.

```sh
mkdir -p evidence/CONTAINMENT-FIXTURE-L652/replay-tmp
TMPDIR="$PWD/evidence/CONTAINMENT-FIXTURE-L652/replay-tmp" cargo test --locked
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
```

The failing-original reproduction uses base `f85dbe3`, a deeply nested TMPDIR and the exact test filter recorded in `inputs.json`. The missing-guard control replaces the condition in `resolve_read_path` from `if !canonical.starts_with(&self.root)` to `if false`, runs that same filter, expects exit101 at `!escape_res.success`, then restores the original guard. Do this only in an owned disposable worktree, never in the runtime candidate. Never overwrite the original receipts while replaying.

This source is a supplemental input to L650, not a separate competing PR. Issue68's gateway/heartbeat, real kill/network, occurrence/receipt and epoch acceptance remain open. Independent whole-feature review and sole Renda integration still follow that implementation.
