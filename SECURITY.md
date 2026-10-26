# Security policy

## Reporting a vulnerability

Report privately. Do not open a public issue, pull request or discussion about it.

1. GitHub: open this repository's Security tab and choose "Report a vulnerability". This
   creates a private advisory that only the maintainers can see. If that option is not shown,
   private vulnerability reporting has not been enabled for this repository yet; use email.
2. Email: drake@aienos.com with the subject `[aegis-runtime-mv] security report`.

Include the commit or release you tested, the component (path), steps to reproduce, and what an
attacker gains. If the report involves a secret, say where it was exposed and do not send the
secret itself. Encrypted mail is not required; do not post proof-of-concept payloads publicly.

## Response targets

This project has one maintainer; the targets below are best effort and stated honestly.

- Acknowledgement: within 3 working days.
- Triage and severity: within 7 days of acknowledgement.
- Fix or mitigation for confirmed issues: aim for 30 days for high and critical, 90 days
  otherwise, with a status update at least every 14 days until closed.
- Disclosure: coordinated. An advisory is published with credit (unless you ask otherwise)
  once the fix is on the default branch. Default embargo 90 days, negotiable.

## Scope

In scope:

- Reaching a protected gateway route (`/api/v1/shell`, `/api/v1/skills/execute`, task list,
  any `POST`, the WebSocket) without the operator token (`src/auth.rs`, `src/gateway.rs`).
- Running a shell command or skill outside the allowlist, or lowering probe gating below its
  default (`AIEN_PROBE_ENFORCE`).
- `write_file` escaping its held workspace directory descriptor (symlinks, hard links,
  non-regular files).
- The gateway listening on a non-loopback address without `--allow-non-loopback-bind`.
- Secrets reaching disk, logs or responses unredacted (`src/vault.rs`), or the environment being
  read without `AIEN_DEV_SECRET_FALLBACK=1`.
- SQLite storage corruption or injection (`src/persistence/`).
- `.github/workflows/`.

Out of scope: the model server reached with `--inference http`; `atlas-vault` itself; shared
protocol crates (report to `aien-dev/aien-protocols-mv`).

## Supported versions

Only the default branch `main` (version 0.2.0, pre-alpha). The GitHub release `v0.1.0` predates
the rename from OpenClaw and is not supported; `v0.2.0` is an unpublished draft. Fixes land on the
default branch only.

## Status

Pre-alpha and not part of the WHOLE-SYSTEM-E2E v1 release (README). There is no certification and
no bug bounty. See AUDIT_SCOPE.md for what to review and how to build and test it.
