#!/usr/bin/env bash
# tests/gates/run.sh: the one entry point for the local verification gates of
# aegis-runtime. Every step runs the SAME command as the hosted job it mirrors
# (.github/workflows/ci.yml, job "Test & Invariant Verification"). Manifests under
# tests/gates/ name this script plus one step word, so no manifest needs shell
# expansion (ADR 0028 P6). Observations print as "AIEN-OBS <name> <json>".
#
# Sibling checkout: Cargo.toml depends on ../aien-protocols/crates/* by path. The hosted
# job checks out aien-dev/aien-protocols (main) next to this repository; the local worker
# must provide the same sibling. (The hosted job also checks out aien-sovereign-core as a
# sibling; nothing in this repository reads it, so it is not required here.)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
step="${1:?usage: tests/gates/run.sh <step>}"
obs() { printf 'AIEN-OBS %s %s\n' "$1" "$2"; }
need_protocols() {
  if [ ! -d ../aien-protocols/crates ]; then
    echo "run.sh: ../aien-protocols is missing; the hosted job checks out aien-dev/aien-protocols as a sibling" >&2
    exit 3
  fi
  obs sibling_protocols_commit "\"$(git -C ../aien-protocols rev-parse HEAD 2>/dev/null || echo unknown)\""
}

case "$step" in
  fmt)
    need_protocols
    cargo fmt --all -- --check
    ;;
  check)
    need_protocols
    cargo check --all-targets
    ;;
  clippy)
    need_protocols
    cargo clippy --all-targets -- -D warnings
    ;;
  test)
    need_protocols
    cargo test --verbose -- --test-threads=1
    ;;
  zero-disk-secrets)
    echo "Auditing repository for prohibited plaintext secret files..."
    if find . -maxdepth 4 -name ".env*" -not -path "*/.git/*" -not -path "*/target/*" | grep -q .; then
      echo "::error::Prohibited plaintext .env file found. Use an approved AIEN SecretProvider."
      obs env_files_found true
      exit 1
    fi
    obs env_files_found false
    echo "Zero plaintext .env files verified."
    ;;
  anti-slop)
    echo "Auditing repository for em dashes, en dashes, and formulaic tropes..."
    # Pattern is em dash OR en dash, built from UTF-8 bytes so this file (inside the
    # scanned tests/ tree) cannot trip its own scan. Same pattern as the hosted step.
    DASH_PATTERN="$(printf '\xe2\x80\x94|\xe2\x80\x93')"
    BAD_CHARS=$(grep -rnE --exclude-dir=target --exclude-dir=.git --exclude="*.rs" --exclude="*.json" --exclude="*.safetensors" "$DASH_PATTERN" src/ tests/ docs/ README.md || true)
    if [ -n "$BAD_CHARS" ]; then
      echo "::error::Em dashes or en dashes detected:"
      echo "$BAD_CHARS"
      obs dash_hits "$(printf '%s\n' "$BAD_CHARS" | grep -c .)"
      exit 1
    fi
    obs dash_hits 0
    echo "Sovereign voice compliance verified."
    ;;
  *)
    echo "run.sh: unknown step '$step'" >&2
    exit 2
    ;;
esac
