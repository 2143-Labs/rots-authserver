#!/usr/bin/env bash
# Unit + integration tests against a throwaway Postgres and Temporal dev server.
# Run inside the devShell: nix develop -c ./scripts/ci-test.sh
set -euo pipefail
WORK=$(mktemp -d)
source "$(dirname "$0")/deps.sh"
trap 'stop_deps; rm -rf "$WORK"' EXIT
# Temporal stays in memory: a file-backed dev server fsyncs every write, and under the
# parallel integration tests that made reports take 0.5-3 s, so healthy test servers
# were reaped (they use a 2 s stale window).
start_deps --headless
# SQLX_OFFLINE keeps the query! macros on .sqlx/ instead of the empty live database.
export DATABASE_URL=postgres://postgres@127.0.0.1:55432/postgres TEMPORAL_TEST_ADDRESS=http://127.0.0.1:17233 SQLX_OFFLINE=true
cargo test -- --include-ignored
