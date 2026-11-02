#!/usr/bin/env bash
# The coordinator side of ROTS on one machine: Postgres, a Temporal dev server and the
# authserver, in the foreground. Ctrl-C stops everything; accounts and workflows persist
# in $WORK between runs. Run inside the devShell: nix develop -c ./scripts/dev-stack.sh
set -euo pipefail
WORK=${WORK:-/tmp/rots-dev}
mkdir -p "$WORK"
source "$(dirname "$0")/deps.sh"
trap stop_deps EXIT
start_deps --db-filename "$WORK/temporal.db"
export DATABASE_URL=postgres://postgres@127.0.0.1:55432/postgres
# Apply the migrations before building: the sqlx query macros check every query against this
# database at compile time, so a fresh $WORK (an empty cluster) cannot compile the authserver
# until the schema exists. The authserver itself migrates again on startup (a no-op here).
cargo sqlx migrate run
echo "Temporal UI: http://localhost:18233   authserver: http://localhost:8000 (internal :8001, token devtoken)"
# The authserver env stays on this command: the temporal CLI also reads TEMPORAL_ADDRESS
# and TEMPORAL_NAMESPACE (as host:port), so exporting the URL form would break it.
# The signing key is new on every start: restart local game servers after restarting this.
env DATABASE_URL="$DATABASE_URL" PUBLIC_URL=http://localhost:8000 \
  TICKET_SIGNING_KEY="$(head -c 32 /dev/urandom | base64)" COORDINATOR_TOKEN=devtoken \
  TEMPORAL_ADDRESS=http://127.0.0.1:17233 TEMPORAL_NAMESPACE=rots TEMPORAL_TASK_QUEUE=rots-dev \
  GAME_SERVER_SLOTS="${GAME_SERVER_SLOTS:-1}" GAME_UDP_HOST=localhost DISABLED_PROVIDERS=steam,discord,au2143 \
  cargo run -- server
