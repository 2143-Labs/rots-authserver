# Sourced by ci-test.sh and dev-stack.sh. Requires WORK. start_deps passes its args to
# `temporal server start-dev`, which keeps its state in memory unless they include
# --db-filename. Postgres on 55432, Temporal gRPC on 17233, UI on 18233.
TPID=
start_deps() {
  [ -d "$WORK/pg" ] || initdb -D "$WORK/pg" -U postgres --auth=trust >/dev/null
  pg_ctl -D "$WORK/pg" -o "-p 55432 -k $WORK -c listen_addresses=127.0.0.1" -l "$WORK/pg.log" start
  temporal server start-dev --port 17233 --ui-port 18233 --namespace rots "$@" >"$WORK/temporal.log" 2>&1 & TPID=$!
  for _ in $(seq 60); do temporal operator namespace describe rots --address 127.0.0.1:17233 >/dev/null 2>&1 && return 0; sleep 1; done
  echo "Temporal dev server did not start; see $WORK/temporal.log" >&2; return 1
}
stop_deps() { pg_ctl -D "$WORK/pg" stop -m fast >/dev/null 2>&1 || true; [ -n "$TPID" ] && kill "$TPID" 2>/dev/null || true; }
