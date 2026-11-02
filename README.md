# rots-authserver

Accounts, sessions and game tickets for ROTS. Players log in with Steam, Discord or
au.2143.me, or play as a guest. Every game-server connection carries a **game ticket**:
an EdDSA JWT valid for 300 s, which the game server verifies offline with the public key
from `GET /api/v1/ticket/public-key`.

Architecture, flows and state machines: [ROTS architecture](https://github.com/2143-Labs/bevy2025/blob/main/docs/architecture.html).

The gateway serves this service under `{PUBLIC_URL}/auth` and strips the `/auth` prefix,
so the routes below are served at the root (`/health`, `/api/v1/...`).

## Running

Local stack: Postgres, a Temporal dev server and the authserver, in the foreground. Ctrl-C
stops everything; accounts and workflows persist in `$WORK` (default `/tmp/rots-dev`).

```
nix develop -c ./scripts/dev-stack.sh
```

The authserver listens on `:8000`, the internal API on `:8001` (token `devtoken`), and the
Temporal UI is at http://localhost:18233. A bevy2025 game server reports to it with:

```
ROTS_AUTH_URL=http://localhost:8000 ROTS_TICKET_AUDIENCE=http://localhost:8000 \
ROTS_SERVER_SLOT=0 ROTS_COORDINATOR_URL=http://localhost:8001 ROTS_COORDINATOR_TOKEN=devtoken \
  nix develop -c cargo run -p server
```

and the native client logs in with `nix develop -c cargo run -p client -- --auth-url http://localhost:8000`.
For a second game server, start the stack with `GAME_SERVER_SLOTS=2` and the server with
`ROTS_SERVER_SLOT=1 ROTS_PORT=25556`. The signing key is new on every start of the stack,
so restart game servers after restarting it.

Tests: `nix develop -c ./scripts/ci-test.sh` starts a throwaway Postgres and an in-memory
Temporal dev server and runs the unit and integration tests; CI runs the same script.

Migrations run at startup. After changing a query, run `cargo sqlx prepare` against a
migrated database and commit `.sqlx/`.

## Configuration

Every option can be passed as `--flag` or as the env var below. An empty string counts as
unset.

| Env var | Default | Meaning |
|---|---|---|
| `PORT` | `8000` | Listen port. |
| `INTERNAL_PORT` | `8001` | Port of the [internal API](#internal-api). Never routed through the gateway. |
| `DATABASE_URL` | required | Postgres URL. |
| `PUBLIC_URL` | required | Public origin, e.g. `https://rots.john2143.com`, without a trailing slash. Used for redirects and callbacks, and as the ticket `aud`. |
| `TICKET_SIGNING_KEY` | required | Standard base64 of a 32-byte ed25519 seed. Startup aborts on any other length. |
| `COORDINATOR_TOKEN` | required | Bearer token game servers present on the internal API. Startup aborts when it is empty. |
| `STEAM_APP_ID` | `4346770` | App ID for Steam Web API tickets. |
| `STEAM_API_KEY` | unset | Turns on `steam_ticket`. |
| `DISCORD_CLIENT_ID`, `DISCORD_CLIENT_SECRET` | unset | Both are needed to turn on `discord`. |
| `AU2143_CLIENT_ID`, `AU2143_CLIENT_SECRET` | unset | `AU2143_CLIENT_ID` turns on `au2143`, provided OIDC discovery succeeds. |
| `AU2143_ISSUER` | `https://au.2143.me` | OIDC issuer used for discovery. |
| `DISABLED_PROVIDERS` | `""` | Comma list of `steam`, `discord` and `au2143` to turn off. |
| `TEMPORAL_ADDRESS` | unset | Temporal frontend, e.g. `http://temporal-frontend.default.svc.cluster.local:7233`. Unset: no worker, and `/play` and game-server joins answer 503. |
| `TEMPORAL_NAMESPACE` | `rots` | Temporal namespace. |
| `TEMPORAL_TASK_QUEUE` | `rots-dev` | This environment's task queue, also part of its workflow IDs. |
| `GAME_SERVER_SLOTS` | `1` | Game servers are slots `0..GAME_SERVER_SLOTS`. Reports for other slots get 400 `bad_slot`. |
| `GAME_UDP_HOST` | host of `PUBLIC_URL` | Host in `/play`'s `server.udp`. |
| `GAME_UDP_PORT_BASE` | `25555` | Slot N's UDP port is this + N. |
| `COORDINATOR_STALE_SECS` | `30` | A game server with no report for this long is treated as dead. |
| `RUST_LOG` | `info` | tracing filter. |

When each provider is enabled:

| Provider | Enabled when |
|---|---|
| `steam_openid` | `steam` is not in `DISABLED_PROVIDERS`. |
| `steam_ticket` | `STEAM_API_KEY` is set and `steam` is not disabled. |
| `discord` | Both Discord values are set and `discord` is not disabled. |
| `au2143` | `AU2143_CLIENT_ID` is set, discovery succeeds, and `au2143` is not disabled. |

Each provider must have `{PUBLIC_URL}/auth/api/v1/{steam,discord,au2143}/callback` registered as its redirect URI.

## API

Errors are JSON `{"error":"<code>"}`. A session is read from the `rots_session` cookie
first, then from `Authorization: Bearer <token>`.
- Responses to `"client":"web"` set `rots_session=<token>; Path=/auth; HttpOnly; Secure; SameSite=Lax; Max-Age=2592000`.
- Responses to `"client":"native"` return the token as `session_token` instead.

| Method + path | Request | Success | Failure |
|---|---|---|---|
| `GET /health` | — | `Ok` | — |
| `GET /ready` | — | 200 `{"status":"ready"}` once the Temporal worker is connected, or right away when `TEMPORAL_ADDRESS` is unset | 503 `{"status":"starting"}` |
| `GET /api/v1/providers` | — | `{"providers":[...]}`, the enabled subset of `steam_openid`, `discord`, `au2143`, `steam_ticket` | — |
| `GET /api/v1/ticket/public-key` | — | `{"alg":"EdDSA","x":"<base64url ed25519 public key>"}` | — |
| `POST /api/v1/guest` | `{"client":"web"\|"native"}` | `{"character_id","claim_code","session_token"?}`. Creates a new guest character `Guest-<id>`. | 400 `bad_client` |
| `GET /api/v1/me` | session | `{"kind":"guest"\|"account","character_id","character_name","account_id"?,"display_name"?,"claim_code"?}` | 401 `no_session` |
| `POST /api/v1/play` | session | `{"ticket","expires_at","character_id","kind","server":{"slot","udp","ws_path"}}`. The ticket is valid only on that slot. | 401 `no_session`, 409 `play_in_progress`, 503 `no_game_server`, 503 `temporal_unavailable`, 500 `internal` |
| `POST /api/v1/steam/ticket` | `{"steam_ticket_hex","client"}`, plus an optional guest session | `{"session_token"?,"character_id"}`. The ticket identity must be `rots-auth`. | 503 `provider_unavailable`, 401 `steam_ticket_rejected`, 400 `bad_request` |
| `GET /api/v1/{steam,discord,au2143}/login?return_to=/path` | optional cookie session | 302 to the provider | 404 `provider_disabled`, 400 `bad_return_to` |
| `GET /api/v1/steam/callback` | OpenID params, `state` | 302 to `{PUBLIC_URL}{return_to}`, sets the cookie | 302 to `{PUBLIC_URL}/?login_error=<code>` |
| `GET /api/v1/{discord,au2143}/callback` | `code`, `state` | 302 to `{PUBLIC_URL}{return_to}`, sets the cookie | 302 to `{PUBLIC_URL}/?login_error=<code>` |
| `POST /api/v1/claim` | `{"claim_code"}` + account session | `{"character_id"}`. Attaches the guest character and ends its guest sessions. | 401 `no_session`, 403 `guest_cannot_claim`, 404 `invalid_claim_code` |
| `POST /api/v1/logout` | session | 204, clears the cookie and sends `logout` to the session's workflow | 204 |

The `login_error` codes are `invalid_state` (the state is unknown, already used, or older
than 10 minutes), `provider_error` and `internal`.

`/play` runs the `play` update on the session's workflow, starting it on the first call.
The coordinator picks the slot the character is already on, else the session's own slot
(its unexpired assignment, or a slot it is playing on), else the least-loaded healthy slot
(players plus reservations, ties to the lowest slot), and reserves it for 360 s. It answers
within 10 s. A play that arrives while another play of the same session is running waits
for it and gets the same slot; two plays that reach the workflow together (for example,
both starting it) get one `409 play_in_progress`.

How a login picks the account and character:
- The account is found through `(provider, subject)`. If none exists, a new account is created.
- If the login was started from a guest session, that guest character is attached to the account and its claim code is cleared.
- Otherwise the account's newest character is used. If it has none, a character named after the account is created.
- The previous session is replaced by a new one, and its session workflow is sent `logout`.

### Game ticket claims

```json
{"iss":"rots-authserver","aud":"<PUBLIC_URL>","sub":"<character_id>","sid":"<16 hex>",
 "kind":"guest|account","prov":"steam|discord|au2143|null","acc":<account_id|null>,
 "name":"<character name>","iat":<unix>,"exp":<iat+300>,"srv":<slot>}
```

The header is `{"alg":"EdDSA","typ":"JWT"}`. `sid` is the first 16 hex characters of the
session's `token_hash`.
`srv` is the game-server slot `/play` assigned. Game servers that set `ROTS_SERVER_SLOT`
reject a ticket whose `srv` differs.

## Internal API

Game servers report through `INTERNAL_PORT`, never through the gateway, with
`Authorization: Bearer $COORDINATOR_TOKEN`. Each report becomes an update (join) or a
signal (heartbeat, leave) on the coordinator workflow, within 4 s. Every 503 is logged as an
error: game servers refuse new players while they cannot get a join answered.

| Method + path | Body | Success | Failure |
|---|---|---|---|
| `POST /internal/v1/servers/heartbeat` | `{"slot","boot_id","players":[{"character_id","sid"}]}`, every 10 s | 204 | 503 `temporal_unavailable` |
| `POST /internal/v1/servers/join` | `{"slot","boot_id","character_id","sid"}`, before admitting a player | 200 `{"result":"admitted"}` | 409 `session_ended`, 409 `already_on_other_server`, 503 `db_unavailable`, 503 `temporal_unavailable` |
| `POST /internal/v1/servers/leave` | `{"slot","boot_id","character_id"}` | 204 | 503 `temporal_unavailable` |

Every route also answers 401 `bad_coordinator_token`, 400 `bad_slot` (slot not below
`GAME_SERVER_SLOTS`) and 400 `bad_request` (malformed JSON).
- `boot_id` is random per game-server process. A new one for a slot clears its players.
- `players` is the full roster, pending joins included. For a known `boot_id` it only removes players; the first heartbeat from an unknown slot adopts it, which is how a coordinator that lost its state recovers.
- A join is refused with `session_ended` when the ticket's session (`sid`) no longer exists: logged out, replaced by a new login, removed by a claim, or expired. Players already in game are not re-checked.
- A game server must send its reports in order, one at a time: dropping players missing from a roster is only correct because a join always arrives before the heartbeat that lists it.

## Temporal

The worker runs in this process on its own thread and reconnects every 10 s while Temporal
is unreachable; `/ready` answers 503 until it is connected. Each environment has:

| Workflow | ID | Started by | State |
|---|---|---|---|
| Coordinator | `rots-coordinator-{TEMPORAL_TASK_QUEUE}` | the first report or `/play` | per slot: `boot_id`, last report, players; reservations |
| Session | `rots-session-{TEMPORAL_TASK_QUEUE}-{sid}` | the session's first `/play` | assigned slot, presence, `in_menus` / `assigned` / `in_game` / `ended` |

The coordinator continues as new every hour or 2000 events and has a 3 h run timeout. A
session ends at logout, or 24 h after it started once none of its characters is in game, and
at the latest after 48 h. The task queue is part of both IDs because all previews share the
`rots-preview` namespace.

Workflow code must be deterministic: time only from `ctx.workflow_time()`,
`BTreeMap`/`BTreeSet` in workflow state, no I/O and no randomness. Wrap any change to the
commands a workflow issues (activities, timers, external signals, continue-as-new) in
`ctx.patched("<change-id>")`: during a rolling deploy the old and new authservers poll the
same task queue. State-machine diagrams: [coordinator](https://github.com/2143-Labs/bevy2025/blob/main/docs/architecture.html#coordinator),
[session](https://github.com/2143-Labs/bevy2025/blob/main/docs/architecture.html#session).
