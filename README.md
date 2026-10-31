# rots-authserver

Accounts, sessions and game tickets for ROTS. Players log in with Steam, Discord or
au.2143.me, or play as a guest. Every game-server connection carries a **game ticket**:
an EdDSA JWT valid for 300 s, which the game server verifies offline with the public key
from `GET /api/v1/ticket/public-key`.

The gateway serves this service under `{PUBLIC_URL}/auth` and strips the `/auth` prefix,
so the routes below are served at the root (`/health`, `/api/v1/...`).

## Running

```
nix develop
export DATABASE_URL=postgres://postgres:pw@localhost/postgres \
       PUBLIC_URL=http://localhost:8000 \
       TICKET_SIGNING_KEY=$(head -c 32 /dev/urandom | base64)
cargo run -- server
```

Migrations run at startup. After changing a query, run `cargo sqlx prepare` against a
migrated database and commit `.sqlx/`.

## Configuration

Every option can be passed as `--flag` or as the env var below. An empty string counts as
unset.

| Env var | Default | Meaning |
|---|---|---|
| `PORT` | `8000` | Listen port. |
| `DATABASE_URL` | required | Postgres URL. |
| `PUBLIC_URL` | required | Public origin, e.g. `https://rots.john2143.com`, without a trailing slash. Used for redirects and callbacks, and as the ticket `aud`. |
| `TICKET_SIGNING_KEY` | required | Standard base64 of a 32-byte ed25519 seed. Startup aborts on any other length. |
| `STEAM_APP_ID` | `4346770` | App ID for Steam Web API tickets. |
| `STEAM_API_KEY` | unset | Turns on `steam_ticket`. |
| `DISCORD_CLIENT_ID`, `DISCORD_CLIENT_SECRET` | unset | Both are needed to turn on `discord`. |
| `AU2143_CLIENT_ID`, `AU2143_CLIENT_SECRET` | unset | `AU2143_CLIENT_ID` turns on `au2143`, provided OIDC discovery succeeds. |
| `AU2143_ISSUER` | `https://au.2143.me` | OIDC issuer used for discovery. |
| `DISABLED_PROVIDERS` | `""` | Comma list of `steam`, `discord` and `au2143` to turn off. |
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
| `GET /api/v1/providers` | — | `{"providers":[...]}`, the enabled subset of `steam_openid`, `discord`, `au2143`, `steam_ticket` | — |
| `GET /api/v1/ticket/public-key` | — | `{"alg":"EdDSA","x":"<base64url ed25519 public key>"}` | — |
| `POST /api/v1/guest` | `{"client":"web"\|"native"}` | `{"character_id","claim_code","session_token"?}`. Creates a new guest character `Guest-<id>`. | 400 `bad_client` |
| `GET /api/v1/me` | session | `{"kind":"guest"\|"account","character_id","character_name","account_id"?,"display_name"?,"claim_code"?}` | 401 `no_session` |
| `POST /api/v1/ticket` | session | `{"ticket","expires_at","character_id","kind"}` | 401 `no_session` |
| `POST /api/v1/steam/ticket` | `{"steam_ticket_hex","client"}`, plus an optional guest session | `{"session_token"?,"character_id"}`. The ticket identity must be `rots-auth`. | 503 `provider_unavailable`, 401 `steam_ticket_rejected`, 400 `bad_request` |
| `GET /api/v1/{steam,discord,au2143}/login?return_to=/path` | optional cookie session | 302 to the provider | 404 `provider_disabled`, 400 `bad_return_to` |
| `GET /api/v1/steam/callback` | OpenID params, `state` | 302 to `{PUBLIC_URL}{return_to}`, sets the cookie | 302 to `{PUBLIC_URL}/?login_error=<code>` |
| `GET /api/v1/{discord,au2143}/callback` | `code`, `state` | 302 to `{PUBLIC_URL}{return_to}`, sets the cookie | 302 to `{PUBLIC_URL}/?login_error=<code>` |
| `POST /api/v1/claim` | `{"claim_code"}` + account session | `{"character_id"}`. Attaches the guest character and ends its guest sessions. | 401 `no_session`, 403 `guest_cannot_claim`, 404 `invalid_claim_code` |
| `POST /api/v1/logout` | session | 204, clears the cookie | 204 |

The `login_error` codes are `invalid_state` (the state is unknown, already used, or older
than 10 minutes), `provider_error` and `internal`.

How a login picks the account and character:
- The account is found through `(provider, subject)`. If none exists, a new account is created.
- If the login was started from a guest session, that guest character is attached to the account and its claim code is cleared.
- Otherwise the account's newest character is used. If it has none, a character named after the account is created.
- The previous session is replaced by a new one.

### Game ticket claims

```json
{"iss":"rots-authserver","aud":"<PUBLIC_URL>","sub":"<character_id>","sid":"<16 hex>",
 "kind":"guest|account","prov":"steam|discord|au2143|null","acc":<account_id|null>,
 "name":"<character name>","iat":<unix>,"exp":<iat+300>}
```

The header is `{"alg":"EdDSA","typ":"JWT"}`. `sid` is the first 16 hex characters of the
session's `token_hash`.
