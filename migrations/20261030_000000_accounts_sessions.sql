-- Replace the prototype login tables with accounts, identities, characters and sessions.
-- No deployment of the prototype schema exists, so there is no data to keep.
DROP TABLE IF EXISTS social.player_valid_logins, social.playerid_by_discord, social.playerid_by_steam,
  social.steam_login_attempts, social.discord_login_attempts, social.discord_accounts CASCADE;
DROP SEQUENCE IF EXISTS social.playerid_seq;

CREATE TABLE social.accounts (
  account_id BIGSERIAL PRIMARY KEY,
  display_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE social.account_identities (
  provider TEXT NOT NULL CHECK (provider IN ('steam','discord','au2143')),
  subject TEXT NOT NULL,
  account_id BIGINT NOT NULL REFERENCES social.accounts(account_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, subject)
);
CREATE TABLE social.characters (
  character_id BIGSERIAL PRIMARY KEY,
  account_id BIGINT REFERENCES social.accounts(account_id) ON DELETE SET NULL,
  name TEXT NOT NULL,
  claim_code TEXT UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_characters_account ON social.characters(account_id);
CREATE TABLE social.sessions (
  token_hash TEXT PRIMARY KEY,           -- hex sha256 of the session token
  account_id BIGINT REFERENCES social.accounts(account_id) ON DELETE CASCADE,
  character_id BIGINT NOT NULL REFERENCES social.characters(character_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE social.oauth_states (
  state TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  pkce_verifier TEXT,
  return_to TEXT NOT NULL,
  guest_token_hash TEXT,                  -- session that started the login, if any
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
