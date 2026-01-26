-- Create social schema
CREATE SCHEMA IF NOT EXISTS social;

-- Create sequence for player IDs
CREATE SEQUENCE IF NOT EXISTS social.playerid_seq START 1;

-- Discord login attempts table
CREATE TABLE social.discord_login_attempts (
    id SERIAL PRIMARY KEY,
    state TEXT NOT NULL UNIQUE,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    linked_discord_user_id TEXT,
    linked_token TEXT
);

-- Discord accounts table
CREATE TABLE social.discord_accounts (
    discord_user_id TEXT PRIMARY KEY,
    discord_username TEXT NOT NULL,
    discord_avatar TEXT,
    discord_global_name TEXT
);

-- Player ID mapping for Discord
CREATE TABLE social.playerid_by_discord (
    discord_user_id TEXT PRIMARY KEY,
    player_id BIGINT NOT NULL UNIQUE
);

-- Steam login attempts table
CREATE TABLE social.steam_login_attempts (
    id SERIAL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL,
    steam_user_id TEXT NOT NULL,
    used_weak_login BOOLEAN NOT NULL,
    login_token TEXT NOT NULL
);

-- Player ID mapping for Steam
CREATE TABLE social.playerid_by_steam (
    steam_user_id TEXT PRIMARY KEY,
    player_id BIGINT NOT NULL UNIQUE
);

-- Valid player logins table
CREATE TABLE social.player_valid_logins (
    login_token TEXT PRIMARY KEY,
    player_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add foreign key constraints
ALTER TABLE social.playerid_by_discord
ADD CONSTRAINT fk_discord_accounts
FOREIGN KEY (discord_user_id) REFERENCES social.discord_accounts(discord_user_id);

-- Add indexes for performance
CREATE INDEX idx_discord_login_attempts_completed_at ON social.discord_login_attempts(completed_at);
CREATE INDEX idx_discord_login_attempts_linked_discord_user_id ON social.discord_login_attempts(linked_discord_user_id);
CREATE INDEX idx_steam_login_attempts_steam_user_id ON social.steam_login_attempts(steam_user_id);
CREATE INDEX idx_player_valid_logins_player_id ON social.player_valid_logins(player_id);
CREATE INDEX idx_player_valid_logins_created_at ON social.player_valid_logins(created_at);

-- Add refresh_token column to discord_login_attempts
ALTER TABLE social.discord_login_attempts
ADD COLUMN refresh_token TEXT;
