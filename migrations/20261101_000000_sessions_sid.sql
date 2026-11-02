-- Game servers ask whether a ticket's session still exists before admitting a join.
-- A ticket carries `sid` = the first 16 hex chars of the session's token_hash.
CREATE INDEX idx_sessions_sid ON social.sessions (left(token_hash, 16));
