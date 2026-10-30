//! Opaque session tokens (only their sha256 is stored), guest creation, and
//! the account login that attaches a guest character.
use anyhow::Context;
use axum::extract::FromRequestParts;
use axum_extra::extract::CookieJar;
use http::request::Parts;
use rand::Rng;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::api::{ApiError, AppState};

pub const SESSION_COOKIE: &str = "rots_session";
pub const SESSION_TTL_DAYS: i32 = 30;

const CLAIM_ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";

#[derive(Debug, Clone)]
pub struct SessionRow {
    pub token_hash: String,
    pub account_id: Option<i64>,
    pub character_id: i64,
}

/// The caller's unexpired session, from the `rots_session` cookie first, then
/// `Authorization: Bearer <token>`. `None` when absent, unknown or expired.
pub struct Session(pub Option<SessionRow>);

impl FromRequestParts<AppState> for Session {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let cookie_token = CookieJar::from_headers(&parts.headers)
            .get(SESSION_COOKIE)
            .map(|c| c.value().to_owned());
        let token = cookie_token.or_else(|| {
            parts
                .headers
                .get(http::header::AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
                .and_then(|h| h.strip_prefix("Bearer "))
                .map(|t| t.trim().to_owned())
        });
        let Some(token) = token.filter(|t| !t.is_empty()) else {
            return Ok(Session(None));
        };
        let row = session_by_hash(&state.db, &hash_token(&token))
            .await
            .map_err(ApiError::internal)?;
        Ok(Session(row))
    }
}

pub fn hash_token(token: &str) -> String {
    hex(&Sha256::digest(token.as_bytes()))
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(out, "{b:02x}").expect("writing to a String cannot fail");
    }
    out
}

/// 32 random bytes, hex-encoded. Used for session tokens and OAuth `state`.
pub fn random_token() -> String {
    hex(&rand::rng().random::<[u8; 32]>())
}

fn claim_code() -> String {
    let mut rng = rand::rng();
    let mut code = String::with_capacity(11);
    for i in 0..10 {
        if i == 5 {
            code.push('-');
        }
        code.push(CLAIM_ALPHABET[rng.random_range(0..CLAIM_ALPHABET.len())] as char);
    }
    code
}

pub async fn session_by_hash(db: &PgPool, token_hash: &str) -> anyhow::Result<Option<SessionRow>> {
    sqlx::query_as!(
        SessionRow,
        "SELECT token_hash, account_id, character_id FROM social.sessions
         WHERE token_hash = $1 AND expires_at > now()",
        token_hash
    )
    .fetch_optional(db)
    .await
    .context("loading session")
}

/// Stores a new session and returns its plaintext token (never stored).
pub async fn create_session(
    db: impl sqlx::PgExecutor<'_>,
    account_id: Option<i64>,
    character_id: i64,
) -> anyhow::Result<String> {
    let token = random_token();
    sqlx::query!(
        "INSERT INTO social.sessions (token_hash, account_id, character_id, expires_at)
         VALUES ($1, $2, $3, now() + make_interval(days => $4))",
        hash_token(&token),
        account_id,
        character_id,
        SESSION_TTL_DAYS
    )
    .execute(db)
    .await
    .context("creating session")?;
    Ok(token)
}

/// Creates a guest character `Guest-<id>` with a claim code, plus its session.
/// Returns `(character_id, claim_code, token)`.
pub async fn create_guest(db: &PgPool) -> anyhow::Result<(i64, String, String)> {
    let mut tx = db.begin().await?;
    let mut inserted = None;
    // A claim-code collision yields no row (ON CONFLICT keeps the transaction
    // usable); retry once with a fresh code.
    for _ in 0..2 {
        let code = claim_code();
        let row = sqlx::query_scalar!(
            "INSERT INTO social.characters (name, claim_code) VALUES ('guest', $1)
             ON CONFLICT (claim_code) DO NOTHING RETURNING character_id",
            code
        )
        .fetch_optional(&mut *tx)
        .await
        .context("inserting guest character")?;
        if let Some(character_id) = row {
            inserted = Some((character_id, code));
            break;
        }
    }
    let (character_id, code) = inserted.context("claim code collided twice")?;
    sqlx::query!(
        "UPDATE social.characters SET name = 'Guest-' || character_id WHERE character_id = $1",
        character_id
    )
    .execute(&mut *tx)
    .await
    .context("naming guest character")?;
    let token = create_session(&mut *tx, None, character_id).await?;
    tx.commit().await?;
    Ok((character_id, code, token))
}

/// Logs `provider`/`subject` into its account (creating it on first sight),
/// picks the character (attached guest > newest > new), replaces `prior` with
/// a fresh session and returns `(token, character_id)`.
pub async fn login_account(
    db: &PgPool,
    provider: &str,
    subject: &str,
    display_name: &str,
    prior: Option<SessionRow>,
) -> anyhow::Result<(String, i64)> {
    let mut tx = db.begin().await?;

    let existing = sqlx::query_scalar!(
        "SELECT account_id FROM social.account_identities WHERE provider = $1 AND subject = $2",
        provider,
        subject
    )
    .fetch_optional(&mut *tx)
    .await
    .context("looking up identity")?;
    let account_id = match existing {
        Some(id) => id,
        None => {
            let id = sqlx::query_scalar!(
                "INSERT INTO social.accounts (display_name) VALUES ($1) RETURNING account_id",
                display_name
            )
            .fetch_one(&mut *tx)
            .await
            .context("creating account")?;
            sqlx::query!(
                "INSERT INTO social.account_identities (provider, subject, account_id) VALUES ($1, $2, $3)",
                provider,
                subject,
                id
            )
            .execute(&mut *tx)
            .await
            .context("creating identity")?;
            id
        }
    };

    let mut character_id = None;
    if let Some(guest) = prior.as_ref().filter(|p| p.account_id.is_none()) {
        character_id = sqlx::query_scalar!(
            "UPDATE social.characters SET account_id = $1, claim_code = NULL
             WHERE character_id = $2 AND account_id IS NULL RETURNING character_id",
            account_id,
            guest.character_id
        )
        .fetch_optional(&mut *tx)
        .await
        .context("attaching guest character")?;
    }
    if character_id.is_none() {
        character_id = sqlx::query_scalar!(
            "SELECT character_id FROM social.characters WHERE account_id = $1
             ORDER BY created_at DESC, character_id DESC LIMIT 1",
            account_id
        )
        .fetch_optional(&mut *tx)
        .await
        .context("finding newest character")?;
    }
    let character_id = match character_id {
        Some(id) => id,
        None => sqlx::query_scalar!(
            "INSERT INTO social.characters (account_id, name) VALUES ($1, $2) RETURNING character_id",
            account_id,
            display_name
        )
        .fetch_one(&mut *tx)
        .await
        .context("creating account character")?,
    };

    if let Some(prior) = &prior {
        sqlx::query!(
            "DELETE FROM social.sessions WHERE token_hash = $1",
            prior.token_hash
        )
        .execute(&mut *tx)
        .await
        .context("deleting prior session")?;
    }
    let token = create_session(&mut *tx, Some(account_id), character_id).await?;
    tx.commit().await?;
    Ok((token, character_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_code_shape() {
        for _ in 0..200 {
            let code = claim_code();
            assert_eq!(code.len(), 11);
            assert_eq!(&code[5..6], "-");
            assert!(
                code.bytes()
                    .enumerate()
                    .all(|(i, b)| i == 5 || CLAIM_ALPHABET.contains(&b))
            );
        }
    }

    #[test]
    fn token_hash_is_hex_sha256() {
        assert_eq!(
            hash_token("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(random_token().len(), 64);
    }
}
